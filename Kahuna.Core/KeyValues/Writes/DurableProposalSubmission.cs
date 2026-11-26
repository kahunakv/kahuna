using Kommander.Data;

using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.Replication;

namespace Kahuna.Server.KeyValues.Writes;

/// <summary>
/// A durable-intent 2PC record (transaction-record init/decision, prepared-intent prepare, or resolution) submitted
/// to the shared partition write scheduler so it coalesces with concurrent transactions' records to the same
/// partition into one <c>ReplicateEntries</c> proposal. The producer adapter for the durable finalizer: its
/// completion resolves a task the finalizer awaits — committed means the batch reached Raft, released means it did
/// not (retryable), and the finalizer maps that to the transaction outcome. Multiple entries form an atomic ordered
/// bundle (e.g. anchor init+prepare). Durable records target a partition resolved at freeze and replicate at
/// Kommander generation zero; the key-range fences they carry are Kahuna's own routing fences, re-checked at dispatch.
/// </summary>
internal sealed class DurableProposalSubmission : IProposalSubmission
{
    private readonly TaskCompletionSource<bool> completion;

    // Runs on Complete() — the scheduler's per-partition completion path — once the batch committed. It does NOT
    // apply the submission's durable records: the record/intent stores are mutated only by the ordered consumer
    // apply that Raft drives in log order, and a completion can run before that apply (the quorum-durable fast
    // path releases the proposal ahead of the leader's own consumer) or long after it. Instead it waits for the
    // ordered apply of each entry — identified by the committed Raft log index the executor reported per entry —
    // and resolves whether every PREPARE in the bundle was acknowledged (took ownership of its key); a rejected
    // prepare resolves the producer's Committed task false so the finalizer aborts instead of committing an
    // unrecoverable mutation. Null in tests that only assert scheduling, not store state.
    private readonly Func<int, IReadOnlyList<RaftProposalEntry>, IReadOnlyList<long>?, Task<DurableCompletionAnswer>>? applyOnCommit;

    // Written before the producer's task resolves (the TrySetResult publishes it), read by the producer after.
    private DurableCompletionAnswer answer = DurableCompletionAnswer.NotCommitted;

    // The dispatch fences: every key-range key this submission's partition group writes, and one key per hash
    // space it touches, each at the generation the key resolved to at freeze. Empty opts out of the fence
    // (post-decision settle/materialize, recovery, and state-transfer imports, which must apply regardless of
    // topology); a fenced submission is re-resolved at dispatch and released retryably when any of its
    // descriptors moved, or when a key it would add lies in a range that is quiesced for a data move, instead of
    // appending to a partition the range no longer routes to or is about to stop routing to.
    private readonly IReadOnlyList<RangeFence> fences;

    // Shared by the submissions that name no key. Never added to.
    private static readonly List<string> NoKeys = [];

    // The keys of ChangedKey; null when one of the entries does not tell them.
    private readonly List<string>? changedKeys;

    public int PartitionId { get; }

    public WriteAdmissionClass AdmissionClass { get; }

    /// <summary>The stage that produced this record, fixed at the creation site (or carried over the wire from
    /// the origin for a forwarded bundle); dispatch tags the queue-delay histogram with it.</summary>
    public WriteSubmissionStage Stage { get; }

    public int ByteLength { get; }

    public IReadOnlyList<RaftProposalEntry> Entries { get; }

    public long EnqueueTicks { get; set; }

    public DurableProposalSubmission(
        int partitionId,
        IReadOnlyList<RaftProposalEntry> entries,
        TaskCompletionSource<bool> completion,
        WriteAdmissionClass admissionClass,
        WriteSubmissionStage stage,
        Func<int, IReadOnlyList<RaftProposalEntry>, IReadOnlyList<long>?, Task<DurableCompletionAnswer>>? applyOnCommit = null,
        IReadOnlyList<RangeFence>? fences = null)
    {
        PartitionId = partitionId;
        AdmissionClass = admissionClass;
        Stage = stage;
        Entries = entries;
        this.completion = completion;
        this.applyOnCommit = applyOnCommit;
        this.fences = fences ?? [];

        int bytes = 0;
        for (int i = 0; i < entries.Count; i++)
            bytes += entries[i].Data.Length;
        ByteLength = bytes;

        changedKeys = KeysChangedBy(entries, stage);
    }

    public int ChangedKeyCount => changedKeys?.Count ?? -1;

    public string ChangedKey(int index) => changedKeys![index];

    /// <summary>
    /// The keys whose operations wait for the delivery of these entries, or null when the entries do not tell them.
    ///
    /// <list type="bullet">
    ///   <item>A <b>prepared-intent delta</b> names the key of each intent it prepares, resolves or removes. A
    ///   one-phase bundle decides its transaction in the same batch, so its keys get their new values with the
    ///   delivery. A settlement hands the committed value of its intent to the actor of the key and then removes the
    ///   intent, and an operation that this node answers while it does both must not find neither.</item>
    ///   <item>A <b>transaction record</b> names none. A read, a staged write or a lock grant that finds the intent of an
    ///   undecided transaction waits for the decision, and a commit-time check refuses. The intents that come in the
    ///   same batch name their keys.</item>
    ///   <item>The <b>materialization</b> of a committed intent names none. It is proposed once the decision is
    ///   terminal, and the intent answers for the key until the settlement that follows the materialization.</item>
    ///   <item>A <b>completion receipt</b> and a <b>progress tick</b> change nothing that an operation answers.</item>
    ///   <item>Any other key-value record, such as a copied page of a range move, does not tell its keys. The move
    ///   completes when a quorum holds the page, so an operation that routes to this partition right after the cutover
    ///   must wait for the delivery.</item>
    /// </list>
    /// </summary>
    private static List<string>? KeysChangedBy(IReadOnlyList<RaftProposalEntry> entries, WriteSubmissionStage stage)
    {
        List<string> keys = NoKeys;

        for (int i = 0; i < entries.Count; i++)
        {
            RaftProposalEntry entry = entries[i];

            switch (entry.Type)
            {
                case ReplicationTypes.TransactionRecord:
                case ReplicationTypes.CompletionReceipt:
                case ReplicationTypes.ProgressTick:
                    break;

                case ReplicationTypes.PreparedIntent:
                    if (ReferenceEquals(keys, NoKeys))
                        keys = [];

                    if (!PreparedIntentStore.TryCollectKeys(entry.Data, keys))
                        return null;

                    break;

                // The prepare part names the keys of the commit, which decides in the same entry. The record parts name
                // none.
                case ReplicationTypes.OnePhaseBundle:
                    if (ReferenceEquals(keys, NoKeys))
                        keys = [];

                    if (!TryCollectBundleKeys(entry.Data, keys))
                        return null;

                    break;

                case ReplicationTypes.KeyValues when stage == WriteSubmissionStage.Materialize:
                    break;

                default:
                    return null;
            }
        }

        return keys;
    }

    private static bool TryCollectBundleKeys(byte[] data, List<string> keys)
    {
        PreparedIntentCommand[] commands;
        try
        {
            commands = OnePhaseBundleEntry.PrepareCommandsOf(data);
        }
        catch (Exception ex) when (ex is not OutOfMemoryException)
        {
            return false;
        }

        foreach (PreparedIntentCommand command in commands)
        {
            if (command is PrepareIntentCommand prepare)
                keys.Add(prepare.Intent.Key);
        }

        return true;
    }

    /// <summary>Resolves true once the batch carrying this record committed to Raft and every prepare it carried
    /// took ownership of its key; false when the batch did not commit, a prepare was refused, or this node did not
    /// observe the ordered apply (<see cref="Answer"/> tells which).</summary>
    public Task<bool> Committed => completion.Task;

    /// <summary>How the submission ended, valid once <see cref="Committed"/> has resolved. A producer that must tell a
    /// durable batch with a refused prepare (drive the truthful outcome against the record) from one whose apply
    /// this node never observed (retry as if nothing were durable) reads this rather than the folded bool.</summary>
    public DurableCompletionAnswer Answer => answer;

    /// <summary>Whether the batch is known durable on this node's account: the ordered apply was observed, whatever
    /// it said about the prepares. False for a released submission and for an unobserved apply, both of which the
    /// producer treats as a clean retry.</summary>
    public bool BatchObserved => answer is DurableCompletionAnswer.Acknowledged or DurableCompletionAnswer.Refused;

    /// <summary>Stale when any fenced key's descriptor moved since freeze: every space the submission touches is
    /// probed with its own key and generation, so a group that spans several key spaces is neither released for a
    /// mismatched pair nor blind to a move of its second space.</summary>
    public bool IsStale(IWriteRangeFence fence)
    {
        for (int i = 0; i < fences.Count; i++)
        {
            if (fence.IsStale(fences[i].Key, fences[i].Generation, PartitionId))
                return true;
        }

        return false;
    }

    /// <summary>
    /// Quiesced when this submission would add new state to a range whose data is being moved — a prepare, a
    /// re-prepare, a one-phase bundle or a standalone record initialization whose fenced key lies in the quiesced
    /// part of its range. Every fenced key is asked, because a split quiesces only the half it is moving and a
    /// group can hold keys on both sides of the split point. A decision, a settlement or a materialization is
    /// never quiesced: the move waits for the range's undecided intents to decide and settle before its
    /// catch-up copy, so refusing those would stall the move against the very transactions it waits for.
    /// </summary>
    public bool IsQuiesced(IWriteRangeFence fence)
    {
        if (Stage is not (WriteSubmissionStage.Prepare or WriteSubmissionStage.RePrepare
            or WriteSubmissionStage.OnePhase or WriteSubmissionStage.RecordInit))
            return false;

        for (int i = 0; i < fences.Count; i++)
        {
            if (fence.IsQuiesced(fences[i].Key))
                return true;
        }

        return false;
    }

    /// <summary>The batch committed: wait for the ordered apply of this submission's records (never applying them
    /// here), then resolve the producer with the answer. Never blocks the scheduler's lane: a wait that is not
    /// already satisfied resolves the producer from its continuation. An adapter that faults resolves
    /// <see cref="DurableCompletionAnswer.Unobserved"/>: the batch is durable but this node cannot say how its apply
    /// went, and a retry against the current leader is idempotent.</summary>
    public void Complete(IReadOnlyList<long>? entryLogIndices)
    {
        if (applyOnCommit is null)
        {
            Resolve(DurableCompletionAnswer.Acknowledged);
            return;
        }

        Task<DurableCompletionAnswer> awaited;
        try
        {
            awaited = applyOnCommit(PartitionId, Entries, entryLogIndices);
        }
        catch (Exception)
        {
            Resolve(DurableCompletionAnswer.Unobserved);
            return;
        }

        if (awaited.IsCompleted)
        {
            Resolve(awaited.IsCompletedSuccessfully ? awaited.Result : DurableCompletionAnswer.Unobserved);
            return;
        }

        _ = awaited.ContinueWith(
            static (task, state) => ((DurableProposalSubmission)state!).Resolve(task.IsCompletedSuccessfully ? task.Result : DurableCompletionAnswer.Unobserved),
            this,
            CancellationToken.None,
            TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default);
    }

    /// <summary>
    /// The commands of this submission when its one entry can go into a merged settle entry: the stage is
    /// <see cref="WriteSubmissionStage.Settle"/>, the entry is one auto-commit prepared-intent delta at generation zero, no range
    /// fence applies, and every command of the delta is a resolve or a remove. <paramref name="expectedTerm"/> is the
    /// term fence of the entry. False for any other submission, which keeps its own entry.
    /// </summary>
    internal bool TryGetMergeableSettle([System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out PreparedIntentCommand[]? commands, out long expectedTerm)
    {
        commands = null;
        expectedTerm = 0;

        if (Stage != WriteSubmissionStage.Settle || Entries.Count != 1 || fences.Count != 0)
            return false;

        RaftProposalEntry entry = Entries[0];
        if (entry.Type != ReplicationTypes.PreparedIntent || !entry.AutoCommit || entry.ExpectedGeneration != 0 || entry.Data is not { Length: > 0 } data)
            return false;

        if (!PreparedIntentStore.TryGetProposedCommands(data, out PreparedIntentCommand[]? decoded))
            return false;

        foreach (PreparedIntentCommand command in decoded)
        {
            if (command is not (ResolveIntentCommand or RemoveIntentCommand))
                return false;
        }

        commands = decoded;
        expectedTerm = entry.ExpectedTerm;
        return true;
    }

    /// <summary>Ends this submission with the answer of the merged settle entry that carried it.</summary>
    internal void ResolveMerged(DurableCompletionAnswer outcome) => Resolve(outcome);

    private void Resolve(DurableCompletionAnswer outcome)
    {
        answer = outcome;
        completion.TrySetResult(outcome == DurableCompletionAnswer.Acknowledged);
    }

    public void Release(bool transient) => Resolve(DurableCompletionAnswer.NotCommitted);
}
