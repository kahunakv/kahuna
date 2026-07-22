
using Nixie;
using Kommander.Time;
using Kommander.Data;
using Kahuna.Shared.KeyValue;
using Kahuna.Server.Replication;
using Kahuna.Server.KeyValues.Writes;

namespace Kahuna.Server.KeyValues;

/// <summary>
/// Immutable envelope for a staged direct (auto-commit, non-transactional) key/value write on its way to
/// Raft. It carries the already-serialized log record and its resolved partition rather than the full
/// <see cref="KeyValueProposal"/> — the proposal itself stays in the owning key actor's proposal dictionary
/// until <c>CompleteProposal</c>/<c>ReleaseProposal</c> applies or releases it, so the value graph is not
/// retained twice. The fence generation, cached byte length, and enqueue tick are carried for the partition
/// write scheduler (flush-time re-fence, byte budgeting, linger). As an <see cref="IProposalSubmission"/> it
/// owns its own staleness rule (the key-range fence) and its completion adapter (route
/// <c>CompleteProposal</c>/<c>ReleaseProposal</c> back to the owning key actor).
/// </summary>
internal sealed class KeyValueProposalRequest : IProposalSubmission
{
    public string Key { get; }

    public int PartitionId { get; }

    /// <summary>A direct client write is ordinary work: it adds new pending state and is bounded strictly by the
    /// base admission budget.</summary>
    public WriteAdmissionClass AdmissionClass => WriteAdmissionClass.Ordinary;

    public int ProposalId { get; }

    public KeyValueDurability Durability { get; }

    /// <summary>The live key-range descriptor generation captured at admission (0 for hash spaces, which never
    /// move). The flush-time fence releases the write if the descriptor covering <see cref="Key"/> no longer
    /// matches this generation and <see cref="PartitionId"/>.</summary>
    public long FenceGeneration { get; }

    public byte[] SerializedMessage { get; }

    public int ByteLength { get; }

    /// <summary>The Raft log type this entry replicates as. Direct key/value writes use
    /// <see cref="ReplicationTypes.KeyValues"/> (the default); the field exists so the partition batch — which is
    /// dispatched as one heterogeneous <c>ReplicateEntries</c> proposal — can carry entries of different types
    /// once other producers share the same scheduler.</summary>
    public string LogType { get; }

    /// <summary>A direct write is a single-entry bundle: one auto-commit, generation-zero entry.</summary>
    public IReadOnlyList<RaftProposalEntry> Entries { get; }

    public IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> KeyValueActor { get; }

    public TaskCompletionSource<KeyValueResponse?> Promise { get; }

    /// <summary>Millisecond tick when the aggregator admitted this write, stamped from the aggregator's
    /// <see cref="System.TimeProvider"/> so queue-age math and its wake timers share one clock. Set by the
    /// facade at admission; the constructor value is a placeholder.</summary>
    public long EnqueueTicks { get; set; }

    public KeyValueProposalRequest(
        string key,
        int partitionId,
        int proposalId,
        KeyValueDurability durability,
        long fenceGeneration,
        byte[] serializedMessage,
        IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> keyValueActor,
        TaskCompletionSource<KeyValueResponse?> promise,
        long enqueueTicks,
        string? logType = null
    )
    {
        Key = key;
        PartitionId = partitionId;
        ProposalId = proposalId;
        Durability = durability;
        FenceGeneration = fenceGeneration;
        SerializedMessage = serializedMessage;
        ByteLength = serializedMessage.Length;
        KeyValueActor = keyValueActor;
        Promise = promise;
        EnqueueTicks = enqueueTicks;
        LogType = logType ?? ReplicationTypes.KeyValues;
        Entries = [new RaftProposalEntry(LogType, serializedMessage, AutoCommit: true, ExpectedGeneration: 0)];
    }

    /// <summary>Key-range fence: release the write if its descriptor moved/split since admission. Hash spaces are
    /// never stale.</summary>
    public bool IsStale(IWriteRangeFence fence) => fence.IsStale(Key, FenceGeneration, PartitionId);

    /// <summary>Batch committed: send <c>CompleteProposal</c> (apply) to the originating key actor — the only
    /// component that mutates the entry and resolves the caller's promise.</summary>
    public void Complete() =>
        KeyValueActor.Send(new(
            KeyValueRequestType.CompleteProposal,
            HLCTimestamp.Zero,
            HLCTimestamp.Zero,
            Key,
            null,
            null,
            -1,
            KeyValueFlags.None,
            0,
            HLCTimestamp.Zero,
            Durability,
            ProposalId,
            PartitionId,
            Promise
        ));

    /// <summary>Batch did not commit (or released before dispatch): send <c>ReleaseProposal</c> (unwind) to the
    /// originating key actor. A transient failure releases with <see cref="KeyValueFlags.ReplicationRetry"/> so
    /// the caller retries.</summary>
    public void Release(bool transient) =>
        KeyValueActor.Send(new(
            KeyValueRequestType.ReleaseProposal,
            HLCTimestamp.Zero,
            HLCTimestamp.Zero,
            Key,
            null,
            null,
            -1,
            transient ? KeyValueFlags.ReplicationRetry : KeyValueFlags.None,
            0,
            HLCTimestamp.Zero,
            Durability,
            ProposalId,
            PartitionId,
            Promise
        ));
}
