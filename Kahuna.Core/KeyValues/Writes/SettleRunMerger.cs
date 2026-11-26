using Kommander.Data;

using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.Replication;

namespace Kahuna.Server.KeyValues.Writes;

/// <summary>
/// Puts each run of settle submissions that are adjacent in a batch of the write scheduler into one log entry.
///
/// <para>A settle resolves and removes the prepared intents of a decided transaction. Before this merge, each settle
/// had an entry of its own, and the batch carried them side by side. The merged entry has the commands of the run in
/// queue order, so the order of the commands in the log does not change: only the borders of the entries move. The
/// entry is serialized from the commands and never joined from the bytes of the members. A delta can have one shared
/// identity header, and the commands of a second transaction would then decode with the identity of the first. The
/// serializer writes a header only when every command agrees on the identity, so each command of a merged entry
/// carries its own.</para>
///
/// <para>A member of a run (see <see cref="DurableProposalSubmission.TryGetMergeableSettle"/>) has one auto-commit
/// prepared-intent entry at generation zero, no range fence, and resolve and remove commands only, and its term fence
/// equals that of the run. A submission that fails a condition ends the run and keeps its own entry. A run of one
/// member keeps its own entry too. A run whose commands pass <c>maxCommands</c> continues in a second entry.</para>
///
/// <para>Each member keeps its admission and its class. The answer of the merged entry goes to every member: a
/// committed and applied entry acknowledges each of them, and a released entry or an apply that this node did not
/// observe answers each producer as not committed, and each sends its settle again. A second copy of a settle is a
/// no-op at apply.</para>
/// </summary>
internal static class SettleRunMerger
{
    /// <summary>
    /// The batch with each run of mergeable settles replaced by one <see cref="MergedSettleSubmission"/>, in the same
    /// order. Returns <paramref name="batch"/> itself when no run has two members.
    /// </summary>
    internal static List<IProposalSubmission> Merge(
        List<IProposalSubmission> batch,
        int maxCommands,
        Func<int, IReadOnlyList<RaftProposalEntry>, IReadOnlyList<long>?, Task<DurableCompletionAnswer>>? awaiter)
    {
        List<IProposalSubmission>? merged = null;
        List<(DurableProposalSubmission Member, PreparedIntentCommand[] Commands)>? run = null;
        long runTerm = 0;
        int runCommands = 0;

        for (int i = 0; i < batch.Count; i++)
        {
            IProposalSubmission item = batch[i];

            if (item is not DurableProposalSubmission durable || !durable.TryGetMergeableSettle(out PreparedIntentCommand[]? commands, out long term))
            {
                if (run is { Count: > 0 })
                    CloseRun(ref merged, batch, i, run, awaiter);

                merged?.Add(item);
                continue;
            }

            if (run is { Count: > 0 } && (term != runTerm || runCommands + commands.Length > maxCommands))
                CloseRun(ref merged, batch, i, run, awaiter);

            run ??= [];
            if (run.Count == 0)
            {
                runTerm = term;
                runCommands = 0;
            }

            run.Add((durable, commands));
            runCommands += commands.Length;
        }

        if (run is { Count: > 0 })
            CloseRun(ref merged, batch, batch.Count, run, awaiter);

        return merged ?? batch;
    }

    /// <summary>Ends the run that stops before <paramref name="end"/>: a run of one member keeps its own entry, a longer
    /// one becomes one merged submission. The result list starts at the first merge, with the members before it.</summary>
    private static void CloseRun(
        ref List<IProposalSubmission>? merged,
        List<IProposalSubmission> batch,
        int end,
        List<(DurableProposalSubmission Member, PreparedIntentCommand[] Commands)> run,
        Func<int, IReadOnlyList<RaftProposalEntry>, IReadOnlyList<long>?, Task<DurableCompletionAnswer>>? awaiter)
    {
        if (run.Count == 1)
        {
            merged?.Add(run[0].Member);
            run.Clear();
            return;
        }

        if (merged is null)
        {
            merged = new List<IProposalSubmission>(batch.Count);
            for (int i = 0; i < end - run.Count; i++)
                merged.Add(batch[i]);
        }

        merged.Add(MergedSettleSubmission.Create(run, awaiter));
        run.Clear();
    }
}

/// <summary>
/// One merged settle entry that replaces a run of settle submissions in a batch (see <see cref="SettleRunMerger"/>). It
/// is not admitted on its own: each member keeps its admission, and the lane releases the admission of each member
/// when this submission ends. It waits one time for the ordered apply of its entry, because the apply-result ledger
/// gives the result of one log index to one waiter, and then ends every member with that answer.
/// </summary>
internal sealed class MergedSettleSubmission : IProposalSubmission
{
    private readonly Func<int, IReadOnlyList<RaftProposalEntry>, IReadOnlyList<long>?, Task<DurableCompletionAnswer>>? awaiter;

    private readonly List<string> changedKeys;

    private MergedSettleSubmission(
        DurableProposalSubmission[] members,
        RaftProposalEntry entry,
        List<string> changedKeys,
        Func<int, IReadOnlyList<RaftProposalEntry>, IReadOnlyList<long>?, Task<DurableCompletionAnswer>>? awaiter)
    {
        Members = members;
        PartitionId = members[0].PartitionId;
        Entries = [entry];
        ByteLength = entry.Data.Length;
        EnqueueTicks = members[0].EnqueueTicks;
        this.changedKeys = changedKeys;
        this.awaiter = awaiter;
    }

    internal static MergedSettleSubmission Create(
        List<(DurableProposalSubmission Member, PreparedIntentCommand[] Commands)> run,
        Func<int, IReadOnlyList<RaftProposalEntry>, IReadOnlyList<long>?, Task<DurableCompletionAnswer>>? awaiter)
    {
        // A member names the keys of its commands: a delta that it could not decode did not join the run.
        int commandCount = 0;
        int keyCount = 0;
        foreach ((DurableProposalSubmission member, PreparedIntentCommand[] commands) in run)
        {
            commandCount += commands.Length;
            keyCount += Math.Max(0, member.ChangedKeyCount);
        }

        PreparedIntentCommand[] all = new PreparedIntentCommand[commandCount];
        DurableProposalSubmission[] members = new DurableProposalSubmission[run.Count];
        List<string> keys = new(keyCount);

        int offset = 0;
        for (int i = 0; i < run.Count; i++)
        {
            (DurableProposalSubmission member, PreparedIntentCommand[] commands) = run[i];
            members[i] = member;
            commands.CopyTo(all, offset);
            offset += commands.Length;

            for (int k = 0; k < member.ChangedKeyCount; k++)
                keys.Add(member.ChangedKey(k));
        }

        // Serialized from the commands, so each command keeps the identity of its own transaction. The call registers
        // the commands for the apply of these bytes on this node.
        byte[] data = PreparedIntentStore.SerializeDelta(all);
        RaftProposalEntry first = members[0].Entries[0];
        RaftProposalEntry entry = new(ReplicationTypes.PreparedIntent, data, AutoCommit: true, ExpectedGeneration: 0, ExpectedTerm: first.ExpectedTerm);

        return new MergedSettleSubmission(members, entry, keys, awaiter);
    }

    /// <summary>The settle submissions this entry carries, in queue order.</summary>
    internal IReadOnlyList<DurableProposalSubmission> Members { get; }

    public int PartitionId { get; }

    public WriteAdmissionClass AdmissionClass => WriteAdmissionClass.Terminal;

    public WriteSubmissionStage Stage => WriteSubmissionStage.Settle;

    /// <summary>The bytes of the merged entry. The admission of each member holds the bytes of its own entry.</summary>
    public int ByteLength { get; }

    public IReadOnlyList<RaftProposalEntry> Entries { get; }

    public long EnqueueTicks { get; set; }

    // The members were judged at the selection of the batch, and a settle has no fence.
    public bool IsStale(IWriteRangeFence fence) => false;

    public bool IsQuiesced(IWriteRangeFence fence) => false;

    public int ChangedKeyCount => changedKeys.Count;

    public string ChangedKey(int index) => changedKeys[index];

    /// <summary>The entry committed: waits for its ordered apply one time, then ends every member with the answer.</summary>
    public void Complete(IReadOnlyList<long>? entryLogIndices)
    {
        if (awaiter is null)
        {
            ResolveAll(DurableCompletionAnswer.Acknowledged);
            return;
        }

        Task<DurableCompletionAnswer> awaited;
        try
        {
            awaited = awaiter(PartitionId, Entries, entryLogIndices);
        }
        catch (Exception)
        {
            ResolveAll(DurableCompletionAnswer.Unobserved);
            return;
        }

        if (awaited.IsCompleted)
        {
            ResolveAll(awaited.IsCompletedSuccessfully ? awaited.Result : DurableCompletionAnswer.Unobserved);
            return;
        }

        _ = awaited.ContinueWith(
            static (task, state) => ((MergedSettleSubmission)state!).ResolveAll(task.IsCompletedSuccessfully ? task.Result : DurableCompletionAnswer.Unobserved),
            this,
            CancellationToken.None,
            TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default);
    }

    /// <summary>The entry did not commit: every member is released, and each producer sends its settle again.</summary>
    public void Release(bool transient)
    {
        foreach (DurableProposalSubmission member in Members)
            member.Release(transient);
    }

    private void ResolveAll(DurableCompletionAnswer answer)
    {
        foreach (DurableProposalSubmission member in Members)
            member.ResolveMerged(answer);
    }
}
