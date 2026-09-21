using System.Collections.Concurrent;

namespace Kahuna.Server.KeyValues.Transactions;

/// <summary>What a completion learned about the ordered apply of one durable entry.</summary>
internal enum DurableApplyWaitStatus
{
    /// <summary>The consumer applied the entry and its result was still recorded.</summary>
    Recorded,

    /// <summary>The consumer applied the entry (the partition's applied cursor is at or past it) but its recorded
    /// result was displaced from the bounded window before the completion claimed it. The apply happened, in order;
    /// only the acknowledgement bit is gone, and the caller reads the store for it.</summary>
    AppliedResultDisplaced,

    /// <summary>The consumer did not apply the entry within the wait: the node no longer replicates the partition, a
    /// snapshot install covered the entry, or the apply stream is stalled. Nothing was applied on the caller's behalf.</summary>
    NotApplied,

    /// <summary>This node stopped leading the partition while the completion waited (or before it asked). Its own
    /// ordered apply no longer decides anything for the producer — the current leader's does — so the wait was
    /// released rather than served out. Nothing was applied on the caller's behalf.</summary>
    LeadershipLost
}

/// <summary>The outcome of <see cref="DurableApplyResultLedger.WaitAppliedAsync"/>.</summary>
internal readonly record struct DurableApplyWaitOutcome(DurableApplyWaitStatus Status, bool Result)
{
    internal static DurableApplyWaitOutcome Recorded(bool result) => new(DurableApplyWaitStatus.Recorded, result);

    internal static readonly DurableApplyWaitOutcome Displaced = new(DurableApplyWaitStatus.AppliedResultDisplaced, false);

    internal static readonly DurableApplyWaitOutcome NotApplied = new(DurableApplyWaitStatus.NotApplied, false);

    internal static readonly DurableApplyWaitOutcome LeadershipLost = new(DurableApplyWaitStatus.LeadershipLost, false);
}

/// <summary>
/// The rendezvous between the ordered consumer apply of a durable record/intent entry and the write scheduler's
/// completion for the same log entry.
///
/// <para>The durable stores are mutated on exactly one live path: the per-partition consumer apply that Raft drives
/// in log order. The scheduler's completion for a proposal runs when the proposal is quorum-durable — which, on the
/// proposing node, can be before the consumer has applied that entry, before it has applied the entries below it,
/// or (when the completion is delayed) long after. It therefore never applies the delta itself: it waits here for
/// the consumer's in-order apply of its entries and reads the result that apply recorded — the prepare
/// acknowledgement for an intent delta, the apply result for a record delta. That is what keeps a node's intent and
/// record state a pure function of its log. An apply whose outcome depends on earlier entries (a prepare judged
/// against a competitor's live intent, a bundled commit judged against the committed-head ledger) is computed at the
/// entry's own log position on every replica, never early against an incomplete prefix on the leader, and never a
/// second time when a late completion re-applied a delta the consumer had already applied and settled — the
/// resurrected-intent fork that made an ex-leader alone reject a bundled commit its peers admitted.</para>
///
/// <para>Results live in a bounded per-partition ring addressed by log index, so recording is a single slot write
/// and the residue of results no completion claims (follower applies, released batches) is bounded by construction.
/// A completion that arrives after its slot was displaced learns from the partition's applied cursor that the apply
/// happened and reads the store instead. A completion whose entry is never applied here times out and answers its
/// producer conservatively (see <see cref="DurableApplyWaitStatus.NotApplied"/>).</para>
///
/// <para>The wait is only meaningful while this node leads the partition. A leader whose device stalls is stepped
/// down by Kommander's durable-write watchdog within seconds, and its own ordered apply then cannot advance until
/// the device heals — but the entries it proposed are quorum-durable and the new leader applies and judges them.
/// Serving the wait out on the ex-leader would only turn the stall into a long unknown outcome at the client, so a
/// leadership loss releases every parked wait at once (<see cref="NoteLeadershipLost"/>) and a wait asked on a
/// partition this node stopped leading is answered immediately; the producer re-drives against the current leader,
/// where the same entry is idempotent.</para>
/// </summary>
internal sealed class DurableApplyResultLedger
{
    // Results are claimed almost immediately, so a window this size is far larger than the real in-flight depth. It
    // bounds the residue left when a completion never claims its result, which would otherwise accumulate for the
    // process's lifetime.
    private const int WindowSize = 1024;

    private sealed class PartitionState
    {
        // One ring, addressed by log index modulo the window: entry N silently displaces entry N-1024.
        internal readonly long[] Ring = new long[WindowSize];

        // Highest durable-entry log index the consumer apply recorded. Monotonic. The consumer applies in log
        // order, so a cursor at or past an index proves that index's apply completed (its result may be displaced).
        internal long AppliedThrough;

        // Completions waiting for an index the consumer has not applied yet. Removed by whichever side settles them.
        internal readonly ConcurrentDictionary<long, TaskCompletionSource<DurableApplyWaitOutcome>> Waiters = new();

        // Set when this node stopped leading the partition and cleared when it leads it again. While set, no
        // completion parks here: the node's ordered apply no longer decides anything for a producer.
        internal volatile bool LeadershipLost;
    }

    private readonly ConcurrentDictionary<int, PartitionState> partitions = new();

    private PartitionState StateOf(int partitionId) =>
        partitions.TryGetValue(partitionId, out PartitionState? existing)
            ? existing
            : partitions.GetOrAdd(partitionId, static _ => new PartitionState());

    /// <summary>Records what the consumer apply of this entry produced: the prepare acknowledgement for an intent
    /// delta, or the apply result for a record delta. Advances the partition's applied cursor and wakes a completion
    /// waiting for the entry. A non-positive index carries no entry identity.</summary>
    public void RecordApplied(int partitionId, long logIndex, bool result)
    {
        if (logIndex <= 0)
            return;

        PartitionState state = StateOf(partitionId);

        Volatile.Write(ref state.Ring[Slot(logIndex)], Encode(logIndex, result));

        long current = Volatile.Read(ref state.AppliedThrough);
        while (logIndex > current)
        {
            long seen = Interlocked.CompareExchange(ref state.AppliedThrough, logIndex, current);
            if (seen == current)
                break;
            current = seen;
        }

        if (!state.Waiters.IsEmpty && state.Waiters.TryRemove(logIndex, out TaskCompletionSource<DurableApplyWaitOutcome>? waiter))
            waiter.TrySetResult(DurableApplyWaitOutcome.Recorded(result));
    }

    /// <summary>This node stopped leading <paramref name="partitionId"/>: releases every completion parked on the
    /// partition with <see cref="DurableApplyWaitStatus.LeadershipLost"/> and answers later waits the same way until
    /// <see cref="NoteLeadershipRegained"/>. Returns how many waits were released. Idempotent.</summary>
    public int NoteLeadershipLost(int partitionId)
    {
        PartitionState state = StateOf(partitionId);
        state.LeadershipLost = true;

        int released = 0;
        foreach (KeyValuePair<long, TaskCompletionSource<DurableApplyWaitOutcome>> parked in state.Waiters)
        {
            if (state.Waiters.TryRemove(parked.Key, out TaskCompletionSource<DurableApplyWaitOutcome>? waiter) && waiter.TrySetResult(DurableApplyWaitOutcome.LeadershipLost))
                released++;
        }

        return released;
    }

    /// <summary>This node leads <paramref name="partitionId"/> (again): completions for entries it proposes park
    /// here as usual.</summary>
    public void NoteLeadershipRegained(int partitionId) => StateOf(partitionId).LeadershipLost = false;

    /// <summary>Whether this node is known to have stopped leading the partition (test seam).</summary>
    internal bool HasLostLeadership(int partitionId) =>
        partitions.TryGetValue(partitionId, out PartitionState? state) && state.LeadershipLost;

    /// <summary>The highest durable-entry log index the consumer apply has recorded for the partition (0 when none).</summary>
    public long AppliedThrough(int partitionId) =>
        partitions.TryGetValue(partitionId, out PartitionState? state) ? Volatile.Read(ref state.AppliedThrough) : 0;

    /// <summary>Takes the recorded result for an entry, meaning its apply already happened. False means no result is
    /// recorded for exactly this entry: not applied yet, displaced, or already taken.</summary>
    public bool TryConsume(int partitionId, long logIndex, out bool result)
    {
        result = false;

        if (logIndex <= 0 || !partitions.TryGetValue(partitionId, out PartitionState? state))
            return false;

        ref long slot = ref state.Ring[Slot(logIndex)];

        // Claim the slot only if it still holds this exact entry: a displaced or already consumed slot reads as some
        // other index. Clearing it makes the take single-shot under concurrency.
        long expected = Encode(logIndex, true);
        long observed = Interlocked.CompareExchange(ref slot, 0, expected);

        if (observed != expected)
        {
            expected = Encode(logIndex, false);

            if (Interlocked.CompareExchange(ref slot, 0, expected) != expected)
                return false;
        }
        else
            result = true;

        DurableTransactionMetrics.RedundantApplySkipped();
        return true;
    }

    /// <summary>
    /// Waits for the consumer apply of the entry at <paramref name="logIndex"/> and answers with the result it
    /// recorded. Returns at once when the result is already recorded, or when the partition's applied cursor shows
    /// the apply happened but the result was displaced. Otherwise parks until the consumer records the entry, the
    /// <paramref name="timeout"/> elapses, or <paramref name="cancellationToken"/> fires — the last two answer
    /// <see cref="DurableApplyWaitStatus.NotApplied"/> (after one last look at the ring and the cursor) rather than
    /// throwing, so a completion always resolves its producer.
    /// </summary>
    public async ValueTask<DurableApplyWaitOutcome> WaitAppliedAsync(int partitionId, long logIndex, TimeSpan timeout, CancellationToken cancellationToken)
    {
        if (logIndex <= 0)
            return DurableApplyWaitOutcome.NotApplied;

        if (TryConsume(partitionId, logIndex, out bool recorded))
            return DurableApplyWaitOutcome.Recorded(recorded);

        PartitionState state = StateOf(partitionId);

        if (Volatile.Read(ref state.AppliedThrough) >= logIndex)
            return DurableApplyWaitOutcome.Displaced;

        if (state.LeadershipLost)
            return DurableApplyWaitOutcome.LeadershipLost;

        TaskCompletionSource<DurableApplyWaitOutcome> waiter = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource<DurableApplyWaitOutcome> registered = state.Waiters.GetOrAdd(logIndex, waiter);

        // Re-check after registering: a RecordApplied that ran between the probes above and the registration wrote
        // the ring and the cursor but found no waiter to wake; a NoteLeadershipLost in the same window snapshotted
        // the waiters before this one was in.
        if (TryConsume(partitionId, logIndex, out recorded))
        {
            state.Waiters.TryRemove(logIndex, out _);
            return DurableApplyWaitOutcome.Recorded(recorded);
        }

        if (Volatile.Read(ref state.AppliedThrough) >= logIndex)
        {
            state.Waiters.TryRemove(logIndex, out _);
            return DurableApplyWaitOutcome.Displaced;
        }

        if (state.LeadershipLost)
        {
            state.Waiters.TryRemove(logIndex, out _);
            return DurableApplyWaitOutcome.LeadershipLost;
        }

        try
        {
            DurableApplyWaitOutcome outcome = await registered.Task.WaitAsync(timeout, cancellationToken).ConfigureAwait(false);

            // A recorded result is also in the ring; take it so the slot is not claimable a second time (and the
            // reuse is counted once, like a result claimed without waiting).
            if (outcome.Status == DurableApplyWaitStatus.Recorded)
                TryConsume(partitionId, logIndex, out _);

            return outcome;
        }
        catch (Exception ex) when (ex is TimeoutException or OperationCanceledException)
        {
            state.Waiters.TryRemove(logIndex, out _);

            if (TryConsume(partitionId, logIndex, out recorded))
                return DurableApplyWaitOutcome.Recorded(recorded);

            return Volatile.Read(ref state.AppliedThrough) >= logIndex
                ? DurableApplyWaitOutcome.Displaced
                : DurableApplyWaitOutcome.NotApplied;
        }
    }

    private static int Slot(long logIndex) => (int)((ulong)logIndex % WindowSize);

    // The index identifies the entry occupying the slot and the low bit carries its result, so a slot is claimed and
    // read in one atomic word. Zero is never a valid encoding, which makes it the empty/consumed marker.
    private static long Encode(long logIndex, bool result) => (logIndex << 1) | (result ? 1L : 0L);
}
