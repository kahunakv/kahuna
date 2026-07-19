using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using Kahuna;
using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Writes;
using Kahuna.Shared.KeyValue;
using Kahuna.Utils;
using Kommander;
using Kommander.Data;
using Kommander.Time;
using Microsoft.Extensions.Logging.Abstractions;
using Nixie;

namespace Kahuna.Server.Tests;

/// <summary>
/// Component tests for the partition write aggregator: the pure per-partition batching state, and the lane
/// actor + facade driven with synthetic items, a recording batch executor (records calls, gates one partition
/// while another proceeds, returns chosen statuses), and a recording completion router. No real cluster.
/// </summary>
public sealed class TestPartitionWriteAggregator
{
    private static KeyValueProposalRequest Item(int partitionId, int proposalId, int bytes = 16, long ageMs = 0, string? key = null) =>
        new(
            key: key ?? "k" + proposalId,
            partitionId: partitionId,
            proposalId: proposalId,
            durability: KeyValueDurability.Persistent,
            fenceGeneration: 0,
            serializedMessage: new byte[bytes],
            keyValueActor: null!,
            promise: new TaskCompletionSource<KeyValueResponse?>(),
            enqueueTicks: Environment.TickCount64 - ageMs
        );

    private sealed class StubFence : IWriteRangeFence
    {
        public readonly HashSet<string> StaleKeys = [];
        public bool IsStale(string key, long admittedGeneration, int admittedPartitionId) => StaleKeys.Contains(key);
    }

    // ── pure state ────────────────────────────────────────────────────────────

    [Fact]
    public void State_SingleItem_OpensBufferWithoutImmediateFlush()
    {
        PartitionWriteState state = new();
        PartitionWriteState.EnqueueResult r = state.Enqueue(Item(1, 1), maxItems: 512, maxBytes: 4096);

        Assert.True(r.OpenedBuffer);
        Assert.False(r.ShouldFlushNow);
        Assert.Equal(1, state.PendingCount);
    }

    [Fact]
    public void State_ItemCapReached_SignalsFlush()
    {
        PartitionWriteState state = new();
        PartitionWriteState.EnqueueResult r1 = state.Enqueue(Item(1, 1), maxItems: 2, maxBytes: 4096);
        Assert.False(r1.ShouldFlushNow);

        PartitionWriteState.EnqueueResult r2 = state.Enqueue(Item(1, 2), maxItems: 2, maxBytes: 4096);
        Assert.True(r2.ShouldFlushNow);
    }

    [Fact]
    public void State_ByteCapReached_SignalsFlush()
    {
        PartitionWriteState state = new();
        PartitionWriteState.EnqueueResult r = state.Enqueue(Item(1, 1, bytes: 5000), maxItems: 512, maxBytes: 4096);
        Assert.True(r.ShouldFlushNow); // a single oversized item flushes immediately (dispatches alone)
    }

    [Fact]
    public void State_SelectBatch_SplitsByItemCap()
    {
        PartitionWriteState state = new();
        for (int i = 0; i < 5; i++)
            state.Enqueue(Item(1, i), maxItems: 512, maxBytes: 4096);

        List<KeyValueProposalRequest> first = state.SelectBatch(maxItems: 3, maxBytes: 4096);
        Assert.Equal(3, first.Count);
        Assert.True(state.InFlight);

        state.OnBatchComplete();
        List<KeyValueProposalRequest> second = state.SelectBatch(maxItems: 3, maxBytes: 4096);
        Assert.Equal(2, second.Count);
    }

    [Fact]
    public void State_SelectBatch_OversizedFirstItem_DispatchesAlone()
    {
        PartitionWriteState state = new();
        state.Enqueue(Item(1, 1, bytes: 5000), maxItems: 512, maxBytes: 4096);
        state.Enqueue(Item(1, 2, bytes: 16), maxItems: 512, maxBytes: 4096);

        List<KeyValueProposalRequest> batch = state.SelectBatch(maxItems: 512, maxBytes: 4096);
        Assert.Single(batch); // the oversized head goes alone; the small item waits for the next batch
        Assert.Equal(1, batch[0].ProposalId);
    }

    [Fact]
    public void State_NextWakeDeadline_EarlierOfLingerAndAge_WhenNotInFlight()
    {
        PartitionWriteState state = new();
        KeyValueProposalRequest it = Item(1, 1);
        it.EnqueueTicks = 1000;
        state.Enqueue(it, maxItems: 512, maxBytes: 4096);

        // Not in flight: min(oldest+linger, oldest+age); with linger <= age that is the linger deadline.
        Assert.Equal(1000 + 50, state.NextWakeDeadline(lingerMs: 50, maxQueueDelayMs: 500));
    }

    [Fact]
    public void State_NextWakeDeadline_AgeOnly_WhileInFlight()
    {
        PartitionWriteState state = new();
        KeyValueProposalRequest first = Item(1, 1);
        first.EnqueueTicks = 1000;
        state.Enqueue(first, maxItems: 512, maxBytes: 4096);
        state.SelectBatch(maxItems: 512, maxBytes: 4096); // in flight, buffer drained

        KeyValueProposalRequest behind = Item(1, 2);
        behind.EnqueueTicks = 1200;
        state.Enqueue(behind, maxItems: 512, maxBytes: 4096); // queued behind the in-flight batch

        // A flush cannot start while in flight, so only the age-expiry deadline is scheduled — never the
        // earlier linger deadline. This is the B2 fix: the item behind the batch still gets an age wake.
        Assert.Equal(1200 + 500, state.NextWakeDeadline(lingerMs: 50, maxQueueDelayMs: 500));
    }

    [Fact]
    public void State_TryArmWake_ArmsOnlyEarlier_AndReArmsAfterClear()
    {
        PartitionWriteState state = new();
        Assert.True(state.TryArmWake(1000));   // nothing armed → arms
        Assert.False(state.TryArmWake(1000));  // equal → no re-arm
        Assert.False(state.TryArmWake(1500));  // later → no re-arm
        Assert.True(state.TryArmWake(800));    // earlier → re-arms
        state.ClearArmedWake();
        Assert.True(state.TryArmWake(2000));   // after clear any deadline arms again
    }

    [Fact]
    public void State_LingerElapsed_RespectsWindowAndInFlight()
    {
        PartitionWriteState state = new();
        KeyValueProposalRequest it = Item(1, 1);
        it.EnqueueTicks = 1000;
        state.Enqueue(it, maxItems: 512, maxBytes: 4096);

        Assert.False(state.LingerElapsed(nowMs: 1040, lingerMs: 50)); // window not yet elapsed
        Assert.True(state.LingerElapsed(nowMs: 1050, lingerMs: 50));  // elapsed

        state.SelectBatch(maxItems: 512, maxBytes: 4096); // in flight
        KeyValueProposalRequest behind = Item(1, 2);
        behind.EnqueueTicks = 1000;
        state.Enqueue(behind, maxItems: 512, maxBytes: 4096);
        Assert.False(state.LingerElapsed(nowMs: 100_000, lingerMs: 50)); // in flight suppresses flush
    }

    // ── recording doubles ──────────────────────────────────────────────────────

    private sealed class RecordingExecutor : IPartitionBatchExecutor
    {
        public readonly ConcurrentQueue<(int Partition, int Count)> Calls = new();
        private readonly ConcurrentDictionary<int, TaskCompletionSource> gates = new();
        public bool SucceedResult = true;
        public RaftOperationStatus ResultStatus = RaftOperationStatus.Success;
        public bool ThrowOnReplicate;

        public void Gate(int partition) => gates[partition] = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public void Release(int partition) { if (gates.TryRemove(partition, out TaskCompletionSource? g)) g.TrySetResult(); }

        public async Task<RaftReplicationResult> ReplicateAsync(int partitionId, IReadOnlyList<byte[]> logs)
        {
            Calls.Enqueue((partitionId, logs.Count));
            if (gates.TryGetValue(partitionId, out TaskCompletionSource? gate))
                await gate.Task;
            if (ThrowOnReplicate)
                throw new InvalidOperationException("simulated Raft round-trip failure");
            return new RaftReplicationResult(SucceedResult, ResultStatus, HLCTimestamp.Zero, 1);
        }
    }

    private sealed class RecordingRouter : IWriteCompletionRouter
    {
        public readonly ConcurrentDictionary<int, byte> Completed = new();
        // Value is the transient flag: true = released retryable (MustRetry), false = terminal.
        public readonly ConcurrentDictionary<int, bool> Released = new();
        public void Complete(KeyValueProposalRequest item) => Completed[item.ProposalId] = 1;
        public void Release(KeyValueProposalRequest item, bool transient) => Released[item.ProposalId] = transient;
    }

    private static async Task WaitUntil(Func<bool> predicate, int timeoutMs = 5000)
    {
        long deadline = Environment.TickCount64 + timeoutMs;
        while (Environment.TickCount64 < deadline)
        {
            if (predicate()) return;
            await Task.Delay(10);
        }
        throw new TimeoutException("condition not met in time");
    }

    /// <summary>Advances a manual clock in steps, giving the mailbox real time between steps to process the
    /// wake the advance triggers, until the predicate holds. Robust to the wake timer being armed just after a
    /// step: a later step (or the already-past deadline) still releases the item.</summary>
    private static async Task AdvanceUntil(ManualTimeProvider time, Func<bool> predicate, int stepMs, int maxSteps = 50)
    {
        for (int i = 0; i < maxSteps; i++)
        {
            if (predicate()) return;
            time.Advance(TimeSpan.FromMilliseconds(stepMs));
            for (int j = 0; j < 40 && !predicate(); j++)
                await Task.Delay(5);
        }
        if (!predicate())
            throw new TimeoutException("condition not met after advancing manual time");
    }

    private static PartitionWriteAggregator Build(RecordingExecutor exec, RecordingRouter router, PartitionWriteAggregatorOptions options, IWriteRangeFence? fence = null, TimeProvider? timeProvider = null) =>
        new(new ActorSystem(), exec, router, options, fence ?? new StubFence(), NullLogger<IKahuna>.Instance, timeProvider);

    /// <summary>Minimal controllable <see cref="TimeProvider"/>: <see cref="GetTimestamp"/> advances only on
    /// <see cref="Advance"/>, and timers created by <c>Task.Delay(delay, provider)</c> fire when advanced past
    /// their due time. Enough for the aggregator's queue-age deadline (it uses GetTimestamp + Task.Delay).</summary>
    private sealed class ManualTimeProvider : TimeProvider
    {
        private readonly object gate = new();
        private long nowStamps;
        private DateTimeOffset utcNow = DateTimeOffset.UnixEpoch;
        private readonly List<ManualTimer> timers = [];

        public override long TimestampFrequency => 10_000_000;

        public override long GetTimestamp()
        {
            lock (gate)
                return nowStamps;
        }

        public override DateTimeOffset GetUtcNow()
        {
            lock (gate)
                return utcNow;
        }

        public void Advance(TimeSpan delta)
        {
            List<ManualTimer> due = [];
            lock (gate)
            {
                nowStamps += (long)(delta.TotalSeconds * TimestampFrequency);
                utcNow += delta;
                foreach (ManualTimer t in timers.ToArray())
                {
                    if (!t.Fired && t.DueStamps <= nowStamps)
                    {
                        t.Fired = true;
                        due.Add(t);
                    }
                }
            }

            foreach (ManualTimer t in due) // fire outside the lock; the callback may create/dispose timers
                t.Fire();
        }

        public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
        {
            lock (gate)
            {
                ManualTimer timer = new(this, callback, state, nowStamps + (long)(dueTime.TotalSeconds * TimestampFrequency));
                timers.Add(timer);
                return timer;
            }
        }

        private void Remove(ManualTimer t)
        {
            lock (gate)
                timers.Remove(t);
        }

        private sealed class ManualTimer(ManualTimeProvider provider, TimerCallback callback, object? state, long dueStamps) : ITimer
        {
            public long DueStamps { get; } = dueStamps;
            public bool Fired;

            public void Fire() => callback(state);
            public bool Change(TimeSpan dueTime, TimeSpan period) => true;
            public void Dispose() => provider.Remove(this);
            public ValueTask DisposeAsync() { provider.Remove(this); return ValueTask.CompletedTask; }
        }
    }

    // ── integration ─────────────────────────────────────────────────────────────

    [Fact]
    public async Task Aggregator_64ItemsSamePartition_ProduceOneBulkCall_AndEachCompletesOnce()
    {
        RecordingExecutor exec = new();
        RecordingRouter router = new();
        PartitionWriteAggregator agg = Build(exec, router, new PartitionWriteAggregatorOptions
        {
            MaxBatchItems = 64,       // 64 admitted → count flush → one batch
            LingerMs = 10_000,        // long enough that the timer never fires first
            MaxQueuedItemsPerPartition = 1024
        });

        for (int i = 0; i < 64; i++)
            Assert.True(agg.TryEnqueue(Item(2, i)));

        await WaitUntil(() => router.Completed.Count == 64);

        Assert.Single(exec.Calls);
        Assert.Equal((2, 64), exec.Calls.First());
        Assert.Empty(router.Released);
    }

    [Fact]
    public async Task Aggregator_IndependentSubmissionsSamePartition_ShareOneBatch()
    {
        // Cross-request coalescing: items from two independent many-key submissions plus a single write, all to
        // the same partition and admitted before the linger elapses, share one Raft batch.
        RecordingExecutor exec = new();
        RecordingRouter router = new();
        PartitionWriteAggregator agg = Build(exec, router, new PartitionWriteAggregatorOptions
        {
            MaxBatchItems = 512,
            LingerMs = 200,          // long enough that all nine arrive before the timer fires
            MaxQueuedItemsPerPartition = 1024
        });

        for (int i = 0; i < 4; i++) agg.TryEnqueue(Item(3, i));         // "call A"
        for (int i = 0; i < 4; i++) agg.TryEnqueue(Item(3, 100 + i));   // "call B"
        agg.TryEnqueue(Item(3, 999));                                    // single write

        await WaitUntil(() => router.Completed.Count == 9);

        Assert.Single(exec.Calls);
        Assert.Equal(9, exec.Calls.First().Count);
    }

    [Fact]
    public async Task Aggregator_BlockedPartition_DoesNotBlockAnother()
    {
        RecordingExecutor exec = new();
        RecordingRouter router = new();
        PartitionWriteAggregator agg = Build(exec, router, new PartitionWriteAggregatorOptions
        {
            MaxBatchItems = 4,
            LingerMs = 10_000,
            MaxQueuedItemsPerPartition = 1024,
            LaneCount = 1 // both partitions share one lane: proves partition independence within a lane
        });

        exec.Gate(7); // hold partition 7's Raft call

        for (int i = 0; i < 4; i++)
            agg.TryEnqueue(Item(7, 100 + i)); // count flush → dispatched, then blocked in the executor

        await WaitUntil(() => exec.Calls.Any(c => c.Partition == 7));

        // Partition 9 must flush and complete while partition 7 is blocked.
        for (int i = 0; i < 4; i++)
            agg.TryEnqueue(Item(9, 200 + i));

        await WaitUntil(() => router.Completed.ContainsKey(200));
        Assert.False(router.Completed.ContainsKey(100)); // partition 7 still blocked, not completed

        exec.Release(7);
        await WaitUntil(() => router.Completed.ContainsKey(100));
    }

    [Fact]
    public async Task Aggregator_FailedBatch_ReleasesEveryItem()
    {
        RecordingExecutor exec = new() { SucceedResult = false, ResultStatus = RaftOperationStatus.Errored };
        RecordingRouter router = new();
        PartitionWriteAggregator agg = Build(exec, router, new PartitionWriteAggregatorOptions
        {
            MaxBatchItems = 8,
            LingerMs = 10_000,
            MaxQueuedItemsPerPartition = 1024
        });

        for (int i = 0; i < 8; i++)
            agg.TryEnqueue(Item(3, 300 + i));

        await WaitUntil(() => router.Released.Count == 8);
        Assert.Empty(router.Completed);
    }

    [Fact]
    public async Task Aggregator_PartitionQueueFull_RejectsRetryable()
    {
        RecordingExecutor exec = new();
        RecordingRouter router = new();
        PartitionWriteAggregator agg = Build(exec, router, new PartitionWriteAggregatorOptions
        {
            MaxBatchItems = 512,
            LingerMs = 0,                          // dispatch the first item immediately
            MaxQueuedItemsPerPartition = 2         // only two admitted per partition at once
        });

        exec.Gate(5); // first dispatched batch blocks, so its reservations are not released

        Assert.True(agg.TryEnqueue(Item(5, 500)));  // dispatched (in flight, gated)
        await WaitUntil(() => exec.Calls.Any(c => c.Partition == 5));
        Assert.True(agg.TryEnqueue(Item(5, 501)));  // queued behind the in-flight batch (reserved 2/2)
        Assert.False(agg.TryEnqueue(Item(5, 502))); // full → retryable rejection

        exec.Release(5);
        await WaitUntil(() => router.Completed.ContainsKey(500) && router.Completed.ContainsKey(501));
        Assert.True(agg.TryEnqueue(Item(5, 503))); // capacity freed after completion
    }

    // ── hardening ───────────────────────────────────────────────────────────────

    [Fact]
    public async Task Aggregator_StaleRangeItem_ReleasedIndividually_SiblingsCommit()
    {
        RecordingExecutor exec = new();
        RecordingRouter router = new();
        StubFence fence = new();
        fence.StaleKeys.Add("moved"); // this key's range moved since admission

        PartitionWriteAggregator agg = Build(exec, router, new PartitionWriteAggregatorOptions
        {
            MaxBatchItems = 3,   // count flush at 3
            LingerMs = 10_000
        }, fence);

        agg.TryEnqueue(Item(4, 1, key: "ok1"));
        agg.TryEnqueue(Item(4, 2, key: "moved"));
        agg.TryEnqueue(Item(4, 3, key: "ok3"));

        await WaitUntil(() => router.Completed.Count == 2);

        Assert.True(router.Released.ContainsKey(2));            // stale item released (retryable)
        Assert.True(router.Completed.ContainsKey(1) && router.Completed.ContainsKey(3));
        Assert.Single(exec.Calls);
        Assert.Equal(2, exec.Calls.First().Count);             // only the two valid siblings were proposed
    }

    [Fact]
    public async Task Aggregator_ItemBehindInflightBatch_ReleasedAtAgeDeadline()
    {
        // B2: an item admitted behind an in-flight batch must be released at its queue-age deadline even while
        // that batch is still in flight — not held until the batch completes. Deterministic via a manual clock.
        ManualTimeProvider time = new();
        RecordingExecutor exec = new();
        RecordingRouter router = new();
        exec.Gate(6); // hold partition 6's Raft call so its batch stays in flight across the age deadline

        PartitionWriteAggregator agg = Build(exec, router, new PartitionWriteAggregatorOptions
        {
            MaxBatchItems = 1,   // the first item dispatches immediately and blocks in the executor
            LingerMs = 0,
            MaxQueueDelayMs = 500,
            MaxQueuedItemsPerPartition = 100
        }, timeProvider: time);

        agg.TryEnqueue(Item(6, 60)); // dispatched immediately, in flight (gated)
        await WaitUntil(() => !exec.Calls.IsEmpty);
        agg.TryEnqueue(Item(6, 61)); // queued behind the in-flight batch

        // Advancing past the age deadline releases the follower while the batch is still gated.
        await AdvanceUntil(time, () => router.Released.ContainsKey(61), stepMs: 600);

        Assert.True(router.Released.ContainsKey(61));    // released at its age deadline
        Assert.False(router.Completed.ContainsKey(61));  // not completed
        Assert.False(router.Completed.ContainsKey(60));  // in-flight batch still gated, not completed

        exec.Release(6);
        await WaitUntil(() => router.Completed.ContainsKey(60)); // the in-flight batch still settles normally
    }

    [Fact]
    public async Task Aggregator_Stop_ReleasesPending_RejectsNew_InFlightStillSettles()
    {
        RecordingExecutor exec = new();
        RecordingRouter router = new();
        exec.Gate(8); // hold partition 8's Raft call so its batch stays in flight across the stop

        PartitionWriteAggregator agg = Build(exec, router, new PartitionWriteAggregatorOptions
        {
            MaxBatchItems = 1,
            LingerMs = 0,
            MaxQueuedItemsPerPartition = 100
        });

        agg.TryEnqueue(Item(8, 20)); // dispatched immediately, in flight (gated)
        await WaitUntil(() => !exec.Calls.IsEmpty);
        agg.TryEnqueue(Item(8, 21)); // queued behind the in-flight batch
        agg.TryEnqueue(Item(8, 22)); // queued

        agg.Stop();

        await WaitUntil(() => router.Released.ContainsKey(21) && router.Released.ContainsKey(22));
        Assert.False(agg.TryEnqueue(Item(8, 23))); // new admissions rejected after stop

        exec.Release(8);
        await WaitUntil(() => router.Completed.ContainsKey(20)); // the in-flight batch still reports its outcome
    }

    [Fact]
    public async Task Aggregator_FullQueueAllStale_MaxBatchOne_DrainsWithoutStackOverflow()
    {
        // B5: with MaxBatchItems = 1 and a full queue of stale items behind a batch, the all-released re-drive
        // must iterate, not recurse — one recursive frame per item would overflow the stack. Every item is
        // released exactly once and the lane stays alive.
        const int stale = 8000;

        StubFence fence = new();
        RecordingExecutor exec = new();
        RecordingRouter router = new();
        exec.Gate(4); // hold the first item's Raft call so the rest queue up behind it before the drain

        PartitionWriteAggregator agg = Build(exec, router, new PartitionWriteAggregatorOptions
        {
            MaxBatchItems = 1,
            LingerMs = 0,
            MaxQueuedItemsPerPartition = stale + 16
        }, fence);

        Assert.True(agg.TryEnqueue(Item(4, 0, key: "k0"))); // dispatched immediately, gated (not stale)
        await WaitUntil(() => !exec.Calls.IsEmpty);

        for (int i = 1; i <= stale; i++)
        {
            fence.StaleKeys.Add("k" + i);              // mark stale before it can be selected
            Assert.True(agg.TryEnqueue(Item(4, i, key: "k" + i))); // queued behind the in-flight batch
        }

        exec.Release(4); // completing the first batch re-drives the all-stale buffer behind it

        await WaitUntil(() => router.Released.Count == stale, timeoutMs: 30_000);

        Assert.Equal(stale, router.Released.Count);              // every stale item released exactly once
        Assert.Single(router.Completed);                         // only the one real write completed
        Assert.True(router.Completed.ContainsKey(0));            // ...and it is item 0, not a stale one
        Assert.Single(exec.Calls);                               // only the first item ever reached Raft
        await WaitUntil(() => agg.ReservedItems(4) == 0);        // all reservations released
    }

    // ── outcome classification (S1) ───────────────────────────────────────────

    [Fact]
    public void Classify_StatusMatrix_OnlyErroredAndInsufficientVotersPermanent()
    {
        foreach (RaftOperationStatus status in Enum.GetValues<RaftOperationStatus>())
        {
            bool permanent = status is RaftOperationStatus.Errored or RaftOperationStatus.InsufficientVoters;
            Assert.Equal(permanent, PartitionWriteAggregatorActor.IsPermanentStatus(status));

            // A returned failure is retryable iff not permanent; a thrown round trip is always retryable.
            Assert.Equal((false, !permanent), PartitionWriteAggregatorActor.ClassifyOutcome(false, status, threw: false));
            Assert.Equal((false, true), PartitionWriteAggregatorActor.ClassifyOutcome(false, status, threw: true));
        }

        // Success commits regardless of the (unused) status.
        Assert.Equal((true, false), PartitionWriteAggregatorActor.ClassifyOutcome(true, RaftOperationStatus.Success, threw: false));

        // The two statuses the pre-fix switch omitted are retryable.
        Assert.False(PartitionWriteAggregatorActor.IsPermanentStatus(RaftOperationStatus.ProposalNotFound));
        Assert.False(PartitionWriteAggregatorActor.IsPermanentStatus(RaftOperationStatus.OperationCancelled));
    }

    [Fact]
    public async Task Aggregator_ExecutorThrows_ReleasesRetryable()
    {
        // An exception around the auto-commit round trip does not prove the write did not commit → MustRetry,
        // never a terminal error.
        RecordingExecutor exec = new() { ThrowOnReplicate = true };
        RecordingRouter router = new();
        PartitionWriteAggregator agg = Build(exec, router, new PartitionWriteAggregatorOptions
        {
            MaxBatchItems = 4, LingerMs = 10_000, MaxQueuedItemsPerPartition = 100
        });

        for (int i = 0; i < 4; i++)
            agg.TryEnqueue(Item(3, 400 + i));

        await WaitUntil(() => router.Released.Count == 4);
        Assert.All(Enumerable.Range(400, 4), id => Assert.True(router.Released[id])); // transient
        Assert.Empty(router.Completed);
    }

    [Fact]
    public async Task Aggregator_RetryableStatus_ReleasesRetryable()
    {
        RecordingExecutor exec = new() { SucceedResult = false, ResultStatus = RaftOperationStatus.ProposalNotFound };
        RecordingRouter router = new();
        PartitionWriteAggregator agg = Build(exec, router, new PartitionWriteAggregatorOptions
        {
            MaxBatchItems = 3, LingerMs = 10_000, MaxQueuedItemsPerPartition = 100
        });

        for (int i = 0; i < 3; i++)
            agg.TryEnqueue(Item(5, 410 + i));

        await WaitUntil(() => router.Released.Count == 3);
        Assert.All(Enumerable.Range(410, 3), id => Assert.True(router.Released[id])); // ProposalNotFound → MustRetry
    }

    [Fact]
    public async Task Aggregator_PermanentStatus_ReleasesNonRetryable()
    {
        RecordingExecutor exec = new() { SucceedResult = false, ResultStatus = RaftOperationStatus.Errored };
        RecordingRouter router = new();
        PartitionWriteAggregator agg = Build(exec, router, new PartitionWriteAggregatorOptions
        {
            MaxBatchItems = 3, LingerMs = 10_000, MaxQueuedItemsPerPartition = 100
        });

        for (int i = 0; i < 3; i++)
            agg.TryEnqueue(Item(7, 420 + i));

        await WaitUntil(() => router.Released.Count == 3);
        Assert.All(Enumerable.Range(420, 3), id => Assert.False(router.Released[id])); // Errored → terminal
    }

    [Fact]
    public async Task Aggregator_ConcurrentFailingPartitionsOneLane_AllReleased_LaneSurvives()
    {
        // S5: failure logging now runs on the single-threaded lane mailbox (OnBatchComplete), not the detached
        // concurrent RunBatch. Drive many partitions' batches to fail and settle concurrently on one lane;
        // every item must be released and the lane must stay consistent (still accept new work afterwards).
        const int partitions = 8;

        RecordingExecutor exec = new() { SucceedResult = false, ResultStatus = RaftOperationStatus.Errored };
        RecordingRouter router = new();
        PartitionWriteAggregator agg = Build(exec, router, new PartitionWriteAggregatorOptions
        {
            MaxBatchItems = 1,
            LingerMs = 0,
            MaxQueuedItemsPerPartition = 100,
            LaneCount = 1 // all partitions share one lane, so their completions interleave on one mailbox
        });

        for (int p = 0; p < partitions; p++)
            exec.Gate(p);

        for (int p = 0; p < partitions; p++)
            agg.TryEnqueue(Item(p, p)); // dispatched immediately, blocked in the executor
        await WaitUntil(() => exec.Calls.Count == partitions);

        for (int p = 0; p < partitions; p++)
            exec.Release(p); // every partition's RunBatch settles concurrently onto the one lane

        await WaitUntil(() => router.Released.Count == partitions);
        Assert.All(Enumerable.Range(0, partitions), p => Assert.False(router.Released[p])); // Errored → terminal
        Assert.Empty(router.Completed);

        // The lane is still consistent after the concurrent failure storm: a fresh succeeding write completes.
        exec.SucceedResult = true;
        exec.ResultStatus = RaftOperationStatus.Success;
        agg.TryEnqueue(Item(0, 9999));
        await WaitUntil(() => router.Completed.ContainsKey(9999));
    }

    // ── observable async drain (B4 / Task 8) ──────────────────────────────────

    [Fact]
    public async Task Aggregator_StopAsync_AwaitsInFlight_ReleasesPending_NoLeak()
    {
        // B4: an observable drain must release queued (not-yet-dispatched) writes AND await in-flight batches
        // settling before returning — so a node teardown that awaits it never strands an item or drops a
        // completion on a to-be-disposed lane.
        RecordingExecutor exec = new();
        RecordingRouter router = new();
        exec.Gate(8); // hold the in-flight batch so we can observe StopAsync waiting for it
        PartitionWriteAggregator agg = Build(exec, router, new PartitionWriteAggregatorOptions
        {
            MaxBatchItems = 1, LingerMs = 0, MaxQueuedItemsPerPartition = 100
        });

        agg.TryEnqueue(Item(8, 80)); // dispatched, in flight (gated)
        await WaitUntil(() => !exec.Calls.IsEmpty);
        agg.TryEnqueue(Item(8, 81)); // queued behind the in-flight batch
        agg.TryEnqueue(Item(8, 82)); // queued behind

        Task stop = agg.StopAsync(TimeSpan.FromSeconds(10));

        // Pending followers are released promptly, but the drain must NOT complete while a batch is in flight.
        await WaitUntil(() => router.Released.ContainsKey(81) && router.Released.ContainsKey(82));
        await Task.Delay(100);
        Assert.False(stop.IsCompleted); // still awaiting the gated in-flight batch

        exec.Release(8); // the in-flight batch settles → drain observes quiescence and returns
        await stop;

        Assert.True(router.Completed.ContainsKey(80));                // the in-flight write completed
        Assert.Equal(3, router.Completed.Count + router.Released.Count); // every item terminated exactly once
        Assert.Equal(0, agg.ReservedItems(8));                        // every reservation freed before return
    }

    // ── inbox admission (B1) ──────────────────────────────────────────────────

    [Fact]
    public async Task Aggregator_LaneInboxFull_RejectsRetryable_NoLeak()
    {
        // B1: when the lane inbox is saturated, TrySend returns false; TryEnqueue must release the reservation
        // and reject (MustRetry) rather than silently drop the write and leak its reservation/intent/promise.
        RecordingExecutor exec = new();
        RecordingRouter router = new();
        exec.Gate(2); // hold the partition's Raft call so dispatched batches stay in flight during the burst
        PartitionWriteAggregator agg = Build(exec, router, new PartitionWriteAggregatorOptions
        {
            MaxBatchItems = 1,
            LingerMs = 0,
            MaxQueuedItemsPerPartition = 100_000,
            MaxQueuedBytesPerPartition = 1L << 30,
            AggregatorInboxSize = 1, // tiny lane inbox → a synchronous burst saturates it
            LaneCount = 1
        });

        int admitted = 0, rejected = 0;
        for (int i = 0; i < 500; i++)
        {
            if (agg.TryEnqueue(Item(2, i)))
                admitted++;
            else
                rejected++;
        }

        Assert.True(rejected > 0, "expected some lane-inbox rejections under a synchronous burst");
        Assert.Equal(500, admitted + rejected);
        Assert.Equal(admitted, agg.ReservedItems(2)); // only admitted items hold a reservation; rejects leaked none

        // Release the gate and let everything settle: every admitted write completes exactly once, no leak.
        exec.Release(2);
        await WaitUntil(() => router.Completed.Count == admitted, timeoutMs: 30_000);
        await WaitUntil(() => agg.ReservedItems(2) == 0);
        Assert.Empty(router.Released);
    }

    // ── lifetime + oversize (S3, S2) ──────────────────────────────────────────

    [Fact]
    public async Task Aggregator_OversizedItem_AdmittedAloneIntoEmptyPartition_ThenCompletes()
    {
        // S2: a value larger than the whole per-partition byte cap must not be rejected forever. It is admitted
        // into an empty partition and dispatched alone; while its bytes occupy the partition, others are
        // rejected; after it drains, capacity is free again.
        RecordingExecutor exec = new();
        RecordingRouter router = new();
        exec.Gate(3);
        PartitionWriteAggregator agg = Build(exec, router, new PartitionWriteAggregatorOptions
        {
            MaxBatchItems = 8, LingerMs = 0, MaxQueuedItemsPerPartition = 100, MaxQueuedBytesPerPartition = 1024
        });

        Assert.True(agg.TryEnqueue(Item(3, 700, bytes: 4096))); // 4x the byte cap → admitted into empty partition
        await WaitUntil(() => !exec.Calls.IsEmpty);             // dispatched alone (gated)
        Assert.False(agg.TryEnqueue(Item(3, 701, bytes: 16)));  // partition's bytes already exceed the cap → rejected

        exec.Release(3);
        await WaitUntil(() => router.Completed.ContainsKey(700));
        await WaitUntil(() => agg.ReservedItems(3) == 0);
        Assert.True(agg.TryEnqueue(Item(3, 702, bytes: 16)));   // capacity freed after it drained
    }

    [Fact]
    public async Task Aggregator_RegistryPruned_AfterAllPartitionsDrain()
    {
        // S3: per-partition admission counters are pruned at zero, so split/churned partitions do not
        // accumulate for the node's lifetime.
        RecordingExecutor exec = new();
        RecordingRouter router = new();
        PartitionWriteAggregator agg = Build(exec, router, new PartitionWriteAggregatorOptions
        {
            MaxBatchItems = 1, LingerMs = 0, MaxQueuedItemsPerPartition = 100
        });

        for (int p = 0; p < 5; p++)
            agg.TryEnqueue(Item(p, p));

        await WaitUntil(() => router.Completed.Count == 5);
        await WaitUntil(() => agg.TrackedPartitionCount == 0); // counters pruned back to zero
    }

    [Fact]
    public void Metrics_GaugeMeter_DisposedOnStop_StopsPublishing()
    {
        // S3: the gauges live on an instance-owned meter disposed by Stop, so repeated node construction does
        // not accumulate gauges that keep dead admission registries reachable.
        RecordingExecutor exec = new();
        RecordingRouter router = new();
        PartitionWriteAggregator agg = Build(exec, router, new PartitionWriteAggregatorOptions());

        int observed = 0;
        using MeterListener listener = new();
        listener.InstrumentPublished = (inst, l) => { if (ReferenceEquals(inst.Meter, agg.GaugeMeter)) l.EnableMeasurementEvents(inst); };
        listener.SetMeasurementEventCallback<long>((inst, val, tags, s) => Interlocked.Increment(ref observed));
        listener.Start();

        listener.RecordObservableInstruments();
        Assert.True(observed > 0); // this aggregator's gauges publish while alive

        agg.Stop();
        Interlocked.Exchange(ref observed, 0);
        listener.RecordObservableInstruments();
        Assert.Equal(0, observed); // after Stop its meter is disposed → gauges no longer publish
    }

    // ── reservations + config ─────────────────────────────────────────────────

    [Fact]
    public async Task Aggregator_Reservations_ReturnToZero_AfterCompletion()
    {
        RecordingExecutor exec = new();
        RecordingRouter router = new();
        exec.Gate(7);
        PartitionWriteAggregator agg = Build(exec, router, new PartitionWriteAggregatorOptions
        {
            MaxBatchItems = 2, LingerMs = 0, MaxQueuedItemsPerPartition = 100
        });

        agg.TryEnqueue(Item(7, 1)); // dispatched, gated
        await WaitUntil(() => !exec.Calls.IsEmpty);
        agg.TryEnqueue(Item(7, 2)); // queued behind
        Assert.True(agg.ReservedItems(7) > 0);

        exec.Release(7);
        await WaitUntil(() => router.Completed.Count == 2);
        await WaitUntil(() => agg.ReservedItems(7) == 0); // capacity fully released after completion
    }

    [Fact]
    public async Task Aggregator_Reservations_ReturnToZero_AfterFailure()
    {
        RecordingExecutor exec = new() { SucceedResult = false, ResultStatus = RaftOperationStatus.Errored };
        RecordingRouter router = new();
        PartitionWriteAggregator agg = Build(exec, router, new PartitionWriteAggregatorOptions
        {
            MaxBatchItems = 4, LingerMs = 10_000, MaxQueuedItemsPerPartition = 100
        });

        for (int i = 0; i < 4; i++)
            agg.TryEnqueue(Item(9, 90 + i));

        await WaitUntil(() => router.Released.Count == 4);
        await WaitUntil(() => agg.ReservedItems(9) == 0); // capacity released even on failure
    }

    [Fact]
    public void Config_NormalizesInvalidValues_AndClampsBatchToQueue()
    {
        KahunaConfiguration c = new()
        {
            KeyValueWriteLingerMs = -5,                          // negative → 0
            KeyValueWriteMaxBatchItems = 999,                    // > queued 10 → clamped
            KeyValueWriteMaxBatchBytes = -1,                     // <=0 → default
            KeyValueWriteMaxQueuedItemsPerPartition = 10,
            KeyValueWriteMaxQueuedBytesPerPartition = 4096,
            KeyValueWriteMaxQueueDelayMs = 0                     // <=0 → default
        };

        ConfigurationValidator.Validate(c);

        Assert.Equal(0, c.KeyValueWriteLingerMs);
        Assert.True(c.KeyValueWriteMaxBatchBytes > 0);
        Assert.True(c.KeyValueWriteMaxQueueDelayMs > 0);
        Assert.True(c.KeyValueWriteMaxBatchItems <= c.KeyValueWriteMaxQueuedItemsPerPartition);
    }

    [Fact]
    public void Config_ClampsLingerToQueueDelay()
    {
        // Linger above the queue-age deadline would let an item pass its release deadline before its buffer's
        // linger timer ever fires; the validator clamps linger down so the wake chain always self-heals.
        KahunaConfiguration c = new() { KeyValueWriteLingerMs = 5_000, KeyValueWriteMaxQueueDelayMs = 1_000 };
        ConfigurationValidator.Validate(c);
        Assert.Equal(1_000, c.KeyValueWriteLingerMs);
        Assert.True(c.KeyValueWriteLingerMs <= c.KeyValueWriteMaxQueueDelayMs);
    }

    [Fact]
    public void Config_RejectsQueueDelayAboveLeaseHeadroom()
    {
        // Just over the headroom boundary (10s lease − 2s scheduling/round-trip) is rejected.
        Assert.Throws<KahunaServerException>(() => ConfigurationValidator.Validate(new KahunaConfiguration { KeyValueWriteMaxQueueDelayMs = 8_001 }));

        // At the boundary it is accepted (defaults for the rest are valid).
        KahunaConfiguration ok = new() { KeyValueWriteMaxQueueDelayMs = 8_000 };
        ConfigurationValidator.Validate(ok);
        Assert.Equal(8_000, ok.KeyValueWriteMaxQueueDelayMs);
    }
}
