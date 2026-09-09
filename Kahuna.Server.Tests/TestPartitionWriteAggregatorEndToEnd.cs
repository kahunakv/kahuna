using System.Collections.Concurrent;
using System.Text;
using Kahuna;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Writes;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Data;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// End-to-end coverage of the partition write aggregator through the REAL public write entry points
/// (LocateAndTrySet/Delete/Extend/SetMany), not the aggregator internals. A recording executor wraps the real
/// Raft batch executor so batch calls can be counted, gated, and forced to a status while the write still
/// commits and applies through real (in-memory) Raft — proving operation counts, observable results, failure
/// cleanup, and that ephemeral/2PC traffic does not traverse the aggregator.
///
/// <para>Scenarios that require a multi-node cluster (leader change between enqueue and dispatch) or a
/// persistent restart are covered elsewhere: leader failover is client-cluster coverage; per-record restore is
/// pinned by the serializer characterization (TestDirectWriteSerialization) plus the existing restore/recovery
/// suites, since the aggregator emits byte-identical log records. Partition independence, admit-during-in-flight,
/// and the flush-time fence are asserted deterministically at the component level (TestPartitionWriteAggregator).</para>
/// </summary>
public sealed class TestPartitionWriteAggregatorEndToEnd
{
    private readonly ILoggerFactory loggerFactory;

    public TestPartitionWriteAggregatorEndToEnd(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper);
    }

    private sealed class RecordingRaftExecutor : IPartitionBatchExecutor
    {
        private readonly IPartitionBatchExecutor inner;
        private readonly ConcurrentDictionary<int, TaskCompletionSource> gates = new();
        private readonly ConcurrentDictionary<int, RaftOperationStatus> forced = new();

        public readonly ConcurrentQueue<(int Partition, int Count)> Calls = new();

        public RecordingRaftExecutor(IPartitionBatchExecutor inner) => this.inner = inner;

        public void Gate(int partition) => gates[partition] = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public void Release(int partition) { if (gates.TryRemove(partition, out TaskCompletionSource? g)) g.TrySetResult(); }
        public void ForceStatus(int partition, RaftOperationStatus status) => forced[partition] = status;
        public void ClearForced(int partition) => forced.TryRemove(partition, out _);

        public async Task<RaftBatchReplicationResult> ReplicateAsync(int partitionId, IReadOnlyList<RaftProposalEntry> entries, CancellationToken cancellationToken)
        {
            Calls.Enqueue((partitionId, entries.Count));

            if (gates.TryGetValue(partitionId, out TaskCompletionSource? gate))
                await gate.Task.WaitAsync(cancellationToken);

            // A forced status short-circuits real Raft (the write does not commit) — for failure/cleanup tests.
            // Every entry carries the forced failure status with LogIndex -1 (nothing appended).
            if (forced.TryGetValue(partitionId, out RaftOperationStatus status))
            {
                List<RaftEntryResult> failed = new(entries.Count);
                for (int i = 0; i < entries.Count; i++)
                    failed.Add(new RaftEntryResult(status, -1, HLCTimestamp.Zero));
                return new RaftBatchReplicationResult(false, status, HLCTimestamp.Zero, failed);
            }

            return await inner.ReplicateAsync(partitionId, entries, cancellationToken);
        }
    }

    private async Task WithRecorder(EmbeddedKahunaOptions options, Func<EmbeddedKahunaNode, RecordingRaftExecutor, CancellationToken, Task> body)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        RecordingRaftExecutor? recorder = null;

        // Per-node seam (not a process-wide static): this node wraps its own real executor, so a concurrent
        // test constructing another node is never accidentally gated by this one's recorder.
        options.WriteBatchExecutorDecorator = real => recorder = new RecordingRaftExecutor(real);

        await using EmbeddedKahunaNode node = new(options, loggerFactory);
        await node.StartAsync(ct);
        await body(node, recorder!, ct);
    }

    private static EmbeddedKahunaOptions MemoryNode(int batchItems = 512, int lingerMs = 50, int maxQueued = 8192) => new()
    {
        Storage = "memory",
        WalStorage = "memory",
        InitialPartitions = 4,
        KeyValueWriteMaxBatchItems = batchItems,
        KeyValueWriteLingerMs = lingerMs,
        KeyValueWriteMaxQueuedItemsPerPartition = maxQueued
    };

    private static async Task<bool> WaitUntil(Func<bool> predicate, int timeoutMs = 8000)
    {
        long deadline = Environment.TickCount64 + timeoutMs;
        while (Environment.TickCount64 < deadline)
        {
            if (predicate()) return true;
            await Task.Delay(10);
        }
        return predicate();
    }

    // ── 1. operation count through the real entry point ──────────────────────────

    [Fact]
    public async Task Sixty4ConcurrentWrites_SamePartition_ProduceOneBulkCall()
    {
        // Count flush at exactly 64 (linger long enough not to fire first): the 64 concurrent single writes to
        // one hash key-space coalesce into ONE aggregator Raft call, not 64.
        await WithRecorder(MemoryNode(batchItems: 64, lingerMs: 10_000), async (node, recorder, ct) =>
        {
            Task<(KeyValueResponseType, long, HLCTimestamp)>[] writes = new Task<(KeyValueResponseType, long, HLCTimestamp)>[64];
            for (int i = 0; i < 64; i++)
            {
                int idx = i;
                writes[i] = node.Kahuna.LocateAndTrySetKeyValue(
                    HLCTimestamp.Zero, $"opcount/k{idx}", Encoding.UTF8.GetBytes("v" + idx),
                    null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct);
            }

            (KeyValueResponseType, long, HLCTimestamp)[] results = await Task.WhenAll(writes);
            Assert.All(results, r => Assert.Equal(KeyValueResponseType.Set, r.Item1));

            Assert.Single(recorder.Calls);
            Assert.Equal(64, recorder.Calls.First().Count);

            // Every value is readable (real Raft committed + applied).
            for (int i = 0; i < 64; i++)
            {
                (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await node.Kahuna.LocateAndTryGetValue(
                    HLCTimestamp.Zero, $"opcount/k{i}", -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
                Assert.Equal(KeyValueResponseType.Get, type);
                Assert.Equal("v" + i, Encoding.UTF8.GetString(entry!.Value!));
            }
        });
    }

    // ── 2. mixed set / delete / extend burst: types + stored values ──────────────

    [Fact]
    public async Task MixedWriteBurst_ReturnsCorrectTypesAndStoresValues()
    {
        await WithRecorder(MemoryNode(lingerMs: 20), async (node, recorder, ct) =>
        {
            (KeyValueResponseType s1, _, _) = await node.Kahuna.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, "mix/keep", Encoding.UTF8.GetBytes("orig"), null, -1, KeyValueFlags.Set,
                (int)TimeSpan.FromMinutes(5).TotalMilliseconds, KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.Set, s1);

            (KeyValueResponseType s2, _, _) = await node.Kahuna.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, "mix/gone", Encoding.UTF8.GetBytes("x"), null, -1, KeyValueFlags.Set, 0,
                KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.Set, s2);

            // A conditional set (SetIfNotExists) on a fresh key succeeds; on an existing key does not.
            (KeyValueResponseType cond, _, _) = await node.Kahuna.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, "mix/fresh", Encoding.UTF8.GetBytes("new"), null, -1, KeyValueFlags.SetIfNotExists, 0,
                KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.Set, cond);

            (KeyValueResponseType ext, _, _) = await node.Kahuna.LocateAndTryExtendKeyValue(
                HLCTimestamp.Zero, "mix/keep", (int)TimeSpan.FromMinutes(10).TotalMilliseconds, KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.Extended, ext);

            (KeyValueResponseType del, _, _) = await node.Kahuna.LocateAndTryDeleteKeyValue(
                HLCTimestamp.Zero, "mix/gone", KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.Deleted, del);

            // Observable results: kept value present, deleted key gone, conditional key stored.
            (KeyValueResponseType g1, ReadOnlyKeyValueEntry? keep) = await node.Kahuna.LocateAndTryGetValue(
                HLCTimestamp.Zero, "mix/keep", -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.Get, g1);
            Assert.Equal("orig", Encoding.UTF8.GetString(keep!.Value!));

            (KeyValueResponseType g2, _) = await node.Kahuna.LocateAndTryGetValue(
                HLCTimestamp.Zero, "mix/gone", -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
            Assert.NotEqual(KeyValueResponseType.Get, g2);

            (KeyValueResponseType g3, ReadOnlyKeyValueEntry? fresh) = await node.Kahuna.LocateAndTryGetValue(
                HLCTimestamp.Zero, "mix/fresh", -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.Get, g3);
            Assert.Equal("new", Encoding.UTF8.GetString(fresh!.Value!));

            Assert.NotEmpty(recorder.Calls); // direct writes went through the aggregator
        });
    }

    // ── 6. forced transient status: released retryably, then a retry commits ──────

    [Fact]
    public async Task ForcedTransientStatus_ReleasesRetryable_ThenRetrySucceeds()
    {
        await WithRecorder(MemoryNode(lingerMs: 20), async (node, recorder, ct) =>
        {
            // Resolve the partition by observing a warm-up write, then force that partition transient.
            (KeyValueResponseType warm, _, _) = await node.Kahuna.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, "fail/warm", Encoding.UTF8.GetBytes("w"), null, -1, KeyValueFlags.Set, 0,
                KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.Set, warm);
            int partition = recorder.Calls.First().Partition;

            recorder.ForceStatus(partition, RaftOperationStatus.NodeIsNotLeader); // transient

            (KeyValueResponseType retry, _, _) = await node.Kahuna.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, "fail/k", Encoding.UTF8.GetBytes("v"), null, -1, KeyValueFlags.Set, 0,
                KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.MustRetry, retry); // released retryably, not a terminal error

            recorder.ClearForced(partition);

            (KeyValueResponseType ok, _, _) = await node.Kahuna.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, "fail/k", Encoding.UTF8.GetBytes("v2"), null, -1, KeyValueFlags.Set, 0,
                KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.Set, ok); // the key is writable again after the transient clears

            (_, ReadOnlyKeyValueEntry? entry) = await node.Kahuna.LocateAndTryGetValue(
                HLCTimestamp.Zero, "fail/k", -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
            Assert.Equal("v2", Encoding.UTF8.GetString(entry!.Value!));
        });
    }

    // ── post-completion hold through the real entry point ────────────────────────

    [Fact]
    public async Task PostCompletionHold_HoldsFollowerWrite_ThenFlushesAndCommits()
    {
        // The hold is plumbed from the embedded options to the aggregator: a sub-threshold write queued
        // behind an in-flight batch is not re-dispatched on that batch's completion but on the hold-end
        // wake — and still commits and reads back through real Raft.
        EmbeddedKahunaOptions options = MemoryNode(lingerMs: 0);
        options.KeyValueWritePostCompletionHoldMs = 1000;
        options.KeyValueWriteMaxQueueDelayMs = 8000; // the held write must not age out while gated/held

        await WithRecorder(options, async (node, recorder, ct) =>
        {
            // Warm-up resolves the partition of the "hold/" key-space so the gate targets the right one.
            (KeyValueResponseType warm, _, _) = await node.Kahuna.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, "hold/warm", Encoding.UTF8.GetBytes("w"), null, -1, KeyValueFlags.Set, 0,
                KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.Set, warm);
            int partition = recorder.Calls.First().Partition;
            int PartitionCalls() => recorder.Calls.Count(c => c.Partition == partition);
            int baseline = PartitionCalls();

            recorder.Gate(partition);
            Task<(KeyValueResponseType, long, HLCTimestamp)> first = node.Kahuna.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, "hold/k1", Encoding.UTF8.GetBytes("v1"), null, -1, KeyValueFlags.Set, 0,
                KeyValueDurability.Persistent, ct);
            Assert.True(await WaitUntil(() => PartitionCalls() == baseline + 1)); // dispatched, gated

            Task<(KeyValueResponseType, long, HLCTimestamp)> second = node.Kahuna.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, "hold/k2", Encoding.UTF8.GetBytes("v2"), null, -1, KeyValueFlags.Set, 0,
                KeyValueDurability.Persistent, ct);
            await Task.Delay(100, ct); // let the second submit reach the lane before the gate releases

            recorder.Release(partition);
            (KeyValueResponseType t1, _, _) = await first;
            Assert.Equal(KeyValueResponseType.Set, t1);

            // The follower write is held: no further Raft call right after the completion.
            await Task.Delay(200, ct);
            Assert.Equal(baseline + 1, PartitionCalls());

            // The hold elapses → the wake dispatches the held write, which commits and reads back.
            (KeyValueResponseType t2, _, _) = await second;
            Assert.Equal(KeyValueResponseType.Set, t2);
            Assert.Equal(baseline + 2, PartitionCalls());

            (KeyValueResponseType g, ReadOnlyKeyValueEntry? entry) = await node.Kahuna.LocateAndTryGetValue(
                HLCTimestamp.Zero, "hold/k2", -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.Get, g);
            Assert.Equal("v2", Encoding.UTF8.GetString(entry!.Value!));
        });
    }

    // ── 7. per-partition queue saturation → MustRetry, then writable ─────────────

    [Fact]
    public async Task PartitionQueueSaturation_ReturnsMustRetry_ThenWritableAfterDrain()
    {
        await WithRecorder(MemoryNode(batchItems: 1, lingerMs: 10_000, maxQueued: 4), async (node, recorder, ct) =>
        {
            // Warm-up resolves the partition, then gate it so the first dispatched batch blocks and the queue
            // fills to its 4-item bound.
            await node.Kahuna.LocateAndTrySetKeyValue(HLCTimestamp.Zero, "sat/warm", Encoding.UTF8.GetBytes("w"),
                null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct);
            int partition = recorder.Calls.First().Partition;
            recorder.Gate(partition);

            Task<(KeyValueResponseType, long, HLCTimestamp)>[] writes = new Task<(KeyValueResponseType, long, HLCTimestamp)>[16];
            for (int i = 0; i < 16; i++)
            {
                int idx = i;
                writes[i] = node.Kahuna.LocateAndTrySetKeyValue(HLCTimestamp.Zero, $"sat/k{idx}", Encoding.UTF8.GetBytes("v"),
                    null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct);
            }

            // The over-limit writes are rejected synchronously with MustRetry; wait for that, then release so the
            // gated/queued writes can drain (avoids awaiting a still-gated batch).
            bool sawRetry = await WaitUntil(() => writes.Any(t => t.IsCompletedSuccessfully && t.Result.Item1 == KeyValueResponseType.MustRetry));
            Assert.True(sawRetry);

            recorder.Release(partition);
            (KeyValueResponseType, long, HLCTimestamp)[] results = await Task.WhenAll(writes);
            Assert.Contains(results, r => r.Item1 == KeyValueResponseType.MustRetry);

            // After the queue drains, the key space is writable again.
            (KeyValueResponseType after, _, _) = await node.Kahuna.LocateAndTrySetKeyValue(HLCTimestamp.Zero, "sat/after",
                Encoding.UTF8.GetBytes("v"), null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.Set, after);
        });
    }

    // ── 11. ephemeral writes bypass the aggregator ───────────────────────────────

    [Fact]
    public async Task EphemeralWrite_DoesNotTraverseAggregator()
    {
        await WithRecorder(MemoryNode(lingerMs: 20), async (node, recorder, ct) =>
        {
            (KeyValueResponseType set, _, _) = await node.Kahuna.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, "eph/k", Encoding.UTF8.GetBytes("v"), null, -1, KeyValueFlags.Set, 0,
                KeyValueDurability.Ephemeral, ct);
            Assert.Equal(KeyValueResponseType.Set, set);

            // The ephemeral value is applied inline on the actor — no aggregator batch was dispatched for it.
            Assert.Empty(recorder.Calls);

            (KeyValueResponseType g, ReadOnlyKeyValueEntry? entry) = await node.Kahuna.LocateAndTryGetValue(
                HLCTimestamp.Zero, "eph/k", -1, HLCTimestamp.Zero, KeyValueDurability.Ephemeral, ct);
            Assert.Equal(KeyValueResponseType.Get, g);
            Assert.Equal("v", Encoding.UTF8.GetString(entry!.Value!));
        });
    }

    // ── pipelined dispatch (in-flight capacity > 1) through the real write path ──

    [Fact]
    public async Task PipelinedPartition_TwoBatchesOverlapInFlight_AllWritesCommitAndReadBack()
    {
        // With two in-flight batches allowed per partition, a gated executor must observe TWO concurrent
        // ReplicateAsync calls for one partition; releasing them drives both through real (in-memory) Raft —
        // proving the Raft layer accepts overlapped per-partition proposals — and every write lands readable.
        EmbeddedKahunaOptions options = MemoryNode(batchItems: 4, lingerMs: 25);
        options.KeyValueWriteMaxInFlightBatchesPerPartition = 2;
        options.KeyValueWriteMaxQueueDelayMs = 8000; // queued items must not age out while the gate is held

        await WithRecorder(options, async (node, recorder, ct) =>
        {
            // Warm-up resolves the partition (its own batch dispatches at the linger), then gate it.
            (KeyValueResponseType warm, _, _) = await node.Kahuna.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, "pipe/warm", Encoding.UTF8.GetBytes("w"), null, -1, KeyValueFlags.Set, 0,
                KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.Set, warm);
            int partition = recorder.Calls.First().Partition;
            int callsBefore = recorder.Calls.Count(c => c.Partition == partition);
            recorder.Gate(partition);

            Task<(KeyValueResponseType, long, HLCTimestamp)>[] writes = new Task<(KeyValueResponseType, long, HLCTimestamp)>[8];
            for (int i = 0; i < 8; i++)
            {
                int idx = i;
                writes[i] = node.Kahuna.LocateAndTrySetKeyValue(
                    HLCTimestamp.Zero, $"pipe/k{idx}", Encoding.UTF8.GetBytes("v" + idx),
                    null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct);
            }

            // Two batches enter the (gated) executor together: neither call can return while gated, so two
            // additional calls prove the overlap the one-in-flight rule used to forbid.
            Assert.True(await WaitUntil(() => recorder.Calls.Count(c => c.Partition == partition) >= callsBefore + 2));
            recorder.Release(partition);

            (KeyValueResponseType, long, HLCTimestamp)[] results = await Task.WhenAll(writes);
            Assert.All(results, r => Assert.Equal(KeyValueResponseType.Set, r.Item1));

            for (int i = 0; i < 8; i++)
            {
                (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await node.Kahuna.LocateAndTryGetValue(
                    HLCTimestamp.Zero, $"pipe/k{i}", -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
                Assert.Equal(KeyValueResponseType.Get, type);
                Assert.Equal("v" + i, Encoding.UTF8.GetString(entry!.Value!));
            }
        });
    }

    [Fact]
    public async Task SameKeyWrite_DefersWhileFirstInFlight_EvenWithPipelineCapacityFree()
    {
        // The pipelined aggregator must never carry two proposals for the SAME key in flight at once. The
        // guard is the key actor's replication intent: with capacity free (2 allowed, 1 used), a second write
        // to the same key is refused (MustRetry) instead of dispatched, and no second executor call appears.
        EmbeddedKahunaOptions options = MemoryNode(batchItems: 1, lingerMs: 0);
        options.KeyValueWriteMaxInFlightBatchesPerPartition = 2;

        await WithRecorder(options, async (node, recorder, ct) =>
        {
            (KeyValueResponseType warm, _, _) = await node.Kahuna.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, "dup/warm", Encoding.UTF8.GetBytes("w"), null, -1, KeyValueFlags.Set, 0,
                KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.Set, warm);
            int partition = recorder.Calls.First().Partition;
            recorder.Gate(partition);

            Task<(KeyValueResponseType, long, HLCTimestamp)> first = node.Kahuna.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, "dup/k", Encoding.UTF8.GetBytes("v1"), null, -1, KeyValueFlags.Set, 0,
                KeyValueDurability.Persistent, ct);

            int callsWithFirst = 0;
            Assert.True(await WaitUntil(() => (callsWithFirst = recorder.Calls.Count(c => c.Partition == partition)) >= 2));

            // The same-key write is refused while the first proposal's intent is live — never a second
            // in-flight proposal for the key, even though an in-flight slot is free.
            (KeyValueResponseType second, _, _) = await node.Kahuna.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, "dup/k", Encoding.UTF8.GetBytes("v2"), null, -1, KeyValueFlags.Set, 0,
                KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.MustRetry, second);
            Assert.Equal(callsWithFirst, recorder.Calls.Count(c => c.Partition == partition));

            recorder.Release(partition);
            (KeyValueResponseType firstResult, _, _) = await first;
            Assert.Equal(KeyValueResponseType.Set, firstResult);

            // After the intent clears, the retried write proceeds and its value wins.
            (KeyValueResponseType retried, _, _) = await node.Kahuna.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, "dup/k", Encoding.UTF8.GetBytes("v2"), null, -1, KeyValueFlags.Set, 0,
                KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.Set, retried);

            (_, ReadOnlyKeyValueEntry? entry) = await node.Kahuna.LocateAndTryGetValue(
                HLCTimestamp.Zero, "dup/k", -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
            Assert.Equal("v2", Encoding.UTF8.GetString(entry!.Value!));
        });
    }

    // 2PC separation (an interactive/multi-key transaction never merges into an auto-commit aggregator batch)
    // is guaranteed by construction and pinned by the direct-write completeness audit: the aggregator executor
    // is the only direct auto-commit ReplicationTypes.KeyValues route; the 2PC coordinator proposes through
    // KeyValuePhaseTwoActor / StageAndProposePartition with autoCommit:false. A single-statement script SET,
    // by contrast, is deliberately an auto-commit direct write and does traverse the aggregator (correctly).
}
