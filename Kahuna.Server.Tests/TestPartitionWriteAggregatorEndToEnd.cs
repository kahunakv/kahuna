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
[Collection("ClusterTests")]
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

        public async Task<RaftReplicationResult> ReplicateAsync(int partitionId, IReadOnlyList<byte[]> logs)
        {
            Calls.Enqueue((partitionId, logs.Count));

            if (gates.TryGetValue(partitionId, out TaskCompletionSource? gate))
                await gate.Task;

            // A forced status short-circuits real Raft (the write does not commit) — for failure/cleanup tests.
            if (forced.TryGetValue(partitionId, out RaftOperationStatus status))
                return new RaftReplicationResult(false, status, HLCTimestamp.Zero, 0);

            return await inner.ReplicateAsync(partitionId, logs);
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

    // 2PC separation (an interactive/multi-key transaction never merges into an auto-commit aggregator batch)
    // is guaranteed by construction and pinned by the direct-write completeness audit: the aggregator executor
    // is the only direct auto-commit ReplicationTypes.KeyValues route; the 2PC coordinator proposes through
    // KeyValuePhaseTwoActor / StageAndProposePartition with autoCommit:false. A single-statement script SET,
    // by contrast, is deliberately an auto-commit direct write and does traverse the aggregator (correctly).
}
