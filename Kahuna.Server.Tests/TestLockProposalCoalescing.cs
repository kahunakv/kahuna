using System.Collections.Concurrent;
using System.Text;
using Kahuna;
using Kahuna.Server.KeyValues.Writes;
using Kahuna.Server.Replication;
using Kahuna.Shared.Locks;
using Kommander;
using Kommander.Data;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// End-to-end coverage of persistent lock mutations through the shared partition write scheduler, driven by
/// the real public entry points (LocateAndTryLock / LocateAndTryUnlock). A recording executor wraps the real
/// Raft batch executor so batch calls can be counted, gated, and forced to a status while the mutation still
/// commits and applies through real (in-memory) Raft.
/// </summary>
public sealed class TestLockProposalCoalescing
{
    private readonly ILoggerFactory loggerFactory;

    public TestLockProposalCoalescing(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper);
    }

    private sealed class RecordingRaftExecutor : IPartitionBatchExecutor
    {
        private readonly IPartitionBatchExecutor inner;
        private readonly ConcurrentDictionary<int, TaskCompletionSource> gates = new();
        private readonly ConcurrentDictionary<int, RaftOperationStatus> forced = new();

        /// <summary>One record per batch call: the partition, the entry count, and how many entries carried the
        /// lock log type.</summary>
        public readonly ConcurrentQueue<(int Partition, int Count, int LockEntries)> Calls = new();

        public RecordingRaftExecutor(IPartitionBatchExecutor inner) => this.inner = inner;

        public void GateAll() { for (int p = 0; p < 16; p++) gates[p] = new(TaskCreationOptions.RunContinuationsAsynchronously); }
        public void ReleaseAll() { foreach (int p in gates.Keys.ToArray()) if (gates.TryRemove(p, out TaskCompletionSource? g)) g.TrySetResult(); }
        public void ForceAll(RaftOperationStatus status) { for (int p = 0; p < 16; p++) forced[p] = status; }
        public void ClearForced() => forced.Clear();

        public async Task<RaftBatchReplicationResult> ReplicateAsync(int partitionId, IReadOnlyList<RaftProposalEntry> entries, CancellationToken cancellationToken)
        {
            int lockEntries = 0;
            for (int i = 0; i < entries.Count; i++)
                if (entries[i].Type == ReplicationTypes.Locks)
                    lockEntries++;

            Calls.Enqueue((partitionId, entries.Count, lockEntries));

            if (gates.TryGetValue(partitionId, out TaskCompletionSource? gate))
                await gate.Task.WaitAsync(cancellationToken);

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

        options.WriteBatchExecutorDecorator = real => recorder = new RecordingRaftExecutor(real);

        await using EmbeddedKahunaNode node = new(options, loggerFactory);
        await node.StartAsync(ct);
        await body(node, recorder!, ct);
    }

    private static EmbeddedKahunaOptions SinglePartitionMemoryNode(int batchItems = 512, int lingerMs = 1, int locksWorkers = 0) => new()
    {
        Storage = "memory",
        WalStorage = "memory",
        InitialPartitions = 1,
        LocksWorkers = locksWorkers,
        KeyValueWriteMaxBatchItems = batchItems,
        KeyValueWriteLingerMs = lingerMs,
        KeyValueWriteMaxQueuedItemsPerPartition = 8192
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

    private static int LockBatches(RecordingRaftExecutor recorder)
    {
        int n = 0;
        foreach ((int, int, int LockEntries) call in recorder.Calls)
            if (call.LockEntries > 0)
                n++;
        return n;
    }

    // ── 1. many concurrent persistent locks share one proposal ─────────────────

    [Fact]
    public async Task SixtyFourConcurrentPersistentLocks_OnePartition_ShareOneProposal()
    {
        // Count flush at exactly 64 with a linger long enough for every concurrent grant to be admitted first
        // (but below the queue-delay cap, so a batch never expires unsent): the 64 concurrent grants to one
        // partition coalesce into ONE batch call carrying 64 lock entries, not 64 Raft proposals.
        await WithRecorder(SinglePartitionMemoryNode(batchItems: 64, lingerMs: 500), async (node, recorder, ct) =>
        {
            byte[] owner = Encoding.UTF8.GetBytes("owner-a");

            Task<(LockResponseType, long)>[] grants = new Task<(LockResponseType, long)>[64];
            for (int i = 0; i < 64; i++)
                grants[i] = node.Kahuna.LocateAndTryLock($"coalesce/r{i}", owner, 30_000, LockDurability.Persistent, ct);

            (LockResponseType, long)[] results = await Task.WhenAll(grants);
            Assert.All(results, r => Assert.Equal(LockResponseType.Locked, r.Item1));
            Assert.All(results, r => Assert.Equal(0, r.Item2));

            Assert.Equal(1, LockBatches(recorder));
            (int, int Count, int LockEntries) batch = recorder.Calls.First(c => c.LockEntries > 0);
            Assert.Equal(64, batch.Count);
            Assert.Equal(64, batch.LockEntries);

            // The releases coalesce the same way.
            Task<LockResponseType>[] releases = new Task<LockResponseType>[64];
            for (int i = 0; i < 64; i++)
                releases[i] = node.Kahuna.LocateAndTryUnlock($"coalesce/r{i}", owner, LockDurability.Persistent, ct);

            LockResponseType[] released = await Task.WhenAll(releases);
            Assert.All(released, r => Assert.Equal(LockResponseType.Unlocked, r));
            Assert.Equal(2, LockBatches(recorder));

            // The committed effect is observable: a second owner is granted every resource with the next
            // fencing token, again through one shared proposal.
            byte[] other = Encoding.UTF8.GetBytes("owner-b");
            Task<(LockResponseType, long)>[] regrants = new Task<(LockResponseType, long)>[64];
            for (int i = 0; i < 64; i++)
                regrants[i] = node.Kahuna.LocateAndTryLock($"coalesce/r{i}", other, 30_000, LockDurability.Persistent, ct);

            (LockResponseType, long)[] regranted = await Task.WhenAll(regrants);
            Assert.All(regranted, r => Assert.Equal(LockResponseType.Locked, r.Item1));
            Assert.All(regranted, r => Assert.Equal(1, r.Item2));
            Assert.Equal(3, LockBatches(recorder));
        });
    }

    // ── 2. the collect sweep must not evict an entry whose grant is still in flight ──

    [Fact]
    public async Task CollectSweep_DuringInFlightFirstTouchGrants_EveryGrantStillCompletes()
    {
        // One persistent lock actor, so its collect sweep (every 500 handled requests) is reached by 600
        // first-touch grants whose proposals are all held at the executor gate. Every one of the 600 entries is
        // resident with an in-flight replication intent when the sweep runs; none may be evicted, or the
        // completion finds no entry and answers Errored for a grant that Raft did commit.
        await WithRecorder(SinglePartitionMemoryNode(locksWorkers: 1), async (node, recorder, ct) =>
        {
            byte[] owner = Encoding.UTF8.GetBytes("sweep-owner");

            recorder.GateAll();

            const int grantCount = 600;
            Task<(LockResponseType, long)>[] grants = new Task<(LockResponseType, long)>[grantCount];
            for (int i = 0; i < grantCount; i++)
                grants[i] = node.Kahuna.LocateAndTryLock($"sweep/r{i}", owner, 60_000, LockDurability.Persistent, ct);

            // The single actor handles requests in arrival order; a read enqueued after the grants completes
            // only once every grant was handled — i.e. once the sweep at request 500 has run.
            (LockResponseType probeType, _) = await node.Kahuna.LocateAndGetLock("sweep/probe", LockDurability.Persistent, ct);
            Assert.NotEqual(LockResponseType.Errored, probeType);

            Assert.True(await WaitUntil(() => LockBatches(recorder) >= 1));

            recorder.ReleaseAll();

            (LockResponseType, long)[] results = await Task.WhenAll(grants);

            int errored = results.Count(r => r.Item1 == LockResponseType.Errored);
            Assert.Equal(0, errored);
            Assert.All(results, r => Assert.Equal(LockResponseType.Locked, r.Item1));

            // Every grant is really held: the same owner re-acquires (idempotent) and a stranger is refused.
            byte[] stranger = Encoding.UTF8.GetBytes("stranger");
            for (int i = 0; i < grantCount; i += 97)
            {
                (LockResponseType again, _) = await node.Kahuna.LocateAndTryLock($"sweep/r{i}", owner, 60_000, LockDurability.Persistent, ct);
                Assert.Equal(LockResponseType.Locked, again);

                (LockResponseType refused, _) = await node.Kahuna.LocateAndTryLock($"sweep/r{i}", stranger, 60_000, LockDurability.Persistent, ct);
                Assert.Equal(LockResponseType.Busy, refused);
            }
        });
    }

    // ── 3. a failed batch releases the grant with the right retryability ───────────

    [Fact]
    public async Task FailedBatch_TransientStatusAnswersMustRetry_PermanentStatusAnswersErrored()
    {
        await WithRecorder(SinglePartitionMemoryNode(), async (node, recorder, ct) =>
        {
            byte[] owner = Encoding.UTF8.GetBytes("owner-a");

            recorder.ForceAll(RaftOperationStatus.NodeIsNotLeader);
            (LockResponseType transientType, _) = await node.Kahuna.LocateAndTryLock("failed/transient", owner, 30_000, LockDurability.Persistent, ct);
            Assert.Equal(LockResponseType.MustRetry, transientType);

            recorder.ForceAll(RaftOperationStatus.Errored);
            (LockResponseType permanentType, _) = await node.Kahuna.LocateAndTryLock("failed/permanent", owner, 30_000, LockDurability.Persistent, ct);
            Assert.Equal(LockResponseType.Errored, permanentType);

            // Neither failure left a stale resident entry or intent behind: both resources grant cleanly now.
            recorder.ClearForced();
            foreach (string resource in new[] { "failed/transient", "failed/permanent" })
            {
                (LockResponseType type, long fencingToken) = await node.Kahuna.LocateAndTryLock(resource, owner, 30_000, LockDurability.Persistent, ct);
                Assert.Equal(LockResponseType.Locked, type);
                Assert.Equal(0, fencingToken);
            }
        });
    }
}
