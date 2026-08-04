using Kahuna.Shared.Sequences;
using Kahuna.Server.Communication.Internode;
using Kommander;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// The failure scenarios block reservation has to survive. A sequence is owned by the leader of its
/// storage key's partition; requests arriving anywhere else are redirected there, and the owner serves
/// values out of a window it reserved by compare-and-swapping the durable high-water mark. The property
/// under test is always the same one: <b>values may be skipped, never repeated</b>.
///
/// <para>Covered here: callers on every node at once, ownership moving to a different node while a
/// window is half-drained, a node restarting on its own persistent storage mid-block, and an idempotent
/// reserve replayed through a node that never issued it.</para>
/// </summary>
public sealed class TestSequencerFailover : BaseCluster
{
    private readonly ILogger<IRaft> raftLogger;

    private readonly ILogger<IKahuna> kahunaLogger;

    private readonly ILoggerFactory loggerFactory;

    public TestSequencerFailover(ITestOutputHelper outputHelper)
    {
        loggerFactory = LoggerFactory.Create(b => b.AddXUnit(outputHelper).SetMinimumLevel(LogLevel.Warning));
        raftLogger = loggerFactory.CreateLogger<IRaft>();
        kahunaLogger = loggerFactory.CreateLogger<IKahuna>();
    }

    private const int DataPartitions = 2;

    /// <summary>
    /// Callers on all three nodes allocate against one owned sequence, then ownership moves to another
    /// node twice while a window is half-drained. The abandoned tail of each window is a gap; not one
    /// value may be issued twice across the whole run.
    /// </summary>
    [Fact]
    public async Task AllocationsStayUniqueAcrossALeadershipChange()
    {
        (IRaft raft1, IRaft raft2, IRaft raft3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3,
         MemoryInterNodeCommmunication transport) =
            await AssembleThreNodeClusterWithTransport("memory", DataPartitions, raftLogger, kahunaLogger, c => c.SequencerBlockSize = 8);

        IRaft[] rafts = [raft1, raft2, raft3];
        IKahuna[] nodes = [kahuna1, kahuna2, kahuna3];

        try
        {
            string name = "failover/" + Guid.NewGuid().ToString("N");

            Assert.Equal(SequenceResponseType.Success, await Create(kahuna1, name));

            HashSet<long> issued = [];

            // Drive traffic from all three nodes so the owner is mid-window when leadership moves, and
            // the two non-owners exercise the redirect.
            foreach (IKahuna node in nodes)
                await AllocateInto(issued, node, name, 5);

            await StepDownDataPartitions(rafts);

            // The previous owner may still hold its window; the new owner's first bump compare-and-swaps
            // above it, so the two windows cannot overlap.
            foreach (IKahuna node in nodes)
                await AllocateInto(issued, node, name, 20);

            // A second round of the same shape, to catch a stale window that only resurfaces once the
            // new owner has drained its own.
            await StepDownDataPartitions(rafts);

            foreach (IKahuna node in nodes)
                await AllocateInto(issued, node, name, 20);

            Assert.Equal(3 * (5 + 20 + 20), issued.Count);

            // Two of the three nodes never own the sequence, so their calls must have been redirected.
            // Without this the test could pass vacuously on a run where one node served everything.
            Assert.True(transport.SequenceForwardCallCount > 0,
                "expected sequence requests to be redirected to the owning node");
        }
        finally
        {
            await LeaveCluster(raft1, raft2, raft3);
        }
    }

    /// <summary>
    /// Single and multi-value reserves issued concurrently from all three nodes. Every value in every
    /// returned range must be unique cluster-wide, and each range must be internally contiguous.
    /// </summary>
    [Fact]
    public async Task ConcurrentAllocationsAcrossNodesAreUnique()
    {
        (IRaft raft1, IRaft raft2, IRaft raft3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3,
         MemoryInterNodeCommmunication transport) =
            await AssembleThreNodeClusterWithTransport("memory", DataPartitions, raftLogger, kahunaLogger, c => c.SequencerBlockSize = 4);

        try
        {
            string name = "hammer/" + Guid.NewGuid().ToString("N");

            Assert.Equal(SequenceResponseType.Success, await Create(kahuna1, name));

            IKahuna[] nodes = [kahuna1, kahuna2, kahuna3];

            List<Task<SequenceAllocation>> work = [];

            for (int i = 0; i < 60; i++)
            {
                IKahuna node = nodes[i % nodes.Length];
                int count = i % 4 == 0 ? 3 : 1;
                work.Add(Reserve(node, name, count));
            }

            SequenceAllocation[] allocations = await Task.WhenAll(work);

            HashSet<long> seen = [];

            foreach (SequenceAllocation allocation in allocations)
            {
                Assert.Equal(allocation.Start + allocation.Count - 1, allocation.End);

                for (long value = allocation.Start; value <= allocation.End; value++)
                    Assert.True(seen.Add(value), $"value {value} was issued more than once");
            }

            Assert.Equal(allocations.Sum(a => a.Count), seen.Count);

            Assert.True(transport.SequenceForwardCallCount > 0,
                "expected sequence requests to be redirected to the owning node");
        }
        finally
        {
            await LeaveCluster(raft1, raft2, raft3);
        }
    }

    /// <summary>
    /// An idempotent reserve is replayable on a node that never issued it, including after the partition
    /// that owns the record has changed leader: the allocation is written to the record before the
    /// caller is answered, so the replay is served from durable state rather than from memory.
    /// </summary>
    [Fact]
    public async Task IdempotentReserveReplaysAcrossNodesAndLeaderChange()
    {
        (IRaft raft1, IRaft raft2, IRaft raft3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster("memory", DataPartitions, raftLogger, kahunaLogger, c => c.SequencerBlockSize = 16);

        IRaft[] rafts = [raft1, raft2, raft3];

        try
        {
            string name = "idempotent/" + Guid.NewGuid().ToString("N");

            Assert.Equal(SequenceResponseType.Success, await Create(kahuna1, name));

            SequenceAllocation original = await ReserveKeyed(kahuna2, name, "charge-7");

            // A node that never saw the request replays it from the record.
            Assert.Equal(original, await ReserveKeyed(kahuna3, name, "charge-7"));

            await StepDownDataPartitions(rafts);

            Assert.Equal(original, await ReserveKeyed(kahuna1, name, "charge-7"));
            Assert.Equal(original, await ReserveKeyed(kahuna2, name, "charge-7"));
        }
        finally
        {
            await LeaveCluster(raft1, raft2, raft3);
        }
    }

    /// <summary>
    /// A node that restarts mid-block abandons whatever the block still held. The restarted node must
    /// resume above the reserved mark — a gap — and must never reissue a value the previous run could
    /// have handed out.
    /// </summary>
    [Fact]
    public async Task RestartResumesAboveTheReservedMarkLeavingAGap()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        string storagePath = CreateTempDir("kahuna-seq-store-");
        string walPath = CreateTempDir("kahuna-seq-wal-");

        try
        {
            string name = "restart/" + Guid.NewGuid().ToString("N");

            List<long> beforeRestart = [];

            await using (EmbeddedKahunaNode node = new(PersistentOptions(storagePath, walPath), loggerFactory))
            {
                await node.StartAsync(ct);

                Assert.Equal(SequenceResponseType.Success, await Create(node.Kahuna, name));

                for (int i = 0; i < 3; i++)
                    beforeRestart.Add((await Reserve(node.Kahuna, name, 1)).Start);
            }

            Assert.Equal([1L, 2L, 3L], beforeRestart);

            List<long> afterRestart = [];

            await using (EmbeddedKahunaNode node = new(PersistentOptions(storagePath, walPath), loggerFactory))
            {
                await node.StartAsync(ct);

                for (int i = 0; i < 3; i++)
                    afterRestart.Add((await Reserve(node.Kahuna, name, 1)).Start);
            }

            // The pre-restart run reserved a whole block of 100 and issued three of them. The rest of
            // that block is gone, not reissued.
            Assert.Equal([101L, 102L, 103L], afterRestart);
        }
        finally
        {
            TryDeleteDir(storagePath);
            TryDeleteDir(walPath);
        }
    }

    // ── harness ─────────────────────────────────────────────────────────────────────────────────

    private static EmbeddedKahunaOptions PersistentOptions(string storagePath, string walPath) => new()
    {
        InitialPartitions = 1,
        Storage = "sqlite",
        StoragePath = storagePath,
        // Stable, non-empty revisions so the reconstructed node reopens the SAME database and WAL files
        // (an empty revision defaults to a fresh GUID per construction).
        StorageRevision = "sequencer-restart",
        WalStorage = "sqlite",
        WalPath = walPath,
        WalRevision = "sequencer-restart-wal",
        WalSyncWrites = true,
        SequencerBlockSize = 100
    };

    private static async Task StepDownDataPartitions(IRaft[] rafts)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        for (int partition = 1; partition <= DataPartitions; partition++)
        {
            foreach (IRaft raft in rafts)
            {
                if (!await raft.AmILeader(partition, ct))
                    continue;

                await raft.StepDownAsync(partition, ct);
                break;
            }
        }

        for (int partition = 1; partition <= DataPartitions; partition++)
            await WaitForAnyLeader(partition, rafts);
    }

    private static async Task WaitForAnyLeader(int partition, IRaft[] rafts)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        while (true)
        {
            foreach (IRaft raft in rafts)
                if (await raft.AmILeader(partition, ct))
                    return;

            await Task.Delay(50, ct);
        }
    }

    private static async Task<SequenceResponseType> Create(IKahuna kahuna, string name)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        // A freshly assembled cluster can still be settling its partition leaders, which surfaces as the
        // retryable outcome rather than a failure.
        for (int attempt = 0; attempt < 60; attempt++)
        {
            (SequenceResponseType response, _) = await kahuna.LocateAndCreateSequence(
                name, 0, 1, null, SequenceDurability.Persistent, ct);

            if (response != SequenceResponseType.MustRetry)
                return response;

            await Task.Delay(Math.Min(10 * (attempt + 1), 100), ct);
        }

        return SequenceResponseType.MustRetry;
    }

    private static async Task AllocateInto(HashSet<long> issued, IKahuna kahuna, string name, int howMany)
    {
        for (int i = 0; i < howMany; i++)
        {
            SequenceAllocation allocation = await Reserve(kahuna, name, 1);

            Assert.True(issued.Add(allocation.Start), $"value {allocation.Start} was issued more than once");
        }
    }

    /// <summary>
    /// Allocates, absorbing the two outcomes a leadership transition produces before it settles.
    /// <c>MustRetry</c> guarantees the attempt consumed nothing durable, so retrying it is what a real
    /// client does.
    ///
    /// <para><c>NotFound</c> is also tolerated here, and deliberately so: for a short window after a
    /// step-down, a committed key-value entry reads back as absent on every node. That window is not
    /// specific to sequences — a plain committed <c>Set</c> followed by a step-down reproduces it with
    /// no sequencer involved — so retrying past it is the only way to test sequencer behaviour across a
    /// failover. The retry never masks a wrong value: the assertions on the values themselves stay
    /// exact.</para>
    /// </summary>
    private static async Task<SequenceAllocation> Reserve(IKahuna kahuna, string name, int count)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        for (int attempt = 0; attempt < 60; attempt++)
        {
            (SequenceResponseType response, SequenceAllocation allocation) = await kahuna.LocateAndReserveSequenceRange(
                name, count, null, SequenceDurability.Persistent, ct);

            if (response == SequenceResponseType.Success)
                return allocation;

            Assert.True(response is SequenceResponseType.MustRetry or SequenceResponseType.NotFound, $"unexpected {response} on attempt {attempt}");

            await Task.Delay(Math.Min(10 * (attempt + 1), 100), ct);
        }

        throw new TimeoutException($"sequence '{name}' never allocated");
    }

    /// <summary>Keyed counterpart of <see cref="Reserve"/>, tolerating the same transitional outcomes.</summary>
    private static async Task<SequenceAllocation> ReserveKeyed(IKahuna kahuna, string name, string idempotencyKey)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        for (int attempt = 0; attempt < 60; attempt++)
        {
            (SequenceResponseType response, SequenceAllocation allocation) = await kahuna.LocateAndNextSequenceValue(
                name, idempotencyKey, SequenceDurability.Persistent, ct);

            if (response == SequenceResponseType.Success)
                return allocation;

            Assert.True(response is SequenceResponseType.MustRetry or SequenceResponseType.NotFound, $"unexpected {response} on attempt {attempt}");

            await Task.Delay(Math.Min(10 * (attempt + 1), 100), ct);
        }

        throw new TimeoutException($"sequence '{name}' never allocated");
    }

    private static string CreateTempDir(string prefix)
    {
        string path = Path.Combine(Path.GetTempPath(), prefix + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(path);
        return path;
    }

    private static void TryDeleteDir(string path)
    {
        try
        {
            if (Directory.Exists(path))
                Directory.Delete(path, recursive: true);
        }
        catch
        {
            // Best-effort test cleanup.
        }
    }
}
