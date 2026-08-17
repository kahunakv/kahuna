using System.Text;
using Kahuna;
using Kommander.Time;

using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.Locks.Data;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Shared.Locks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kahuna.Server.Tests;

/// <summary>
/// Fencing-token monotonicity across a whole-partition snapshot install — the seeding path replica
/// placement uses when a node re-gains a partition or falls below the WAL compaction floor. Every
/// lock mutation below the snapshot boundary reaches the node only through the install (the
/// replicators never see it), so the install must leave nothing node-local that predates it: a
/// lock actor's resident lease is trusted unconditionally by the grant path, and a stale one would
/// mint fencing tokens below the installed high-water mark on the next promotion — regressing and
/// reusing tokens already granted through other replicas.
/// </summary>
public sealed class TestLockSnapshotInstallCoherence
{
    private const int Partitions = 4;

    private readonly ILoggerFactory loggerFactory;

    public TestLockSnapshotInstallCoherence(ITestOutputHelper outputHelper) =>
        loggerFactory = TestLogFactory.Create(outputHelper);

    private static async Task<EmbeddedKahunaNode> StartNode(ILoggerFactory loggerFactory, CancellationToken ct)
    {
        EmbeddedKahunaNode node = new(new EmbeddedKahunaOptions
        {
            ReadIOThreads = 1,
            WriteIOThreads = 1,
            PartitionExecutorPoolSize = 1,
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = Partitions
        }, loggerFactory);
        await node.StartAsync(ct);
        return node;
    }

    /// <summary>
    /// Builds a snapshot stream for <paramref name="partitionId"/> whose only content is
    /// <paramref name="resource"/> at fencing token <paramref name="fencingToken"/>, released —
    /// the state a seeding leader would export after granting and releasing the lock many times
    /// while this node was away. Classification uses the same empty range map (pure hash routing)
    /// a fresh node runs with, so the export and the live node's import agree on ownership.
    /// </summary>
    private static async Task<byte[]> BuildSnapshotWithReleasedLock(
        int partitionId, string resource, long fencingToken, CancellationToken ct)
    {
        MemoryPersistenceBackend backend = new();
        RangeMap map = new([]);

        PartitionStateTransfer transfer = new(
            new PartitionDataEnumerator(backend, () => map, Partitions),
            backend,
            new CompletionReceiptStore(),
            new TransactionRecordStore(),
            new PreparedIntentStore(),
            () => map, Partitions,
            () => Task.CompletedTask,
            storagePath: null, "rev", NullLogger<IKahuna>.Instance);

        Assert.True(backend.StoreLocks([new PersistenceRequestItem(
            resource,
            null,
            fencingToken,
            expiresNode: 0, expiresPhysical: 0, expiresCounter: 0,
            lastUsedNode: 0, lastUsedPhysical: 1, lastUsedCounter: 0,
            lastModifiedNode: 0, lastModifiedPhysical: 1, lastModifiedCounter: 0,
            state: (int)LockState.Unlocked)]));

        await using Stream stream = await transfer.ExportPartitionState(partitionId, upToIndex: 1000, ct);
        using MemoryStream buffer = new();
        await stream.CopyToAsync(buffer, ct);
        return buffer.ToArray();
    }

    /// <summary>
    /// The node grants a lock (so its lock actor holds a resident lease), then receives a
    /// whole-partition snapshot install carrying a far newer fencing-token high-water mark for the
    /// same resource — the shape of a re-gained or re-seeded replica whose partition kept granting
    /// through other replicas while this node was away. The next grant must mint strictly above the
    /// installed mark; minting from the stale resident lease instead would re-issue tokens that
    /// other owners already hold.
    /// </summary>
    [Fact]
    public async Task NextGrantAfterSnapshotInstall_MintsAboveTheInstalledHighWaterMark()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);
        KahunaManager manager = (KahunaManager)node.Kahuna;

        const string resource = "seed0/lease";
        int partitionId = PartitionDataEnumerator.HashPartitionOfKeySpace("seed0", Partitions);

        // Grant with a lease short enough to expire before the re-acquisition below: an expired
        // resident lease is exactly what a stale re-promoted grant path would mint from.
        byte[] firstOwner = Encoding.UTF8.GetBytes("owner-before-install");
        (LockResponseType granted, long firstToken) = await node.Kahuna.LocateAndTryLock(
            resource, firstOwner, 1, LockDurability.Persistent, ct);
        Assert.Equal(LockResponseType.Locked, granted);
        await Task.Delay(100, ct);

        // While this node was "away", the partition granted up to token 300 elsewhere; the install
        // is the only channel that state arrives through.
        const long installedHighWaterMark = 300;
        Assert.True(installedHighWaterMark > firstToken);
        byte[] snapshot = await BuildSnapshotWithReleasedLock(partitionId, resource, installedHighWaterMark, ct);
        await manager.KeyValues.PartitionStateTransfer.ImportPartitionState(
            partitionId, new MemoryStream(snapshot), ct);

        byte[] secondOwner = Encoding.UTF8.GetBytes("owner-after-install");
        (LockResponseType regranted, long secondToken) = await node.Kahuna.LocateAndTryLock(
            resource, secondOwner, 10_000, LockDurability.Persistent, ct);

        Assert.Equal(LockResponseType.Locked, regranted);
        Assert.True(secondToken > installedHighWaterMark,
            $"grant after the snapshot install minted token {secondToken}, at or below the installed " +
            $"high-water mark {installedHighWaterMark} — the stale resident lease (token {firstToken}) " +
            "survived the install and regressed the fencing contract");
    }
}
