using System.Security.Cryptography;
using Google.Protobuf;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Server.Persistence.Pitr;
using Kahuna.Server.Replication;
using Kahuna.Server.Replication.Protos;
using Kahuna.Shared.Communication.Rest;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Data;
using Kommander.System;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kahuna.Server.Tests;

/// <summary>
/// A PITR bootstrap trusts the chain it seeds from as much as a restore does, and more: every manifest
/// is authenticated against the node's manifest key, and a chain stamped with another cluster's id is
/// refused, because the seeded checkpoints tell this cluster's leader the node already holds entries
/// that only the other cluster wrote. Every refusal must happen before the backend or the WAL changes.
/// The node-level cases drive <see cref="IKahuna.BootstrapFromPitrBackupAsync"/> on an embedded node, so
/// they prove the key and the cluster id are read from the node's own configuration and that a failure
/// reaches the caller as a typed <see cref="KahunaBackupException"/>.
/// </summary>
public sealed class TestPitrBootstrapChainTrust : IDisposable
{
    private static readonly ILogger<IRaft> Log = NullLogger<IRaft>.Instance;

    private readonly string _tempRoot =
        Path.Combine(Path.GetTempPath(), "kahuna_boottrust_" + Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        if (Directory.Exists(_tempRoot))
            try { Directory.Delete(_tempRoot, recursive: true); } catch { /* best-effort */ }
    }

    private static HLCTimestamp T(long ms) => new(0, ms, 0);
    private static DateTime NowUtc(long ms) => DateTime.UnixEpoch + TimeSpan.FromMilliseconds(ms);
    private static RaftPartitionRange Part(int id) => new() { PartitionId = id, State = RaftPartitionState.Active };

    private static RaftLog KvLog(long id, long timeMs, string key, string value, long revision)
    {
        KeyValueMessage msg = new()
        {
            Type = (int)KeyValueRequestType.TrySet,
            Key = key,
            Value = UnsafeByteOperations.UnsafeWrap(System.Text.Encoding.UTF8.GetBytes(value)),
            Revision = revision,
            LastModifiedPhysical = timeMs
        };
        return new RaftLog
        {
            Id = id, Term = 1, Type = RaftLogType.Committed, Time = new HLCTimestamp(0, timeMs, 0),
            LogType = ReplicationTypes.KeyValues, LogData = ReplicationSerializer.Serialize(msg),
        };
    }

    private static bool HasCheckpoint(IWAL wal, int partitionId) =>
        wal.ReadLogsRange(partitionId, 0).Any(l => l.Type == RaftLogType.CommittedCheckpoint);

    private string WriteKeyFile(string name)
    {
        Directory.CreateDirectory(_tempRoot);
        string path = Path.Combine(_tempRoot, name);
        File.WriteAllBytes(path, RandomNumberGenerator.GetBytes(32));
        return path;
    }

    /// <summary>
    /// Takes a full backup of one partition into <paramref name="backupDir"/>, which holds both the
    /// manifests and the artifacts (the layout a node's backup directory has), stamped with
    /// <paramref name="clusterId"/>. Returns the resolved chain, the catalog, and a destination backend
    /// pre-loaded with the full backup's image, as a restored data root would be.
    /// </summary>
    private static async Task<(IReadOnlyList<BackupManifest> Chain, BackupCatalog Catalog, MemoryPersistenceBackend Dst)>
        BuildChain(string backupDir, string? clusterId, long timeMs)
    {
        BackupCatalog catalog = new(BackupTestStores.Manifests(backupDir));
        InMemoryWAL wal = new(Log);
        wal.Write([(1, [KvLog(5, timeMs, "k", "v", 1)])]);

        BackupManifest full = await BackupDriver.RunFullAsync(
            wal, [Part(1)], new MemoryPersistenceBackend(), BackupTestStores.Artifacts(backupDir), catalog,
            identity: new BackupOwnerIdentity(clusterId, null, null, null, null, null));

        IReadOnlyList<BackupManifest> chain = await catalog.ResolveAndValidateAsync(full.BackupId);
        string checkpoint = Path.Combine(backupDir, full.BackupId.ToString("N"), "checkpoint");
        return (chain, catalog, MemoryPersistenceBackend.OpenCheckpoint(checkpoint));
    }

    private string BackupDir(string tag) => Path.Combine(_tempRoot, "backups_" + tag);

    // ── the checks in BootstrapHelper ───────────────────────────────────────────────────────────────

    [Fact]
    public async Task SignedChain_WithTheNodeKey_Seeds()
    {
        byte[] key = RandomNumberGenerator.GetBytes(32);
        string dir = BackupDir("signed");
        (IReadOnlyList<BackupManifest> chain, _, MemoryPersistenceBackend dst) = await BuildChain(dir, null, 200);
        foreach (BackupManifest m in chain)
            BackupManifestMac.Sign(m, key);

        InMemoryWAL wal = new(Log);
        await BootstrapHelper.BootstrapNodeAsync(chain, BackupTestStores.Artifacts(dir), T(200), dst, wal,
            TimeSpan.FromHours(1), NowUtc(300), manifestMacKey: key);

        Assert.True(HasCheckpoint(wal, 1));
    }

    [Fact]
    public async Task UnsignedChain_WhenTheNodeHasAKey_IsRefusedBeforeTheWalChanges()
    {
        string dir = BackupDir("unsigned");
        (IReadOnlyList<BackupManifest> chain, _, MemoryPersistenceBackend dst) = await BuildChain(dir, null, 200);

        InMemoryWAL wal = new(Log);
        await Assert.ThrowsAsync<BackupArtifactException>(() => BootstrapHelper.BootstrapNodeAsync(
            chain, BackupTestStores.Artifacts(dir), T(200), dst, wal, TimeSpan.FromHours(1), NowUtc(300),
            manifestMacKey: RandomNumberGenerator.GetBytes(32)));

        Assert.False(HasCheckpoint(wal, 1));
    }

    [Fact]
    public async Task ChainSignedWithAnotherKey_IsRefused()
    {
        string dir = BackupDir("otherkey");
        (IReadOnlyList<BackupManifest> chain, _, MemoryPersistenceBackend dst) = await BuildChain(dir, null, 200);
        foreach (BackupManifest m in chain)
            BackupManifestMac.Sign(m, RandomNumberGenerator.GetBytes(32));

        InMemoryWAL wal = new(Log);
        await Assert.ThrowsAsync<BackupArtifactException>(() => BootstrapHelper.BootstrapNodeAsync(
            chain, BackupTestStores.Artifacts(dir), T(200), dst, wal, TimeSpan.FromHours(1), NowUtc(300),
            manifestMacKey: RandomNumberGenerator.GetBytes(32)));

        Assert.False(HasCheckpoint(wal, 1));
    }

    [Fact]
    public async Task ChainOfAnotherCluster_IsRefusedBeforeTheWalChanges()
    {
        string dir = BackupDir("foreign");
        (IReadOnlyList<BackupManifest> chain, _, MemoryPersistenceBackend dst) = await BuildChain(dir, "cluster-a", 200);

        InMemoryWAL wal = new(Log);
        BackupChainException ex = await Assert.ThrowsAsync<BackupChainException>(() => BootstrapHelper.BootstrapNodeAsync(
            chain, BackupTestStores.Artifacts(dir), T(200), dst, wal, TimeSpan.FromHours(1), NowUtc(300),
            clusterId: "cluster-b"));

        Assert.Contains("cluster-a", ex.Message);
        Assert.False(HasCheckpoint(wal, 1));
    }

    [Fact]
    public async Task ChainOfTheSameCluster_Seeds()
    {
        string dir = BackupDir("same");
        (IReadOnlyList<BackupManifest> chain, _, MemoryPersistenceBackend dst) = await BuildChain(dir, "cluster-a", 200);

        InMemoryWAL wal = new(Log);
        await BootstrapHelper.BootstrapNodeAsync(chain, BackupTestStores.Artifacts(dir), T(200), dst, wal,
            TimeSpan.FromHours(1), NowUtc(300), clusterId: "cluster-a");

        Assert.True(HasCheckpoint(wal, 1));
    }

    [Fact]
    public async Task ChainWithoutAClusterId_IsAcceptedByANodeWithOne()
    {
        string dir = BackupDir("unstamped");
        (IReadOnlyList<BackupManifest> chain, _, MemoryPersistenceBackend dst) = await BuildChain(dir, null, 200);

        InMemoryWAL wal = new(Log);
        await BootstrapHelper.BootstrapNodeAsync(chain, BackupTestStores.Artifacts(dir), T(200), dst, wal,
            TimeSpan.FromHours(1), NowUtc(300), clusterId: "cluster-b");

        Assert.True(HasCheckpoint(wal, 1));
    }

    // ── the node entry point: configuration in, typed outcome out ───────────────────────────────────

    private static EmbeddedKahunaNode Node(string macKeyFile = "", string clusterId = "") => new(new EmbeddedKahunaOptions
    {
        NodeName = "boottrust-" + Guid.NewGuid().ToString("N")[..8],
        Storage = "memory",
        WalStorage = "memory",
        InitialPartitions = 1,
        BackupMacKeyFile = macKeyFile,
        BackupClusterId = clusterId,
    });

    private static long NowMs() => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

    [Fact]
    public async Task Node_ReadsTheKeyFromItsConfiguration_AndSeedsASignedChain()
    {
        string keyFile = WriteKeyFile("mac.key");
        string dir = BackupDir("node_signed");
        (IReadOnlyList<BackupManifest> chain, BackupCatalog catalog, _) = await BuildChain(dir, null, NowMs());
        foreach (BackupManifest m in chain)
        {
            BackupManifestMac.Sign(m, File.ReadAllBytes(keyFile));
            await catalog.PutAsync(m, TestContext.Current.CancellationToken);
        }

        await using EmbeddedKahunaNode node = Node(macKeyFile: keyFile);
        await node.Kahuna.BootstrapFromPitrBackupAsync(
            dir, chain[^1].BackupId, HLCTimestamp.Zero, node.Raft.WalAdapter, TimeSpan.FromHours(1), TimeSpan.FromMinutes(30));

        Assert.True(HasCheckpoint(node.Raft.WalAdapter, 1));
    }

    [Fact]
    public async Task Node_WithAKey_RefusesAnUnsignedChain_AsATypedOutcome()
    {
        string dir = BackupDir("node_unsigned");
        (IReadOnlyList<BackupManifest> chain, _, _) = await BuildChain(dir, null, NowMs());

        await using EmbeddedKahunaNode node = Node(macKeyFile: WriteKeyFile("mac2.key"));
        KahunaBackupException ex = await Assert.ThrowsAsync<KahunaBackupException>(() => node.Kahuna.BootstrapFromPitrBackupAsync(
            dir, chain[^1].BackupId, HLCTimestamp.Zero, node.Raft.WalAdapter, TimeSpan.FromHours(1), TimeSpan.FromMinutes(30)));

        Assert.Equal(KahunaBackupOutcome.CorruptArtifact, ex.Outcome);
        Assert.False(HasCheckpoint(node.Raft.WalAdapter, 1));
    }

    [Fact]
    public async Task Node_RefusesAChainOfAnotherCluster_AsATypedOutcome()
    {
        string dir = BackupDir("node_foreign");
        (IReadOnlyList<BackupManifest> chain, _, _) = await BuildChain(dir, "cluster-a", NowMs());

        await using EmbeddedKahunaNode node = Node(clusterId: "cluster-b");
        KahunaBackupException ex = await Assert.ThrowsAsync<KahunaBackupException>(() => node.Kahuna.BootstrapFromPitrBackupAsync(
            dir, chain[^1].BackupId, HLCTimestamp.Zero, node.Raft.WalAdapter, TimeSpan.FromHours(1), TimeSpan.FromMinutes(30)));

        Assert.Equal(KahunaBackupOutcome.CorruptChain, ex.Outcome);
        Assert.False(HasCheckpoint(node.Raft.WalAdapter, 1));
    }

    [Fact]
    public async Task Node_ReportsAnUnknownLeaf_AsATypedOutcome()
    {
        string dir = BackupDir("node_unknown");
        Directory.CreateDirectory(dir);

        await using EmbeddedKahunaNode node = Node();
        KahunaBackupException ex = await Assert.ThrowsAsync<KahunaBackupException>(() => node.Kahuna.BootstrapFromPitrBackupAsync(
            dir, Guid.NewGuid(), HLCTimestamp.Zero, node.Raft.WalAdapter, TimeSpan.FromHours(1), TimeSpan.FromMinutes(30)));

        Assert.Equal(KahunaBackupOutcome.CorruptChain, ex.Outcome);
    }
}
