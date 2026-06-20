
using System.Text;
using Google.Protobuf;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Handlers;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Server.Persistence.Pitr;
using Kahuna.Server.Replication;
using Kahuna.Server.Replication.Protos;
using Kahuna.Shared.KeyValue;
using Kahuna.Utils;
using Kommander;
using Kommander.Data;
using Kommander.System;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kahuna.Server.Tests;

/// <summary>
/// Tests for coordinated multi-partition backup at a single cluster-wide HLC T.
///
/// The cut is per-partition <c>Time ≤ T</c> on each WAL entry's HLC. Because a committed
/// transaction's mutations are stamped with each shard's *local* commit HLC (not one shared
/// commit timestamp — see <c>TryCommitMutationsHandler</c>), the cut is consistent only when the
/// coordinator picks T such that it does not fall inside any committed transaction's per-shard HLC
/// span. These tests pin both sides: a "safe" T (below or above a committed cross-shard transaction)
/// keeps it all-or-nothing, while a T chosen *inside* the span tears it — the documented
/// precondition the coordinator must honour.
/// </summary>
public sealed class TestPitrCoordinatedSnapshot : IDisposable
{
    private readonly string _tempRoot =
        Path.Combine(Path.GetTempPath(), "kahuna_coordsnap_" + Guid.NewGuid().ToString("N"));

    private static readonly ILogger<IRaft> Log = NullLogger<IRaft>.Instance;

    public void Dispose()
    {
        if (Directory.Exists(_tempRoot))
            Directory.Delete(_tempRoot, recursive: true);
    }

    // ── helpers ──────────────────────────────────────────────────────────────────────────

    private string ArtifactsDir(string name) =>
        Path.Combine(_tempRoot, "artifacts_" + name);

    private BackupCatalog NewCatalog(string name) =>
        new(new LocalDirectoryStorageTarget(Path.Combine(_tempRoot, "catalog_" + name)));

    private static RaftPartitionRange Part(int id) => new()
    {
        PartitionId = id,
        State = RaftPartitionState.Active
    };

    private static HLCTimestamp T(long ms) => new(0, ms, 0);

    private static RaftLog KvLog(long id, long timeMs, string key, string value, long revision,
        KeyValueRequestType type = KeyValueRequestType.TrySet)
    {
        KeyValueMessage msg = new()
        {
            Type = (int)type,
            Key = key,
            Value = UnsafeByteOperations.UnsafeWrap(Encoding.UTF8.GetBytes(value)),
            Revision = revision,
            LastModifiedPhysical = timeMs
        };
        return new RaftLog
        {
            Id = id,
            Type = RaftLogType.Committed,
            Time = T(timeMs),
            LogType = ReplicationTypes.KeyValues,
            LogData = ReplicationSerializer.Serialize(msg)
        };
    }

    private static RaftLog ProposedLog(long id, long timeMs, string key, string value, long revision)
    {
        KeyValueMessage msg = new()
        {
            Type = (int)KeyValueRequestType.TrySet,
            Key = key,
            Value = UnsafeByteOperations.UnsafeWrap(Encoding.UTF8.GetBytes(value)),
            Revision = revision,
            LastModifiedPhysical = timeMs
        };
        return new RaftLog
        {
            Id = id,
            Type = RaftLogType.Proposed,
            Time = T(timeMs),
            LogType = ReplicationTypes.KeyValues,
            LogData = ReplicationSerializer.Serialize(msg)
        };
    }

    private static InMemoryWAL BuildWal(params (int partition, RaftLog log)[] entries)
    {
        InMemoryWAL wal = new(Log);
        Dictionary<int, List<RaftLog>> byPartition = [];
        foreach ((int partition, RaftLog log) in entries)
        {
            if (!byPartition.TryGetValue(partition, out List<RaftLog>? list))
                byPartition[partition] = list = [];
            list.Add(log);
        }
        foreach ((int p, List<RaftLog> logs) in byPartition)
            wal.Write([(p, logs)]);
        return wal;
    }

    private static void FlushWalToBackend(InMemoryWAL wal, int partitionId, MemoryPersistenceBackend backend,
        HLCTimestamp? upToT = null)
    {
        List<RaftLog> logs = wal.ReadLogsRange(partitionId, 1);
        foreach (RaftLog log in logs)
        {
            if (log.Type != RaftLogType.Committed) continue;
            if (log.LogType != ReplicationTypes.KeyValues) continue;
            if (log.LogData is null) continue;
            if (upToT.HasValue && log.Time.CompareTo(upToT.Value) > 0) continue;

            KeyValueMessage msg = ReplicationSerializer.UnserializeKeyValueMessage(log.LogData);
            (KeyValueState state, byte[]? value) = KeyValueMessageDecoder.Decode(msg);
            if (state == KeyValueState.Undefined) continue;

            backend.StoreKeyValues([new PersistenceRequestItem(
                msg.Key, value, msg.Revision,
                msg.ExpireNode, msg.ExpirePhysical, msg.ExpireCounter,
                msg.LastUsedNode, msg.LastUsedPhysical, msg.LastUsedCounter,
                msg.LastModifiedNode, msg.LastModifiedPhysical, msg.LastModifiedCounter,
                (int)state)]);
        }
    }

    private static string? GetValue(MemoryPersistenceBackend backend, string key)
    {
        object? entry = backend.GetKeyValue(key);
        if (entry is null) return null;
        System.Reflection.PropertyInfo? prop = entry.GetType().GetProperty("Value");
        byte[]? bytes = prop?.GetValue(entry) as byte[];
        return bytes is null ? null : Encoding.UTF8.GetString(bytes);
    }

    // ── tests ────────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Two partitions with independent committed writes up to T.  Full backup at T = 250.
    /// Checks: per-partition ToIndex is capped at the last committed entry ≤ T, and
    /// ClusterSnapshotTime is recorded in the manifest.
    /// </summary>
    [Fact]
    public async Task FullBackup_SnapshotT_CapsToIndexAndStoresT()
    {
        HLCTimestamp snapshotT = T(250);

        // Partition 1: t=100, t=200, t=300 (post-T).
        // Partition 2: t=150, t=250, t=350 (post-T).
        InMemoryWAL wal = BuildWal(
            (1, KvLog(1, 100, "k1", "v1", 1)),
            (1, KvLog(2, 200, "k2", "v2", 1)),
            (1, KvLog(3, 300, "k3", "v3", 1)),
            (2, KvLog(1, 150, "p1", "u1", 1)),
            (2, KvLog(2, 250, "p2", "u2", 1)),
            (2, KvLog(3, 350, "p3", "u3", 1)));

        MemoryPersistenceBackend backend = new();
        BackupCatalog catalog = NewCatalog("cap_toidx");
        string artifacts = ArtifactsDir("cap_toidx");

        BackupManifest manifest = await BackupDriver.RunFullAsync(
            wal, [Part(1), Part(2)], backend, artifacts, catalog,
            flushBeforeCheckpoint: null, snapshotT: snapshotT);

        // Partition 1: last committed ≤ T(250) is index 2 (t=200).
        PartitionBackupRange p1 = manifest.PartitionRanges.Single(r => r.PartitionId == 1);
        Assert.Equal(2L, p1.ToIndex);
        Assert.Equal(T(200), p1.ToHlc);

        // Partition 2: last committed ≤ T(250) is index 2 (t=250).
        PartitionBackupRange p2 = manifest.PartitionRanges.Single(r => r.PartitionId == 2);
        Assert.Equal(2L, p2.ToIndex);
        Assert.Equal(T(250), p2.ToHlc);

        // Cluster snapshot timestamp is stored in the manifest.
        Assert.Equal(snapshotT, manifest.ClusterSnapshotTime);
    }

    /// <summary>
    /// Proposed (prepared, never committed before T) entries are absent from both shards after
    /// a backup+restore at T.  The absence is structural: <c>BackupDriver</c> skips
    /// <c>RaftLogType.Proposed</c> entries regardless of T.  This is not a safe-T guarantee —
    /// it is a consequence of the log-type filter.
    /// </summary>
    [Fact]
    public async Task CoordinatedRestore_CrossShardTransactionAbsentWhenCommittedAfterT()
    {
        // T = 250.  Transaction A (both shards, t=100) committed before T.
        // Transaction B (both shards): Proposed at t=200, commit delivered after T.
        HLCTimestamp snapshotT = T(250);

        InMemoryWAL wal = BuildWal(
            // Partition 1 — committed before T.
            (1, KvLog(1, 100, "txA_p1", "committed", 1)),
            // Partition 1 — 2PC prepare (Proposed), never committed at T.
            (1, ProposedLog(2, 200, "txB_p1", "pending", 1)),
            // Partition 2 — committed before T.
            (2, KvLog(1, 100, "txA_p2", "committed", 1)),
            // Partition 2 — 2PC prepare (Proposed), never committed at T.
            (2, ProposedLog(2, 200, "txB_p2", "pending", 1)));

        // Backend is seeded with only the committed-before-T entries (simulates flush at T).
        MemoryPersistenceBackend backend = new();
        FlushWalToBackend(wal, 1, backend, upToT: snapshotT);
        FlushWalToBackend(wal, 2, backend, upToT: snapshotT);

        BackupCatalog catalog = NewCatalog("cross_shard");
        string artifacts = ArtifactsDir("cross_shard");

        BackupManifest full = await BackupDriver.RunFullAsync(
            wal, [Part(1), Part(2)], backend, artifacts, catalog,
            flushBeforeCheckpoint: null, snapshotT: snapshotT);

        Assert.Equal(snapshotT, full.ClusterSnapshotTime);

        // Restore to T.
        string checkpointPath = Path.Combine(artifacts, full.BackupId.ToString("N"), "checkpoint");
        MemoryPersistenceBackend restored = MemoryPersistenceBackend.OpenCheckpoint(checkpointPath);

        IReadOnlyList<BackupManifest> chain = catalog.ResolveAndValidate(full.BackupId);
        RestoreResult result = RestoreEngine.Restore(chain, artifacts, snapshotT, restored);

        // Transaction A: both shards fully committed before T → present.
        Assert.Equal("committed", GetValue(restored, "txA_p1"));
        Assert.Equal("committed", GetValue(restored, "txA_p2"));

        // Transaction B: both shards only Proposed (never committed) before T → absent.
        // Absence is structural: BackupDriver skips RaftLogType.Proposed regardless of T.
        Assert.Null(GetValue(restored, "txB_p1"));
        Assert.Null(GetValue(restored, "txB_p2"));

        // No incremental entries applied — all state came from the checkpoint.
        Assert.Equal(0, result.EntriesApplied);
    }

    /// <summary>
    /// Incremental backup at snapshotT caps the WAL segment at T.  Entries committed after T
    /// are written to the WAL but must not appear in the segment file or in the restore.
    /// </summary>
    [Fact]
    public async Task IncrementalBackup_SnapshotT_ExcludesPostTEntries()
    {
        HLCTimestamp snapshotT = T(250);

        // Full backup covers indices 1–2 on partition 1.
        InMemoryWAL wal = BuildWal(
            (1, KvLog(1, 100, "k1", "v1", 1)),
            (1, KvLog(2, 200, "k2", "v2", 1)));

        MemoryPersistenceBackend backend = new();
        FlushWalToBackend(wal, 1, backend);

        BackupCatalog catalog = NewCatalog("inc_cap");
        string artifacts = ArtifactsDir("inc_cap");

        BackupManifest full = await BackupDriver.RunFullAsync(
            wal, [Part(1)], backend, artifacts, catalog);

        // Add entries: k3 at t=200 (≤ T=250), k4 at t=300 (> T=250), k5 at t=400 (> T=250).
        wal.Write([(1, [
            KvLog(3, 200, "k3", "v3", 1),
            KvLog(4, 300, "k4", "v4", 1),
            KvLog(5, 400, "k5", "v5", 1)
        ])]);

        BackupManifest inc = BackupDriver.RunIncremental(
            wal, [Part(1)], full.BackupId, artifacts, catalog, snapshotT);

        // Segment must stop before k4 (t=300 > T=250).
        PartitionBackupRange r = inc.PartitionRanges.Single(r => r.PartitionId == 1);
        Assert.Equal(3L, r.FromIndex);
        Assert.Equal(3L, r.ToIndex);    // only k3 (t=200) included
        Assert.Equal(T(200), r.ToHlc);
        Assert.Equal(snapshotT, inc.ClusterSnapshotTime);

        // Restore to T=250: k1+k2 from checkpoint, k3 from segment; k4, k5 absent.
        string checkpointPath = Path.Combine(artifacts, full.BackupId.ToString("N"), "checkpoint");
        MemoryPersistenceBackend restored = MemoryPersistenceBackend.OpenCheckpoint(checkpointPath);

        IReadOnlyList<BackupManifest> chain = catalog.ResolveAndValidate(inc.BackupId);
        RestoreResult result = RestoreEngine.Restore(chain, artifacts, snapshotT, restored);

        Assert.Equal("v1", GetValue(restored, "k1"));
        Assert.Equal("v2", GetValue(restored, "k2"));
        Assert.Equal("v3", GetValue(restored, "k3"));
        Assert.Null(GetValue(restored, "k4"));
        Assert.Null(GetValue(restored, "k5"));
        Assert.Equal(1, result.EntriesApplied);
    }

    /// <summary>
    /// When no partition has any committed entry at or before T, the full backup produces
    /// no partition ranges — the coordinator chose a T that predates all data.
    /// </summary>
    [Fact]
    public async Task FullBackup_SnapshotT_BeforeAnyData_EmptyRanges()
    {
        // All entries are at t=500, T=100 — nothing qualifies.
        InMemoryWAL wal = BuildWal(
            (1, KvLog(1, 500, "k1", "v1", 1)),
            (2, KvLog(1, 600, "k2", "v2", 1)));

        BackupCatalog catalog = NewCatalog("empty_snap");
        string artifacts = ArtifactsDir("empty_snap");

        BackupManifest manifest = await BackupDriver.RunFullAsync(
            wal, [Part(1), Part(2)], new MemoryPersistenceBackend(), artifacts, catalog,
            flushBeforeCheckpoint: null, snapshotT: T(100));

        Assert.Empty(manifest.PartitionRanges);
        Assert.Equal(T(100), manifest.ClusterSnapshotTime);
    }

    /// <summary>
    /// Multi-partition full + incremental chain at a coordinated T, followed by restore
    /// to that T: verifies the complete round-trip including the ClusterSnapshotTime stored
    /// in each manifest.
    /// </summary>
    [Fact]
    public async Task CoordinatedChain_FullThenIncremental_RoundTripToT()
    {
        HLCTimestamp snapshotT = T(300);

        // Partition 1: entries at t=100, t=200.  Partition 2: entries at t=150, t=250.
        InMemoryWAL wal = BuildWal(
            (1, KvLog(1, 100, "p1k1", "a", 1)),
            (1, KvLog(2, 200, "p1k2", "b", 1)),
            (2, KvLog(1, 150, "p2k1", "c", 1)),
            (2, KvLog(2, 250, "p2k2", "d", 1)));

        MemoryPersistenceBackend backend = new();
        FlushWalToBackend(wal, 1, backend, upToT: snapshotT);
        FlushWalToBackend(wal, 2, backend, upToT: snapshotT);

        BackupCatalog catalog = NewCatalog("chain_roundtrip");
        string artifacts = ArtifactsDir("chain_roundtrip");

        BackupManifest full = await BackupDriver.RunFullAsync(
            wal, [Part(1), Part(2)], backend, artifacts, catalog,
            flushBeforeCheckpoint: null, snapshotT: snapshotT);

        // Add post-full entries on both partitions (still ≤ T=300).
        wal.Write([(1, [KvLog(3, 280, "p1k3", "e", 1)])]);
        wal.Write([(2, [KvLog(3, 290, "p2k3", "f", 1)])]);
        // Add entries beyond T.
        wal.Write([(1, [KvLog(4, 400, "p1k4", "x", 1)])]);
        wal.Write([(2, [KvLog(4, 450, "p2k4", "y", 1)])]);

        BackupManifest inc = BackupDriver.RunIncremental(
            wal, [Part(1), Part(2)], full.BackupId, artifacts, catalog, snapshotT);

        Assert.Equal(snapshotT, full.ClusterSnapshotTime);
        Assert.Equal(snapshotT, inc.ClusterSnapshotTime);

        // Restore to T=300.
        string checkpointPath = Path.Combine(artifacts, full.BackupId.ToString("N"), "checkpoint");
        MemoryPersistenceBackend restored = MemoryPersistenceBackend.OpenCheckpoint(checkpointPath);

        IReadOnlyList<BackupManifest> chain = catalog.ResolveAndValidate(inc.BackupId);
        RestoreResult result = RestoreEngine.Restore(chain, artifacts, snapshotT, restored);

        // Pre-full entries: from checkpoint.
        Assert.Equal("a", GetValue(restored, "p1k1"));
        Assert.Equal("b", GetValue(restored, "p1k2"));
        Assert.Equal("c", GetValue(restored, "p2k1"));
        Assert.Equal("d", GetValue(restored, "p2k2"));

        // Incremental entries ≤ T: from segment.
        Assert.Equal("e", GetValue(restored, "p1k3"));
        Assert.Equal("f", GetValue(restored, "p2k3"));

        // Post-T entries: absent.
        Assert.Null(GetValue(restored, "p1k4"));
        Assert.Null(GetValue(restored, "p2k4"));

        Assert.Equal(2, result.EntriesApplied);  // p1k3 + p2k3
        Assert.Equal(2, result.PartitionsRestored);
    }

    // ── coordinated-cut consistency: the safe-T precondition ─────────────────────────────────
    //
    // One committed cross-shard transaction whose two mutations carry DIFFERENT per-shard commit
    // HLCs (the realistic case: each shard stamps its own local clock at commit, so the two ends
    // differ even for a single logical transaction). txC_p1 commits at t=240, txC_p2 at t=260.
    // The transaction's HLC "span" is [240, 260]. The coordinator must pick T outside that span.

    private const long TxCP1Hlc = 240;   // shard 1's local commit HLC for the cross-shard txn
    private const long TxCP2Hlc = 260;   // shard 2's local commit HLC for the same txn

    private InMemoryWAL BuildCrossShardCommittedTxnWal() => BuildWal(
        (1, KvLog(1, TxCP1Hlc, "txC_p1", "c1", 1)),
        (2, KvLog(1, TxCP2Hlc, "txC_p2", "c2", 1)));

    /// <summary>
    /// Safe T BELOW the committed transaction's span (T = 230 &lt; 240): neither shard's mutation
    /// is in the cut, so the transaction is fully absent — a consistent cut.
    /// </summary>
    [Fact]
    public async Task CoordinatedCut_SafeT_BelowCommittedTxn_ExcludesBothShards()
    {
        HLCTimestamp snapshotT = T(230);
        InMemoryWAL wal = BuildCrossShardCommittedTxnWal();

        MemoryPersistenceBackend backend = new();
        FlushWalToBackend(wal, 1, backend, upToT: snapshotT);
        FlushWalToBackend(wal, 2, backend, upToT: snapshotT);

        BackupCatalog catalog = NewCatalog("safe_below");
        string artifacts = ArtifactsDir("safe_below");

        BackupManifest full = await BackupDriver.RunFullAsync(
            wal, [Part(1), Part(2)], backend, artifacts, catalog,
            flushBeforeCheckpoint: null, snapshotT: snapshotT);

        string checkpointPath = Path.Combine(artifacts, full.BackupId.ToString("N"), "checkpoint");
        MemoryPersistenceBackend restored = MemoryPersistenceBackend.OpenCheckpoint(checkpointPath);
        RestoreEngine.Restore(catalog.ResolveAndValidate(full.BackupId), artifacts, snapshotT, restored);

        Assert.Null(GetValue(restored, "txC_p1"));
        Assert.Null(GetValue(restored, "txC_p2"));   // both absent → consistent
    }

    /// <summary>
    /// Safe T ABOVE the committed transaction's span (T = 270 &gt; 260): both shards' mutations are
    /// in the cut, so the transaction is fully present — a consistent cut.
    /// </summary>
    [Fact]
    public async Task CoordinatedCut_SafeT_AboveCommittedTxn_IncludesBothShards()
    {
        HLCTimestamp snapshotT = T(270);
        InMemoryWAL wal = BuildCrossShardCommittedTxnWal();

        MemoryPersistenceBackend backend = new();
        FlushWalToBackend(wal, 1, backend, upToT: snapshotT);
        FlushWalToBackend(wal, 2, backend, upToT: snapshotT);

        BackupCatalog catalog = NewCatalog("safe_above");
        string artifacts = ArtifactsDir("safe_above");

        BackupManifest full = await BackupDriver.RunFullAsync(
            wal, [Part(1), Part(2)], backend, artifacts, catalog,
            flushBeforeCheckpoint: null, snapshotT: snapshotT);

        string checkpointPath = Path.Combine(artifacts, full.BackupId.ToString("N"), "checkpoint");
        MemoryPersistenceBackend restored = MemoryPersistenceBackend.OpenCheckpoint(checkpointPath);
        RestoreEngine.Restore(catalog.ResolveAndValidate(full.BackupId), artifacts, snapshotT, restored);

        Assert.Equal("c1", GetValue(restored, "txC_p1"));
        Assert.Equal("c2", GetValue(restored, "txC_p2"));   // both present → consistent
    }

    /// <summary>
    /// HAZARD (documented, not a bug in the capping mechanism): a T chosen INSIDE the committed
    /// transaction's per-shard HLC span (T = 250, between 240 and 260) tears the transaction —
    /// shard 1's mutation is in the cut, shard 2's is not. This is exactly the half-applied outcome
    /// P1.8's capping cannot prevent on its own; the coordinator MUST avoid picking such a T. The
    /// test pins the tearing so the precondition is explicit rather than hidden.
    /// </summary>
    [Fact]
    public async Task CoordinatedCut_UnsafeMidTransactionT_TearsCommittedTxn()
    {
        HLCTimestamp snapshotT = T(250);   // inside [240, 260] — an unsafe choice
        InMemoryWAL wal = BuildCrossShardCommittedTxnWal();

        MemoryPersistenceBackend backend = new();
        FlushWalToBackend(wal, 1, backend, upToT: snapshotT);
        FlushWalToBackend(wal, 2, backend, upToT: snapshotT);

        BackupCatalog catalog = NewCatalog("unsafe_mid");
        string artifacts = ArtifactsDir("unsafe_mid");

        BackupManifest full = await BackupDriver.RunFullAsync(
            wal, [Part(1), Part(2)], backend, artifacts, catalog,
            flushBeforeCheckpoint: null, snapshotT: snapshotT);

        string checkpointPath = Path.Combine(artifacts, full.BackupId.ToString("N"), "checkpoint");
        MemoryPersistenceBackend restored = MemoryPersistenceBackend.OpenCheckpoint(checkpointPath);
        RestoreEngine.Restore(catalog.ResolveAndValidate(full.BackupId), artifacts, snapshotT, restored);

        // Torn: shard 1 (t=240 ≤ 250) present, shard 2 (t=260 > 250) absent. This documents the
        // hazard a safe-T choice must avoid — it is NOT the desired production outcome.
        Assert.Equal("c1", GetValue(restored, "txC_p1"));
        Assert.Null(GetValue(restored, "txC_p2"));
    }

    // ── SnapshotCoordinator / GetSafeTimestampHandler unit tests ─────────────────────────────

    /// <summary>
    /// Store is empty — no write intents anywhere.  The per-shard scanner returns Zero, and the
    /// coordinator falls back to the WAL's max committed HLC across all partitions.
    /// </summary>
    [Fact]
    public async Task SafeTimestamp_NoInFlight_FallsBackToWalMaxCommitted()
    {
        // WAL: partition 1 max committed at t=300, partition 2 at t=200.
        InMemoryWAL wal = BuildWal(
            (1, KvLog(1, 100, "k1", "v1", 1)),
            (1, KvLog(2, 300, "k2", "v2", 1)),
            (2, KvLog(1, 200, "p1", "u1", 1)));

        // Simulate quiesced cluster: delegate returns Zero.
        HLCTimestamp safeT = await SnapshotCoordinator.ComputeSafeSnapshotTimeAsync(
            () => Task.FromResult(HLCTimestamp.Zero),
            wal,
            [Part(1), Part(2)]);

        // Expected: max committed across both partitions = t=300 (partition 1, index 2).
        Assert.Equal(T(300), safeT);
    }

    /// <summary>
    /// Store has in-flight prepared write intents.  The coordinator applies a one-tick decrement
    /// to the minimum CommitTimestamp so that entries whose <c>Time == minInFlight</c> are
    /// excluded by the inclusive <c>≤ T</c> segment cap.
    /// </summary>
    [Fact]
    public async Task SafeTimestamp_WithInFlight_ReturnsPredecessorOfMinCommitTimestamp()
    {
        HLCTimestamp inFlight = T(400);

        HLCTimestamp safeT = await SnapshotCoordinator.ComputeSafeSnapshotTimeAsync(
            () => Task.FromResult(inFlight),
            new InMemoryWAL(Log),
            [Part(1)]);

        // Safe T must be strictly below the in-flight minimum.
        Assert.True(safeT.CompareTo(inFlight) < 0);
        Assert.Equal(SnapshotCoordinator.Predecessor(inFlight), safeT);
    }

    /// <summary>
    /// Per-shard scan: store contains two entries, one with a pending write intent at t=350 and
    /// one at t=200.  <see cref="GetSafeTimestampHandler.FindMinInFlightCommitTimestamp"/> must
    /// return T(200) — the minimum prepared CommitTimestamp.
    /// </summary>
    [Fact]
    public void FindMinInFlightCommitTimestamp_ReturnsMin()
    {
        BTree<string, KeyValueEntry> store = new(4);

        store.Insert("k1", new KeyValueEntry
        {
            WriteIntent = new KeyValueWriteIntent
            {
                CommitTimestamp = T(350),
                TransactionId = T(350),
                Expires = T(99999)
            }
        });
        store.Insert("k2", new KeyValueEntry
        {
            WriteIntent = new KeyValueWriteIntent
            {
                CommitTimestamp = T(200),
                TransactionId = T(200),
                Expires = T(99999)
            }
        });
        // k3 has no write intent — should not affect the result.
        store.Insert("k3", new KeyValueEntry());

        Dictionary<string, KeyValueWriteIntent> locksByPrefix = [];

        HLCTimestamp result = GetSafeTimestampHandler.FindMinInFlightCommitTimestamp(store, locksByPrefix);

        Assert.Equal(T(200), result);
    }

    /// <summary>
    /// Per-shard scan: both store and locksByPrefix contribute write intents.  The minimum across
    /// both sources is selected.
    /// </summary>
    [Fact]
    public void FindMinInFlightCommitTimestamp_IncludesLocksByPrefix()
    {
        BTree<string, KeyValueEntry> store = new(4);
        store.Insert("k1", new KeyValueEntry
        {
            WriteIntent = new KeyValueWriteIntent { CommitTimestamp = T(500) }
        });

        Dictionary<string, KeyValueWriteIntent> locksByPrefix = new()
        {
            ["pfx:"] = new KeyValueWriteIntent { CommitTimestamp = T(150) }
        };

        HLCTimestamp result = GetSafeTimestampHandler.FindMinInFlightCommitTimestamp(store, locksByPrefix);

        Assert.Equal(T(150), result);
    }

    /// <summary>
    /// Per-shard scan: no prepared intents anywhere (all CommitTimestamp == Zero or no intents).
    /// Must return Zero so the coordinator knows the shard is quiesced.
    /// </summary>
    [Fact]
    public void FindMinInFlightCommitTimestamp_EmptyStore_ReturnsZero()
    {
        BTree<string, KeyValueEntry> store = new(4);
        store.Insert("k1", new KeyValueEntry()); // no write intent
        store.Insert("k2", new KeyValueEntry
        {
            WriteIntent = new KeyValueWriteIntent { CommitTimestamp = HLCTimestamp.Zero }
        });

        Dictionary<string, KeyValueWriteIntent> locksByPrefix = [];

        HLCTimestamp result = GetSafeTimestampHandler.FindMinInFlightCommitTimestamp(store, locksByPrefix);

        Assert.Equal(HLCTimestamp.Zero, result);
    }

    /// <summary>
    /// <see cref="SnapshotCoordinator.Predecessor"/> must return the largest HLC strictly below
    /// the input across all three structural cases.
    /// </summary>
    [Fact]
    public void Predecessor_AllBranches()
    {
        // Case 1: C > 0 — decrement counter, keep same (N, L).
        HLCTimestamp ts1 = new(1, 500, 7);
        HLCTimestamp pre1 = SnapshotCoordinator.Predecessor(ts1);
        Assert.Equal(new HLCTimestamp(1, 500, 6), pre1);
        Assert.True(pre1.CompareTo(ts1) < 0);

        // Case 2: C == 0, L > 0 — wrap to previous millisecond with counter = uint.MaxValue.
        HLCTimestamp ts2 = new(0, 300, 0);
        HLCTimestamp pre2 = SnapshotCoordinator.Predecessor(ts2);
        Assert.Equal(new HLCTimestamp(0, 299, uint.MaxValue), pre2);
        Assert.True(pre2.CompareTo(ts2) < 0);

        // Case 3: already at Zero — returns Zero (can't go lower).
        Assert.Equal(HLCTimestamp.Zero, SnapshotCoordinator.Predecessor(HLCTimestamp.Zero));
    }

    /// <summary>
    /// End-to-end boundary correctness: in-flight 2PC prepare at CommitTimestamp = t=300.
    /// <c>SnapshotCoordinator</c> must return <c>Predecessor(T(300))</c> — strictly below the
    /// in-flight min — so that any WAL entry with <c>Time == T(300)</c> is excluded by the
    /// inclusive segment cap.  Committed entries at t=100 and t=200 remain in the backup.
    /// </summary>
    [Fact]
    public async Task SafeTimestamp_InFlight_CapBackupBelowPreparedCommit()
    {
        // WAL: committed writes at t=100 and t=200; in-flight (Proposed) at t=300.
        InMemoryWAL wal = BuildWal(
            (1, KvLog(1, 100, "settled1", "a", 1)),
            (1, KvLog(2, 200, "settled2", "b", 1)),
            (1, ProposedLog(3, 300, "pending1", "x", 1)));

        // Actor reports min in-flight CommitTimestamp = T(300).
        // Coordinator must return Predecessor(T(300)) so T < T(300) and the segment cap excludes
        // any committed entry landing at exactly T(300).
        HLCTimestamp inFlight = T(300);
        HLCTimestamp safeT = await SnapshotCoordinator.ComputeSafeSnapshotTimeAsync(
            () => Task.FromResult(inFlight),
            wal,
            [Part(1)]);

        Assert.True(safeT.CompareTo(inFlight) < 0, "safe T must be strictly below in-flight min");

        // Take a backup at safeT (= Predecessor(T(300))).
        MemoryPersistenceBackend backend = new();
        FlushWalToBackend(wal, 1, backend, upToT: safeT);
        BackupCatalog catalog = NewCatalog("infl_cap");
        string artifacts = ArtifactsDir("infl_cap");

        BackupManifest full = await BackupDriver.RunFullAsync(
            wal, [Part(1)], backend, artifacts, catalog,
            flushBeforeCheckpoint: null, snapshotT: safeT);

        // FindLastCommittedAtOrBefore at safeT: entries at t=100 and t=200 qualify (L < 300),
        // Proposed at t=300 is skipped by type filter. ToIndex == 2.
        PartitionBackupRange r = full.PartitionRanges.Single(r => r.PartitionId == 1);
        Assert.Equal(2L, r.ToIndex);

        string checkpointPath = Path.Combine(artifacts, full.BackupId.ToString("N"), "checkpoint");
        MemoryPersistenceBackend restored = MemoryPersistenceBackend.OpenCheckpoint(checkpointPath);
        RestoreEngine.Restore(catalog.ResolveAndValidate(full.BackupId), artifacts, safeT, restored);

        Assert.Equal("a", GetValue(restored, "settled1"));
        Assert.Equal("b", GetValue(restored, "settled2"));
        Assert.Null(GetValue(restored, "pending1"));
    }
}
