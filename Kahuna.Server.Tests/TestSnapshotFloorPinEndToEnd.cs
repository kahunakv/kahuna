
using System.Diagnostics.Metrics;
using Kahuna.Server.Communication.Internode;
using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Server.Persistence.Pitr;
using Kahuna.Server.Locks;
using Kahuna.Server.Locks.Data;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Communication.Memory;
using Kommander.Data;
using Kommander.Discovery;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Nixie;

namespace Kahuna.Server.Tests;

/// <summary>
/// End-to-end integration test verifying that the snapshot-floor boundary-only in-memory retention
/// keeps the floor-boundary revision pinned in the actor's in-memory archive.
///
/// <para>The unit tests in <see cref="TestSnapshotFloorBoundaryTrim"/> prove that
/// <c>RemoveExpiredRevisions</c> pins the floor-boundary revision in the in-memory
/// dict. This file proves that, in a running node, the pinned boundary is actually
/// served from memory — i.e. <c>GetKeyValueRevisionAtOrBefore</c> (the disk fallback)
/// is never called for the boundary revision while a hold is active.</para>
///
/// <para>The discriminating fixture: a counting backend records every call to
/// <c>GetKeyValueRevisionAtOrBefore</c>. If the boundary pin is broken (the boundary is trimmed
/// from memory), the snapshot read falls back to disk and the counter goes to 1.
/// If the pin is working, the boundary is found in memory and the counter stays at 0.</para>
/// </summary>
[Collection("ClusterTests")]
public sealed class TestSnapshotFloorPinEndToEnd
{
    private readonly ILogger<IRaft>    raftLogger   = NullLogger<IRaft>.Instance;
    private readonly ILogger<IKahuna>  kahunaLogger = NullLogger<IKahuna>.Instance;

    // ── counting backend ─────────────────────────────────────────────────────────

    private sealed class CountingBackend(IPersistenceBackend inner) : IPersistenceBackend, IDisposable
    {
        private int revisionAtOrBeforeCalls;

        public int RevisionAtOrBeforeCalls => revisionAtOrBeforeCalls;

        public bool StoreLocks(List<PersistenceRequestItem> items) => inner.StoreLocks(items);
        public bool StoreKeyValues(List<PersistenceRequestItem> items) => inner.StoreKeyValues(items);
        public LockEntry? GetLock(string resource) => inner.GetLock(resource);
        public KeyValueEntry? GetKeyValue(string keyName) => inner.GetKeyValue(keyName);
        public KeyValueEntry? GetKeyValueRevision(string keyName, long revision) => inner.GetKeyValueRevision(keyName, revision);
        public List<(string, ReadOnlyKeyValueEntry)> GetKeyValueByPrefix(string prefixKeyName) => inner.GetKeyValueByPrefix(prefixKeyName);
        public List<(string, ReadOnlyKeyValueEntry)> GetKeyValueByRange(string prefix, string? startKey, int limit) => inner.GetKeyValueByRange(prefix, startKey, limit);
        public bool PruneKeyValueRevisions(IReadOnlyCollection<string>? keys, int retentionCount, TimeSpan retentionAge, int batchSize, HLCTimestamp floorTimestamp, out RevisionPruneResult result) =>
            inner.PruneKeyValueRevisions(keys, retentionCount, retentionAge, batchSize, floorTimestamp, out result);
        public CheckpointResult CreateCheckpoint(string destinationPath, long appliedIndex, HLCTimestamp appliedTime) =>
            inner.CreateCheckpoint(destinationPath, appliedIndex, appliedTime);

        public KeyValueEntry? GetKeyValueRevisionAtOrBefore(string keyName, long maxRevision, HLCTimestamp readTimestamp)
        {
            Interlocked.Increment(ref revisionAtOrBeforeCalls);
            return inner.GetKeyValueRevisionAtOrBefore(keyName, maxRevision, readTimestamp);
        }

        public void Dispose()
        {
            if (inner is IDisposable d)
                d.Dispose();
        }
    }

    // ── node builder ─────────────────────────────────────────────────────────────

    private (RaftManager Raft, KahunaManager Kahuna, CountingBackend Backend) BuildSingleNode(int revisionRetention)
    {
        ActorSystem actorSystem = new(logger: raftLogger);
        EmbeddedRaftCommunication raftComm = new();

        RaftManager raft = new(
            new RaftConfiguration
            {
                NodeName          = "pin-e2e",
                NodeId            = 1,
                Host              = "localhost",
                Port              = 0,
                InitialPartitions = 1,
                StartElectionTimeout = 50,
                EndElectionTimeout   = 150,
                EnableQuiescence  = false
            },
            new StaticDiscovery(EmbeddedRaftCommunication.Witnesses),
            new InMemoryWAL(raftLogger),
            raftComm,
            new HybridLogicalClock(),
            raftLogger);

        KahunaConfiguration config = ConfigurationValidator.Validate(new()
        {
            HttpsCertificate        = "",
            HttpsCertificatePassword = "",
            LocksWorkers            = 1,
            KeyValueWorkers         = 1,
            BackgroundWriterWorkers = 1,
            Storage                 = "memory",
            StorageRevision         = Guid.NewGuid().ToString(),
            RevisionRetention       = revisionRetention,
            MaxEntriesPerActor      = 50_000,
            MaxBytesPerActor        = 256L * 1024 * 1024,
            CacheEntriesToRemove    = 1_000,
            CollectBatchMax         = 1_000,
            CacheEntryTtl           = TimeSpan.FromMinutes(5)
        });

        CountingBackend countingBackend = new(new MemoryPersistenceBackend());

        MemoryInterNodeCommmunication interNode = new();
        KahunaManager kahuna = new(actorSystem, raft, config, interNode, countingBackend, kahunaLogger);

        raft.OnLogRestored          += kahuna.OnLogRestored;
        raft.OnReplicationReceived  += kahuna.OnReplicationReceived;
        raft.OnReplicationError     += kahuna.OnReplicationError;
        raft.OnLeaderChanged        += kahuna.OnLeaderChanged;

        string localEndpoint = raft.GetLocalEndpoint();
        interNode.SetNodes(new() { { localEndpoint, kahuna } });

        TestClusterNodeRegistry.Register(raft, kahuna, actorSystem);

        return (raft, kahuna, countingBackend);
    }

    // ── test ─────────────────────────────────────────────────────────────────────

    /// <summary>
    /// With a live snapshot hold at T, writes that push the held revision below the
    /// retention window must not evict it from the in-memory revision archive.
    /// A point-read at T must be served entirely from memory — the disk fallback
    /// (<c>GetKeyValueRevisionAtOrBefore</c>) must not be called.
    /// </summary>
    [Fact]
    public async Task SnapshotHold_PinsBoundaryRevisionInMemory_NoDiskFallbackOnRead()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        (RaftManager raft, KahunaManager kahuna, CountingBackend backend) = BuildSingleNode(revisionRetention: 2);

        try
        {
            await raft.JoinCluster(ct);
            await raft.WaitForLeader(0, ct);
            await raft.WaitForLeader(1, ct);

            string key = "pin/e2e/" + Guid.NewGuid().ToString("N")[..8];

            // ── rev 1: the value that must remain readable at T1 ──────────────────────
            (KeyValueResponseType setType1, _, _) =
                await kahuna.LocateAndTrySetKeyValue(
                    HLCTimestamp.Zero, key,
                    "value-at-T1"u8.ToArray(), null, -1,
                    KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct);

            Assert.Equal(KeyValueResponseType.Set, setType1);

            // Read back to capture the committed LastModified timestamp for the hold.
            (KeyValueResponseType readType1, ReadOnlyKeyValueEntry? readEntry1) =
                await kahuna.LocateAndTryGetValue(HLCTimestamp.Zero, key, -1,
                    HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);

            Assert.Equal(KeyValueResponseType.Get, readType1);
            Assert.NotNull(readEntry1);

            HLCTimestamp t1 = readEntry1.LastModified;
            Assert.NotEqual(HLCTimestamp.Zero, t1);

            // ── acquire a floor hold at T1 ────────────────────────────────────────────
            (KeyValueResponseType holdType, string holdId, _) =
                await kahuna.LocateAndAcquireSnapshotHold("e2e-holder", t1, 60_000, ct);

            Assert.Equal(KeyValueResponseType.Set, holdType);
            Assert.NotEmpty(holdId);

            // Confirm the floor is now pinned at T1.
            (HLCTimestamp floor, int live) = await kahuna.GetSnapshotFloor(ct);
            Assert.Equal(t1, floor);
            Assert.Equal(1, live);

            // ── revs 2, 3, 4: push rev 1 below the retention window ───────────────────
            // With RevisionRetention=2, each write trims the in-memory archive.
            // The boundary-only retention must keep rev 1 (the floor-boundary) despite falling below the cutoff.
            for (int i = 2; i <= 4; i++)
            {
                (KeyValueResponseType setTypeN, _, _) =
                    await kahuna.LocateAndTrySetKeyValue(
                        HLCTimestamp.Zero, key,
                        System.Text.Encoding.UTF8.GetBytes($"value-{i}"), null, -1,
                        KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct);

                Assert.Equal(KeyValueResponseType.Set, setTypeN);
            }

            // ── snapshot read at T1 ───────────────────────────────────────────────────
            // The entry is resident (just written). entry.LastModified > T1 triggers the
            // snapshot path: TryGetRevisionAtOrBefore(T1) must find rev 1 in memory.
            int callsBefore = backend.RevisionAtOrBeforeCalls;

            (KeyValueResponseType getType, ReadOnlyKeyValueEntry? snap) =
                await kahuna.LocateAndTryGetValue(
                    HLCTimestamp.Zero, key, -1, t1, KeyValueDurability.Persistent, ct);

            int callsAfter = backend.RevisionAtOrBeforeCalls;

            // ── assertions ────────────────────────────────────────────────────────────
            Assert.Equal(KeyValueResponseType.Get, getType);
            Assert.NotNull(snap);
            Assert.Equal("value-at-T1", System.Text.Encoding.UTF8.GetString(snap.Value ?? []));

            // If the boundary pin is broken, the revision is trimmed and the read falls back
            // to GetKeyValueRevisionAtOrBefore — that fallback call would make this non-zero.
            Assert.Equal(0, callsAfter - callsBefore);
        }
        finally
        {
            await TestClusterNodeRegistry.DisposeAsync(raft, TestContext.Current.CancellationToken);
        }
    }

    /// <summary>
    /// Regression guard for the MissingProtectedVersion counter: a snapshot read for a key
    /// that was never written before the hold timestamp is a legitimate DoesNotExist — the
    /// counter must stay at 0. This would have false-fired with the original read-site check.
    /// </summary>
    [Fact]
    public async Task MissingProtectedVersion_KeyCreatedAfterHoldTimestamp_CounterStaysZero()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        (RaftManager raft, KahunaManager kahuna, CountingBackend backend) = BuildSingleNode(revisionRetention: 2);

        long counterTotal = 0;
        using MeterListener listener = new();
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Name == "kahuna.snapshot_floor.missing_protected_version_total")
                l.EnableMeasurementEvents(instrument);
        };
        listener.SetMeasurementEventCallback<long>((_, measurement, _, _) =>
            Interlocked.Add(ref counterTotal, measurement));
        listener.Start();

        try
        {
            await raft.JoinCluster(ct);
            await raft.WaitForLeader(0, ct);
            await raft.WaitForLeader(1, ct);

            // Capture a timestamp before the key is written and hold the floor at it.
            HLCTimestamp forkT = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());
            (KeyValueResponseType holdType, _, _) =
                await kahuna.LocateAndAcquireSnapshotHold("counter-test-holder", forkT, 60_000, ct);
            Assert.Equal(KeyValueResponseType.Set, holdType);

            // Write the key AFTER forkT — no revision at forkT will ever exist.
            string key = "counter/test/" + Guid.NewGuid().ToString("N")[..8];
            (KeyValueResponseType setType, _, _) =
                await kahuna.LocateAndTrySetKeyValue(
                    HLCTimestamp.Zero, key,
                    "after-fork"u8.ToArray(), null, -1,
                    KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.Set, setType);

            // Snapshot read at forkT: legitimately DoesNotExist — key not present at that time.
            (KeyValueResponseType getType, _) =
                await kahuna.LocateAndTryGetValue(
                    HLCTimestamp.Zero, key, -1, forkT, KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.DoesNotExist, getType);

            // Counter must stay 0 — this is a normal absent-at-snapshot result, not a violation.
            listener.RecordObservableInstruments();
            Assert.Equal(0, Interlocked.Read(ref counterTotal));
        }
        finally
        {
            await TestClusterNodeRegistry.DisposeAsync(raft, TestContext.Current.CancellationToken);
        }
    }

    /// <summary>
    /// Regression guard: a snapshot read that hits a deleted tombstone (key existed then was
    /// deleted) is a legitimate DoesNotExist — the MissingProtectedVersion counter must stay 0.
    /// </summary>
    [Fact]
    public async Task MissingProtectedVersion_TombstoneAtSnapshotTimestamp_CounterStaysZero()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        (RaftManager raft, KahunaManager kahuna, CountingBackend backend) = BuildSingleNode(revisionRetention: 2);

        long counterTotal = 0;
        using MeterListener listener = new();
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Name == "kahuna.snapshot_floor.missing_protected_version_total")
                l.EnableMeasurementEvents(instrument);
        };
        listener.SetMeasurementEventCallback<long>((_, measurement, _, _) =>
            Interlocked.Add(ref counterTotal, measurement));
        listener.Start();

        try
        {
            await raft.JoinCluster(ct);
            await raft.WaitForLeader(0, ct);
            await raft.WaitForLeader(1, ct);

            string key = "counter/tomb/" + Guid.NewGuid().ToString("N")[..8];

            // Write, then delete, to produce a tombstone.
            (KeyValueResponseType setType, _, _) =
                await kahuna.LocateAndTrySetKeyValue(
                    HLCTimestamp.Zero, key,
                    "v1"u8.ToArray(), null, -1,
                    KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.Set, setType);

            (KeyValueResponseType delType, _, _) =
                await kahuna.LocateAndTryDeleteKeyValue(
                    HLCTimestamp.Zero, key, KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.Deleted, delType);

            // Read the current entry to capture the tombstone's LastModified timestamp.
            // (TryGet on a deleted key still returns the entry so we can capture the ts.)
            HLCTimestamp deleteTs = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

            // Acquire a hold AFTER the delete — the hold's floor is past the tombstone.
            (KeyValueResponseType holdType, _, _) =
                await kahuna.LocateAndAcquireSnapshotHold("tomb-test-holder", deleteTs, 60_000, ct);
            Assert.Equal(KeyValueResponseType.Set, holdType);

            // Write more revisions to push the tombstone below the in-memory retention window,
            // forcing the snapshot read to fall back to the disk revision lookup.
            for (int i = 0; i < 4; i++)
            {
                await kahuna.LocateAndTrySetKeyValue(
                    HLCTimestamp.Zero, key,
                    System.Text.Encoding.UTF8.GetBytes($"v{i + 2}"), null, -1,
                    KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct);
            }

            // Snapshot read at deleteTs: the tombstone is the as-of revision — DoesNotExist.
            (KeyValueResponseType getType, _) =
                await kahuna.LocateAndTryGetValue(
                    HLCTimestamp.Zero, key, -1, deleteTs, KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.DoesNotExist, getType);

            // Counter must stay 0 — tombstone-at-snapshot is legitimate, not a floor violation.
            listener.RecordObservableInstruments();
            Assert.Equal(0, Interlocked.Read(ref counterTotal));
        }
        finally
        {
            await TestClusterNodeRegistry.DisposeAsync(raft, TestContext.Current.CancellationToken);
        }
    }
}
