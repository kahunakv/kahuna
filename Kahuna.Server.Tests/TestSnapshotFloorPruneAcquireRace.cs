
using Kahuna.Server.Communication.Internode;
using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Server.Persistence.Pitr;
using Kahuna.Server.Locks.Data;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Communication.Memory;
using Kommander.Discovery;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Nixie;
using System.Collections.Concurrent;

namespace Kahuna.Server.Tests;

/// <summary>
/// Verifies that hold acquisition and revision pruning agree on a single order: a hold committed
/// before the prune floor is sampled is reflected in that floor, and a hold committed after the
/// sample (while the prune-delete window is open) causes acquisition to fail closed rather than
/// letting the delete remove a boundary the sample never saw.
///
/// <para><b>Sampled-before-delete.</b> The floor is sampled inside the prune scheduler task under
/// the store's commit lock (<c>SnapshotFloorStore.BeginPrune</c>). A hold acquired while the task
/// is queued, or during the pre-sample pause, is committed before the sample and therefore
/// observed — its boundary is protected.</para>
///
/// <para><b>Committed-during-delete.</b> A hold that commits after the floor sample sees the open
/// prune-delete window and returns <c>MustRetry</c>. The delete about to run was computed without
/// it, so a "successful" acquire would be a lie; failing closed lets the caller retry once the
/// window is clear (the committed hold then protects the timestamp for every subsequent prune).
/// This closes the former sample→delete residual window, for both targeted cleanup and full
/// sweep, which share the identical <c>BeginPrune</c>/<c>EndPrune</c> protocol.</para>
/// </summary>
public sealed class TestSnapshotFloorPruneAcquireRace : RaftTrackingTest
{
    private readonly ILogger<IRaft>   raftLogger   = NullLogger<IRaft>.Instance;
    private readonly ILogger<IKahuna> kahunaLogger = NullLogger<IKahuna>.Instance;

    // ── custom in-process pruning backend ────────────────────────────────────────────────

    /// <summary>
    /// In-memory backend that actually prunes old revisions. Necessary because
    /// <c>MemoryPersistenceBackend.PruneKeyValueRevisions</c> is a no-op.
    /// </summary>
    private sealed class PruningBackend : IPersistenceBackend
    {
        private readonly ConcurrentDictionary<string, KeyValueEntry> current = new();
        private readonly ConcurrentDictionary<string, ConcurrentDictionary<long, KeyValueEntry>> revisions = new();

        public bool StoreLocks(List<PersistenceRequestItem> _) => true;
        public LockEntry? GetLock(string _) => null;
        public List<(string, ReadOnlyKeyValueEntry)> GetKeyValueByPrefix(string _) => [];
        public List<(string, ReadOnlyKeyValueEntry)> GetKeyValueByRange(string _, string? __, int ___) => [];
        public CheckpointResult CreateCheckpoint(string d, long _, HLCTimestamp __) => new(d, null!);

        public bool StoreKeyValues(List<PersistenceRequestItem> items)
        {
            foreach (PersistenceRequestItem item in items)
            {
                KeyValueEntry e = new()
                {
                    Value        = item.Value,
                    Revision     = item.Revision,
                    Expires      = new(item.ExpiresNode, item.ExpiresPhysical, item.ExpiresCounter),
                    LastUsed     = new(item.LastUsedNode, item.LastUsedPhysical, item.LastUsedCounter),
                    LastModified = new(item.LastModifiedNode, item.LastModifiedPhysical, item.LastModifiedCounter),
                    State        = (KeyValueState)item.State
                };
                current[item.Key] = e;
                if (!item.NoRevision)
                    revisions.GetOrAdd(item.Key, _ => new())[item.Revision] = e;
            }
            return true;
        }

        public KeyValueEntry? GetKeyValue(string key) => current.GetValueOrDefault(key);

        public KeyValueEntry? GetKeyValueRevision(string key, long revision) =>
            revisions.TryGetValue(key, out ConcurrentDictionary<long, KeyValueEntry>? r) &&
            r.TryGetValue(revision, out KeyValueEntry? e) ? e : null;

        public KeyValueEntry? GetKeyValueRevisionAtOrBefore(string key, long maxRevision, HLCTimestamp ts)
        {
            if (!revisions.TryGetValue(key, out ConcurrentDictionary<long, KeyValueEntry>? r))
                return null;
            KeyValueEntry? best = null;
            foreach (KeyValueEntry e in r.Values)
                if (e.Revision <= maxRevision && e.LastModified.CompareTo(ts) <= 0 &&
                    (best is null || e.Revision > best.Revision))
                    best = e;
            return best;
        }

        public bool PruneKeyValueRevisions(
            IReadOnlyCollection<string>? keys,
            int retentionCount,
            TimeSpan retentionAge,
            int batchSize,
            HLCTimestamp floorTimestamp,
            out RevisionPruneResult result)
        {
            int deleted = 0;
            IEnumerable<string> targets = keys ?? (IEnumerable<string>)revisions.Keys;

            foreach (string key in targets)
            {
                if (!revisions.TryGetValue(key, out ConcurrentDictionary<long, KeyValueEntry>? r))
                    continue;

                long? currentRev = current.TryGetValue(key, out KeyValueEntry? cur) ? cur.Revision : null;

                // Determine floor revision: the highest revision whose LastModified ≤ floorTimestamp.
                long floorRevision = -1;
                if (floorTimestamp != HLCTimestamp.Zero)
                {
                    foreach (KeyValueEntry e in r.Values)
                        if (e.LastModified.CompareTo(floorTimestamp) <= 0 && e.Revision > floorRevision)
                            floorRevision = e.Revision;
                }

                // Keep the newest retentionCount revisions.
                List<long> sorted = r.Keys.OrderByDescending(x => x).ToList();
                HashSet<long> keep = retentionCount > 0
                    ? new HashSet<long>(sorted.Take(retentionCount))
                    : new HashSet<long>();

                foreach (long rev in sorted)
                {
                    if (rev == currentRev)                          continue; // never delete current
                    if (keep.Contains(rev))                         continue; // within retention window
                    if (floorRevision >= 0 && rev >= floorRevision) continue; // floor-protected
                    r.TryRemove(rev, out _);
                    deleted++;
                }
            }

            result = new(deleted, keys?.Count ?? revisions.Count, BatchLimitReached: false);
            return true;
        }
    }

    // ── node builder ──────────────────────────────────────────────────────────────────────

    private (RaftManager Raft, KahunaManager Kahuna, PruningBackend Backend)
        BuildNode(
            int retentionCount = 1,
            bool cleanupOnWrite = true,
            TimeSpan? cleanupInterval = null,
            int dirtyObjectsWriterDelay = 0)
    {
        ActorSystem actorSystem = new(logger: raftLogger);
        EmbeddedRaftCommunication raftComm = new();

        RaftManager raft = new(
            new RaftConfiguration
            {
                NodeName             = "r2race",
                NodeId               = 1,
                Host                 = "localhost",
                Port                 = 0,
                InitialPartitions    = 1,
                HeartbeatInterval = TimeSpan.FromMilliseconds(10),
                CheckLeaderInterval = TimeSpan.FromMilliseconds(25),
                StartElectionTimeout = 50,
                EndElectionTimeout   = 150,
                EnableQuiescence = false, PartitionExecutorPoolSize = 1
            },
            new StaticDiscovery(EmbeddedRaftCommunication.Witnesses),
            new InMemoryWAL(raftLogger),
            raftComm,
            new HybridLogicalClock(),
            raftLogger);

        KahunaConfiguration config = ConfigurationValidator.Validate(new()
        {
            HttpsCertificate         = "",
            HttpsCertificatePassword = "",
            LocksWorkers             = 1,
            KeyValueWorkers          = 1,
            BackgroundWriterWorkers  = 1,
            Storage                  = "memory",
            StorageRevision          = Guid.NewGuid().ToString(),
            RevisionRetention        = 10,
            MaxEntriesPerActor       = 50_000,
            MaxBytesPerActor         = 256L * 1024 * 1024,
            CacheEntriesToRemove     = 1_000,
            CollectBatchMax          = 1_000,
            CacheEntryTtl            = TimeSpan.FromMinutes(5),
            PersistentRevisionCleanupOnWrite   = cleanupOnWrite,
            PersistentRevisionRetentionCount   = retentionCount,
            PersistentRevisionCleanupBatchSize = 1000,
            PersistentRevisionCleanupInterval  = cleanupInterval ?? TimeSpan.FromMinutes(5),
            DirtyObjectsWriterDelay            = dirtyObjectsWriterDelay
        });

        PruningBackend backend = new();

        MemoryInterNodeCommmunication interNode = new();
        KahunaManager kahuna = new(actorSystem, Track(raft), config, interNode, backend, kahunaLogger);
        raft.OnLogRestored         += kahuna.OnLogRestored;
        raft.OnReplicationReceived += kahuna.OnReplicationReceived;
        raft.OnReplicationError    += kahuna.OnReplicationError;
        raft.OnLeaderChanged       += kahuna.OnLeaderChanged;

        interNode.SetNodes(new() { { raft.GetLocalEndpoint(), kahuna } });

        TestClusterNodeRegistry.Register(raft, kahuna, actorSystem);

        return (raft, kahuna, backend);
    }

    private static async Task Cleanup(RaftManager raft, KahunaManager kahuna)
    {
        try { await TestClusterNodeRegistry.DisposeAsync(raft); } catch (ObjectDisposedException) { }
    }

    // ── tests ─────────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Drives the real <c>BackgroundWriterActor</c> prune path via <c>FlushPersistenceAsync</c>.
    /// The <c>BeforePruneSampleHook</c> blocks the scheduler thread immediately before the floor
    /// is sampled; the main thread acquires a hold at T1 while the actor is blocked. After the
    /// gate opens, the actor calls <c>GetFloorForPrune</c> inside the task, sees the new hold,
    /// and prunes with floor=T1 — leaving the revision written at T1 intact.
    ///
    /// This test would fail if <c>BackgroundWriterActor</c> sampled the floor before
    /// <c>EnqueueTask</c> instead of inside the scheduler callback (the pre-fix behaviour).
    /// </summary>
    [Fact]
    public async Task BackgroundWriter_HoldAcquiredBeforePruneSample_RevisionRetained()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        (RaftManager raft, KahunaManager kahuna, PruningBackend backend) = BuildNode(retentionCount: 1);

        try
        {
            await raft.JoinCluster(ct);
            await raft.WaitForLeader(0, ct);
            await raft.WaitForLeader(1, ct);

            const string key = "prune/race/wired";

            // Warm-up flush with no dirty writes — instantiates the BackgroundWriterActor
            // without triggering any cleanup (pendingRevisionCleanupKeys is empty).
            await kahuna.FlushPersistenceAsync();

            BackgroundWriterActor? actor = kahuna.BackgroundWriterActor;
            Assert.NotNull(actor);

            // Write revision 1 (T1) and revision 2. Both go to the background writer queue
            // but are NOT flushed yet — the hook is set first so the single flush below can
            // be intercepted before the floor is sampled.
            (KeyValueResponseType r1, _, _) = await kahuna.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, key, "v1"u8.ToArray(), null, -1,
                KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.Set, r1);

            (KeyValueResponseType r2, _, _) = await kahuna.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, key, "v2"u8.ToArray(), null, -1,
                KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.Set, r2);

            // ── gate ─────────────────────────────────────────────────────────────────────
            ManualResetEventSlim hookEntered = new(false);
            ManualResetEventSlim gate        = new(false);

            // The hook fires on the scheduler thread immediately before GetFloorForPrune.
            // By the time it fires, FlushKeyValues has already stored both revisions to the
            // backend — so we can read T1 from revision 1 and acquire a hold while blocked.
            actor.BeforePruneSampleHook = () =>
            {
                hookEntered.Set();
                gate.Wait(ct);
            };

            // This flush stores revisions 1 and 2, then runs targeted cleanup with the hook.
            Task flushTask = kahuna.FlushPersistenceAsync();

            // Wait until the actor is blocked before floor sampling.
            bool entered = hookEntered.Wait(TimeSpan.FromSeconds(10), ct);
            Assert.True(entered, "BackgroundWriterActor must reach BeforePruneSampleHook within 10 s");

            // Both revisions are now in the backend (stored before the hook fired).
            // Read T1 — the LastModified of revision 1 — to anchor the hold.
            KeyValueEntry? rev1 = backend.GetKeyValueRevision(key, 1);
            Assert.NotNull(rev1);
            HLCTimestamp t1 = rev1.LastModified;

            // Acquire a hold at T1 while the actor is blocked before the floor sample.
            (KeyValueResponseType holdType, _, _) =
                await kahuna.LocateAndAcquireSnapshotHold("race-wired-holder", t1, leaseMs: 60_000, ct);
            Assert.Equal(KeyValueResponseType.Set, holdType);

            // Release the gate — actor calls GetFloorForPrune (sees the hold), then prunes.
            gate.Set();
            actor.BeforePruneSampleHook = null;

            await flushTask;

            // Revision 1 (at T1) must still exist — the floor protected it from pruning.
            Assert.NotNull(backend.GetKeyValueRevision(key, 1));
        }
        finally
        {
            await Cleanup(raft, kahuna);
        }
    }

    /// <summary>
    /// Control case: when the floor is sampled at enqueue time (before the hold is acquired),
    /// the prune uses a stale floor=Zero and deletes the revision — confirming the defect that
    /// the fix addresses.
    /// </summary>
    [Fact]
    public void PruneWithFloorSampledBeforeAcquire_ZeroFloor_RevisionDeleted()
    {
        PruningBackend backend = new();

        HLCTimestamp t1 = new(1, 1000L, 0);
        HLCTimestamp t2 = new(1, 2000L, 0);

        const string key = "prune/ctrl/key";
        backend.StoreKeyValues(
        [
            new(key, "v1"u8.ToArray(), 1L, 0, 0, 0, 0, 0, 0, t1.N, t1.L, (uint)t1.C, (int)KeyValueState.Set),
            new(key, "v2"u8.ToArray(), 2L, 0, 0, 0, 0, 0, 0, t2.N, t2.L, (uint)t2.C, (int)KeyValueState.Set)
        ]);

        Assert.NotNull(backend.GetKeyValueRevision(key, 1));

        // Stale floor (sampled before any holds exist) — zero means no protection.
        backend.PruneKeyValueRevisions([key], retentionCount: 1, TimeSpan.Zero, 1000, HLCTimestamp.Zero, out _);

        // Revision 1 is gone — this is the defect the fix addresses.
        Assert.Null(backend.GetKeyValueRevision(key, 1));
    }

    /// <summary>
    /// Unit test for the epoch-retry branch of <c>GetFloorForPrune</c>: a hold is acquired
    /// before <c>GetFloorForPrune</c> runs (epoch already bumped), so the loop must complete in
    /// one iteration with the hold reflected in the returned floor.
    /// </summary>
    [Fact]
    public async Task GetFloorForPrune_HoldCommittedBeforeScan_FloorReflectsHold()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        (RaftManager raft, KahunaManager kahuna, PruningBackend _) = BuildNode();

        try
        {
            await raft.JoinCluster(ct);
            await raft.WaitForLeader(0, ct);
            await raft.WaitForLeader(1, ct);

            SnapshotFloorStore store = kahuna.SnapshotFloorStore;

            HLCTimestamp holdTs = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

            // Acquire a hold: this bumps mutationEpoch before GetFloorForPrune is called.
            (KeyValueResponseType holdType, _, _) =
                await kahuna.LocateAndAcquireSnapshotHold("epoch-retry-holder", holdTs, leaseMs: 60_000, ct);
            Assert.Equal(KeyValueResponseType.Set, holdType);

            // Call GetFloorForPrune from a scheduler task. The epoch already advanced (via the
            // hold above), so epoch1 == epoch2 in the retry loop (stable scan) and the floor
            // reflects the hold.
            HLCTimestamp floor = HLCTimestamp.Zero;
            bool ok = await raft.ReadScheduler.EnqueueTask(0, () =>
            {
                floor = store.GetFloorForPrune(raft);
                return true;
            });

            Assert.True(ok);
            Assert.Equal(holdTs, floor);
        }
        finally
        {
            await Cleanup(raft, kahuna);
        }
    }

    /// <summary>
    /// Core protocol, deterministic and site-agnostic: a hold acquired while a prune-delete window
    /// is open (<c>BeginPrune</c> called, <c>EndPrune</c> not yet) fails closed with
    /// <c>MustRetry</c>, because the delete about to run sampled its floor without this hold. Once
    /// the window closes, the idempotent re-acquire succeeds. Both targeted cleanup and full sweep
    /// open the window through this exact pair, so this is the shared guarantee both rely on.
    /// </summary>
    [Fact]
    public async Task Acquire_WhilePruneDeleteWindowOpen_FailsClosed_ThenSucceedsAfterClose()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        (RaftManager raft, KahunaManager kahuna, PruningBackend _) = BuildNode();

        try
        {
            await raft.JoinCluster(ct);
            await raft.WaitForLeader(0, ct);
            await raft.WaitForLeader(1, ct);

            SnapshotFloorStore store = kahuna.SnapshotFloorStore;
            HLCTimestamp ts = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

            // Open a prune-delete window exactly as the background prune does before deleting.
            (HLCTimestamp _, long token) = store.BeginPrune(raft);
            try
            {
                (KeyValueResponseType inWindow, _, _) =
                    await kahuna.LocateAndAcquireSnapshotHold("window-holder", ts, leaseMs: 60_000, ct);

                // The delete's floor was sampled without this hold, so the acquire must fail closed.
                Assert.Equal(KeyValueResponseType.MustRetry, inWindow);
            }
            finally
            {
                store.EndPrune(token);
            }

            // Window closed: the idempotent (same holder + timestamp) re-acquire now succeeds.
            (KeyValueResponseType afterClose, _, _) =
                await kahuna.LocateAndAcquireSnapshotHold("window-holder", ts, leaseMs: 60_000, ct);
            Assert.Equal(KeyValueResponseType.Set, afterClose);
        }
        finally
        {
            await Cleanup(raft, kahuna);
        }
    }

    /// <summary>
    /// Wired targeted-cleanup path: a hold acquired after the floor is sampled but before the delete
    /// runs (inside the open window, via <c>AfterPruneSampleHook</c>) fails closed, and the delete —
    /// which sampled floor=Zero before the hold existed — removes the older revision. This is the
    /// residual [sample → delete] window, now closed by failing the acquire rather than silently
    /// dropping a boundary the sample never saw.
    /// </summary>
    [Fact]
    public async Task BackgroundWriter_HoldAcquiredInsideDeleteWindow_TargetedCleanup_FailsClosed()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        (RaftManager raft, KahunaManager kahuna, PruningBackend backend) = BuildNode(retentionCount: 1);

        try
        {
            await raft.JoinCluster(ct);
            await raft.WaitForLeader(0, ct);
            await raft.WaitForLeader(1, ct);

            const string key = "prune/race/after-sample";

            await kahuna.FlushPersistenceAsync(); // warm up the background writer

            BackgroundWriterActor? actor = kahuna.BackgroundWriterActor;
            Assert.NotNull(actor);

            (KeyValueResponseType r1, _, _) = await kahuna.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, key, "v1"u8.ToArray(), null, -1,
                KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.Set, r1);

            (KeyValueResponseType r2, _, _) = await kahuna.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, key, "v2"u8.ToArray(), null, -1,
                KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.Set, r2);

            ManualResetEventSlim hookEntered = new(false);
            ManualResetEventSlim gate        = new(false);

            // Fires after the floor is sampled and the window is open, before the backend delete.
            actor.AfterPruneSampleHook = () =>
            {
                hookEntered.Set();
                gate.Wait(ct);
            };

            Task flushTask = kahuna.FlushPersistenceAsync();

            bool entered = hookEntered.Wait(TimeSpan.FromSeconds(10), ct);
            Assert.True(entered, "prune must reach AfterPruneSampleHook within 10 s");

            // Both revisions are persisted by now (stored before the prune step). Acquire a hold at
            // revision 1's timestamp while the delete window is open — it must fail closed.
            KeyValueEntry? rev1 = backend.GetKeyValueRevision(key, 1);
            Assert.NotNull(rev1);
            HLCTimestamp t1 = rev1.LastModified;

            // The delete's floor was sampled without this hold, so the acquire cannot claim its
            // boundary is protected: it fails closed. (Whether the delete then removes the revision
            // is moot — a non-successful acquire makes no protection promise.)
            (KeyValueResponseType holdType, _, _) =
                await kahuna.LocateAndAcquireSnapshotHold("after-sample-holder", t1, leaseMs: 60_000, ct);
            Assert.Equal(KeyValueResponseType.MustRetry, holdType);

            gate.Set();
            actor.AfterPruneSampleHook = null;
            await flushTask;
        }
        finally
        {
            await Cleanup(raft, kahuna);
        }
    }

    /// <summary>
    /// Wired full-sweep path (the other prune site): with per-write cleanup disabled and the sweep
    /// interval elapsed, the periodic sweep opens the same window. A hold acquired inside it fails
    /// closed, mirroring the targeted case.
    /// </summary>
    [Fact]
    public async Task BackgroundWriter_HoldAcquiredInsideDeleteWindow_FullSweep_FailsClosed()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        // A 1 ms sweep interval survives config validation (which clamps only <= 0 back to the
        // 5-minute default), so the periodic timer runs the full sweep almost immediately.
        (RaftManager raft, KahunaManager kahuna, PruningBackend backend) = BuildNode(
            retentionCount: 1, cleanupOnWrite: false,
            cleanupInterval: TimeSpan.FromMilliseconds(1), dirtyObjectsWriterDelay: 100);

        try
        {
            await raft.JoinCluster(ct);
            await raft.WaitForLeader(0, ct);
            await raft.WaitForLeader(1, ct);

            const string key = "prune/race/sweep";

            BackgroundWriterActor? actor = kahuna.BackgroundWriterActor;
            Assert.NotNull(actor);

            (KeyValueResponseType r1, _, _) = await kahuna.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, key, "v1"u8.ToArray(), null, -1,
                KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.Set, r1);

            (KeyValueResponseType r2, _, _) = await kahuna.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, key, "v2"u8.ToArray(), null, -1,
                KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.Set, r2);

            // Persist the writes without pruning (per-write cleanup is off; FlushAndNotify never sweeps).
            // Capture revision 1's timestamp now, before the periodic sweep can prune it.
            await kahuna.FlushPersistenceAsync();
            KeyValueEntry? rev1 = backend.GetKeyValueRevision(key, 1);
            Assert.NotNull(rev1);
            HLCTimestamp t1 = rev1.LastModified;

            ManualResetEventSlim hookEntered = new(false);
            ManualResetEventSlim gate        = new(false);

            actor.AfterPruneSampleHook = () =>
            {
                hookEntered.Set();
                gate.Wait(ct);
            };

            // The periodic timer drives BackgroundWriteType.Flush, which runs the full sweep.
            bool entered = hookEntered.Wait(TimeSpan.FromSeconds(10), ct);
            Assert.True(entered, "full sweep must reach AfterPruneSampleHook within 10 s");

            (KeyValueResponseType holdType, _, _) =
                await kahuna.LocateAndAcquireSnapshotHold("sweep-holder", t1, leaseMs: 60_000, ct);
            Assert.Equal(KeyValueResponseType.MustRetry, holdType);

            actor.AfterPruneSampleHook = null;
            gate.Set();

            // Let the gated sweep handler drain before teardown so no delete runs against a disposed node.
            await kahuna.FlushPersistenceAsync();
        }
        finally
        {
            await Cleanup(raft, kahuna);
        }
    }

    /// <summary>
    /// A node that cannot confirm its meta-partition application is caught up must not run the
    /// targeted revision cleanup: its local hold registry may be missing committed acquires, so a
    /// prune from it could delete revisions the cluster still holds. The skipped cycle keeps its
    /// keys queued and retries — once catch-up confirms, the same keys are pruned.
    /// </summary>
    [Fact]
    public async Task BackgroundWriter_UnconfirmedCatchUp_SkipsTargetedCleanupUntilConfirmed()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        (RaftManager raft, KahunaManager kahuna, PruningBackend backend) = BuildNode(retentionCount: 1);

        try
        {
            await raft.JoinCluster(ct);
            await raft.WaitForLeader(0, ct);
            await raft.WaitForLeader(1, ct);

            const string key = "prune/unconfirmed/targeted";

            // Warm-up flush instantiates the BackgroundWriterActor without triggering cleanup.
            await kahuna.FlushPersistenceAsync();

            BackgroundWriterActor? actor = kahuna.BackgroundWriterActor;
            Assert.NotNull(actor);

            // Simulate a node that cannot prove catch-up (partitioned / still applying P0).
            actor.ConfirmPruneFreshnessOverride = () => ValueTask.FromResult(false);

            (KeyValueResponseType r1, _, _) = await kahuna.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, key, "v1"u8.ToArray(), null, -1,
                KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.Set, r1);

            (KeyValueResponseType r2, _, _) = await kahuna.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, key, "v2"u8.ToArray(), null, -1,
                KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.Set, r2);

            // Flush stores both revisions and would prune revision 0 (retentionCount=1) —
            // but the unconfirmed gate must skip the cleanup and keep the key queued.
            await kahuna.FlushPersistenceAsync();
            Assert.NotNull(backend.GetKeyValueRevision(key, 0));

            // Still unconfirmed on a later cycle: still skipped.
            await kahuna.FlushPersistenceAsync();
            Assert.NotNull(backend.GetKeyValueRevision(key, 0));

            // Catch-up confirms (single-node leader: the real path also succeeds) — the queued
            // key from the skipped cycles is pruned on the next cycle.
            actor.ConfirmPruneFreshnessOverride = null;
            await kahuna.FlushPersistenceAsync();
            Assert.Null(backend.GetKeyValueRevision(key, 0));
        }
        finally
        {
            await Cleanup(raft, kahuna);
        }
    }

    /// <summary>
    /// Same gate for the backend-wide sweep: an unconfirmed node skips the sweep without
    /// consuming the interval, so the sweep runs on the next eligible periodic cycle once
    /// catch-up confirms. The sweep only runs from the periodic flush timer (explicit flushes
    /// do not sweep), so this test drives it with a short timer delay and polls.
    /// </summary>
    [Fact]
    public async Task BackgroundWriter_UnconfirmedCatchUp_SkipsFullSweepUntilConfirmed()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        // A 1 ms sweep interval survives config validation (which clamps only <= 0 back to the
        // 5-minute default), so every 50 ms periodic cycle is sweep-eligible.
        (RaftManager raft, KahunaManager kahuna, PruningBackend backend) = BuildNode(
            retentionCount: 1, cleanupOnWrite: false, cleanupInterval: TimeSpan.FromMilliseconds(1),
            dirtyObjectsWriterDelay: 50);

        try
        {
            await raft.JoinCluster(ct);
            await raft.WaitForLeader(0, ct);
            await raft.WaitForLeader(1, ct);

            const string key = "prune/unconfirmed/sweep";

            await kahuna.FlushPersistenceAsync();

            BackgroundWriterActor? actor = kahuna.BackgroundWriterActor;
            Assert.NotNull(actor);

            actor.ConfirmPruneFreshnessOverride = () => ValueTask.FromResult(false);

            (KeyValueResponseType r1, _, _) = await kahuna.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, key, "v1"u8.ToArray(), null, -1,
                KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.Set, r1);

            (KeyValueResponseType r2, _, _) = await kahuna.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, key, "v2"u8.ToArray(), null, -1,
                KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.Set, r2);

            // Flush the revisions to the backend, then let several sweep-eligible periodic
            // cycles pass. The unconfirmed gate must skip every one, leaving revision 0 in place.
            await kahuna.FlushPersistenceAsync();
            await Task.Delay(500, ct);
            Assert.NotNull(backend.GetKeyValueRevision(key, 0));

            // Catch-up confirms — the next periodic cycle sweeps revision 0 away.
            actor.ConfirmPruneFreshnessOverride = null;
            bool pruned = false;
            for (int attempt = 0; attempt < 100 && !pruned; attempt++)
            {
                pruned = backend.GetKeyValueRevision(key, 0) is null;
                if (!pruned)
                    await Task.Delay(100, ct);
            }
            Assert.True(pruned, "the full sweep must prune revision 0 once catch-up confirms");
        }
        finally
        {
            await Cleanup(raft, kahuna);
        }
    }
}
