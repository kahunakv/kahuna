
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
/// Verifies that the snapshot floor is sampled inside the prune scheduler task (late binding),
/// so a hold acquired after the task is enqueued but before it starts executing is still observed.
///
/// <para><b>What the fix closes.</b> Before the fix, <c>BackgroundWriterActor</c> sampled the
/// effective floor on the actor thread, before calling <c>EnqueueTask</c>. A hold acquired between
/// that sample and the moment the scheduler task started executing was invisible to the prune. The
/// fix moves the sample inside the callback so the scheduler-queue-latency window is eliminated.
/// The epoch-retry loop in <c>GetFloorForPrune</c> additionally detects mutations that land while
/// the floor is being scanned and re-samples in that case.</para>
///
/// <para><b>Residual window.</b> A hold acquired in the interval
/// [<c>GetFloorForPrune</c> returns → <c>PruneKeyValueRevisions</c> completes] is still invisible
/// to the running prune batch — the floor was already captured and the delete cannot be undone.
/// This window is now micro- to low-millisecond (sample and delete are adjacent); the tests here
/// cover only the scheduler-queue window (the one the fix closes), not this residual.</para>
/// </summary>
[Collection("ClusterTests")]
public sealed class TestSnapshotFloorPruneAcquireRace
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
        BuildNode(int retentionCount = 1)
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
                StartElectionTimeout = 50,
                EndElectionTimeout   = 150,
                EnableQuiescence     = false
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
            PersistentRevisionCleanupOnWrite   = true,
            PersistentRevisionRetentionCount   = retentionCount,
            PersistentRevisionCleanupBatchSize = 1000
        });

        PruningBackend backend = new();

        MemoryInterNodeCommmunication interNode = new();
        KahunaManager kahuna = new(actorSystem, raft, config, interNode, backend, kahunaLogger);
        raft.OnLogRestored         += kahuna.OnLogRestored;
        raft.OnReplicationReceived += kahuna.OnReplicationReceived;
        raft.OnReplicationError    += kahuna.OnReplicationError;
        raft.OnLeaderChanged       += kahuna.OnLeaderChanged;

        interNode.SetNodes(new() { { raft.GetLocalEndpoint(), kahuna } });

        return (raft, kahuna, backend);
    }

    private static async Task Cleanup(RaftManager raft, KahunaManager kahuna)
    {
        raft.OnLogRestored         -= kahuna.OnLogRestored;
        raft.OnReplicationReceived -= kahuna.OnReplicationReceived;
        raft.OnReplicationError    -= kahuna.OnReplicationError;
        raft.OnLeaderChanged       -= kahuna.OnLeaderChanged;

        try { await raft.LeaveCluster(dispose: true); } catch (ObjectDisposedException) { }
        kahuna.Dispose();
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
}
