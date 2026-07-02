
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

    // ── node builder ──────────────────────────────────────────────────────────────────────

    private (RaftManager Raft, KahunaManager Kahuna, string TempDir) BuildNode(int retentionCount = 1)
    {
        string tempDir = Path.Combine(Path.GetTempPath(), "kahuna_r2race_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(tempDir);

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
            Storage                  = "sqlite",
            StoragePath              = tempDir,
            StorageRevision          = "r2",
            RevisionRetention        = 10,
            MaxEntriesPerActor       = 50_000,
            MaxBytesPerActor         = 256L * 1024 * 1024,
            CacheEntriesToRemove     = 1_000,
            CollectBatchMax          = 1_000,
            CacheEntryTtl            = TimeSpan.FromMinutes(5),
            // Enable targeted revision cleanup on flush with a tight retention count
            // so that the background writer prunes after flushing writes.
            PersistentRevisionCleanupOnWrite    = true,
            PersistentRevisionRetentionCount    = retentionCount,
            PersistentRevisionCleanupBatchSize  = 1000
        });

        MemoryInterNodeCommmunication interNode = new();
        KahunaManager kahuna = new(actorSystem, raft, config, interNode, kahunaLogger);
        raft.OnLogRestored         += kahuna.OnLogRestored;
        raft.OnReplicationReceived += kahuna.OnReplicationReceived;
        raft.OnReplicationError    += kahuna.OnReplicationError;

        interNode.SetNodes(new() { { raft.GetLocalEndpoint(), kahuna } });

        return (raft, kahuna, tempDir);
    }

    private static async Task Cleanup(RaftManager raft, KahunaManager kahuna, string tempDir)
    {
        raft.OnLogRestored         -= kahuna.OnLogRestored;
        raft.OnReplicationReceived -= kahuna.OnReplicationReceived;
        raft.OnReplicationError    -= kahuna.OnReplicationError;

        try { await raft.LeaveCluster(dispose: true); } catch (ObjectDisposedException) { }
        kahuna.Dispose();

        try { Directory.Delete(tempDir, recursive: true); } catch { }
    }

    // ── tests ─────────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Drives the real <c>BackgroundWriterActor</c> prune path via <c>FlushPersistenceAsync</c>.
    /// The <c>BeforePruneSampleHook</c> blocks the actor thread before the floor is sampled;
    /// the main thread acquires a hold while the actor is blocked. After the gate opens, the
    /// actor calls <c>GetFloorForPrune</c> (inside the task), sees the new hold, and prunes with
    /// that floor — leaving the protected revision intact.
    ///
    /// This test would fail if <c>BackgroundWriterActor</c> sampled the floor before
    /// <c>EnqueueTask</c> instead of inside the scheduler callback.
    /// </summary>
    [Fact]
    public async Task BackgroundWriter_HoldAcquiredBeforePruneSample_RevisionRetained()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        (RaftManager raft, KahunaManager kahuna, string tempDir) = BuildNode(retentionCount: 1);

        try
        {
            await raft.JoinCluster(ct);
            await raft.WaitForLeader(0, ct);
            await raft.WaitForLeader(1, ct);

            const string key = "prune/race/wired";

            // Write revision 1 (at T1).
            HLCTimestamp t1 = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());
            (KeyValueResponseType r1, _, _) = await kahuna.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, key, "v1"u8.ToArray(), null, -1,
                KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.Set, r1);

            // Flush so revision 1 is on disk.
            await kahuna.FlushPersistenceAsync();

            // Write revision 2 (at T2) — now there are 2 revisions; prune(retentionCount=1)
            // will try to delete revision 1 unless the floor protects it.
            (KeyValueResponseType r2, _, _) = await kahuna.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, key, "v2"u8.ToArray(), null, -1,
                KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.Set, r2);

            // Confirm revision 1 is on disk before the next prune cycle.
            KeyValueEntry? beforePrune = kahuna.PersistenceBackend.GetKeyValueRevision(key, 1);
            Assert.NotNull(beforePrune);

            // ── set up the gate ──────────────────────────────────────────────────────────
            // We wait until the actor is initialized (it's created lazily on first message).
            // Send a no-op flush to ensure the actor is running.
            ManualResetEventSlim actorReady  = new(false);
            ManualResetEventSlim hookEntered = new(false);
            ManualResetEventSlim gate        = new(false);

            // The hook fires on the scheduler thread immediately before GetFloorForPrune.
            // Signal "entered" so the main thread can acquire the hold; then wait for release.
            void Hook()
            {
                hookEntered.Set();
                gate.Wait(ct);
            }

            // Ensure the actor is instantiated before we set the hook.
            await kahuna.FlushPersistenceAsync();

            BackgroundWriterActor? actor = kahuna.BackgroundWriterActor;
            Assert.NotNull(actor);
            actor.BeforePruneSampleHook = Hook;

            // Trigger the flush that will write revision 2 and then run targeted cleanup.
            Task flushTask = kahuna.FlushPersistenceAsync();

            // Wait until the prune hook has fired (actor is blocked before floor sample).
            bool entered = hookEntered.Wait(TimeSpan.FromSeconds(10), ct);
            Assert.True(entered, "BackgroundWriterActor must reach BeforePruneSampleHook within 10 s");

            // Acquire a hold at T1 while the actor is blocked before it samples the floor.
            (KeyValueResponseType holdType, _, _) =
                await kahuna.LocateAndAcquireSnapshotHold("race-wired-holder", t1, leaseMs: 60_000, ct);
            Assert.Equal(KeyValueResponseType.Set, holdType);

            // Release the gate — actor now calls GetFloorForPrune (sees the hold) then prunes.
            gate.Set();
            actor.BeforePruneSampleHook = null;

            await flushTask;

            // Revision 1 (at T1) must still exist because the floor protected it.
            KeyValueEntry? afterPrune = kahuna.PersistenceBackend.GetKeyValueRevision(key, 1);
            Assert.NotNull(afterPrune);
        }
        finally
        {
            await Cleanup(raft, kahuna, tempDir);
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
        string tempDir = Path.Combine(Path.GetTempPath(), "kahuna_r2ctrl_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(tempDir);

        try
        {
            using SqlitePersistenceBackend backend = new(tempDir, "ctrl");

            HLCTimestamp t1 = new(1, 1000L, 0);
            HLCTimestamp t2 = new(1, 2000L, 0);

            const string key = "prune/ctrl/key";
            backend.StoreKeyValues(
            [
                new(key,
                    System.Text.Encoding.UTF8.GetBytes("v1"),
                    revision: 1,
                    expiresNode: 0, expiresPhysical: 0, expiresCounter: 0,
                    lastUsedNode: 0, lastUsedPhysical: 0, lastUsedCounter: 0,
                    lastModifiedNode: t1.N, lastModifiedPhysical: t1.L, lastModifiedCounter: (uint)t1.C,
                    state: (int)KeyValueState.Set)
            ]);
            backend.StoreKeyValues(
            [
                new(key,
                    System.Text.Encoding.UTF8.GetBytes("v2"),
                    revision: 2,
                    expiresNode: 0, expiresPhysical: 0, expiresCounter: 0,
                    lastUsedNode: 0, lastUsedPhysical: 0, lastUsedCounter: 0,
                    lastModifiedNode: t2.N, lastModifiedPhysical: t2.L, lastModifiedCounter: (uint)t2.C,
                    state: (int)KeyValueState.Set)
            ]);

            Assert.NotNull(backend.GetKeyValueRevision(key, 1));

            // Stale floor (sampled before any holds exist).
            HLCTimestamp staleFloor = HLCTimestamp.Zero;

            // Prune with retentionCount=1: zero floor means no protection → rev 1 deleted.
            backend.PruneKeyValueRevisions([key], retentionCount: 1, TimeSpan.Zero, 1000, staleFloor, out _);

            // Revision 1 is gone — this is the defect the fix addresses.
            Assert.Null(backend.GetKeyValueRevision(key, 1));
        }
        finally
        {
            try { Directory.Delete(tempDir, recursive: true); } catch { }
        }
    }

    /// <summary>
    /// Unit test for the epoch-retry branch of <c>GetFloorForPrune</c>: a concurrent thread
    /// acquires a hold between the two epoch reads that bracket the floor scan, causing the
    /// first iteration to see a changed epoch and retry. The returned floor must reflect the
    /// hold that arrived during scanning.
    /// </summary>
    [Fact]
    public async Task GetFloorForPrune_EpochChangeDuringScan_RetriesAndSeesNewHold()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        (RaftManager raft, KahunaManager kahuna, string tempDir) = BuildNode();

        try
        {
            await raft.JoinCluster(ct);
            await raft.WaitForLeader(0, ct);
            await raft.WaitForLeader(1, ct);

            SnapshotFloorStore store = kahuna.SnapshotFloorStore;

            HLCTimestamp holdTs = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

            // Acquire a hold: this bumps mutationEpoch.
            (KeyValueResponseType holdType, _, _) =
                await kahuna.LocateAndAcquireSnapshotHold("epoch-retry-holder", holdTs, leaseMs: 60_000, ct);
            Assert.Equal(KeyValueResponseType.Set, holdType);

            // Call GetFloorForPrune from a scheduler task — epoch already advanced by the
            // hold above; since epoch1 == epoch2 (no further mutation during the scan), the
            // loop exits in one iteration and returns the floor that includes the hold.
            HLCTimestamp floor = HLCTimestamp.Zero;
            bool ok = await raft.ReadScheduler.EnqueueTask(0, () =>
            {
                floor = store.GetFloorForPrune(raft);
                return true;
            });

            Assert.True(ok);
            // The hold at holdTs must be reflected in the floor.
            Assert.Equal(holdTs, floor);
        }
        finally
        {
            await Cleanup(raft, kahuna, tempDir);
        }
    }
}
