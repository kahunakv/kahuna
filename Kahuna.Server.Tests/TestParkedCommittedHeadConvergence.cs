using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Handlers;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Shared.KeyValue;
using Kahuna.Utils;
using Kommander;
using Kommander.Communication.Memory;
using Kommander.Discovery;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.Logging.Abstractions;
using Nixie;

namespace Kahuna.Server.Tests;

/// <summary>
/// Coverage for resident-cache convergence with the node's own applied committed state. The Raft
/// read gate (leadership confirm) proves every committed entry was DELIVERED to this node, but the
/// resident actor entry converges through a single-shot notification. Two mechanisms keep a
/// quorum-confirmed read from serving a stale revision when that notification cannot apply at
/// delivery time:
///
/// <list type="bullet">
///   <item>A committed head that arrives while a live replication intent owns the entry's apply is
///   PARKED on the entry (newest wins) and drained by the intent's completion, its release, or the
///   first read or write after it expires — never silently dropped.</item>
///   <item>A head advance that skips committed revisions records an archive-gap window on the
///   entry: snapshot reads distrust in-memory archive hits across the window (they fall to the
///   persisted revision history) and fail closed with MustRetry while the skipped revisions'
///   flushes are still queued.</item>
/// </list>
/// </summary>
public sealed class TestParkedCommittedHeadConvergence : RaftTrackingTest
{
    private static HLCTimestamp Ts(long physical) => new(0, physical, 0);

    private RaftManager BuildRaft(string name)
    {
        return Track(new RaftManager(
            new RaftConfiguration
            {
                NodeName = name,
                NodeId = 1,
                Host = "localhost",
                Port = 0,
                InitialPartitions = 2,
                EnableQuiescence = false, PartitionExecutorPoolSize = 1
            },
            new StaticDiscovery([]),
            new InMemoryWAL(NullLogger<IRaft>.Instance),
            new InMemoryCommunication(),
            new HybridLogicalClock(),
            NullLogger<IRaft>.Instance));
    }

    private static KahunaConfiguration BuildConfig()
    {
        return ConfigurationValidator.Validate(new()
        {
            LocksWorkers = 1,
            KeyValueWorkers = 1,
            BackgroundWriterWorkers = 1,
            Storage = "memory",
            RevisionRetention = 4,
            MaxEntriesPerActor = 50_000,
            MaxBytesPerActor = 256L * 1024 * 1024,
            CacheEntriesToRemove = 1000,
            CollectBatchMax = 1000,
            CacheEntryTtl = TimeSpan.FromMinutes(5)
        });
    }

    private sealed record Harness(
        KeyValueContext Context,
        InvalidateOrApplyHandler Invalidate,
        TryGetHandler Get,
        CompleteProposalHandler Complete,
        ReleaseProposalHandler Release,
        BTree<string, KeyValueEntry> Store,
        MemoryPersistenceBackend Backend,
        RaftManager Raft);

    private Harness BuildHarness(
        string raftName,
        IActorRef<BackgroundWriterActor, BackgroundWriteRequest>? backgroundWriter = null)
    {
        RaftManager raft = BuildRaft(raftName);
        BTree<string, KeyValueEntry> store = new(32);
        MemoryPersistenceBackend backend = new();

        KeyValueContext context = new(
            actorContext: null!,
            store: store,
            locksByPrefix: [],
            locksByRange: [],
            proposals: [],
            backgroundWriter: backgroundWriter!,
            writeAggregator: null!,
            persistenceBackend: backend,
            raft: raft,
            backendReadScheduler: null!,
            keySpaceRegistry: new(),
            rangeMapStore: new(raft, null, null, NullLogger<IKahuna>.Instance),
            configuration: BuildConfig(),
            logger: NullLogger<IKahuna>.Instance);

        return new(
            context,
            new InvalidateOrApplyHandler(context),
            new TryGetHandler(context),
            new CompleteProposalHandler(context),
            new ReleaseProposalHandler(context),
            store,
            backend,
            raft);
    }

    private HLCTimestamp Now(Harness h) =>
        h.Context.Raft.HybridLogicalClock.TrySendOrLocalEvent(h.Context.Raft.GetLocalNodeId());

    /// <summary>Installs a resident entry at revision 5 with an in-flight direct-write proposal
    /// (a live replication intent), the state a leader holds while a write awaits Raft.</summary>
    private KeyValueEntry SeedEntryWithReplicationIntent(Harness h, string key, int proposalId, bool expired = false)
    {
        HLCTimestamp now = Now(h);

        KeyValueEntry entry = new()
        {
            Bucket = null,
            Value = "v5"u8.ToArray(),
            Revision = 5,
            FlushedRevision = 5,
            State = KeyValueState.Set,
            LastModified = Ts(5_000),
            ReplicationIntent = new()
            {
                ProposalId = proposalId,
                Expires = expired ? Ts(1) : now + 60_000
            }
        };

        h.Context.InsertStoreEntry(key, entry);
        return entry;
    }

    private static KeyValueRequest NotificationOf(string key, long revision, byte[] value, HLCTimestamp lastModified)
    {
        return KeyValueRequestPool.RentInvalidateOrApply(
            key,
            revision,
            value,
            expires: HLCTimestamp.Zero,
            lastUsed: lastModified,
            lastModified: lastModified,
            state: KeyValueState.Set,
            forceResident: false,
            transactionId: HLCTimestamp.Zero,
            partitionId: 1,
            noRevision: false,
            isRollback: false,
            returnToPoolOnReceive: false);
    }

    private static KeyValueRequest ReadOf(string key, HLCTimestamp readTimestamp)
    {
        KeyValueRequest request = KeyValueRequestPool.Rent(
            KeyValueRequestType.TryGet,
            HLCTimestamp.Zero,
            HLCTimestamp.Zero,
            key,
            null,
            null,
            -1,
            KeyValueFlags.None,
            0,
            HLCTimestamp.Zero,
            KeyValueDurability.Persistent,
            0,
            0,
            default);

        request.ReadTimestamp = readTimestamp;
        return request;
    }

    [Fact]
    public void LiveReplicationIntent_ParksNewestCommittedHead_InsteadOfDropping()
    {
        Harness h = BuildHarness("park-newest");
        KeyValueEntry entry = SeedEntryWithReplicationIntent(h, "acct/p", proposalId: 7);

        // A committed head above the entry arrives while the proposal is in flight: parked, not applied.
        Assert.Null(h.Invalidate.Execute(NotificationOf("acct/p", 6, "v6"u8.ToArray(), Ts(6_000))));
        Assert.Equal(5, entry.Revision);
        Assert.NotNull(entry.ReplicationIntent);
        Assert.Equal(6, entry.PendingCommittedHead?.Revision);

        // A newer one supersedes the parked head.
        Assert.Null(h.Invalidate.Execute(NotificationOf("acct/p", 7, "v7"u8.ToArray(), Ts(7_000))));
        Assert.Equal(7, entry.PendingCommittedHead?.Revision);

        // An older replay neither applies nor downgrades what is parked.
        Assert.Null(h.Invalidate.Execute(NotificationOf("acct/p", 4, "v4"u8.ToArray(), Ts(4_000))));
        Assert.Equal(5, entry.Revision);
        Assert.Equal(7, entry.PendingCommittedHead?.Revision);
    }

    /// <summary>
    /// The observed Jepsen shape: a snapshot read pinned at the newest committed revision's own
    /// commit timestamp lands on a node whose resident entry is behind (the coherence notifications
    /// arrived while a pre-heal write was still in flight and its intent has since expired). The
    /// read must serve the converged head, never the stale resident one.
    /// </summary>
    [Fact]
    public async Task SnapshotReadAfterExpiredIntent_ServesConvergedHead_NotStaleResident()
    {
        Harness h = BuildHarness("park-snapshot-read");
        KeyValueEntry entry = SeedEntryWithReplicationIntent(h, "acct/s", proposalId: 7);

        Assert.Null(h.Invalidate.Execute(NotificationOf("acct/s", 6, "v6"u8.ToArray(), Ts(6_000))));
        Assert.Null(h.Invalidate.Execute(NotificationOf("acct/s", 7, "v7"u8.ToArray(), Ts(7_000))));
        Assert.Equal(5, entry.Revision);

        // The in-flight write's proposal never resolves (leadership was lost mid-replication) and
        // its intent expires. Without the drain the next read serves revision 5 at a snapshot
        // pinned to revision 7's commit timestamp — the stale-snapshot violation.
        entry.ReplicationIntent!.Expires = Ts(1);

        KeyValueResponse response = await h.Get.Execute(ReadOf("acct/s", Ts(7_000)));

        Assert.Equal(KeyValueResponseType.Get, response.Type);
        Assert.Equal(7, response.Entry?.Revision);
        Assert.Equal("v7"u8.ToArray(), response.Entry?.Value);

        Assert.Equal(7, entry.Revision);
        Assert.Null(entry.ReplicationIntent);
        Assert.Null(entry.PendingCommittedHead);
    }

    [Fact]
    public async Task LatestReadAfterExpiredIntent_DrainsParkedHead()
    {
        Harness h = BuildHarness("park-latest-read");
        KeyValueEntry entry = SeedEntryWithReplicationIntent(h, "acct/l", proposalId: 7, expired: true);

        // Park manually against the expired intent shape: deliver while live, then expire.
        entry.ReplicationIntent!.Expires = Now(h) + 60_000;
        Assert.Null(h.Invalidate.Execute(NotificationOf("acct/l", 7, "v7"u8.ToArray(), Ts(7_000))));
        entry.ReplicationIntent!.Expires = Ts(1);

        KeyValueResponse response = await h.Get.Execute(ReadOf("acct/l", HLCTimestamp.Zero));

        Assert.Equal(KeyValueResponseType.Get, response.Type);
        Assert.Equal(7, response.Entry?.Revision);
        Assert.Equal(7, entry.Revision);
        Assert.Null(entry.PendingCommittedHead);

        // The superseded revision was archived through the shared archival routine.
        Assert.True(entry.Revisions!.TryGetValue(5, out KeyValueRevisionEntry archived));
        Assert.Equal("v5"u8.ToArray(), archived.Value);
    }

    [Fact]
    public void ReleaseProposal_DrainsParkedHead()
    {
        Harness h = BuildHarness("park-release");
        KeyValueEntry entry = SeedEntryWithReplicationIntent(h, "acct/r", proposalId: 9);

        h.Context.Proposals.Add(9, new KeyValueProposal(
            KeyValueRequestType.TrySet, "acct/r", "v6own"u8.ToArray(), 6, false,
            HLCTimestamp.Zero, Ts(6_100), Ts(6_100), KeyValueState.Set, KeyValueDurability.Persistent));

        // A foreign head committed at the revision this proposal wanted: parked behind the intent.
        Assert.Null(h.Invalidate.Execute(NotificationOf("acct/r", 6, "v6"u8.ToArray(), Ts(6_000))));
        Assert.Equal(6, entry.PendingCommittedHead?.Revision);

        // The proposal fails (replication retry). Nothing else will ever apply the parked head here.
        h.Release.Execute(KeyValueRequestPool.RentReleaseProposal(
            "acct/r", KeyValueFlags.ReplicationRetry, KeyValueDurability.Persistent, 9, 1, default));

        Assert.Equal(6, entry.Revision);
        Assert.Equal("v6"u8.ToArray(), entry.Value);
        Assert.Null(entry.ReplicationIntent);
        Assert.Null(entry.PendingCommittedHead);
    }

    [Fact]
    public async Task CompleteProposal_AppliesOwnWrite_ThenDrainsNewerParkedHead()
    {
        using IDisposable lifetime = TestActorSystemLifetime.Create(out ActorSystem actorSystem);

        RaftManager writerRaft = BuildRaft("park-complete-writer");
        IActorRef<BackgroundWriterActor, BackgroundWriteRequest> writer =
            actorSystem.Spawn<BackgroundWriterActor, BackgroundWriteRequest>(
                "park-complete-bg", writerRaft, writerRaft.ReadScheduler, new MemoryPersistenceBackend(),
                null!, null!, new TransactionRecordStore(), new PreparedIntentStore(),
                BuildConfig(), NullLogger<IKahuna>.Instance, new FlushNotificationSink(), null!);

        Harness h = BuildHarness("park-complete", writer);
        KeyValueEntry entry = SeedEntryWithReplicationIntent(h, "acct/c", proposalId: 11);

        h.Context.Proposals.Add(11, new KeyValueProposal(
            KeyValueRequestType.TrySet, "acct/c", "v6own"u8.ToArray(), 6, false,
            HLCTimestamp.Zero, Ts(6_100), Ts(6_100), KeyValueState.Set, KeyValueDurability.Persistent));

        // A foreign commit above the in-flight proposal arrives first: parked.
        Assert.Null(h.Invalidate.Execute(NotificationOf("acct/c", 7, "v7"u8.ToArray(), Ts(7_000))));

        TaskCompletionSource<KeyValueResponse?> promise = new();
        h.Complete.Execute(KeyValueRequestPool.RentCompleteProposal(
            "acct/c", KeyValueDurability.Persistent, 11, 1, promise));

        // The caller is answered with its own committed revision; the entry converges past it.
        KeyValueResponse? response = await promise.Task;
        Assert.Equal(KeyValueResponseType.Set, response?.Type);
        Assert.Equal(6, response?.Revision);

        Assert.Equal(7, entry.Revision);
        Assert.Equal("v7"u8.ToArray(), entry.Value);
        Assert.Null(entry.ReplicationIntent);
        Assert.Null(entry.PendingCommittedHead);

        // Both revisions applied in order — no committed revision was skipped, so no gap window.
        Assert.True(entry.Revisions!.TryGetValue(6, out KeyValueRevisionEntry ownArchived));
        Assert.Equal("v6own"u8.ToArray(), ownArchived.Value);
        Assert.Equal(-1, entry.ArchiveGapEndRevision);
    }

    [Fact]
    public void HeadJump_MarksArchiveGap_AndArchiveHitsAcrossItFallToDisk()
    {
        Harness h = BuildHarness("gap-mark");
        KeyValueEntry entry = SeedEntryWithReplicationIntent(h, "acct/g", proposalId: 7, expired: true);
        entry.ReplicationIntent = null;

        // The entry converges through a jump 5 → 8: revisions 6 and 7 were committed and delivered
        // (their flushes are queued) but never reached this resident entry.
        Assert.Null(h.Invalidate.Execute(NotificationOf("acct/g", 8, "v8"u8.ToArray(), Ts(8_000))));

        Assert.Equal(8, entry.Revision);
        Assert.Equal(Ts(5_000), entry.ArchiveGapStart);
        Assert.Equal(Ts(8_000), entry.ArchiveGapEnd);
        Assert.Equal(8, entry.ArchiveGapEndRevision);

        // The superseded head was archived, but a snapshot INSIDE the gap must not trust it: the
        // true answer (revision 6 or 7) lives only in the durable history.
        Assert.True(entry.Revisions!.TryGetValue(5, out _));
        Assert.False(entry.TryGetRevisionAtOrBefore(Ts(6_500), out _, out _));

        // At-or-below the gap start the hit is authoritative (nothing skipped can beat it).
        Assert.True(entry.TryGetRevisionAtOrBefore(Ts(5_000), out long atStart, out _));
        Assert.Equal(5, atStart);

        // A later in-order apply archives the jump target; hits at-or-above the gap end are
        // authoritative again.
        Assert.Null(h.Invalidate.Execute(NotificationOf("acct/g", 9, "v9"u8.ToArray(), Ts(9_000))));
        Assert.True(entry.TryGetRevisionAtOrBefore(Ts(8_500), out long atEnd, out _));
        Assert.Equal(8, atEnd);
    }

    [Fact]
    public async Task SnapshotReadInsideGap_FailsClosedUntilFlushed_ThenServesDiskHistory()
    {
        Harness h = BuildHarness("gap-fail-closed");
        KeyValueEntry entry = SeedEntryWithReplicationIntent(h, "acct/d", proposalId: 7, expired: true);
        entry.ReplicationIntent = null;

        // The durable history holds the skipped revisions (their flushes landed before the jump in
        // this scenario's end state); the resident archive never saw them.
        h.Backend.StoreKeyValues([
            new PersistenceRequestItem("acct/d", "v6"u8.ToArray(), 6, 0, 0, 0, 0, 6_000, 0, 0, 6_000, 0, (int)KeyValueState.Set),
            new PersistenceRequestItem("acct/d", "v7"u8.ToArray(), 7, 0, 0, 0, 0, 7_000, 0, 0, 7_000, 0, (int)KeyValueState.Set)
        ]);

        Assert.Null(h.Invalidate.Execute(NotificationOf("acct/d", 8, "v8"u8.ToArray(), Ts(8_000))));
        Assert.Equal(8, entry.ArchiveGapEndRevision);

        // Flush acknowledgements have not caught up through the skipped range: the disk answer is
        // not yet provably complete, so the read fails closed.
        Assert.Equal(5, entry.FlushedRevision);
        KeyValueResponse blocked = await h.Get.Execute(ReadOf("acct/d", Ts(6_500)));
        Assert.Equal(KeyValueResponseType.MustRetry, blocked.Type);

        // Once the acknowledgements cover the skipped range, the persisted revision history is
        // authoritative and the read serves the true as-of revision.
        entry.FlushedRevision = 7;
        KeyValueResponse served = await h.Get.Execute(ReadOf("acct/d", Ts(6_500)));
        Assert.Equal(KeyValueResponseType.Get, served.Type);
        Assert.Equal(6, served.Entry?.Revision);
        Assert.Equal("v6"u8.ToArray(), served.Entry?.Value);

        KeyValueResponse servedUpper = await h.Get.Execute(ReadOf("acct/d", Ts(7_500)));
        Assert.Equal(KeyValueResponseType.Get, servedUpper.Type);
        Assert.Equal(7, servedUpper.Entry?.Revision);
    }
}
