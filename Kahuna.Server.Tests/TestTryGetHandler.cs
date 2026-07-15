
using System.Text;
using Kahuna;
using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Handlers;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Shared.KeyValue;
using Kahuna.Utils;
using Kommander;
using Kommander.Communication.Memory;
using Kommander.Discovery;
using Kommander.Time;
using Kommander.WAL;
using Kommander.WAL.IO;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Nixie;

namespace Kahuna.Server.Tests;

/// <summary>
/// Tests for <see cref="TryGetHandler"/>. The stage-1 synchronous paths (resident hits, ephemeral
/// misses, transactional MVCC, by-revision) are driven directly against the handler. The
/// non-transactional persistent cache-miss (detach) path — dispatch off the actor, resume via
/// ResumeRead — is driven end-to-end through a real spawned actor and a started FairReadScheduler:
/// <see cref="PersistentCacheMiss_DetachPath_ResolvesViaResumeRead"/> covers a successful disk read
/// and <see cref="PersistentCacheMiss_BackendReadFaults_ResolvesMustRetry"/> covers a faulting read
/// (which must surface as MustRetry, never a false-negative DoesNotExist).
/// </summary>
[Collection("ClusterTests")]
public sealed class TestTryGetHandler
{
    // ── Resident cache hit — no disk load, no detour ──────────────────────────────────────

    [Fact]
    public async Task ResidentHit_Ephemeral_ReturnsGet()
    {
        (TryGetHandler handler, KeyValueContext context, RaftManager raft) = CreateHandler();
        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

        KeyValueEntry resident = new()
        {
            Value = Encoding.UTF8.GetBytes("hello"),
            Revision = 3,
            FlushedRevision = 3,
            LastModified = now,
            LastUsed = now,
            State = KeyValueState.Set
        };
        context.InsertStoreEntry("key1", resident);

        KeyValueResponse resp = await handler.Execute(MakeGet("key1", KeyValueDurability.Ephemeral));

        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.NotNull(resp.Entry);
        Assert.Equal(3L, resp.Entry!.Revision);
        Assert.Equal("hello", Encoding.UTF8.GetString(resp.Entry.Value!));
    }

    [Fact]
    public async Task ResidentHit_Persistent_ReturnsGetWithoutTouchingActorContext()
    {
        (TryGetHandler handler, KeyValueContext context, RaftManager raft) = CreateHandler();
        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

        KeyValueEntry resident = new()
        {
            Value = Encoding.UTF8.GetBytes("world"),
            Revision = 7,
            FlushedRevision = 7,
            LastModified = now,
            LastUsed = now,
            State = KeyValueState.Set
        };
        context.InsertStoreEntry("key2", resident);

        // actorContext is null! — if the detach path were taken, a NullReferenceException
        // would surface here. Passing means the resident-hit path was taken instead.
        KeyValueResponse resp = await handler.Execute(MakeGet("key2", KeyValueDurability.Persistent));

        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(7L, resp.Entry!.Revision);
    }

    // ── Resident tombstone — DoesNotExist ─────────────────────────────────────────────────

    [Fact]
    public async Task ResidentTombstone_ReturnsDoesNotExist()
    {
        (TryGetHandler handler, KeyValueContext context, RaftManager raft) = CreateHandler();
        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

        KeyValueEntry tombstone = new()
        {
            Value = null,
            Revision = 2,
            LastModified = now,
            LastUsed = now,
            State = KeyValueState.Deleted
        };
        context.InsertStoreEntry("tombstoned", tombstone);

        // A Persistent request with a resident tombstone must NOT detach — the tombstone is
        // in cache so it qualifies as a cache hit.
        KeyValueResponse resp = await handler.Execute(MakeGet("tombstoned", KeyValueDurability.Persistent));

        Assert.Equal(KeyValueResponseType.DoesNotExist, resp.Type);
    }

    // ── Ephemeral miss — synchronous DoesNotExist, no disk attempted ──────────────────────

    [Fact]
    public async Task EphemeralMiss_ReturnsDoesNotExistWithoutDisk()
    {
        (TryGetHandler handler, _, _) = CreateHandler();

        // null! persistenceBackend — if the Persistent disk path ran, this would throw.
        KeyValueResponse resp = await handler.Execute(MakeGet("missing", KeyValueDurability.Ephemeral));

        Assert.Equal(KeyValueResponseType.DoesNotExist, resp.Type);
    }

    // ── Transactional MVCC path (never detaches) ──────────────────────────────────────────

    [Fact]
    public async Task TransactionalRead_Ephemeral_MissingKey_ReturnsDoesNotExist()
    {
        (TryGetHandler handler, _, RaftManager raft) = CreateHandler();
        HLCTimestamp txId = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

        KeyValueResponse resp = await handler.Execute(
            MakeGet("tx-missing", KeyValueDurability.Ephemeral, transactionId: txId));

        // Undefined phantom entry → DoesNotExist via MVCC path.
        Assert.Equal(KeyValueResponseType.DoesNotExist, resp.Type);
    }

    [Fact]
    public async Task TransactionalRead_ResidentKey_ReturnsGet()
    {
        (TryGetHandler handler, KeyValueContext context, RaftManager raft) = CreateHandler();
        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());
        HLCTimestamp txId = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

        KeyValueEntry resident = new()
        {
            Value = Encoding.UTF8.GetBytes("tx-value"),
            Revision = 4,
            FlushedRevision = 4,
            LastModified = now,
            LastUsed = now,
            State = KeyValueState.Set
        };
        context.InsertStoreEntry("tx-key", resident);

        KeyValueResponse resp = await handler.Execute(
            MakeGet("tx-key", KeyValueDurability.Ephemeral, transactionId: txId));

        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(4L, resp.Entry!.Revision);
        Assert.Equal("tx-value", Encoding.UTF8.GetString(resp.Entry.Value!));
    }

    // ── By-revision path ──────────────────────────────────────────────────────────────────

    [Fact]
    public async Task ByRevision_ResidentMatchingRevision_ReturnsGet()
    {
        (TryGetHandler handler, KeyValueContext context, RaftManager raft) = CreateHandler();
        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

        KeyValueEntry resident = new()
        {
            Value = Encoding.UTF8.GetBytes("rev5-value"),
            Revision = 5,
            LastModified = now,
            LastUsed = now,
            State = KeyValueState.Set
        };
        context.InsertStoreEntry("rev-key", resident);

        KeyValueResponse resp = await handler.Execute(
            MakeGet("rev-key", KeyValueDurability.Ephemeral, compareRevision: 5));

        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(5L, resp.Entry!.Revision);
    }

    [Fact]
    public async Task ByRevision_MissingRevision_Ephemeral_ReturnsDoesNotExist()
    {
        (TryGetHandler handler, _, _) = CreateHandler();

        KeyValueResponse resp = await handler.Execute(
            MakeGet("no-such", KeyValueDurability.Ephemeral, compareRevision: 99));

        Assert.Equal(KeyValueResponseType.DoesNotExist, resp.Type);
    }

    // ── Detach path: real actor, real scheduler, disk miss resolves via ResumeRead ──────────
    //
    // This test exercises the full three-stage pipeline:
    //   Stage 1 — TryGetHandler detects a persistent cache miss, creates a PointReadContinuation,
    //             sets ByPassReply=true so Nixie does not auto-resolve the Ask promise.
    //   Stage 2 — FairReadScheduler runs GetKeyValue off the actor thread.
    //   Stage 3 — ContinueWith fires Self.Send(ResumeRead), the actor processes ResumeReadHandler,
    //             which calls cont.Execute(context) and resolves the original Ask promise.
    //
    // Without this test the detach path has zero coverage: TestTryGetHandler unit tests pass
    // null! for actorContext and are constructed to take non-detach branches, and TestKeyValues
    // cluster tests are leader-read-cache-hits.

    [Fact]
    public async Task PersistentCacheMiss_DetachPath_ResolvesViaResumeRead()
    {
        KahunaConfiguration config = ConfigurationValidator.Validate(new()
        {
            LocksWorkers = 1,
            KeyValueWorkers = 1,
            BackgroundWriterWorkers = 1,
            Storage = "memory",
            CacheEntryTtl = TimeSpan.FromMinutes(5),
            CacheEntriesToRemove = 1000,
            MaxEntriesPerActor = 50_000,
            MaxBytesPerActor = 256L * 1024 * 1024,
            CollectBatchMax = 1000,
            RevisionRetention = 16
        });

        ILogger<IKahuna> logger = NullLogger<IKahuna>.Instance;
        ILogger<IRaft> raftLogger = NullLogger<IRaft>.Instance;

        RaftManager raft = new(
            new RaftConfiguration
            {
                NodeName = "detach-test",
                NodeId = 1,
                Host = "localhost",
                Port = 0,
                InitialPartitions = 1,
                EnableQuiescence = false
            },
            new StaticDiscovery([]),
            new InMemoryWAL(raftLogger),
            new InMemoryCommunication(),
            new HybridLogicalClock(),
            raftLogger
        );

        // The FairReadScheduler must be started before EnqueueTask is called;
        // it starts background reader threads and must be stopped after the test.
        FairReadScheduler scheduler = (FairReadScheduler)raft.ReadScheduler;
        scheduler.Start();

        try
        {
            MemoryPersistenceBackend backend = new();
            backend.StoreKeyValues(
            [
                new PersistenceRequestItem(
                    key: "backend-key",
                    value: Encoding.UTF8.GetBytes("backend-value"),
                    revision: 1,
                    expiresNode: 0, expiresPhysical: 0, expiresCounter: 0,
                    lastUsedNode: 0, lastUsedPhysical: 1000, lastUsedCounter: 0,
                    lastModifiedNode: 0, lastModifiedPhysical: 1000, lastModifiedCounter: 0,
                    state: (int)KeyValueState.Set)
            ]);

            using ActorSystem actorSystem = new();

            // Spawn a real KeyValueActor. backgroundWriter and proposalRouter are null because
            // TryGet never touches them; actorContext is injected by Nixie.
            IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> actorRef =
                actorSystem.Spawn<KeyValueActor, KeyValueRequest, KeyValueResponse>(
                    "detach-test-actor",
                    null!,  // backgroundWriter
                    null!,  // proposalRouter
                    backend,
                    raft,
                    new KeySpaceRegistry(),
                    new RangeMapStore(raft, null, null, logger),
                    config,
                    logger
                );

            // "backend-key" is on disk but NOT in the actor's in-memory BTree (cache).
            // Ask uses Reply.HasValue=true so the detach path can capture the promise.
            KeyValueResponse? resp = await actorRef.Ask(
                MakeGet("backend-key", KeyValueDurability.Persistent),
                TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken
            );

            Assert.NotNull(resp);
            Assert.Equal(KeyValueResponseType.Get, resp!.Type);
            Assert.NotNull(resp.Entry);
            Assert.Equal(1L, resp.Entry!.Revision);
            Assert.Equal("backend-value", Encoding.UTF8.GetString(resp.Entry.Value!));
        }
        finally
        {
            scheduler.Stop();
        }
    }

    // ── Detach path: backend read FAULTS → MustRetry, never a false-negative DoesNotExist ───
    //
    // Regression guard for the failure-path bug: a faulted (or cancelled) backend read on the
    // detach path must not be reported as DoesNotExist. The synchronous path propagates the
    // exception (Nixie sets TrySetException on the Ask); the detached path must produce the
    // equivalent retryable outcome (MustRetry) rather than silently claiming the key is absent.

    [Fact]
    public async Task PersistentCacheMiss_BackendReadFaults_ResolvesMustRetry()
    {
        KahunaConfiguration config = ConfigurationValidator.Validate(new()
        {
            LocksWorkers = 1,
            KeyValueWorkers = 1,
            BackgroundWriterWorkers = 1,
            Storage = "memory",
            CacheEntryTtl = TimeSpan.FromMinutes(5),
            CacheEntriesToRemove = 1000,
            MaxEntriesPerActor = 50_000,
            MaxBytesPerActor = 256L * 1024 * 1024,
            CollectBatchMax = 1000,
            RevisionRetention = 16
        });

        ILogger<IKahuna> logger = NullLogger<IKahuna>.Instance;
        ILogger<IRaft> raftLogger = NullLogger<IRaft>.Instance;

        RaftManager raft = new(
            new RaftConfiguration
            {
                NodeName = "detach-fault-test",
                NodeId = 1,
                Host = "localhost",
                Port = 0,
                InitialPartitions = 1,
                EnableQuiescence = false
            },
            new StaticDiscovery([]),
            new InMemoryWAL(raftLogger),
            new InMemoryCommunication(),
            new HybridLogicalClock(),
            raftLogger
        );

        FairReadScheduler scheduler = (FairReadScheduler)raft.ReadScheduler;
        scheduler.Start();

        try
        {
            // GetKeyValue throws → the scheduler task faults → the detach continuation must
            // resolve the Ask promise with MustRetry, not feed null into PointReadContinuation.
            FaultyReadBackend backend = new();

            using ActorSystem actorSystem = new();

            IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> actorRef =
                actorSystem.Spawn<KeyValueActor, KeyValueRequest, KeyValueResponse>(
                    "detach-fault-actor",
                    null!,  // backgroundWriter
                    null!,  // proposalRouter
                    backend,
                    raft,
                    new KeySpaceRegistry(),
                    new RangeMapStore(raft, null, null, logger),
                    config,
                    logger
                );

            KeyValueResponse? resp = await actorRef.Ask(
                MakeGet("faulting-key", KeyValueDurability.Persistent),
                TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken
            );

            Assert.NotNull(resp);
            Assert.Equal(KeyValueResponseType.MustRetry, resp!.Type);
            Assert.NotEqual(KeyValueResponseType.DoesNotExist, resp.Type);
        }
        finally
        {
            scheduler.Stop();
        }
    }

    // ── Single-flight coalescing: N concurrent misses → exactly 1 backend read ────────────
    //
    // When N requests for the same persistent key all miss the cache simultaneously the
    // coalescing path must dispatch exactly ONE backend read. Each additional stage-1 miss
    // finds the in-flight PendingReads entry and attaches its Promise via AddWaiter. Stage 3
    // (PointReadContinuation.Execute) removes the entry from PendingReads and resolves all
    // attached waiters with the same result.
    //
    // Determinism guarantee: the backend blocks GetKeyValue on a SemaphoreSlim gate. A sentinel
    // ephemeral-miss Ask is sent after all N Asks; when its reply arrives (mailbox is FIFO) every
    // TryGet ahead of it has been processed and coalesced. Only then is the gate released. No
    // fixed delays — a broken implementation fails via timeout, not a timing race.

    [Fact]
    public async Task ConcurrentMisses_SameKey_ExactlyOneBackendRead_AllReceiveValue()
    {
        const int concurrency = 8;

        KahunaConfiguration config = ConfigurationValidator.Validate(new()
        {
            LocksWorkers = 1,
            KeyValueWorkers = 1,
            BackgroundWriterWorkers = 1,
            Storage = "memory",
            CacheEntryTtl = TimeSpan.FromMinutes(5),
            CacheEntriesToRemove = 1000,
            MaxEntriesPerActor = 50_000,
            MaxBytesPerActor = 256L * 1024 * 1024,
            CollectBatchMax = 1000,
            RevisionRetention = 16
        });

        ILogger<IKahuna> logger = NullLogger<IKahuna>.Instance;
        ILogger<IRaft> raftLogger = NullLogger<IRaft>.Instance;

        RaftManager raft = new(
            new RaftConfiguration
            {
                NodeName = "coalesce-test",
                NodeId = 1,
                Host = "localhost",
                Port = 0,
                InitialPartitions = 1,
                EnableQuiescence = false
            },
            new StaticDiscovery([]),
            new InMemoryWAL(raftLogger),
            new InMemoryCommunication(),
            new HybridLogicalClock(),
            raftLogger
        );

        FairReadScheduler scheduler = (FairReadScheduler)raft.ReadScheduler;
        scheduler.Start();

        try
        {
            // Gate that blocks the backend read until all N stage-1 misses have been queued.
            SemaphoreSlim gate = new(0, 1);
            CountingBlockingBackend backend = new(gate);
            backend.StoreKeyValues(
            [
                new PersistenceRequestItem(
                    key: "shared-key",
                    value: Encoding.UTF8.GetBytes("shared-value"),
                    revision: 1,
                    expiresNode: 0, expiresPhysical: 0, expiresCounter: 0,
                    lastUsedNode: 0, lastUsedPhysical: 1000, lastUsedCounter: 0,
                    lastModifiedNode: 0, lastModifiedPhysical: 1000, lastModifiedCounter: 0,
                    state: (int)KeyValueState.Set)
            ]);

            using ActorSystem actorSystem = new();

            IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> actorRef =
                actorSystem.Spawn<KeyValueActor, KeyValueRequest, KeyValueResponse>(
                    "coalesce-actor",
                    null!,
                    null!,
                    backend,
                    raft,
                    new KeySpaceRegistry(),
                    new RangeMapStore(raft, null, null, logger),
                    config,
                    logger
                );

            // Fire N concurrent Asks — they all land in the actor mailbox together.
            Task<KeyValueResponse?>[] asks = new Task<KeyValueResponse?>[concurrency];
            for (int i = 0; i < concurrency; i++)
                asks[i] = actorRef.Ask(MakeGet("shared-key", KeyValueDurability.Persistent), TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);

            // Send a sentinel ephemeral-miss Ask after all N Asks. Because the mailbox is FIFO,
            // when the sentinel reply arrives every TryGet ahead of it has been processed —
            // all N have either dispatched the read or coalesced onto the in-flight continuation.
            // The gate keeps the disk read blocked throughout, so the coalescing window cannot
            // close before the sentinel confirms all stage-1 misses are done.
            KeyValueResponse? sentinel = await actorRef.Ask(
                MakeGet("sentinel-key", KeyValueDurability.Ephemeral),
                TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);
            Assert.Equal(KeyValueResponseType.DoesNotExist, sentinel!.Type);

            // All N coalesced — unblock the disk read.
            gate.Release();

            KeyValueResponse?[] results = await Task.WhenAll(asks);

            Assert.Equal(1, backend.ReadCount);
            foreach (KeyValueResponse? resp in results)
            {
                Assert.NotNull(resp);
                Assert.Equal(KeyValueResponseType.Get, resp!.Type);
                Assert.NotNull(resp.Entry);
                Assert.Equal(1L, resp.Entry!.Revision);
                Assert.Equal("shared-value", Encoding.UTF8.GetString(resp.Entry.Value!));
            }
        }
        finally
        {
            scheduler.Stop();
        }
    }

    // ── By-revision: stage-1 shortcuts (no disk, no detach) ──────────────────────────────

    [Fact]
    public async Task ByRevision_CurrentRevisionMatch_ShortcutReturnsGetWithoutDetach()
    {
        (TryGetHandler handler, KeyValueContext context, RaftManager raft) = CreateHandler();
        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

        KeyValueEntry resident = new()
        {
            Value = Encoding.UTF8.GetBytes("v1"),
            Revision = 7,
            LastModified = now,
            LastUsed = now,
            State = KeyValueState.Set
        };
        context.InsertStoreEntry("rev-key", resident);

        // actorContext is null! — if the detach path were reached, a NullReferenceException
        // would surface. A clean response proves the stage-1 shortcut was taken instead.
        KeyValueResponse resp = await handler.Execute(
            MakeGet("rev-key", KeyValueDurability.Persistent, compareRevision: 7));

        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(7L, resp.Entry!.Revision);
        Assert.Equal("v1", Encoding.UTF8.GetString(resp.Entry.Value!));
    }

    [Fact]
    public async Task ByRevision_InMemoryRevisionDict_ShortcutReturnsGet()
    {
        (TryGetHandler handler, KeyValueContext context, RaftManager raft) = CreateHandler();
        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

        KeyValueEntry resident = new()
        {
            Value = Encoding.UTF8.GetBytes("current"),
            Revision = 10,
            LastModified = now,
            LastUsed = now,
            State = KeyValueState.Set,
            Revisions = new KeyValueRevisionHistory { [3] = new() { Value = Encoding.UTF8.GetBytes("old-v3"), State = KeyValueState.Set } }
        };
        context.InsertStoreEntry("rev-dict-key", resident);

        KeyValueResponse resp = await handler.Execute(
            MakeGet("rev-dict-key", KeyValueDurability.Persistent, compareRevision: 3));

        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(3L, resp.Entry!.Revision);
        Assert.Equal("old-v3", Encoding.UTF8.GetString(resp.Entry.Value!));
    }

    [Fact]
    public async Task ByRevision_Ephemeral_MissingRevision_ReturnsDoesNotExist()
    {
        (TryGetHandler handler, _, _) = CreateHandler();

        // Ephemeral: no disk fallback, so a revision not in cache → DoesNotExist, no detach.
        KeyValueResponse resp = await handler.Execute(
            MakeGet("missing", KeyValueDurability.Ephemeral, compareRevision: 99));

        Assert.Equal(KeyValueResponseType.DoesNotExist, resp.Type);
    }

    // ── By-revision: persistent disk miss → detach → resolves via ResumeRead ─────────────
    //
    // When the target revision is not resident in cache or in the entry's Revisions dict and
    // durability is Persistent, the handler detaches GetKeyValueRevision off the actor mailbox
    // (ByRevisionReadContinuation) and resolves all waiters at stage 3.

    [Fact]
    public async Task ByRevision_PersistentDiskMiss_DetachPath_ResolvesGet()
    {
        (RaftManager raft, FairReadScheduler scheduler, KahunaConfiguration config,
            ILogger<IKahuna> logger) = CreateRaftAndConfig("byrev-detach");

        scheduler.Start();
        try
        {
            RevisionBackend backend = new(targetRevision: 5, Encoding.UTF8.GetBytes("hist-v5"));

            using ActorSystem actorSystem = new();
            IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> actorRef =
                actorSystem.Spawn<KeyValueActor, KeyValueRequest, KeyValueResponse>(
                    "byrev-actor", null!, null!, backend, raft,
                    new KeySpaceRegistry(), new RangeMapStore(raft, null, null, logger), config, logger);

            // Key is not in cache; revision 5 is only on disk.
            KeyValueResponse? resp = await actorRef.Ask(
                MakeGet("hist-key", KeyValueDurability.Persistent, compareRevision: 5),
                TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

            Assert.NotNull(resp);
            Assert.Equal(KeyValueResponseType.Get, resp!.Type);
            Assert.Equal(5L, resp.Entry!.Revision);
            Assert.Equal("hist-v5", Encoding.UTF8.GetString(resp.Entry.Value!));
        }
        finally { scheduler.Stop(); }
    }

    [Fact]
    public async Task ByRevision_PersistentDiskMiss_RevisionNotOnDisk_ResolvesDoesNotExist()
    {
        (RaftManager raft, FairReadScheduler scheduler, KahunaConfiguration config,
            ILogger<IKahuna> logger) = CreateRaftAndConfig("byrev-notfound");

        scheduler.Start();
        try
        {
            // Backend returns null for all GetKeyValueRevision calls.
            RevisionBackend backend = new(targetRevision: -1, null);

            using ActorSystem actorSystem = new();
            IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> actorRef =
                actorSystem.Spawn<KeyValueActor, KeyValueRequest, KeyValueResponse>(
                    "byrev-notfound-actor", null!, null!, backend, raft,
                    new KeySpaceRegistry(), new RangeMapStore(raft, null, null, logger), config, logger);

            KeyValueResponse? resp = await actorRef.Ask(
                MakeGet("hist-key", KeyValueDurability.Persistent, compareRevision: 99),
                TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

            Assert.NotNull(resp);
            Assert.Equal(KeyValueResponseType.DoesNotExist, resp!.Type);
        }
        finally { scheduler.Stop(); }
    }

    // ── Cross-resolve isolation: latest-read and by-revision-read for the same key ─────────
    //
    // PendingReads uses composite key (key, revision) where latest-point reads use revision -1
    // and by-revision reads use the actual revision. This test verifies they register under
    // different keys and each resolves to the correct response — no cross-contamination.

    [Fact]
    public async Task LatestRead_And_ByRevisionRead_SameKey_ResolveIndependently()
    {
        (RaftManager raft, FairReadScheduler scheduler, KahunaConfiguration config,
            ILogger<IKahuna> logger) = CreateRaftAndConfig("cross-resolve");

        scheduler.Start();
        try
        {
            // Both reads are blocked until the gate is opened, ensuring they are in-flight
            // simultaneously and each occupies a distinct slot in PendingReads.
            ManualResetEventSlim gate = new(false);
            CrossResolveBackend backend = new(gate,
                latestValue: Encoding.UTF8.GetBytes("latest-value"),
                revision: 3, revisionValue: Encoding.UTF8.GetBytes("rev3-value"));

            using ActorSystem actorSystem = new();
            IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> actorRef =
                actorSystem.Spawn<KeyValueActor, KeyValueRequest, KeyValueResponse>(
                    "cross-resolve-actor", null!, null!, backend, raft,
                    new KeySpaceRegistry(), new RangeMapStore(raft, null, null, logger), config, logger);

            // Both Asks for the same key — one is a latest read, one is a by-revision read.
            Task<KeyValueResponse?> latestAsk = actorRef.Ask(
                MakeGet("shared", KeyValueDurability.Persistent),
                TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);
            Task<KeyValueResponse?> revisionAsk = actorRef.Ask(
                MakeGet("shared", KeyValueDurability.Persistent, compareRevision: 3),
                TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);

            // Drain the mailbox: when the sentinel replies, both stage-1 dispatches are done
            // and both reads are in-flight (blocked on the gate).
            KeyValueResponse? sentinel = await actorRef.Ask(
                MakeGet("sentinel", KeyValueDurability.Ephemeral),
                TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);
            Assert.Equal(KeyValueResponseType.DoesNotExist, sentinel!.Type);

            // Unblock both backend reads simultaneously.
            gate.Set();

            KeyValueResponse?[] results = await Task.WhenAll(latestAsk, revisionAsk);

            // Latest-point read must return "latest-value", NOT "rev3-value".
            Assert.NotNull(results[0]);
            Assert.Equal(KeyValueResponseType.Get, results[0]!.Type);
            Assert.Equal("latest-value", Encoding.UTF8.GetString(results[0]!.Entry!.Value!));

            // By-revision read must return "rev3-value", NOT "latest-value".
            Assert.NotNull(results[1]);
            Assert.Equal(KeyValueResponseType.Get, results[1]!.Type);
            Assert.Equal(3L, results[1]!.Entry!.Revision);
            Assert.Equal("rev3-value", Encoding.UTF8.GetString(results[1]!.Entry!.Value!));
        }
        finally { scheduler.Stop(); }
    }

    // ── Shape-axis isolation: Get(key,rev) and Exists(key,rev) in flight simultaneously ───
    //
    // TryGet and TryExists for the same (key, revision) must NOT share a continuation:
    // ByRevisionReadContinuation fixes its responseType at construction, so one instance
    // cannot serve mixed-shape waiters. The coalescing key includes an isExists flag —
    // (key, rev, false) for TryGet, (key, rev, true) for TryExists — so each occupies its
    // own PendingReads slot and issues its own backend read. This test is the shape-axis
    // analogue of LatestRead_And_ByRevisionRead_SameKey_ResolveIndependently.

    [Fact]
    public async Task GetAndExists_SameKeyRevision_ResolveToCorrectShapes()
    {
        (RaftManager raft, FairReadScheduler scheduler, KahunaConfiguration config,
            ILogger<IKahuna> logger) = CreateRaftAndConfig("shape-isolation");

        scheduler.Start();
        try
        {
            ManualResetEventSlim gate = new(false);
            BlockingRevisionBackend backend = new(gate,
                targetRevision: 3, Encoding.UTF8.GetBytes("shape-value"));

            using ActorSystem actorSystem = new();
            IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> actorRef =
                actorSystem.Spawn<KeyValueActor, KeyValueRequest, KeyValueResponse>(
                    "shape-actor", null!, null!, backend, raft,
                    new KeySpaceRegistry(), new RangeMapStore(raft, null, null, logger), config, logger);

            Task<KeyValueResponse?> getAsk = actorRef.Ask(
                MakeGet("shape-key", KeyValueDurability.Persistent, compareRevision: 3),
                TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);
            Task<KeyValueResponse?> existsAsk = actorRef.Ask(
                MakeExists("shape-key", 3),
                TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);

            // Sentinel drains the mailbox — both dispatches are in-flight, gate keeps them blocked.
            KeyValueResponse? sentinel = await actorRef.Ask(
                MakeGet("sentinel", KeyValueDurability.Ephemeral),
                TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);
            Assert.Equal(KeyValueResponseType.DoesNotExist, sentinel!.Type);

            gate.Set();

            KeyValueResponse?[] results = await Task.WhenAll(getAsk, existsAsk);

            // TryGet must return Get with the value — not Exists with null.
            Assert.NotNull(results[0]);
            Assert.Equal(KeyValueResponseType.Get, results[0]!.Type);
            Assert.NotNull(results[0]!.Entry!.Value);
            Assert.Equal("shape-value", Encoding.UTF8.GetString(results[0]!.Entry!.Value!));

            // TryExists must return Exists with null value — not Get with a value payload.
            Assert.NotNull(results[1]);
            Assert.Equal(KeyValueResponseType.Exists, results[1]!.Type);
            Assert.Null(results[1]!.Entry!.Value);
        }
        finally { scheduler.Stop(); }
    }

    // ── helpers ──────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Backend whose GetKeyValueRevision blocks on a ManualResetEvent gate, then returns
    /// the target revision if matched. GetKeyValue returns null (cache miss forced).
    /// Used to keep by-revision reads in-flight simultaneously for shape-isolation testing.
    /// </summary>
    private sealed class BlockingRevisionBackend : IPersistenceBackend, IDisposable
    {
        private readonly MemoryPersistenceBackend inner = new();
        private readonly ManualResetEventSlim gate;
        private readonly long targetRevision;
        private readonly byte[] revisionValue;

        internal BlockingRevisionBackend(ManualResetEventSlim gate, long targetRevision, byte[] revisionValue)
        {
            this.gate = gate;
            this.targetRevision = targetRevision;
            this.revisionValue = revisionValue;
        }

        public KeyValueEntry? GetKeyValue(string keyName) => null;

        public KeyValueEntry? GetKeyValueRevision(string keyName, long revision)
        {
            gate.Wait();
            if (revision == targetRevision)
                return new() { Value = revisionValue, Revision = revision, State = KeyValueState.Set };
            return null;
        }

        public bool StoreLocks(List<PersistenceRequestItem> items) => inner.StoreLocks(items);
        public bool StoreKeyValues(List<PersistenceRequestItem> items) => inner.StoreKeyValues(items);
        public Kahuna.Server.Locks.Data.LockEntry? GetLock(string resource) => inner.GetLock(resource);
        public KeyValueEntry? GetKeyValueRevisionAtOrBefore(string keyName, long maxRevision, HLCTimestamp readTimestamp) =>
            inner.GetKeyValueRevisionAtOrBefore(keyName, maxRevision, readTimestamp);
        public List<(string, ReadOnlyKeyValueEntry)> GetKeyValueByPrefix(string prefixKeyName) => inner.GetKeyValueByPrefix(prefixKeyName);
        public List<(string, ReadOnlyKeyValueEntry)> GetKeyValueByRange(string prefix, string? startKey, int limit) =>
            inner.GetKeyValueByRange(prefix, startKey, limit);
        public bool PruneKeyValueRevisions(IReadOnlyCollection<string>? keys, int retentionCount, TimeSpan retentionAge, int batchSize, HLCTimestamp floorTimestamp, out RevisionPruneResult result) =>
            inner.PruneKeyValueRevisions(keys, retentionCount, retentionAge, batchSize, floorTimestamp, out result);
        public Kahuna.Server.Persistence.Pitr.CheckpointResult CreateCheckpoint(string destinationPath, long appliedIndex, HLCTimestamp appliedTime) =>
            inner.CreateCheckpoint(destinationPath, appliedIndex, appliedTime);
        public void Dispose() => inner.Dispose();
    }

    private static KeyValueRequest MakeExists(string key, long compareRevision,
        KeyValueDurability durability = KeyValueDurability.Persistent)
    {
        return new KeyValueRequest(
            KeyValueRequestType.TryExists,
            HLCTimestamp.Zero,
            HLCTimestamp.Zero,
            key,
            null,
            null,
            compareRevision,
            KeyValueFlags.None,
            0,
            HLCTimestamp.Zero,
            durability,
            0,
            0,
            null
        );
    }

    /// <summary>
    /// Backend that serves a specific historical revision from GetKeyValueRevision.
    /// GetKeyValue returns null (cache miss) so the by-revision detach path is exercised.
    /// </summary>
    private sealed class RevisionBackend : IPersistenceBackend, IDisposable
    {
        private readonly MemoryPersistenceBackend inner = new();
        private readonly long targetRevision;
        private readonly byte[]? revisionValue;

        internal RevisionBackend(long targetRevision, byte[]? revisionValue)
        {
            this.targetRevision = targetRevision;
            this.revisionValue = revisionValue;
        }

        public KeyValueEntry? GetKeyValue(string keyName) => null;

        public KeyValueEntry? GetKeyValueRevision(string keyName, long revision)
        {
            if (revision == targetRevision && revisionValue is not null)
                return new() { Value = revisionValue, Revision = revision, State = KeyValueState.Set };
            return null;
        }

        public bool StoreLocks(List<PersistenceRequestItem> items) => inner.StoreLocks(items);
        public bool StoreKeyValues(List<PersistenceRequestItem> items) => inner.StoreKeyValues(items);
        public Kahuna.Server.Locks.Data.LockEntry? GetLock(string resource) => inner.GetLock(resource);
        public KeyValueEntry? GetKeyValueRevisionAtOrBefore(string keyName, long maxRevision, HLCTimestamp readTimestamp) =>
            inner.GetKeyValueRevisionAtOrBefore(keyName, maxRevision, readTimestamp);
        public List<(string, ReadOnlyKeyValueEntry)> GetKeyValueByPrefix(string prefixKeyName) => inner.GetKeyValueByPrefix(prefixKeyName);
        public List<(string, ReadOnlyKeyValueEntry)> GetKeyValueByRange(string prefix, string? startKey, int limit) =>
            inner.GetKeyValueByRange(prefix, startKey, limit);
        public bool PruneKeyValueRevisions(IReadOnlyCollection<string>? keys, int retentionCount, TimeSpan retentionAge, int batchSize, HLCTimestamp floorTimestamp, out RevisionPruneResult result) =>
            inner.PruneKeyValueRevisions(keys, retentionCount, retentionAge, batchSize, floorTimestamp, out result);
        public Kahuna.Server.Persistence.Pitr.CheckpointResult CreateCheckpoint(string destinationPath, long appliedIndex, HLCTimestamp appliedTime) =>
            inner.CreateCheckpoint(destinationPath, appliedIndex, appliedTime);
        public void Dispose() => inner.Dispose();
    }

    /// <summary>
    /// Backend for the cross-resolve isolation test. GetKeyValue and GetKeyValueRevision both
    /// block on the same ManualResetEvent gate so both reads are in-flight simultaneously,
    /// but return distinct values so cross-contamination would produce a wrong assertion.
    /// </summary>
    private sealed class CrossResolveBackend : IPersistenceBackend, IDisposable
    {
        private readonly MemoryPersistenceBackend inner = new();
        private readonly ManualResetEventSlim gate;
        private readonly byte[] latestValue;
        private readonly long revision;
        private readonly byte[] revisionValue;

        internal CrossResolveBackend(ManualResetEventSlim gate, byte[] latestValue,
            long revision, byte[] revisionValue)
        {
            this.gate = gate;
            this.latestValue = latestValue;
            this.revision = revision;
            this.revisionValue = revisionValue;
        }

        public KeyValueEntry? GetKeyValue(string keyName)
        {
            gate.Wait();
            return new() { Value = latestValue, Revision = 1, State = KeyValueState.Set };
        }

        public KeyValueEntry? GetKeyValueRevision(string keyName, long rev)
        {
            gate.Wait();
            if (rev == revision)
                return new() { Value = revisionValue, Revision = rev, State = KeyValueState.Set };
            return null;
        }

        public bool StoreLocks(List<PersistenceRequestItem> items) => inner.StoreLocks(items);
        public bool StoreKeyValues(List<PersistenceRequestItem> items) => inner.StoreKeyValues(items);
        public Kahuna.Server.Locks.Data.LockEntry? GetLock(string resource) => inner.GetLock(resource);
        public KeyValueEntry? GetKeyValueRevisionAtOrBefore(string keyName, long maxRevision, HLCTimestamp readTimestamp) =>
            inner.GetKeyValueRevisionAtOrBefore(keyName, maxRevision, readTimestamp);
        public List<(string, ReadOnlyKeyValueEntry)> GetKeyValueByPrefix(string prefixKeyName) => inner.GetKeyValueByPrefix(prefixKeyName);
        public List<(string, ReadOnlyKeyValueEntry)> GetKeyValueByRange(string prefix, string? startKey, int limit) =>
            inner.GetKeyValueByRange(prefix, startKey, limit);
        public bool PruneKeyValueRevisions(IReadOnlyCollection<string>? keys, int retentionCount, TimeSpan retentionAge, int batchSize, HLCTimestamp floorTimestamp, out RevisionPruneResult result) =>
            inner.PruneKeyValueRevisions(keys, retentionCount, retentionAge, batchSize, floorTimestamp, out result);
        public Kahuna.Server.Persistence.Pitr.CheckpointResult CreateCheckpoint(string destinationPath, long appliedIndex, HLCTimestamp appliedTime) =>
            inner.CreateCheckpoint(destinationPath, appliedIndex, appliedTime);
        public void Dispose() => inner.Dispose();
    }

    /// <summary>
    /// Backend that blocks GetKeyValue on a gate semaphore and counts how many times it is
    /// called. Used by the coalescing test to verify exactly one backend read serves N misses.
    /// </summary>
    private sealed class CountingBlockingBackend : IPersistenceBackend, IDisposable
    {
        private readonly MemoryPersistenceBackend inner = new();
        private readonly SemaphoreSlim gate;
        private int readCount;

        internal int ReadCount => readCount;

        internal CountingBlockingBackend(SemaphoreSlim gate) => this.gate = gate;

        public KeyValueEntry? GetKeyValue(string keyName)
        {
            Interlocked.Increment(ref readCount);
            gate.Wait();
            return inner.GetKeyValue(keyName);
        }

        public bool StoreLocks(List<PersistenceRequestItem> items) => inner.StoreLocks(items);
        public bool StoreKeyValues(List<PersistenceRequestItem> items) => inner.StoreKeyValues(items);
        public Kahuna.Server.Locks.Data.LockEntry? GetLock(string resource) => inner.GetLock(resource);
        public KeyValueEntry? GetKeyValueRevision(string keyName, long revision) => inner.GetKeyValueRevision(keyName, revision);
        public KeyValueEntry? GetKeyValueRevisionAtOrBefore(string keyName, long maxRevision, HLCTimestamp readTimestamp) =>
            inner.GetKeyValueRevisionAtOrBefore(keyName, maxRevision, readTimestamp);
        public List<(string, ReadOnlyKeyValueEntry)> GetKeyValueByPrefix(string prefixKeyName) => inner.GetKeyValueByPrefix(prefixKeyName);
        public List<(string, ReadOnlyKeyValueEntry)> GetKeyValueByRange(string prefix, string? startKey, int limit) =>
            inner.GetKeyValueByRange(prefix, startKey, limit);
        public bool PruneKeyValueRevisions(IReadOnlyCollection<string>? keys, int retentionCount, TimeSpan retentionAge, int batchSize, HLCTimestamp floorTimestamp, out RevisionPruneResult result) =>
            inner.PruneKeyValueRevisions(keys, retentionCount, retentionAge, batchSize, floorTimestamp, out result);
        public Kahuna.Server.Persistence.Pitr.CheckpointResult CreateCheckpoint(string destinationPath, long appliedIndex, HLCTimestamp appliedTime) =>
            inner.CreateCheckpoint(destinationPath, appliedIndex, appliedTime);
        public void Dispose() => inner.Dispose();
    }

    /// <summary>
    /// Persistence backend whose point read always throws, to exercise the detach path's
    /// faulted-read branch. All other members delegate to an in-memory backend so the actor
    /// can spawn and operate normally.
    /// </summary>
    private sealed class FaultyReadBackend : IPersistenceBackend, IDisposable
    {
        private readonly MemoryPersistenceBackend inner = new();

        public KeyValueEntry? GetKeyValue(string keyName) =>
            throw new IOException("simulated backend read failure");

        public bool StoreLocks(List<PersistenceRequestItem> items) => inner.StoreLocks(items);
        public bool StoreKeyValues(List<PersistenceRequestItem> items) => inner.StoreKeyValues(items);
        public Kahuna.Server.Locks.Data.LockEntry? GetLock(string resource) => inner.GetLock(resource);
        public KeyValueEntry? GetKeyValueRevision(string keyName, long revision) => inner.GetKeyValueRevision(keyName, revision);
        public KeyValueEntry? GetKeyValueRevisionAtOrBefore(string keyName, long maxRevision, HLCTimestamp readTimestamp) =>
            inner.GetKeyValueRevisionAtOrBefore(keyName, maxRevision, readTimestamp);
        public List<(string, ReadOnlyKeyValueEntry)> GetKeyValueByPrefix(string prefixKeyName) => inner.GetKeyValueByPrefix(prefixKeyName);
        public List<(string, ReadOnlyKeyValueEntry)> GetKeyValueByRange(string prefix, string? startKey, int limit) =>
            inner.GetKeyValueByRange(prefix, startKey, limit);
        public bool PruneKeyValueRevisions(IReadOnlyCollection<string>? keys, int retentionCount, TimeSpan retentionAge, int batchSize, HLCTimestamp floorTimestamp, out RevisionPruneResult result) =>
            inner.PruneKeyValueRevisions(keys, retentionCount, retentionAge, batchSize, floorTimestamp, out result);
        public Kahuna.Server.Persistence.Pitr.CheckpointResult CreateCheckpoint(string destinationPath, long appliedIndex, HLCTimestamp appliedTime) =>
            inner.CreateCheckpoint(destinationPath, appliedIndex, appliedTime);
        public void Dispose() => inner.Dispose();
    }

    private static KeyValueRequest MakeGet(
        string key,
        KeyValueDurability durability,
        HLCTimestamp transactionId = default,
        long compareRevision = -1,
        HLCTimestamp readTimestamp = default)
    {
        return new KeyValueRequest(
            KeyValueRequestType.TryGet,
            transactionId,
            HLCTimestamp.Zero,
            key,
            null,
            null,
            compareRevision,
            KeyValueFlags.None,
            0,
            HLCTimestamp.Zero,
            durability,
            0,
            0,
            null
        ) { ReadTimestamp = readTimestamp };
    }

    private static (RaftManager Raft, FairReadScheduler Scheduler, KahunaConfiguration Config, ILogger<IKahuna> Logger)
        CreateRaftAndConfig(string nodeName)
    {
        KahunaConfiguration config = ConfigurationValidator.Validate(new()
        {
            LocksWorkers = 1,
            KeyValueWorkers = 1,
            BackgroundWriterWorkers = 1,
            Storage = "memory",
            CacheEntryTtl = TimeSpan.FromMinutes(5),
            CacheEntriesToRemove = 1000,
            MaxEntriesPerActor = 50_000,
            MaxBytesPerActor = 256L * 1024 * 1024,
            CollectBatchMax = 1000,
            RevisionRetention = 16
        });

        ILogger<IKahuna> logger = NullLogger<IKahuna>.Instance;
        ILogger<IRaft> raftLogger = NullLogger<IRaft>.Instance;

        RaftManager raft = new(
            new RaftConfiguration
            {
                NodeName = nodeName,
                NodeId = 1,
                Host = "localhost",
                Port = 0,
                InitialPartitions = 1,
                EnableQuiescence = false
            },
            new StaticDiscovery([]),
            new InMemoryWAL(raftLogger),
            new InMemoryCommunication(),
            new HybridLogicalClock(),
            raftLogger
        );

        return (raft, (FairReadScheduler)raft.ReadScheduler, config, logger);
    }

    private static (TryGetHandler, KeyValueContext, RaftManager) CreateHandler()
    {
        KahunaConfiguration config = ConfigurationValidator.Validate(new()
        {
            LocksWorkers = 1,
            KeyValueWorkers = 1,
            BackgroundWriterWorkers = 1,
            Storage = "memory",
            CacheEntryTtl = TimeSpan.FromMinutes(5),
            CacheEntriesToRemove = 1000,
            MaxEntriesPerActor = 50_000,
            MaxBytesPerActor = 256L * 1024 * 1024,
            CollectBatchMax = 1000,
            RevisionRetention = 16
        });

        BTree<string, KeyValueEntry> store = new(32);
        ILogger<IKahuna> logger = NullLogger<IKahuna>.Instance;
        ILogger<IRaft> raftLogger = NullLogger<IRaft>.Instance;

        RaftManager raft = new(
            new RaftConfiguration
            {
                NodeName = "tryget-test",
                NodeId = 1,
                Host = "localhost",
                Port = 0,
                InitialPartitions = 1,
                EnableQuiescence = false
            },
            new StaticDiscovery([]),
            new InMemoryWAL(raftLogger),
            new InMemoryCommunication(),
            new HybridLogicalClock(),
            raftLogger
        );

        MemoryPersistenceBackend backend = new();

        KeyValueContext context = new(
            null!,   // actorContext — only accessed by the detach path; not exercised here
            store,
            new Dictionary<string, KeyValueWriteIntent>(),
            new Dictionary<string, List<KeyValueRangeLock>>(),
            new Dictionary<int, KeyValueProposal>(),
            null!,   // backgroundWriter
            null!,   // proposalRouter
            backend,
            raft,
            new KeySpaceRegistry(),
            new RangeMapStore(raft, null, null, logger),
            config,
            logger
        );

        return (new TryGetHandler(context), context, raft);
    }
}
