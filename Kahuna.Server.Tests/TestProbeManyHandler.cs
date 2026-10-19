using System.Collections.Concurrent;
using System.Text;
using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Shared.KeyValue;
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
/// Batched probe at the actor: a real spawned <see cref="KeyValueActor"/> over a started read scheduler and a backend
/// that records every read it serves. Each answer is compared with what single <c>TryGet</c> requests answer against
/// an identically seeded twin actor, so a probe can never answer a key differently from a single read of it.
/// </summary>
public sealed class TestProbeManyHandler : RaftTrackingTest
{
    private const string Space = "probe";

    private static string Key(int i) => $"{Space}/k{i:D3}";

    // ── Resident and non-resident batches ─────────────────────────────────────────────────

    [Fact]
    public async Task NonResidentBatch_IsOneBackendBatchRead_AndThenResident()
    {
        await using ProbeRig rig = ProbeRig.Create(this, "probe-misses");
        for (int i = 0; i < 10; i++)
            rig.Backend.Seed(Key(i), $"v{i}", revision: 1);

        string[] keys = [.. Enumerable.Range(0, 15).Select(Key)];

        KeyValueResponse first = await rig.Probe(keys);

        Assert.NotNull(first.ProbeResults);
        Assert.Equal(15, first.ProbeResults!.Length);
        for (int i = 0; i < 10; i++)
        {
            Assert.Equal(KeyValueResponseType.Get, first.ProbeResults[i].Type);
            Assert.Equal($"v{i}", Encoding.UTF8.GetString(first.ProbeResults[i].Entry!.Value!));
        }
        for (int i = 10; i < 15; i++)
            Assert.Equal(KeyValueResponseType.DoesNotExist, first.ProbeResults[i].Type);

        // Every miss went to the backend in one batched read; no key was read on its own.
        Assert.Single(rig.Backend.BatchReads);
        Assert.Equal(15, rig.Backend.BatchReads.Single().Length);
        Assert.Equal(0, rig.Backend.SingleReads);

        // The heads it loaded are resident now: the same probe touches the backend no more.
        KeyValueResponse second = await rig.Probe(keys[..10]);
        Assert.All(second.ProbeResults!, r => Assert.Equal(KeyValueResponseType.Get, r.Type));
        Assert.Single(rig.Backend.BatchReads);
    }

    [Fact]
    public async Task EveryKey_AnswersWhatASingleGetAnswers()
    {
        await using ProbeRig probeRig = ProbeRig.Create(this, "probe-twin-a");
        await using ProbeRig singleRig = ProbeRig.Create(this, "probe-twin-b");

        HLCTimestamp past = new(0, 1_000, 0);
        foreach (ProbeRig rig in new[] { probeRig, singleRig })
        {
            rig.Backend.Seed(Key(0), "live", revision: 3);
            rig.Backend.Seed(Key(1), null, revision: 4, state: KeyValueState.Deleted);
            rig.Backend.Seed(Key(2), "expired", revision: 2, expires: past);
            rig.Backend.Seed(Key(3), "", revision: 1);
        }

        string[] keys = [Key(0), Key(1), Key(2), Key(3), Key(4)];

        await AssertSameAsSingleGets(probeRig, singleRig, keys, HLCTimestamp.Zero, HLCTimestamp.Zero);

        // Resident now on both: the inline answers must agree too.
        await AssertSameAsSingleGets(probeRig, singleRig, keys, HLCTimestamp.Zero, HLCTimestamp.Zero);
    }

    [Fact]
    public async Task TransactionalLatestBatch_PreloadsMissingHeadsInOneRead_AndAnswersAsSingleGets()
    {
        await using ProbeRig probeRig = ProbeRig.Create(this, "probe-tx-a");
        await using ProbeRig singleRig = ProbeRig.Create(this, "probe-tx-b");

        foreach (ProbeRig rig in new[] { probeRig, singleRig })
            for (int i = 0; i < 6; i++)
                rig.Backend.Seed(Key(i), $"tx{i}", revision: 2);

        string[] keys = [.. Enumerable.Range(0, 8).Select(Key)];
        HLCTimestamp txId = probeRig.Now();

        await AssertSameAsSingleGets(probeRig, singleRig, keys, txId, HLCTimestamp.Zero);

        // The transactional read loads every missing head with one batched read inside the turn.
        Assert.Single(probeRig.Backend.BatchReads);
        Assert.Equal(8, probeRig.Backend.BatchReads.Single().Length);

        // The read pinned an MVCC view for the transaction, as a single transactional read does: reading again in
        // the same transaction answers the pinned revisions without a backend read.
        KeyValueResponse again = await probeRig.Probe(keys, transactionId: txId);
        for (int i = 0; i < 6; i++)
            Assert.Equal(2L, again.ProbeResults![i].Entry!.Revision);
        Assert.Single(probeRig.Backend.BatchReads);
    }

    [Fact]
    public async Task SnapshotBatch_AnswersAsSingleSnapshotGets()
    {
        await using ProbeRig probeRig = ProbeRig.Create(this, "probe-snap-a");
        await using ProbeRig singleRig = ProbeRig.Create(this, "probe-snap-b");

        HLCTimestamp t1 = new(0, 10_000, 0);
        HLCTimestamp t2 = new(0, 20_000, 0);
        HLCTimestamp between = new(0, 15_000, 0);

        foreach (ProbeRig rig in new[] { probeRig, singleRig })
        {
            // k0: revision 0 at t1, revision 1 at t2 — the snapshot between them must answer revision 0.
            rig.Backend.Seed(Key(0), "old", revision: 0, lastModified: t1);
            rig.Backend.Seed(Key(0), "new", revision: 1, lastModified: t2);
            // k1: created after the snapshot.
            rig.Backend.Seed(Key(1), "late", revision: 0, lastModified: t2);
            // k2: unchanged since before it.
            rig.Backend.Seed(Key(2), "stable", revision: 0, lastModified: t1);
        }

        string[] keys = [Key(0), Key(1), Key(2), Key(3)];

        await AssertSameAsSingleGets(probeRig, singleRig, keys, HLCTimestamp.Zero, between);

        KeyValueResponse probe = await probeRig.Probe(keys, readTimestamp: between);
        Assert.Equal("old", Encoding.UTF8.GetString(probe.ProbeResults![0].Entry!.Value!));
        Assert.Equal(KeyValueResponseType.DoesNotExist, probe.ProbeResults[1].Type);
        Assert.Equal("stable", Encoding.UTF8.GetString(probe.ProbeResults[2].Entry!.Value!));
    }

    // ── Coalescing with single reads in both directions ───────────────────────────────────

    [Fact]
    public async Task SingleReadArrivingDuringProbe_JoinsTheProbesRead()
    {
        await using ProbeRig rig = ProbeRig.Create(this, "probe-join-single");
        rig.Backend.Seed(Key(0), "a", revision: 1);
        rig.Backend.Seed(Key(1), "b", revision: 1);
        rig.Backend.Hold();

        Task<KeyValueResponse> probe = rig.Probe([Key(0), Key(1)]);
        Assert.True(rig.Backend.Entered.Wait(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken), "the probe's batched read never started");

        Task<KeyValueResponse?> single = rig.Get(Key(1));

        // Give the single read time to reach the actor and attach before the backend answers.
        await Task.Delay(100, TestContext.Current.CancellationToken);
        rig.Backend.Gate.Set();

        KeyValueResponse probed = await probe;
        KeyValueResponse? read = await single;

        Assert.Equal(KeyValueResponseType.Get, probed.ProbeResults![1].Type);
        Assert.Equal(KeyValueResponseType.Get, read!.Type);
        Assert.Equal("b", Encoding.UTF8.GetString(read.Entry!.Value!));

        // One backend read served both: the single read joined the probe's.
        Assert.Single(rig.Backend.BatchReads);
    }

    [Fact]
    public async Task ProbeArrivingDuringSingleRead_JoinsThatRead()
    {
        await using ProbeRig rig = ProbeRig.Create(this, "probe-join-probe");
        rig.Backend.Seed(Key(0), "a", revision: 1);
        rig.Backend.Seed(Key(1), "b", revision: 1);
        rig.Backend.Hold();

        Task<KeyValueResponse?> single = rig.Get(Key(0));
        Assert.True(rig.Backend.Entered.Wait(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken), "the single read never started");

        Task<KeyValueResponse> probe = rig.Probe([Key(0), Key(1)]);
        await Task.Delay(100, TestContext.Current.CancellationToken);
        rig.Backend.Gate.Set();

        KeyValueResponse probed = await probe;
        Assert.Equal("a", Encoding.UTF8.GetString(probed.ProbeResults![0].Entry!.Value!));
        Assert.Equal("b", Encoding.UTF8.GetString(probed.ProbeResults[1].Entry!.Value!));
        Assert.Equal(KeyValueResponseType.Get, (await single)!.Type);

        // The probe read only the key that had no read in flight.
        Assert.Equal(2, rig.Backend.BatchReads.Count);
        Assert.Contains(rig.Backend.BatchReads, batch => batch.SequenceEqual([Key(1)]));
        Assert.DoesNotContain(rig.Backend.BatchReads, batch => batch.Length == 2);
    }

    [Fact]
    public async Task HungBackendRead_ExpiresEveryWaitingKey_AndTheJoinedSingleRead()
    {
        await using ProbeRig rig = ProbeRig.Create(this, "probe-expire", readTimeoutMs: 200);
        rig.Backend.Seed(Key(0), "a", revision: 1);

        // Key 1 is resident: it answers inline whatever happens to the read of key 0.
        await rig.Set(Key(1), "resident");
        rig.Backend.Hold();

        Task<KeyValueResponse> probe = rig.Probe([Key(0), Key(1)]);
        Assert.True(rig.Backend.Entered.Wait(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken));
        Task<KeyValueResponse?> single = rig.Get(Key(0));
        await Task.Delay(400, TestContext.Current.CancellationToken);

        rig.Actor.Send(new KeyValueRequest(KeyValueRequestType.Collect));

        KeyValueResponse probed = await probe.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        Assert.Equal(KeyValueResponseType.MustRetry, probed.ProbeResults![0].Type);
        Assert.Equal(KeyValueResponseType.Get, probed.ProbeResults[1].Type);
        Assert.Equal(KeyValueResponseType.MustRetry, (await single.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken))!.Type);

        rig.Backend.Gate.Set();
    }

    // ── Refusals ──────────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task MixedKeySpaces_OversizedBatch_AndLockWithoutTransaction_AreRefusedWhole()
    {
        await using ProbeRig rig = ProbeRig.Create(this, "probe-refuse", maxProbe: 4);

        Assert.Equal(KeyValueResponseType.InvalidInput, (await rig.Probe([Key(0), "other/k1"])).Type);
        Assert.Equal(KeyValueResponseType.InvalidInput, (await rig.Probe([Key(0), $"{Space}/deeper/k1"])).Type);
        Assert.Equal(KeyValueResponseType.InvalidInput, (await rig.Probe([.. Enumerable.Range(0, 5).Select(Key)])).Type);
        Assert.Equal(KeyValueResponseType.InvalidInput, (await rig.Probe([Key(0)], lockMode: KeyValueProbeLock.Shared)).Type);
        Assert.Equal(KeyValueResponseType.InvalidInput, (await rig.Probe([])).Type);

        Assert.Null((await rig.Probe([Key(0), "other/k1"])).ProbeResults);
        Assert.Empty(rig.Backend.BatchReads);
    }

    // ── Shared lock per key ───────────────────────────────────────────────────────────────

    [Fact]
    public async Task SharedLocks_AreOrdinaryPointRangeLocks()
    {
        await using ProbeRig rig = ProbeRig.Create(this, "probe-locks");
        rig.Backend.Seed(Key(0), "a", revision: 1);
        HLCTimestamp tx = rig.Now();

        KeyValueResponse probe = await rig.Probe([Key(0), Key(1)], transactionId: tx, lockMode: KeyValueProbeLock.Shared);

        Assert.All(probe.ProbeResults!, r => Assert.True(r.Locked));
        Assert.Equal(KeyValueResponseType.Get, probe.ProbeResults![0].Type);
        Assert.Equal(KeyValueResponseType.DoesNotExist, probe.ProbeResults[1].Type);

        List<KeyValueRangeLock> locks = await rig.RangeLocks();
        Assert.Equal(2, locks.Count);
        Assert.All(locks, l =>
        {
            Assert.Equal(tx, l.TransactionId);
            Assert.Equal(RangeLockMode.Shared, l.Mode);
            Assert.True(l.StartInclusive && l.EndInclusive);
            Assert.Equal(l.StartKey, l.EndKey);
        });

        // Released by bounds like any [k,k] lock.
        Assert.Equal(KeyValueResponseType.Unlocked, (await rig.ReleaseRangeLock(tx, Key(0))).Type);
        Assert.Single(await rig.RangeLocks());

        // A second Shared transaction coexists.
        HLCTimestamp other = rig.Now();
        KeyValueResponse shared = await rig.Probe([Key(1)], transactionId: other, lockMode: KeyValueProbeLock.Shared);
        Assert.True(shared.ProbeResults![0].Locked);
    }

    [Fact]
    public async Task DenialInTheMiddle_StopsTheBatch_AndKeepsEarlierLocks()
    {
        await using ProbeRig rig = ProbeRig.Create(this, "probe-deny");
        HLCTimestamp holder = rig.Now();
        HLCTimestamp tx = rig.Now();

        Assert.Equal(KeyValueResponseType.Locked, (await rig.AcquireRangeLock(holder, Key(2), RangeLockMode.Exclusive)).Type);

        KeyValueResponse probe = await rig.Probe([Key(0), Key(1), Key(2), Key(3), Key(4)], transactionId: tx, lockMode: KeyValueProbeLock.Shared);
        KeyValueProbeResult[] results = probe.ProbeResults!;

        Assert.Equal(5, results.Length);
        Assert.True(results[0].Locked && results[1].Locked);
        Assert.Equal(KeyValueResponseType.DoesNotExist, results[0].Type);

        Assert.Equal(KeyValueResponseType.AlreadyLocked, results[2].Type);
        Assert.Equal(holder, results[2].HolderTransactionId);
        Assert.False(results[2].Locked);
        Assert.Null(results[2].Entry);

        Assert.All(results[3..], r =>
        {
            Assert.Equal(KeyValueResponseType.MustRetry, r.Type);
            Assert.False(r.Locked);
            Assert.Null(r.Entry);
        });

        List<KeyValueRangeLock> held = (await rig.RangeLocks()).Where(l => l.TransactionId == tx).ToList();
        Assert.Equal([Key(0), Key(1)], held.Select(l => l.StartKey!).Order(StringComparer.Ordinal));
    }

    [Fact]
    public async Task LockAndRead_AreOneTurn_SoALaterForeignWriteIsRefused()
    {
        await using ProbeRig rig = ProbeRig.Create(this, "probe-atomic");
        await rig.Set(Key(0), "before");
        HLCTimestamp tx = rig.Now();

        KeyValueResponse probe = await rig.Probe([Key(0)], transactionId: tx, lockMode: KeyValueProbeLock.Shared);
        Assert.True(probe.ProbeResults![0].Locked);
        Assert.Equal("before", Encoding.UTF8.GetString(probe.ProbeResults[0].Entry!.Value!));

        // A foreign transactional write into the locked key is refused, so the value read stays the one locked.
        HLCTimestamp writer = rig.Now();
        KeyValueResponse? write = await rig.Set(Key(0), "after", writer);
        Assert.NotEqual(KeyValueResponseType.Set, write!.Type);

        KeyValueResponse? read = await rig.Get(Key(0));
        Assert.Equal("before", Encoding.UTF8.GetString(read!.Entry!.Value!));
    }

    [Fact]
    public async Task SingleRangeLock_OverAHeadLoadedFromTheBackend_RefusesAForeignWrite()
    {
        await using ProbeRig rig = ProbeRig.Create(this, "probe-loaded-bucket");

        // The head becomes resident through a read of the backend row, not through a write on this node.
        await rig.Set(Key(0), "on-disk");

        HLCTimestamp reader = rig.Now();
        Assert.Equal(KeyValueResponseType.Locked, (await rig.AcquireRangeLock(reader, Key(0), RangeLockMode.Shared)).Type);

        KeyValueResponse? write = await rig.Set(Key(0), "after", rig.Now());
        Assert.Equal(KeyValueResponseType.MustRetry, write!.Type);
    }

    [Fact]
    public async Task ReentryOnlyReplay_RenewsHeldLocks_AndNeverTakesANewOne()
    {
        await using ProbeRig rig = ProbeRig.Create(this, "probe-reentry");
        HLCTimestamp tx = rig.Now();

        KeyValueResponse first = await rig.Probe([Key(0), Key(1)], transactionId: tx, lockMode: KeyValueProbeLock.Shared);
        Assert.All(first.ProbeResults!, r => Assert.True(r.Locked));

        KeyValueResponse replay = await rig.Probe([Key(0), Key(1), Key(2)], transactionId: tx, lockMode: KeyValueProbeLock.SharedReentryOnly);
        KeyValueProbeResult[] results = replay.ProbeResults!;

        Assert.True(results[0].Locked && results[1].Locked);
        Assert.Equal(KeyValueResponseType.DoesNotExist, results[0].Type);
        Assert.Equal(KeyValueResponseType.MustRetry, results[2].Type);
        Assert.False(results[2].Locked);

        Assert.DoesNotContain(await rig.RangeLocks(), l => l.StartKey == Key(2));
    }

    // ── Helpers ───────────────────────────────────────────────────────────────────────────

    private static async Task AssertSameAsSingleGets(ProbeRig probeRig, ProbeRig singleRig, string[] keys, HLCTimestamp transactionId, HLCTimestamp readTimestamp)
    {
        KeyValueResponse probe = await probeRig.Probe(keys, transactionId: transactionId, readTimestamp: readTimestamp);
        Assert.NotNull(probe.ProbeResults);

        for (int i = 0; i < keys.Length; i++)
        {
            KeyValueResponse? single = await singleRig.Get(keys[i], transactionId, readTimestamp);
            KeyValueProbeResult probed = probe.ProbeResults![i];

            Assert.Equal(single!.Type, probed.Type);
            Assert.Equal(single.Entry is null, probed.Entry is null);

            if (single.Entry is null)
                continue;

            Assert.Equal(single.Entry.Revision, probed.Entry!.Revision);
            Assert.Equal(single.Entry.State, probed.Entry.State);
            Assert.Equal(single.Entry.Expires, probed.Entry.Expires);
            Assert.Equal(single.Entry.LastModified, probed.Entry.LastModified);
            Assert.Equal(single.Entry.Value is null, probed.Entry.Value is null);
            if (single.Entry.Value is not null)
                Assert.Equal(single.Entry.Value, probed.Entry.Value);
        }
    }

    /// <summary>A spawned actor over a started read scheduler and a recording backend.</summary>
    private sealed class ProbeRig : IAsyncDisposable
    {
        private readonly IDisposable actorSystemLifetime;
        private readonly FairReadScheduler scheduler;

        public RaftManager Raft { get; }
        public RecordingBackend Backend { get; }
        public IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> Actor { get; }

        private ProbeRig(RaftManager raft, FairReadScheduler scheduler, RecordingBackend backend, IDisposable lifetime, IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> actor)
        {
            Raft = raft;
            this.scheduler = scheduler;
            Backend = backend;
            actorSystemLifetime = lifetime;
            Actor = actor;
        }

        public static ProbeRig Create(TestProbeManyHandler test, string name, int readTimeoutMs = 30_000, int maxProbe = 512)
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
                RevisionRetention = 16,
                ReadContinuationTimeout = readTimeoutMs,
                MaxProbeBatchSize = maxProbe
            });

            ILogger<IKahuna> logger = NullLogger<IKahuna>.Instance;
            ILogger<IRaft> raftLogger = NullLogger<IRaft>.Instance;

            RaftManager raft = test.Track(new RaftManager(
                new RaftConfiguration
                {
                    NodeName = name,
                    NodeId = 1,
                    Host = "localhost",
                    Port = 0,
                    InitialPartitions = 1,
                    EnableQuiescence = false,
                    PartitionExecutorPoolSize = 1
                },
                new StaticDiscovery([]),
                new InMemoryWAL(raftLogger),
                new InMemoryCommunication(),
                new HybridLogicalClock(),
                raftLogger
            ));

            FairReadScheduler scheduler = (FairReadScheduler)raft.ReadScheduler;
            scheduler.Start();

            RecordingBackend backend = new();
            IDisposable lifetime = TestActorSystemLifetime.Create(out ActorSystem actorSystem);

            IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> actor =
                actorSystem.Spawn<KeyValueActor, KeyValueRequest, KeyValueResponse>(
                    name + "-actor", null!, null!, backend, raft, raft.ReadScheduler,
                    new KeySpaceRegistry(), new RangeMapStore(raft, null, null, logger), config, logger);

            return new(raft, scheduler, backend, lifetime, actor);
        }

        public HLCTimestamp Now() => Raft.HybridLogicalClock.TrySendOrLocalEvent(Raft.GetLocalNodeId());

        public async Task<KeyValueResponse> Probe(
            string[] keys,
            HLCTimestamp transactionId = default,
            HLCTimestamp readTimestamp = default,
            KeyValueProbeLock lockMode = KeyValueProbeLock.None)
        {
            KeyValueRequest request = new(
                KeyValueRequestType.TryProbeMany, transactionId, HLCTimestamp.Zero, keys.Length > 0 ? keys[0] : $"{Space}/none",
                null, null, -1, KeyValueFlags.None, 0, HLCTimestamp.Zero, KeyValueDurability.Persistent, 0, 0, default)
            {
                ReadTimestamp = readTimestamp,
                ProbeKeys = keys,
                ProbeLock = lockMode
            };

            KeyValueResponse? response = await Actor.Ask(request, TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);
            Assert.NotNull(response);
            return response!;
        }

        public Task<KeyValueResponse?> Get(string key, HLCTimestamp transactionId = default, HLCTimestamp readTimestamp = default) =>
            Actor.Ask(new KeyValueRequest(
                KeyValueRequestType.TryGet, transactionId, HLCTimestamp.Zero, key, null, null, -1,
                KeyValueFlags.None, 0, HLCTimestamp.Zero, KeyValueDurability.Persistent, 0, 0, default)
            {
                ReadTimestamp = readTimestamp
            }, TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);

        /// <summary>Makes a key resident without the proposal path: an ephemeral-style direct insert is not
        /// available through the actor, so the head is written to the backend and loaded by a read.</summary>
        public async Task<KeyValueResponse?> Set(string key, string value, HLCTimestamp transactionId = default)
        {
            if (transactionId == HLCTimestamp.Zero)
            {
                Backend.Seed(key, value, revision: 1);
                return await Get(key);
            }

            return await Actor.Ask(new KeyValueRequest(
                KeyValueRequestType.TrySet, transactionId, HLCTimestamp.Zero, key, Encoding.UTF8.GetBytes(value), null, -1,
                KeyValueFlags.Set, 0, HLCTimestamp.Zero, KeyValueDurability.Persistent, 0, 0, default),
                TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);
        }

        public async Task<KeyValueResponse> AcquireRangeLock(HLCTimestamp transactionId, string key, RangeLockMode mode)
        {
            KeyValueRequest request = new(
                KeyValueRequestType.TryAcquireExclusiveRangeLock, transactionId, HLCTimestamp.Zero, Space, null, null, -1,
                KeyValueFlags.None, 0, HLCTimestamp.Zero, KeyValueDurability.Persistent, 0, 0, default)
            {
                StartKey = key, StartInclusive = true, EndKey = key, EndInclusive = true, RangeLockMode = mode
            };
            return (await Actor.Ask(request, TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken))!;
        }

        public async Task<KeyValueResponse> ReleaseRangeLock(HLCTimestamp transactionId, string key)
        {
            KeyValueRequest request = new(
                KeyValueRequestType.TryReleaseExclusiveRangeLock, transactionId, HLCTimestamp.Zero, Space, null, null, -1,
                KeyValueFlags.None, 0, HLCTimestamp.Zero, KeyValueDurability.Persistent, 0, 0, default)
            {
                StartKey = key, StartInclusive = true, EndKey = key, EndInclusive = true
            };
            return (await Actor.Ask(request, TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken))!;
        }

        public async Task<List<KeyValueRangeLock>> RangeLocks()
        {
            KeyValueRequest request = new(
                KeyValueRequestType.GetRangeLocks, HLCTimestamp.Zero, HLCTimestamp.Zero, Space, null, null, -1,
                KeyValueFlags.None, 0, HLCTimestamp.Zero, KeyValueDurability.Persistent, 0, 0, default);
            KeyValueResponse? response = await Actor.Ask(request, TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);
            return response?.RangeLockList ?? [];
        }

        public ValueTask DisposeAsync()
        {
            Backend.Gate.Set();
            scheduler.Stop();
            actorSystemLifetime.Dispose();
            Backend.Dispose();
            return ValueTask.CompletedTask;
        }
    }

    /// <summary>
    /// Memory backend that records every point read: each batched read with its keys, and each single read. Its
    /// point reads can be held at a gate, and it signals when one has entered.
    /// </summary>
    private sealed class RecordingBackend : IPersistenceBackend, IDisposable
    {
        private readonly MemoryPersistenceBackend inner = new();
        private readonly ConcurrentQueue<string[]> batchReads = new();
        private int singleReads;

        public ManualResetEventSlim Gate { get; } = new(true);
        public ManualResetEventSlim Entered { get; } = new(false);

        /// <summary>Holds every point read from now on at the gate, and forgets reads that entered before.</summary>
        public void Hold()
        {
            Gate.Reset();
            Entered.Reset();
        }
        public IReadOnlyCollection<string[]> BatchReads => batchReads;
        public int SingleReads => singleReads;

        public void Seed(string key, string? value, long revision, KeyValueState state = KeyValueState.Set,
            HLCTimestamp lastModified = default, HLCTimestamp expires = default)
        {
            HLCTimestamp modified = lastModified == HLCTimestamp.Zero ? new HLCTimestamp(0, 1_000 + revision, 0) : lastModified;
            inner.StoreKeyValues(
            [
                new PersistenceRequestItem(
                    key: key,
                    value: value is null ? null : Encoding.UTF8.GetBytes(value),
                    revision: revision,
                    expiresNode: expires.N, expiresPhysical: expires.L, expiresCounter: expires.C,
                    lastUsedNode: modified.N, lastUsedPhysical: modified.L, lastUsedCounter: modified.C,
                    lastModifiedNode: modified.N, lastModifiedPhysical: modified.L, lastModifiedCounter: modified.C,
                    state: (int)state)
            ]);
        }

        public KeyValueEntry? GetKeyValue(string keyName)
        {
            Interlocked.Increment(ref singleReads);
            Entered.Set();
            Gate.Wait();
            return inner.GetKeyValue(keyName);
        }

        public KeyValueEntry?[] GetKeyValues(string[] keyNames)
        {
            batchReads.Enqueue(keyNames);
            Entered.Set();
            Gate.Wait();
            return ((IPersistenceBackend)inner).GetKeyValues(keyNames);
        }

        public KeyValueHydration GetKeyValueWithRecentRevisions(string keyName, int recentRevisions) =>
            ((IPersistenceBackend)inner).GetKeyValueWithRecentRevisions(keyName, recentRevisions);

        public KeyValueEntry? GetKeyValueRevisionAtOrBefore(string keyName, long maxRevision, HLCTimestamp readTimestamp) =>
            inner.GetKeyValueRevisionAtOrBefore(keyName, maxRevision, readTimestamp);

        public bool StoreLocks(List<PersistenceRequestItem> items) => inner.StoreLocks(items);
        public bool StoreKeyValues(List<PersistenceRequestItem> items) => inner.StoreKeyValues(items);
        public Kahuna.Server.Locks.Data.LockEntry? GetLock(string resource) => inner.GetLock(resource);
        public KeyValueEntry? GetKeyValueRevision(string keyName, long revision) => inner.GetKeyValueRevision(keyName, revision);
        public List<(string, ReadOnlyKeyValueEntry)> GetKeyValueByPrefix(string prefixKeyName) => inner.GetKeyValueByPrefix(prefixKeyName);
        public List<(string, ReadOnlyKeyValueEntry)> GetKeyValueByRange(string prefix, string? startKey, int limit) =>
            inner.GetKeyValueByRange(prefix, startKey, limit);
        public bool PruneKeyValueRevisions(IReadOnlyCollection<string>? keys, int retentionCount, TimeSpan retentionAge, int batchSize, HLCTimestamp floorTimestamp, out RevisionPruneResult result) =>
            inner.PruneKeyValueRevisions(keys, retentionCount, retentionAge, batchSize, floorTimestamp, out result);
        public Kahuna.Server.Persistence.Pitr.CheckpointResult CreateCheckpoint(string destinationPath, long appliedIndex, HLCTimestamp appliedTime) =>
            inner.CreateCheckpoint(destinationPath, appliedIndex, appliedTime);
        public void Dispose() => inner.Dispose();
    }
}
