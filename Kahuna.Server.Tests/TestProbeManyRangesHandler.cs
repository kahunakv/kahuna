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
/// Batched range probe at the actor: a real spawned <see cref="KeyValueActor"/> over a started read scheduler and a
/// backend that counts the range pages it serves, run once with a backend that is read off the actor thread and once
/// with one that serves reads from memory. Each seek's answer is compared with what a single <c>GetByRange</c> page
/// over the same bounds answers against an identically seeded twin actor, so a probe can never answer a seek
/// differently from a single page of it.
/// </summary>
public sealed class TestProbeManyRangesHandler : RaftTrackingTest
{
    private const string Space = "rng";

    /// <summary>Row <paramref name="row"/> of group <paramref name="group"/>.</summary>
    private static string Key(int group, int row) => $"{Space}/g{group:D2}-{row:D2}";

    /// <summary>A seek over every row of <paramref name="group"/>: '.' sorts right after '-'.</summary>
    private static KeyValueProbeRange Group(int group, int limit) =>
        new($"{Space}/g{group:D2}-", true, $"{Space}/g{group:D2}.", false, limit);

    private static void SeedGroups(ProbeRig rig, int groups, int rows)
    {
        for (int g = 0; g < groups; g++)
            for (int r = 0; r < rows; r++)
                rig.Backend.Seed(Key(g, r), $"v{g}-{r}", revision: 1);
    }

    // ── Disk pages: one scheduler operation for the batch ─────────────────────────────────

    [Fact]
    public async Task DiskSeeks_AreOneSchedulerRead_AndAnswerAsSinglePages()
    {
        await using ProbeRig probeRig = ProbeRig.Create(this, "rng-disk-a", servesFromMemory: false);
        await using ProbeRig singleRig = ProbeRig.Create(this, "rng-disk-b", servesFromMemory: false);

        foreach (ProbeRig rig in new[] { probeRig, singleRig })
            SeedGroups(rig, groups: 8, rows: 11);

        KeyValueProbeRange[] seeks = [.. Enumerable.Range(0, 8).Select(g => Group(g, 11))];

        long probeReads = probeRig.SchedulerReads;
        long singleReads = singleRig.SchedulerReads;

        await AssertSameAsSinglePages(probeRig, singleRig, seeks, HLCTimestamp.Zero, HLCTimestamp.Zero);

        // The probe read every seek's page in one scheduler operation; single pages took one each.
        await AssertSchedulerReads(probeRig, probeReads + 1);
        await AssertSchedulerReads(singleRig, singleReads + seeks.Length);
        Assert.Equal(seeks.Length, probeRig.Backend.RangeReads);
    }

    [Fact]
    public async Task MemoryBackendSeeks_AnswerInTheTurn_WithNoSchedulerRead()
    {
        await using ProbeRig probeRig = ProbeRig.Create(this, "rng-mem-a", servesFromMemory: true);
        await using ProbeRig singleRig = ProbeRig.Create(this, "rng-mem-b", servesFromMemory: true);

        foreach (ProbeRig rig in new[] { probeRig, singleRig })
            SeedGroups(rig, groups: 8, rows: 11);

        KeyValueProbeRange[] seeks = [.. Enumerable.Range(0, 8).Select(g => Group(g, 11))];
        long probeReads = probeRig.SchedulerReads;

        await AssertSameAsSinglePages(probeRig, singleRig, seeks, HLCTimestamp.Zero, HLCTimestamp.Zero);

        await AssertSchedulerReads(probeRig, probeReads);
        Assert.Equal(seeks.Length, probeRig.Backend.RangeReads);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task HasMore_IsExactAtTheLimit(bool servesFromMemory)
    {
        await using ProbeRig probeRig = ProbeRig.Create(this, $"rng-limit-a-{servesFromMemory}", servesFromMemory);
        await using ProbeRig singleRig = ProbeRig.Create(this, $"rng-limit-b-{servesFromMemory}", servesFromMemory);

        foreach (ProbeRig rig in new[] { probeRig, singleRig })
            SeedGroups(rig, groups: 1, rows: 11);

        KeyValueProbeRange[] seeks = [Group(0, 10), Group(0, 11), Group(0, 12), Group(0, 1)];

        KeyValueProbeRangeResult[] results = await AssertSameAsSinglePages(probeRig, singleRig, seeks, HLCTimestamp.Zero, HLCTimestamp.Zero);

        Assert.True(results[0].HasMore);
        Assert.Equal(10, results[0].Items.Count);
        Assert.Equal(Key(0, 9), results[0].ResumeKey);

        Assert.False(results[1].HasMore);
        Assert.Equal(11, results[1].Items.Count);
        Assert.Null(results[1].ResumeKey);

        Assert.False(results[2].HasMore);
        Assert.Equal(11, results[2].Items.Count);

        Assert.True(results[3].HasMore);
        Assert.Equal(Key(0, 0), results[3].ResumeKey);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task SnapshotSeeks_AnswerAsSingleSnapshotPages(bool servesFromMemory)
    {
        await using ProbeRig probeRig = ProbeRig.Create(this, $"rng-snap-a-{servesFromMemory}", servesFromMemory);
        await using ProbeRig singleRig = ProbeRig.Create(this, $"rng-snap-b-{servesFromMemory}", servesFromMemory);

        HLCTimestamp t1 = new(0, 10_000, 0);
        HLCTimestamp t2 = new(0, 20_000, 0);
        HLCTimestamp between = new(0, 15_000, 0);

        foreach (ProbeRig rig in new[] { probeRig, singleRig })
        {
            // Row 0: revision 0 at t1, revision 1 at t2 — the snapshot between them answers revision 0.
            rig.Backend.Seed(Key(0, 0), "old", revision: 0, lastModified: t1);
            rig.Backend.Seed(Key(0, 0), "new", revision: 1, lastModified: t2);
            // Row 1: created after the snapshot.
            rig.Backend.Seed(Key(0, 1), "late", revision: 0, lastModified: t2);
            // Row 2: unchanged since before it. Row 3: deleted after it.
            rig.Backend.Seed(Key(0, 2), "stable", revision: 0, lastModified: t1);
            rig.Backend.Seed(Key(0, 3), "gone", revision: 0, lastModified: t1);
            rig.Backend.Seed(Key(0, 3), null, revision: 1, lastModified: t2, state: KeyValueState.Deleted);
            rig.Backend.Seed(Key(1, 0), "other", revision: 0, lastModified: t1);
        }

        KeyValueProbeRange[] seeks = [Group(0, 10), Group(1, 10), Group(2, 10)];

        KeyValueProbeRangeResult[] results = await AssertSameAsSinglePages(probeRig, singleRig, seeks, HLCTimestamp.Zero, between);

        Assert.Equal([Key(0, 0), Key(0, 2), Key(0, 3)], results[0].Items.Select(i => i.Item1));
        Assert.Equal("old", Encoding.UTF8.GetString(results[0].Items[0].Item2.Value!));
        Assert.Equal("gone", Encoding.UTF8.GetString(results[0].Items[2].Item2.Value!));
        Assert.Single(results[1].Items);
        Assert.Empty(results[2].Items);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ResidentAndDiskRows_MergeAsASinglePage_LatestAndTransactional(bool servesFromMemory)
    {
        await using ProbeRig probeRig = ProbeRig.Create(this, $"rng-mix-a-{servesFromMemory}", servesFromMemory);
        await using ProbeRig singleRig = ProbeRig.Create(this, $"rng-mix-b-{servesFromMemory}", servesFromMemory);

        foreach (ProbeRig rig in new[] { probeRig, singleRig })
        {
            SeedGroups(rig, groups: 3, rows: 6);

            // Some rows resident, the rest only on disk.
            await rig.Get(Key(0, 1));
            await rig.Get(Key(1, 4));
            await rig.Get(Key(2, 0));
        }

        KeyValueProbeRange[] seeks = [Group(0, 4), Group(1, 6), Group(2, 3)];

        await AssertSameAsSinglePages(probeRig, singleRig, seeks, HLCTimestamp.Zero, HLCTimestamp.Zero);
        await AssertSameAsSinglePages(probeRig, singleRig, seeks, probeRig.Now(), HLCTimestamp.Zero);
    }

    [Fact]
    public async Task DeadRowsPastTheSharedSlack_AreReadOffTheActor_AndAnswerAsSinglePages()
    {
        await using ProbeRig probeRig = ProbeRig.Create(this, "rng-slack-a", servesFromMemory: true);
        await using ProbeRig singleRig = ProbeRig.Create(this, "rng-slack-b", servesFromMemory: true);

        // Two seeks of 5,000 deleted rows and one live row each: together they pass the inspection slack one turn may
        // spend, so the second is finished off the actor thread.
        foreach (ProbeRig rig in new[] { probeRig, singleRig })
        {
            for (int g = 0; g < 2; g++)
            {
                for (int r = 0; r < 5_000; r++)
                    rig.Backend.Seed($"{Space}/g{g:D2}-{r:D5}", null, revision: 1, state: KeyValueState.Deleted);
                rig.Backend.Seed($"{Space}/g{g:D2}-99999", "live", revision: 1);
            }
        }

        KeyValueProbeRange[] seeks = [Group(0, 1), Group(1, 1)];
        long probeReads = probeRig.SchedulerReads;

        KeyValueProbeRangeResult[] results = await AssertSameAsSinglePages(probeRig, singleRig, seeks, HLCTimestamp.Zero, HLCTimestamp.Zero);

        Assert.All(results, r =>
        {
            Assert.Equal(KeyValueResponseType.Get, r.Type);
            Assert.Equal("live", Encoding.UTF8.GetString(Assert.Single(r.Items).Item2.Value!));
        });

        Assert.True(probeRig.SchedulerReads > probeReads, "the seek past the shared slack was not read off the actor thread");
    }

    [Fact]
    public async Task EphemeralSeeks_AnswerAsSingleEphemeralPages()
    {
        await using ProbeRig rig = ProbeRig.Create(this, "rng-ephemeral", servesFromMemory: false);

        for (int g = 0; g < 3; g++)
            for (int r = 0; r < 4; r++)
                Assert.Equal(KeyValueResponseType.Set, (await rig.SetEphemeral(Key(g, r), $"e{g}-{r}"))!.Type);

        KeyValueProbeRange[] seeks = [Group(0, 4), Group(1, 2), Group(2, 9), Group(3, 5)];

        KeyValueResponse probe = await rig.Probe(seeks, durability: KeyValueDurability.Ephemeral);
        Assert.NotNull(probe.ProbeRangeResults);

        for (int i = 0; i < seeks.Length; i++)
            AssertSamePage(await rig.Page(seeks[i], durability: KeyValueDurability.Ephemeral), probe.ProbeRangeResults![i]);

        Assert.Equal(4, probe.ProbeRangeResults![0].Items.Count);
        Assert.True(probe.ProbeRangeResults[1].HasMore);
        Assert.Empty(probe.ProbeRangeResults[3].Items);
    }

    [Fact]
    public async Task HungDiskPage_ExpiresEveryOpenSeek_AndKeepsItsLock()
    {
        await using ProbeRig rig = ProbeRig.Create(this, "rng-expire", servesFromMemory: false, readTimeoutMs: 200);
        SeedGroups(rig, groups: 2, rows: 3);
        HLCTimestamp tx = rig.Now();

        rig.Backend.Hold();

        Task<KeyValueResponse> probe = rig.Probe([Group(0, 5), Group(1, 5)], transactionId: tx, lockMode: KeyValueProbeLock.Shared);
        Assert.True(rig.Backend.Entered.Wait(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken), "the probe's disk pages never started");
        await Task.Delay(400, TestContext.Current.CancellationToken);

        rig.Actor.Send(new KeyValueRequest(KeyValueRequestType.Collect));

        KeyValueResponse expired = await probe.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        Assert.All(expired.ProbeRangeResults!, r =>
        {
            Assert.Equal(KeyValueResponseType.MustRetry, r.Type);
            Assert.True(r.Locked);
            Assert.Empty(r.Items);
        });

        rig.Backend.Gate.Set();
        Assert.Equal(2, (await rig.RangeLocks()).Count(l => l.TransactionId == tx));
    }

    // ── Refusals ──────────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task MalformedProbes_AreRefusedWhole()
    {
        await using ProbeRig rig = ProbeRig.Create(this, "rng-refuse", servesFromMemory: false, maxProbe: 4, maxEntries: 100);

        Assert.Equal(KeyValueResponseType.InvalidInput, (await rig.Probe([])).Type);
        Assert.Equal(KeyValueResponseType.InvalidInput, (await rig.Probe([Group(0, 0)])).Type);
        Assert.Equal(KeyValueResponseType.InvalidInput, (await rig.Probe([.. Enumerable.Range(0, 5).Select(g => Group(g, 1))])).Type);
        Assert.Equal(KeyValueResponseType.InvalidInput, (await rig.Probe([Group(0, 60), Group(1, 41)])).Type);
        Assert.Equal(KeyValueResponseType.InvalidInput, (await rig.Probe([Group(0, 1)], lockMode: KeyValueProbeLock.Shared)).Type);

        Assert.Null((await rig.Probe([Group(0, 0)])).ProbeRangeResults);
        Assert.Equal(0, rig.Backend.RangeReads);

        // At the bounds exactly, the probe is served.
        Assert.NotNull((await rig.Probe([Group(0, 60), Group(1, 40)])).ProbeRangeResults);
    }

    // ── Shared lock per seek ──────────────────────────────────────────────────────────────

    [Fact]
    public async Task SharedLocks_AreOrdinaryRangeLocksOverTheSeeksBounds()
    {
        await using ProbeRig rig = ProbeRig.Create(this, "rng-locks", servesFromMemory: false);
        SeedGroups(rig, groups: 3, rows: 2);
        HLCTimestamp tx = rig.Now();

        KeyValueProbeRange[] seeks = [Group(0, 5), Group(1, 5), Group(2, 5)];
        KeyValueResponse probe = await rig.Probe(seeks, transactionId: tx, lockMode: KeyValueProbeLock.Shared);

        Assert.All(probe.ProbeRangeResults!, r =>
        {
            Assert.True(r.Locked);
            Assert.Equal(KeyValueResponseType.Get, r.Type);
            Assert.Equal(2, r.Items.Count);
        });

        List<KeyValueRangeLock> locks = await rig.RangeLocks();
        Assert.Equal(3, locks.Count);
        foreach (KeyValueProbeRange seek in seeks)
        {
            Assert.Contains(locks, l => l.TransactionId == tx && l.Mode == RangeLockMode.Shared
                && l.StartKey == seek.StartKey && l.StartInclusive == seek.StartInclusive
                && l.EndKey == seek.EndKey && l.EndInclusive == seek.EndInclusive);
        }

        // A foreign Exclusive acquire over a locked seek is refused, as over any Shared range lock.
        HLCTimestamp other = rig.Now();
        KeyValueResponse refused = await rig.AcquireRangeLock(other, seeks[1], RangeLockMode.Exclusive);
        Assert.Equal(KeyValueResponseType.AlreadyLocked, refused.Type);
        Assert.Equal(tx, refused.HolderTransactionId);

        // Released by bounds like any range lock, after which the foreign acquire is granted.
        Assert.Equal(KeyValueResponseType.Unlocked, (await rig.ReleaseRangeLock(tx, seeks[1])).Type);
        Assert.Equal(KeyValueResponseType.Locked, (await rig.AcquireRangeLock(other, seeks[1], RangeLockMode.Exclusive)).Type);

        // A second Shared transaction coexists with the remaining locks.
        KeyValueResponse shared = await rig.Probe([seeks[0], seeks[2]], transactionId: rig.Now(), lockMode: KeyValueProbeLock.Shared);
        Assert.All(shared.ProbeRangeResults!, r => Assert.True(r.Locked));
    }

    [Fact]
    public async Task DenialInTheMiddle_StopsTheBatch_AndKeepsEarlierLocks()
    {
        await using ProbeRig rig = ProbeRig.Create(this, "rng-deny", servesFromMemory: false);
        SeedGroups(rig, groups: 4, rows: 2);
        HLCTimestamp holder = rig.Now();
        HLCTimestamp tx = rig.Now();

        // The holder locks one row inside seek 1: an overlap is enough to refuse the Shared acquire over the seek.
        KeyValueProbeRange held = new(Key(1, 1), true, Key(1, 1), true, 1);
        Assert.Equal(KeyValueResponseType.Locked, (await rig.AcquireRangeLock(holder, held, RangeLockMode.Exclusive)).Type);

        KeyValueProbeRange[] seeks = [Group(0, 5), Group(1, 5), Group(2, 5), Group(3, 5)];
        KeyValueProbeRangeResult[] results = (await rig.Probe(seeks, transactionId: tx, lockMode: KeyValueProbeLock.Shared)).ProbeRangeResults!;

        Assert.Equal(4, results.Length);
        Assert.True(results[0].Locked);
        Assert.Equal(2, results[0].Items.Count);

        Assert.Equal(KeyValueResponseType.AlreadyLocked, results[1].Type);
        Assert.Equal(holder, results[1].HolderTransactionId);
        Assert.False(results[1].Locked);
        Assert.Empty(results[1].Items);

        Assert.All(results[2..], r =>
        {
            Assert.Equal(KeyValueResponseType.MustRetry, r.Type);
            Assert.False(r.Locked);
            Assert.Empty(r.Items);
        });

        List<KeyValueRangeLock> mine = (await rig.RangeLocks()).Where(l => l.TransactionId == tx).ToList();
        Assert.Equal(seeks[0].StartKey, Assert.Single(mine).StartKey);
    }

    [Fact]
    public async Task LockAndRead_AreOneTurn_SoALaterForeignWriteIsRefused()
    {
        await using ProbeRig rig = ProbeRig.Create(this, "rng-atomic", servesFromMemory: false);
        SeedGroups(rig, groups: 1, rows: 3);

        // The row the writer targets is resident, loaded from the backend.
        await rig.Get(Key(0, 1));

        HLCTimestamp tx = rig.Now();
        KeyValueProbeRangeResult locked = (await rig.Probe([Group(0, 5)], transactionId: tx, lockMode: KeyValueProbeLock.Shared)).ProbeRangeResults![0];
        Assert.True(locked.Locked);
        Assert.Equal("v0-1", Encoding.UTF8.GetString(locked.Items[1].Item2.Value!));

        // A foreign transactional write into the locked bounds is refused, so the rows read stay the ones locked.
        KeyValueResponse? write = await rig.Set(Key(0, 1), "after", rig.Now());
        Assert.Equal(KeyValueResponseType.MustRetry, write!.Type);

        KeyValueResponse? newRow = await rig.Set(Key(0, 7), "inserted", rig.Now());
        Assert.Equal(KeyValueResponseType.MustRetry, newRow!.Type);

        KeyValueGetByRangeResult page = (await rig.Page(Group(0, 5)))!;
        Assert.Equal(3, page.Items.Count);
        Assert.Equal("v0-1", Encoding.UTF8.GetString(page.Items[1].Item2.Value!));
    }

    [Fact]
    public async Task ReentryOnlyReplay_RenewsHeldLocks_AndNeverTakesANewOne()
    {
        await using ProbeRig rig = ProbeRig.Create(this, "rng-reentry", servesFromMemory: false);
        SeedGroups(rig, groups: 3, rows: 2);
        HLCTimestamp tx = rig.Now();

        KeyValueResponse first = await rig.Probe([Group(0, 5), Group(1, 5)], transactionId: tx, lockMode: KeyValueProbeLock.Shared);
        Assert.All(first.ProbeRangeResults!, r => Assert.True(r.Locked));

        // A different limit over the same bounds is the same lock; a seek over other bounds holds none.
        KeyValueProbeRangeResult[] replay = (await rig.Probe(
            [Group(0, 5), Group(1, 1), Group(2, 5)], transactionId: tx, lockMode: KeyValueProbeLock.SharedReentryOnly)).ProbeRangeResults!;

        Assert.True(replay[0].Locked && replay[1].Locked);
        Assert.Equal(2, replay[0].Items.Count);
        Assert.Single(replay[1].Items);
        Assert.Equal(KeyValueResponseType.MustRetry, replay[2].Type);
        Assert.False(replay[2].Locked);
        Assert.Empty(replay[2].Items);

        Assert.DoesNotContain(await rig.RangeLocks(), l => l.StartKey == Group(2, 5).StartKey);
    }

    // ── Helpers ───────────────────────────────────────────────────────────────────────────

    private static async Task<KeyValueProbeRangeResult[]> AssertSameAsSinglePages(
        ProbeRig probeRig, ProbeRig singleRig, KeyValueProbeRange[] seeks, HLCTimestamp transactionId, HLCTimestamp readTimestamp)
    {
        KeyValueResponse probe = await probeRig.Probe(seeks, transactionId: transactionId, readTimestamp: readTimestamp);
        Assert.NotNull(probe.ProbeRangeResults);
        Assert.Equal(seeks.Length, probe.ProbeRangeResults!.Length);

        for (int i = 0; i < seeks.Length; i++)
        {
            KeyValueGetByRangeResult? single = await singleRig.Page(seeks[i], transactionId, readTimestamp);
            AssertSamePage(single, probe.ProbeRangeResults[i]);
        }

        return probe.ProbeRangeResults;
    }

    private static void AssertSamePage(KeyValueGetByRangeResult? single, KeyValueProbeRangeResult probed)
    {
        Assert.NotNull(single);
        Assert.Equal(single!.Type, probed.Type);
        Assert.Equal(single.HasMore, probed.HasMore);
        Assert.Equal(single.Items.Count, probed.Items.Count);

        for (int j = 0; j < single.Items.Count; j++)
        {
            (string key, ReadOnlyKeyValueEntry entry) = single.Items[j];
            (string probedKey, ReadOnlyKeyValueEntry probedEntry) = probed.Items[j];

            Assert.Equal(key, probedKey);
            Assert.Equal(entry.Revision, probedEntry.Revision);
            Assert.Equal(entry.State, probedEntry.State);
            Assert.Equal(entry.Expires, probedEntry.Expires);
            Assert.Equal(entry.LastModified, probedEntry.LastModified);
            Assert.Equal(entry.Value, probedEntry.Value);
        }

        // The resume key is where a single page's cursor resumes.
        if (single.HasMore)
        {
            Assert.True(KeyValueRangeCursor.TryDecode(single.NextCursor!, out string lastKey, out _, out _, out _));
            Assert.Equal(lastKey, probed.ResumeKey);
        }
        else
            Assert.Null(probed.ResumeKey);
    }

    private static async Task AssertSchedulerReads(ProbeRig rig, long expected)
    {
        // The scheduler counts an operation as it finishes, which can trail the answer it produced by a moment.
        for (int i = 0; i < 100 && rig.SchedulerReads < expected; i++)
            await Task.Delay(10, TestContext.Current.CancellationToken);

        await Task.Delay(50, TestContext.Current.CancellationToken);
        Assert.Equal(expected, rig.SchedulerReads);
    }

    /// <summary>A spawned actor over a started read scheduler and a backend that counts its range pages.</summary>
    private sealed class ProbeRig : IAsyncDisposable
    {
        private readonly IDisposable actorSystemLifetime;
        private readonly FairReadScheduler scheduler;

        public RaftManager Raft { get; }
        public RangeBackend Backend { get; }
        public IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> Actor { get; }

        /// <summary>Operations the read scheduler finished: one per disk page round trip off the actor thread.</summary>
        public long SchedulerReads => scheduler.TotalReadsCompleted;

        private ProbeRig(RaftManager raft, FairReadScheduler scheduler, RangeBackend backend, IDisposable lifetime, IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> actor)
        {
            Raft = raft;
            this.scheduler = scheduler;
            Backend = backend;
            actorSystemLifetime = lifetime;
            Actor = actor;
        }

        public static ProbeRig Create(
            TestProbeManyRangesHandler test, string name, bool servesFromMemory, int readTimeoutMs = 30_000, int maxProbe = 512,
            int maxEntries = 65_536)
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
                MaxProbeBatchSize = maxProbe,
                MaxProbeRangeEntries = maxEntries
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

            RangeBackend backend = new(servesFromMemory);
            IDisposable lifetime = TestActorSystemLifetime.Create(out ActorSystem actorSystem);

            IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> actor =
                actorSystem.Spawn<KeyValueActor, KeyValueRequest, KeyValueResponse>(
                    name + "-actor", null!, null!, backend, raft, raft.ReadScheduler,
                    new KeySpaceRegistry(), new RangeMapStore(raft, null, null, logger), config, logger);

            return new(raft, scheduler, backend, lifetime, actor);
        }

        public HLCTimestamp Now() => Raft.HybridLogicalClock.TrySendOrLocalEvent(Raft.GetLocalNodeId());

        public async Task<KeyValueResponse> Probe(
            KeyValueProbeRange[] seeks,
            HLCTimestamp transactionId = default,
            HLCTimestamp readTimestamp = default,
            KeyValueProbeLock lockMode = KeyValueProbeLock.None,
            KeyValueDurability durability = KeyValueDurability.Persistent)
        {
            KeyValueRequest request = new(
                KeyValueRequestType.TryProbeManyRanges, transactionId, HLCTimestamp.Zero, Space,
                null, null, -1, KeyValueFlags.None, 0, HLCTimestamp.Zero, durability, 0, 0, default)
            {
                ReadTimestamp = readTimestamp,
                ProbeRanges = seeks,
                ProbeLock = lockMode
            };

            KeyValueResponse? response = await Actor.Ask(request, TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);
            Assert.NotNull(response);
            return response!;
        }

        /// <summary>One single range page over the seek's bounds and limit.</summary>
        public async Task<KeyValueGetByRangeResult?> Page(
            KeyValueProbeRange seek,
            HLCTimestamp transactionId = default,
            HLCTimestamp readTimestamp = default,
            KeyValueDurability durability = KeyValueDurability.Persistent)
        {
            KeyValueRequest request = new(
                KeyValueRequestType.GetByRange, transactionId, HLCTimestamp.Zero, Space, null, null, -1,
                KeyValueFlags.None, 0, HLCTimestamp.Zero, durability, 0, 0, default)
            {
                StartKey = seek.StartKey,
                StartInclusive = seek.StartInclusive,
                EndKey = seek.EndKey,
                EndInclusive = seek.EndInclusive,
                Limit = seek.Limit,
                ReadTimestamp = readTimestamp
            };

            KeyValueResponse? response = await Actor.Ask(request, TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);
            return response?.RangeResult;
        }

        public Task<KeyValueResponse?> Get(string key) =>
            Actor.Ask(new KeyValueRequest(
                KeyValueRequestType.TryGet, HLCTimestamp.Zero, HLCTimestamp.Zero, key, null, null, -1,
                KeyValueFlags.None, 0, HLCTimestamp.Zero, KeyValueDurability.Persistent, 0, 0, default),
                TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);

        /// <summary>A transactional write: it plants a write intent, which a foreign range lock refuses.</summary>
        public Task<KeyValueResponse?> Set(string key, string value, HLCTimestamp transactionId) =>
            Actor.Ask(new KeyValueRequest(
                KeyValueRequestType.TrySet, transactionId, HLCTimestamp.Zero, key, Encoding.UTF8.GetBytes(value), null, -1,
                KeyValueFlags.Set, 0, HLCTimestamp.Zero, KeyValueDurability.Persistent, 0, 0, default),
                TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);

        public Task<KeyValueResponse?> SetEphemeral(string key, string value) =>
            Actor.Ask(new KeyValueRequest(
                KeyValueRequestType.TrySet, HLCTimestamp.Zero, HLCTimestamp.Zero, key, Encoding.UTF8.GetBytes(value), null, -1,
                KeyValueFlags.Set, 0, HLCTimestamp.Zero, KeyValueDurability.Ephemeral, 0, 0, default),
                TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);

        public async Task<KeyValueResponse> AcquireRangeLock(HLCTimestamp transactionId, KeyValueProbeRange bounds, RangeLockMode mode)
        {
            KeyValueRequest request = new(
                KeyValueRequestType.TryAcquireExclusiveRangeLock, transactionId, HLCTimestamp.Zero, Space, null, null, -1,
                KeyValueFlags.None, 0, HLCTimestamp.Zero, KeyValueDurability.Persistent, 0, 0, default)
            {
                StartKey = bounds.StartKey, StartInclusive = bounds.StartInclusive,
                EndKey = bounds.EndKey, EndInclusive = bounds.EndInclusive, RangeLockMode = mode
            };
            return (await Actor.Ask(request, TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken))!;
        }

        public async Task<KeyValueResponse> ReleaseRangeLock(HLCTimestamp transactionId, KeyValueProbeRange bounds)
        {
            KeyValueRequest request = new(
                KeyValueRequestType.TryReleaseExclusiveRangeLock, transactionId, HLCTimestamp.Zero, Space, null, null, -1,
                KeyValueFlags.None, 0, HLCTimestamp.Zero, KeyValueDurability.Persistent, 0, 0, default)
            {
                StartKey = bounds.StartKey, StartInclusive = bounds.StartInclusive,
                EndKey = bounds.EndKey, EndInclusive = bounds.EndInclusive
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
    /// Memory backend that counts the bounded range pages it serves, and can hold them at a gate. It says it serves
    /// reads from memory only when asked to, so the same rows can be read on the actor thread or off it.
    /// </summary>
    private sealed class RangeBackend(bool servesFromMemory) : IPersistenceBackend, IDisposable
    {
        private readonly MemoryPersistenceBackend inner = new();
        private int rangeReads;

        public ManualResetEventSlim Gate { get; } = new(true);
        public ManualResetEventSlim Entered { get; } = new(false);

        public bool ServesReadsFromMemory => servesFromMemory;

        public int RangeReads => Volatile.Read(ref rangeReads);

        /// <summary>Holds every range page from now on at the gate, and forgets pages that entered before.</summary>
        public void Hold()
        {
            Gate.Reset();
            Entered.Reset();
        }

        public void Seed(string key, string? value, long revision, KeyValueState state = KeyValueState.Set, HLCTimestamp lastModified = default)
        {
            HLCTimestamp modified = lastModified == HLCTimestamp.Zero ? new HLCTimestamp(0, 1_000 + revision, 0) : lastModified;
            inner.StoreKeyValues(
            [
                new PersistenceRequestItem(
                    key: key,
                    value: value is null ? null : Encoding.UTF8.GetBytes(value),
                    revision: revision,
                    expiresNode: 0, expiresPhysical: 0, expiresCounter: 0,
                    lastUsedNode: modified.N, lastUsedPhysical: modified.L, lastUsedCounter: modified.C,
                    lastModifiedNode: modified.N, lastModifiedPhysical: modified.L, lastModifiedCounter: modified.C,
                    state: (int)state)
            ]);
        }

        public List<(string, ReadOnlyKeyValueEntry)> GetKeyValueByRange(
            string prefix, string? startKey, string? endKey, bool endInclusive, int limit, ref KeyValueScanTally tally)
        {
            Interlocked.Increment(ref rangeReads);
            Entered.Set();
            Gate.Wait();
            return ((IPersistenceBackend)inner).GetKeyValueByRange(prefix, startKey, endKey, endInclusive, limit, ref tally);
        }

        public ReadOnlyKeyValueEntry? GetKeyValue(string keyName) => inner.GetKeyValue(keyName);
        public ReadOnlyKeyValueEntry?[] GetKeyValues(string[] keyNames) => ((IPersistenceBackend)inner).GetKeyValues(keyNames);
        public KeyValueHydration GetKeyValueWithRecentRevisions(string keyName, int recentRevisions) =>
            ((IPersistenceBackend)inner).GetKeyValueWithRecentRevisions(keyName, recentRevisions);
        public ReadOnlyKeyValueEntry? GetKeyValueRevisionAtOrBefore(string keyName, long maxRevision, HLCTimestamp readTimestamp) =>
            inner.GetKeyValueRevisionAtOrBefore(keyName, maxRevision, readTimestamp);
        public bool StoreLocks(List<PersistenceRequestItem> items) => inner.StoreLocks(items);
        public bool StoreKeyValues(List<PersistenceRequestItem> items) => inner.StoreKeyValues(items);
        public Kahuna.Server.Locks.Data.LockRow? GetLock(string resource) => inner.GetLock(resource);
        public ReadOnlyKeyValueEntry? GetKeyValueRevision(string keyName, long revision) => inner.GetKeyValueRevision(keyName, revision);
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
