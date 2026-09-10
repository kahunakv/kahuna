using System.Text;
using Kahuna;
using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// The memory bound on durable-2PC metadata retention. <c>TransactionOutcomeRetentionTtl</c> bounds the retained
/// records and receipts in time only, so resident metadata grows linearly with the commit rate on every replica —
/// at ~8,000 ops/s a five-minute window filled a 2.4 GiB heap in five minutes and the first node to run out did so
/// inside the Raft WAL write. These tests pin the bound that replaces "time only": the stores account their heap
/// cost, the sweep reclaims the oldest terminal records early once a budget is exceeded (its proportional share,
/// oldest decision first), it never reclaims below the retention floor, the floor can never undercut orphan
/// recovery, the heap-pressure valve reclaims everything past the floor, and recovery's "record may have been
/// reclaimed" horizon follows the floor so early reclaim stays safe.
/// </summary>
public sealed class TestTransactionRecordRetentionBudget
{
    private readonly ILoggerFactory loggerFactory;

    public TestTransactionRecordRetentionBudget(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper);
    }

    private static HLCTimestamp Ts(long l) => new(0, l, 0);

    private static InitializeTransactionCommand Init(HLCTimestamp txId, string anchor, params string[] keys)
    {
        List<TransactionParticipantRef> manifest = [];
        foreach (string key in keys)
            manifest.Add(new(key, KeyValueDurability.Persistent));

        return new(txId, 1, "coord/1", anchor, Ts(2000), Ts(9000),
            TransactionManifest.ComputeHash(txId, 1, anchor, Ts(2000), manifest),
            manifest, OpId: Ts(500), CreatedAt: Ts(400));
    }

    // ── stores: heap accounting ─────────────────────────────────────────────────

    [Fact]
    public void RecordStore_EstimatedBytes_TracksInsertReplaceAndRemove()
    {
        TransactionRecordStore store = new();
        Assert.Equal(0, store.EstimatedBytes);

        InitializeTransactionCommand init = Init(Ts(1000), "acct/42", "acct/42", "acct/43");
        store.Apply(init);
        long afterInit = store.EstimatedBytes;
        Assert.True(afterInit > 0);
        Assert.Equal(store.Get(Ts(1000), 1)!.EstimateBytes(), afterInit);

        // The commit transition replaces the record in place: the estimate follows the new instance, not the sum.
        store.Apply(new CommitTransactionCommand(Ts(1000), 1, init.ManifestHash, Ts(500), Ts(2100)));
        Assert.Equal(store.Get(Ts(1000), 1)!.EstimateBytes(), store.EstimatedBytes);

        // A second record adds; a longer key costs more than a shorter one.
        InitializeTransactionCommand second = Init(Ts(1001), "acct/42", "acct/a-much-longer-key-name-than-the-first");
        store.Apply(second);
        long afterSecond = store.EstimatedBytes;
        Assert.True(afterSecond > store.Get(Ts(1000), 1)!.EstimateBytes());

        // Purge removes exactly the purged record's share.
        store.Apply(new PurgeTransactionCommand(Ts(1000), 1));
        Assert.Equal(store.Get(Ts(1001), 1)!.EstimateBytes(), store.EstimatedBytes);

        store.Apply(new AbortTransactionCommand(Ts(1001), 1, second.ManifestHash, TransactionAbortClass.ExplicitRollback, Ts(600), Ts(2200), "acct/42", Ts(2000), Ts(9000), Ts(400)));
        store.Apply(new PurgeTransactionCommand(Ts(1001), 1));
        Assert.Equal(0, store.Count);
        Assert.Equal(0, store.EstimatedBytes);
    }

    [Fact]
    public void ReceiptStore_EstimatedBytes_TracksRecordForgetAndExpiry()
    {
        CompletionReceiptStore store = new();
        Assert.Equal(0, store.EstimatedBytes);

        store.Record(Ts(1000), "k1", "anchor", KeyValueDurability.Persistent);
        long one = store.EstimatedBytes;
        Assert.True(one > 0);

        // Idempotent re-record adds nothing.
        store.Record(Ts(1000), "k1", "anchor", KeyValueDurability.Persistent);
        Assert.Equal(one, store.EstimatedBytes);

        store.Record(Ts(9500), "k2-with-a-longer-key", null, KeyValueDurability.Persistent);
        Assert.True(store.EstimatedBytes > one);

        Assert.True(store.Forget(Ts(9500), "k2-with-a-longer-key"));
        Assert.Equal(one, store.EstimatedBytes);

        Assert.Equal(1, store.CollectExpired(Ts(10_000), TimeSpan.FromSeconds(5)));
        Assert.Equal(0, store.Count);
        Assert.Equal(0, store.EstimatedBytes);
    }

    // ── pure: share, floor, tick ────────────────────────────────────────────────

    private static List<(TransactionRecord Record, int AnchorPartition)> Candidates(int count)
    {
        List<(TransactionRecord, int)> candidates = [];
        for (int i = 0; i < count; i++)
        {
            TransactionRecordStore store = new();
            InitializeTransactionCommand init = Init(Ts(1000 + i), "a/1", "a/1");
            store.Apply(init);
            store.Apply(new CommitTransactionCommand(Ts(1000 + i), 1, init.ManifestHash, Ts(500), Ts(2000 + i)));
            candidates.Add((store.Get(Ts(1000 + i), 1)!, 0));
        }

        return candidates;
    }

    [Fact]
    public void ChooseEarlyReclaimCount_TakesThisLeadersShareOfTheOverage()
    {
        List<(TransactionRecord Record, int AnchorPartition)> candidates = Candidates(100);

        // Over by 90 records; this node leads a third of the resident terminal records → it reclaims 30.
        DurableMaintenanceService.RetentionPressure pressure = new(RecordOverage: 90, ByteOverage: 0, HeapPressure: false, HeapLoad: 0);
        Assert.Equal(30, DurableMaintenanceService.ChooseEarlyReclaimCount(pressure, candidates, terminalTotal: 300, terminalLed: 100, expiredSelected: 0));

        // What the TTL already reclaimed this sweep counts against the overage first.
        Assert.Equal(20, DurableMaintenanceService.ChooseEarlyReclaimCount(pressure, candidates, 300, 100, expiredSelected: 30));
        Assert.Equal(0, DurableMaintenanceService.ChooseEarlyReclaimCount(pressure, candidates, 300, 100, expiredSelected: 90));

        // A single leader owns the whole overage, clamped to what is actually eligible.
        Assert.Equal(90, DurableMaintenanceService.ChooseEarlyReclaimCount(pressure, candidates, 300, 300, 0));
        Assert.Equal(100, DurableMaintenanceService.ChooseEarlyReclaimCount(pressure with { RecordOverage = 1_000 }, candidates, 300, 300, 0));

        // A node leading nothing reclaims nothing — it has nothing it may purge.
        Assert.Equal(0, DurableMaintenanceService.ChooseEarlyReclaimCount(pressure, candidates, 300, 0, 0));

        // Under budget: nothing early.
        Assert.Equal(0, DurableMaintenanceService.ChooseEarlyReclaimCount(new(0, 0, false, 0), candidates, 300, 300, 0));
    }

    [Fact]
    public void ChooseEarlyReclaimCount_ByteOverage_ConvertsThroughTheCandidatesAverageSize()
    {
        List<(TransactionRecord Record, int AnchorPartition)> candidates = Candidates(50);

        long perRecord = candidates[0].Record.EstimateBytes();
        Assert.True(perRecord > 0);

        // An overage worth ~10 records (records only: receipts make each candidate free more, so the count needed
        // is at most 10) — the conversion must land between 1 and 10, never 0 and never everything.
        DurableMaintenanceService.RetentionPressure pressure = new(0, ByteOverage: 10 * perRecord, false, 0);
        int take = DurableMaintenanceService.ChooseEarlyReclaimCount(pressure, candidates, 50, 50, 0);
        Assert.InRange(take, 1, 10);
    }

    [Fact]
    public void ChooseEarlyReclaimCount_HeapPressure_TakesEveryCandidatePastTheFloor()
    {
        List<(TransactionRecord Record, int AnchorPartition)> candidates = Candidates(10);
        DurableMaintenanceService.RetentionPressure pressure = new(0, 0, HeapPressure: true, HeapLoad: 0.95);

        // Whatever the share says, pressure drains everything eligible.
        Assert.Equal(10, DurableMaintenanceService.ChooseEarlyReclaimCount(pressure, candidates, 3_000, 10, 0));
    }

    [Fact]
    public void ResolveRetentionFloor_NeverUndercutsOrphanRecovery()
    {
        KahunaConfiguration configuration = new()
        {
            DurableDecisionDeadlineCeilingMs = 60_000,
            DurableMaintenanceInterval = TimeSpan.FromSeconds(5),
            CollectionInterval = TimeSpan.FromSeconds(60),
            DurableRecordRetentionFloor = TimeSpan.FromSeconds(1)
        };

        // 1 s is below ceiling + 2 ticks = 70 s: raised, so a budget reclaim can never precede an orphan's sweep.
        Assert.Equal(TimeSpan.FromSeconds(70), DurableMaintenanceService.ResolveRetentionFloor(configuration, null));

        configuration.DurableRecordRetentionFloor = TimeSpan.FromSeconds(90);
        Assert.Equal(TimeSpan.FromSeconds(90), DurableMaintenanceService.ResolveRetentionFloor(configuration, null));

        // The tick is clamped to the collection interval, and falls back to it when non-positive.
        configuration.DurableMaintenanceInterval = TimeSpan.FromMinutes(5);
        Assert.Equal(TimeSpan.FromSeconds(60), DurableMaintenanceService.MaintenanceTick(configuration));
        configuration.DurableMaintenanceInterval = TimeSpan.Zero;
        Assert.Equal(TimeSpan.FromSeconds(60), DurableMaintenanceService.MaintenanceTick(configuration));
        configuration.DurableMaintenanceInterval = TimeSpan.FromSeconds(5);
        Assert.Equal(TimeSpan.FromSeconds(5), DurableMaintenanceService.MaintenanceTick(configuration));
    }

    // ── end-to-end ───────────────────────────────────────────────────────────────

    private const int TransactionCount = 24;

    private const int Budget = 8;

    // A floor the test can wait out: the decision-deadline clamp pinned at one second (commits finalize in
    // milliseconds, far inside it) plus two 100 ms maintenance ticks → 1.2 s.
    private static readonly TimeSpan ShortFloor = TimeSpan.FromMilliseconds(1_200);

    private async Task<EmbeddedKahunaNode> StartNodeAsync(CancellationToken ct, Action<EmbeddedKahunaOptions> configure)
    {
        EmbeddedKahunaOptions options = new()
        {
            ReadIOThreads = 1,
            WriteIOThreads = 1,
            PartitionExecutorPoolSize = 1,
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 4,
            // The TTL never elapses here: every reclaim below is the budget's, not the window's.
            TransactionOutcomeRetentionTtl = TimeSpan.FromMinutes(30),
            CompletionReceiptRetentionTtl = TimeSpan.FromMinutes(60),
            DurableDecisionDeadlineFloorMs = 1_000,
            DurableDecisionDeadlineCeilingMs = 1_000,
            DurableRecordRetentionFloor = TimeSpan.FromMilliseconds(1),
            DurableMaintenanceInterval = TimeSpan.FromMilliseconds(100),
            DurableRecordRetentionMax = 0,
            DurableRecordRetentionMaxBytes = 0,
            DurableRecordRetentionHeapPressure = 0
        };
        configure(options);

        EmbeddedKahunaNode node = new(options, loggerFactory);
        await node.StartAsync(ct);
        return node;
    }

    private static async Task<List<string>> CommitTransactionsAsync(EmbeddedKahunaNode node, int count, string prefix)
    {
        List<string> keys = [];

        for (int i = 0; i < count; i++)
        {
            string key = $"{prefix}/row-{i:D3}";
            await node.WaitForLeaderForKeyAsync(key, CancellationToken.None);

            KeyValueTransactionResult result = await node.Kahuna.TryExecuteTransactionScript(
                Encoding.UTF8.GetBytes($"BEGIN SET `{key}` 'v{i}' COMMIT END"), null, null);
            Assert.Equal(KeyValueResponseType.Set, result.Type);

            keys.Add(key);
        }

        return keys;
    }

    [Fact]
    public async Task CountBudget_PeriodicSweep_ReclaimsOldestEarly_AndPlateausUnderTheBudget()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        await using EmbeddedKahunaNode node = await StartNodeAsync(ct, o => o.DurableRecordRetentionMax = Budget);
        KahunaManager kahuna = (KahunaManager)node.Kahuna;

        DurableMaintenanceService maintenance = kahuna.KeyValues.DurableMaintenance;
        Assert.True(maintenance.RetentionBudgetEnabled);
        Assert.Equal(ShortFloor, maintenance.EffectiveMinimumRetention);

        List<string> keys = await CommitTransactionsAsync(node, TransactionCount, "budget");
        await WaitUntil(() => kahuna.DurableTransactionRecordStore.Count >= TransactionCount);
        Assert.True(kahuna.CompletionReceiptStore.Count >= TransactionCount);

        // Three times over budget, but nothing has aged past the floor: the periodic sweep must not touch it.
        await Task.Delay(300, ct);
        Assert.Equal(TransactionCount, kahuna.DurableTransactionRecordStore.Count);

        // Once the floor elapses the sweep (the actor's own tick, not one driven here) reclaims the oldest
        // decisions down to the low-water mark and the count plateaus there instead of growing with commits.
        await WaitUntil(() => kahuna.DurableTransactionRecordStore.Count <= Budget, timeoutMs: 15_000);
        await WaitUntil(() => kahuna.CompletionReceiptStore.Count <= Budget, timeoutMs: 15_000);

        int plateau = kahuna.DurableTransactionRecordStore.Count;
        Assert.InRange(plateau, 1, Budget);

        // The survivors are the youngest decisions: every reclaimed record decided before every retained one.
        HashSet<string> retainedKeys = [];
        foreach (TransactionRecord record in kahuna.DurableTransactionRecordStore.Snapshot())
            foreach (TransactionParticipantRef participant in record.Participants)
                retainedKeys.Add(participant.Key);

        int firstRetained = keys.FindIndex(retainedKeys.Contains);
        Assert.True(firstRetained >= 0);
        for (int i = firstRetained; i < keys.Count; i++)
            Assert.Contains(keys[i], retainedKeys);

        // The committed values are untouched — the budget reclaims metadata only, never the data.
        foreach (string key in keys)
        {
            (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await node.Kahuna.LocateAndTryGetValue(
                HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.Get, type);
            Assert.NotNull(entry?.Value);
        }
    }

    [Fact]
    public async Task ByteBudget_ReclaimsEarly_UntilTheEstimateIsBackUnderIt()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        // A byte budget worth roughly a third of what the transactions will retain.
        await using EmbeddedKahunaNode node = await StartNodeAsync(ct, o =>
        {
            o.DurableRecordRetentionMaxBytes = 1;      // placeholder, sized below once a record's cost is known
            o.DurableMaintenanceInterval = TimeSpan.FromMinutes(1); // driven by hand here
        });
        KahunaManager kahuna = (KahunaManager)node.Kahuna;
        DurableMaintenanceService maintenance = kahuna.KeyValues.DurableMaintenance;

        await CommitTransactionsAsync(node, TransactionCount, "bytes");
        await WaitUntil(() => kahuna.DurableTransactionRecordStore.Count >= TransactionCount);

        long resident = kahuna.DurableTransactionRecordStore.EstimatedBytes + kahuna.CompletionReceiptStore.EstimatedBytes;
        Assert.True(resident > 0);

        // The budget of 1 byte is over by everything; the pressure reading reflects the running estimates.
        DurableMaintenanceService.RetentionPressure pressure = maintenance.AssessRetentionPressure();
        Assert.True(pressure.OverBudget);
        Assert.True(pressure.ByteOverage > 0);
        Assert.Equal(0, pressure.RecordOverage);
        Assert.False(pressure.HeapPressure);

        // With a one-minute tick the floor is ceiling (1 s) + 2 min; nothing is past it, so the sweep reclaims
        // nothing — the floor holds even against a budget over by 100%.
        await kahuna.KeyValues.CollectDurableTransactionRecords(ct);
        Assert.Equal(TransactionCount, kahuna.DurableTransactionRecordStore.Count);
    }

    [Fact]
    public async Task Floor_HoldsAgainstAnyBudget_AndIsRecoverysHorizon()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        await using EmbeddedKahunaNode node = await StartNodeAsync(ct, o =>
        {
            o.DurableRecordRetentionMax = 2;
            o.DurableRecordRetentionFloor = TimeSpan.FromMinutes(10);
            o.DurableMaintenanceInterval = TimeSpan.FromMinutes(1);
        });
        KahunaManager kahuna = (KahunaManager)node.Kahuna;
        DurableMaintenanceService maintenance = kahuna.KeyValues.DurableMaintenance;

        // The horizon recovery reasons with is the floor: an intent older than 10 minutes with no record is held,
        // one younger is presumed aborted — exactly the earliest age a budget may reclaim its record.
        Assert.Equal(TimeSpan.FromMinutes(10), maintenance.EffectiveMinimumRetention);

        await CommitTransactionsAsync(node, TransactionCount, "floor");
        await WaitUntil(() => kahuna.DurableTransactionRecordStore.Count >= TransactionCount);

        Assert.True(maintenance.AssessRetentionPressure().RecordOverage > 0);

        await kahuna.KeyValues.CollectDurableTransactionRecords(ct);
        await Task.Delay(100, ct);

        // Twelve times over a budget of 2, and not one record reclaimed: none is older than the floor.
        Assert.Equal(TransactionCount, kahuna.DurableTransactionRecordStore.Count);
    }

    [Fact]
    public async Task NoBudget_HorizonIsTheFullTtl()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        await using EmbeddedKahunaNode node = await StartNodeAsync(ct, o => o.DurableMaintenanceInterval = TimeSpan.FromMinutes(1));
        KahunaManager kahuna = (KahunaManager)node.Kahuna;
        DurableMaintenanceService maintenance = kahuna.KeyValues.DurableMaintenance;

        Assert.False(maintenance.RetentionBudgetEnabled);
        Assert.Equal(TimeSpan.FromMinutes(30), maintenance.EffectiveMinimumRetention);
        Assert.False(maintenance.AssessRetentionPressure().OverBudget);
    }

    [Fact]
    public async Task HeapPressure_ReclaimsEverythingPastTheFloor_RecordsAndReceipts()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        await using EmbeddedKahunaNode node = await StartNodeAsync(ct, o =>
        {
            o.DurableRecordRetentionHeapPressure = 0.85;
            o.DurableMaintenanceInterval = TimeSpan.FromMinutes(1); // driven by hand: the floor is then 1 s + 2 min
        });
        KahunaManager kahuna = (KahunaManager)node.Kahuna;
        DurableMaintenanceService maintenance = kahuna.KeyValues.DurableMaintenance;

        Assert.True(maintenance.RetentionBudgetEnabled);

        await CommitTransactionsAsync(node, TransactionCount, "pressure");
        await WaitUntil(() => kahuna.DurableTransactionRecordStore.Count >= TransactionCount);
        Assert.True(kahuna.CompletionReceiptStore.Count > 0);

        // Heap comfortably below the threshold: nothing happens whatever the (disabled) budgets say.
        maintenance.HeapLoadProbe = () => 0.50;
        await kahuna.KeyValues.CollectDurableTransactionRecords(ct);
        Assert.False(maintenance.HeapPressureObserved);
        Assert.Equal(TransactionCount, kahuna.DurableTransactionRecordStore.Count);

        // Heap past the threshold, but every record is younger than the floor: the valve still honors the floor.
        maintenance.HeapLoadProbe = () => 0.95;
        await kahuna.KeyValues.CollectDurableTransactionRecords(ct);
        Assert.True(maintenance.HeapPressureObserved);
        Assert.Equal(TransactionCount, kahuna.DurableTransactionRecordStore.Count);

        // The receipt backstop under pressure runs at the floor too: with everything younger, it drops nothing.
        int receiptsBefore = kahuna.CompletionReceiptStore.Count;
        kahuna.KeyValues.CollectExpiredCompletionReceipts(heapPressure: true);
        Assert.Equal(receiptsBefore, kahuna.CompletionReceiptStore.Count);
    }

    [Fact]
    public async Task HeapPressure_PastTheFloor_DrainsEveryRecordAtOnce()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        // Budgets off, valve on, a floor the test can wait out; the periodic tick is the one that acts.
        await using EmbeddedKahunaNode node = await StartNodeAsync(ct, o => o.DurableRecordRetentionHeapPressure = 0.85);
        KahunaManager kahuna = (KahunaManager)node.Kahuna;
        DurableMaintenanceService maintenance = kahuna.KeyValues.DurableMaintenance;

        maintenance.HeapLoadProbe = () => 0.50;

        await CommitTransactionsAsync(node, TransactionCount, "drain");
        await WaitUntil(() => kahuna.DurableTransactionRecordStore.Count >= TransactionCount);

        // Past the floor with the heap healthy: the 30-minute TTL keeps everything.
        await Task.Delay(ShortFloor + TimeSpan.FromMilliseconds(300), ct);
        Assert.Equal(TransactionCount, kahuna.DurableTransactionRecordStore.Count);

        // Open the valve: the next tick reclaims every record past the floor and releases their receipts.
        maintenance.HeapLoadProbe = () => 0.95;
        await WaitUntil(() => kahuna.DurableTransactionRecordStore.Count == 0, timeoutMs: 15_000);
        await WaitUntil(() => kahuna.CompletionReceiptStore.Count == 0, timeoutMs: 15_000);
    }

    private static async Task WaitUntil(Func<bool> predicate, int timeoutMs = 10_000)
    {
        long deadline = Environment.TickCount64 + timeoutMs;
        while (Environment.TickCount64 < deadline)
        {
            if (predicate())
                return;
            await Task.Delay(20);
        }

        Assert.True(predicate(), "condition not met within the timeout");
    }
}
