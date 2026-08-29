using System.Text;
using Kahuna;
using Kahuna.Server.KeyValues;
using Kahuna.Shared.KeyValue;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// A range scan whose page keeps answering transient must fail loudly instead of retrying forever.
/// The durable producer of that state is a foreign write intent whose commit timestamp never
/// resolves: a snapshot page containing the key answers WaitingForReplication on every attempt, and
/// before the budget existed the scan loop retried in silence for the life of the process — an
/// aggregate read or a reconciliation pass over the range simply never returned, with no log line
/// and no counter. The budget converts that permanent invisible hang into a retryable server error
/// that names the range, while a scan whose pages make progress never trips it.
/// </summary>
public sealed class TestScanRetryBudget
{
    private readonly ILoggerFactory loggerFactory;

    public TestScanRetryBudget(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper);
    }

    private static async Task<EmbeddedKahunaNode> StartNode(
        ILoggerFactory loggerFactory, CancellationToken ct,
        int scanPageRetryBudgetMs = 0, int sessionOwnedIntentCeilingMs = 0)
    {
        EmbeddedKahunaOptions options = new()
        {
            ReadIOThreads = 1,
            WriteIOThreads = 1,
            PartitionExecutorPoolSize = 1,
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 1
        };
        if (scanPageRetryBudgetMs > 0)
            options.ScanPageRetryBudgetMs = scanPageRetryBudgetMs;

        if (sessionOwnedIntentCeilingMs > 0)
        {
            // The ceiling is floor-checked against the session bound at load, so the shortest legal one is
            // reached by shrinking the session timeout first. This keeps the node on the real validated
            // path — a test hatch around the floor would leave the operator-facing wiring unexercised.
            options.DefaultTransactionTimeout = 1_000;
            options.MaxTransactionTimeout = 1_000;
            options.SessionOwnedIntentCeilingMs = sessionOwnedIntentCeilingMs;
        }

        EmbeddedKahunaNode node = new(options, loggerFactory);
        await node.StartAsync(ct);
        await node.WaitForLeaderForKeyAsync("scanbudget/k00", ct);
        return node;
    }

    private static async Task<List<(string Key, ReadOnlyKeyValueEntry Entry)>> DrainScan(
        IAsyncEnumerable<(string Key, ReadOnlyKeyValueEntry Entry)> scan)
    {
        List<(string, ReadOnlyKeyValueEntry)> rows = [];
        await foreach ((string key, ReadOnlyKeyValueEntry entry) in scan)
            rows.Add((key, entry));
        return rows;
    }

    [Fact]
    public async Task ScanPage_BlockedByUnresolvedForeignIntent_FailsLoudlyWithinBudget()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);

        KahunaManager manager = (KahunaManager)node.Kahuna;
        manager.KeyValues.RoutedScans.TestScanPageRetryBudgetMs = 1_500;

        const string prefix = "scanbudget";

        // Ten committed rows so the poisoned key sits past page 0 (page size 4 below).
        for (int i = 0; i < 10; i++)
        {
            (KeyValueResponseType set, _, _) = await node.Kahuna.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, $"{prefix}/k{i:D2}", Encoding.UTF8.GetBytes($"v{i}"), null, -1,
                KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.Set, set);
        }

        // A foreign transaction stages a write on one mid-range key and never decides: the staged
        // write intent carries no commit timestamp, so a snapshot page containing the key cannot
        // prove the write lands outside its snapshot and answers WaitingForReplication every time.
        HLCTimestamp foreignTx = manager.Raft.HybridLogicalClock.TrySendOrLocalEvent(manager.Raft.GetLocalNodeId());
        (KeyValueResponseType staged, _, _) = await node.Kahuna.LocateAndTrySetKeyValue(
            foreignTx, $"{prefix}/k06", Encoding.UTF8.GetBytes("staged"), null, -1,
            KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Set, staged);

        // The snapshot is minted after the staging, so the intent's fate genuinely matters to it.
        HLCTimestamp snapshot = manager.Raft.HybridLogicalClock.TrySendOrLocalEvent(manager.Raft.GetLocalNodeId());

        KahunaServerException thrown = await Assert.ThrowsAsync<KahunaServerException>(() =>
            DrainScan(node.Kahuna.LocateAndScanRange(
                HLCTimestamp.Zero, prefix,
                null, true, null, false,
                pageSize: 4, snapshot,
                KeyValueDurability.Persistent, ct)));

        Assert.Contains("did not settle", thrown.Message);

        // Once the foreign intent is released the same scan completes: the budget only converts a
        // permanent obstruction into an error, it never fails a range that can serve.
        (KeyValueResponseType released, _) = await manager.KeyValues.LocateAndTryReleaseExclusiveLock(
            foreignTx, $"{prefix}/k06", KeyValueDurability.Persistent, ct);
        Assert.True(
            released is KeyValueResponseType.Unlocked or KeyValueResponseType.DoesNotExist,
            $"release answered {released}");

        List<(string Key, ReadOnlyKeyValueEntry Entry)> rows = await DrainScan(node.Kahuna.LocateAndScanRange(
            HLCTimestamp.Zero, prefix,
            null, true, null, false,
            pageSize: 4, snapshot,
            KeyValueDurability.Persistent, ct));

        Assert.Equal(10, rows.Count);
    }

    [Fact]
    public async Task ScanPageRetryBudget_ConfiguredThroughOptions_ReachesTheScanPath()
    {
        // Same blocked-page shape as above, but the budget arrives through the configuration
        // (EmbeddedKahunaOptions → KahunaConfiguration → the scan path) instead of the test override,
        // proving the operator-facing knob is actually wired end to end. The knob exists because the
        // budget must stay below the smallest client command deadline in front of the scan; a
        // deployment that raises its deadline tunes this alongside it.
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct, scanPageRetryBudgetMs: 1_500);

        KahunaManager manager = (KahunaManager)node.Kahuna;
        const string prefix = "scanbudget";

        for (int i = 0; i < 10; i++)
        {
            (KeyValueResponseType set, _, _) = await node.Kahuna.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, $"{prefix}/k{i:D2}", Encoding.UTF8.GetBytes($"v{i}"), null, -1,
                KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.Set, set);
        }

        HLCTimestamp foreignTx = manager.Raft.HybridLogicalClock.TrySendOrLocalEvent(manager.Raft.GetLocalNodeId());
        (KeyValueResponseType staged, _, _) = await node.Kahuna.LocateAndTrySetKeyValue(
            foreignTx, $"{prefix}/k06", Encoding.UTF8.GetBytes("staged"), null, -1,
            KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Set, staged);

        HLCTimestamp snapshot = manager.Raft.HybridLogicalClock.TrySendOrLocalEvent(manager.Raft.GetLocalNodeId());

        KahunaServerException thrown = await Assert.ThrowsAsync<KahunaServerException>(() =>
            DrainScan(node.Kahuna.LocateAndScanRange(
                HLCTimestamp.Zero, prefix,
                null, true, null, false,
                pageSize: 4, snapshot,
                KeyValueDurability.Persistent, ct)));

        Assert.Contains("1500 ms", thrown.Message);
    }

    /// <summary>
    /// The wedge heals on its own. The same orphaned foreign intent that makes a snapshot page unservable
    /// is dropped once it outlives the session-owned ceiling, so the scan that failed loudly a moment
    /// earlier completes — with no release ever issued, which is the whole point: in the field the owning
    /// session is gone and nobody is left to release anything. Before the ceiling this range stayed
    /// unscannable for the life of the process, and on a system key space that took the cluster
    /// read-dead.
    /// </summary>
    [Fact]
    public async Task ScanPage_BlockedByOrphanedForeignIntent_HealsOnceTheCeilingPasses()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        // The shortest ceiling a node will accept: the reaper's grace window plus a one-second session
        // timeout. A deployed node derives it from the shipped 300 s session timeout instead, which is why
        // this test costs its wait — the ceiling is a real deadline, not a test-only shortcut.
        const int ceilingMs = 1_000 + 15_000;
        await using EmbeddedKahunaNode node = await StartNode(
            loggerFactory, ct, scanPageRetryBudgetMs: 1_000, sessionOwnedIntentCeilingMs: ceilingMs);

        KahunaManager manager = (KahunaManager)node.Kahuna;
        const string prefix = "scanheal";

        // The knob reached the node: options → validated configuration → the liveness policy.
        Assert.Equal(ceilingMs, EmbeddedKahunaNode
            .CreateKahunaConfiguration(new()
            {
                Storage = "memory",
                DefaultTransactionTimeout = 1_000,
                MaxTransactionTimeout = 1_000,
                SessionOwnedIntentCeilingMs = ceilingMs
            }, false)
            .SessionOwnedIntentCeilingMs);

        for (int i = 0; i < 10; i++)
        {
            (KeyValueResponseType set, _, _) = await node.Kahuna.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, $"{prefix}/k{i:D2}", Encoding.UTF8.GetBytes($"v{i}"), null, -1,
                KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.Set, set);
        }

        HLCTimestamp foreignTx = manager.Raft.HybridLogicalClock.TrySendOrLocalEvent(manager.Raft.GetLocalNodeId());
        (KeyValueResponseType staged, _, _) = await node.Kahuna.LocateAndTrySetKeyValue(
            foreignTx, $"{prefix}/k06", Encoding.UTF8.GetBytes("staged"), null, -1,
            KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Set, staged);

        HLCTimestamp snapshot = manager.Raft.HybridLogicalClock.TrySendOrLocalEvent(manager.Raft.GetLocalNodeId());

        // While the intent is inside its ceiling the page cannot serve, and the budget says so loudly.
        KahunaServerException thrown = await Assert.ThrowsAsync<KahunaServerException>(() =>
            DrainScan(node.Kahuna.LocateAndScanRange(
                HLCTimestamp.Zero, prefix,
                null, true, null, false,
                pageSize: 4, snapshot,
                KeyValueDurability.Persistent, ct)));

        Assert.Contains("did not settle", thrown.Message);

        // Past the ceiling no session can still own the intent. Nothing releases it — the same scan simply
        // starts serving again.
        await Task.Delay(ceilingMs + 2_000, ct);

        List<(string Key, ReadOnlyKeyValueEntry Entry)> rows = await DrainScan(node.Kahuna.LocateAndScanRange(
            HLCTimestamp.Zero, prefix,
            null, true, null, false,
            pageSize: 4, snapshot,
            KeyValueDurability.Persistent, ct));

        Assert.Equal(10, rows.Count);
    }
}
