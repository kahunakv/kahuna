using System.Text;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Time;

namespace Kahuna.Server.Tests;

/// <summary>
/// A cross-partition transaction whose second leg meets a split quiesce is aborted; atomicity
/// demands that its first leg — already prepared on an untouched partition — is discarded, never
/// materialized. A first leg that survives an abort is a silent conserved-total drift: the client
/// counted nothing, the row changed anyway. These tests freeze exactly the incident interleaving:
/// prepare leg 1, refuse leg 2 at the quiesced moving range, abort, then require leg 1's key
/// byte-identical and revision-identical through settlement and through the cutover itself.
/// </summary>
public sealed class TestSplitQuiesceAbortAtomicity : BaseCluster
{
    private const int Partitions = 6;

    /// <summary>The ranged space that is split; its moving half carries leg 2.</summary>
    private const string Space = "xq:m";

    private const string SplitKey = Space + "/m";

    /// <summary>Leg 2: lives in the moving half [SplitKey, +inf), so its prepare meets the quiesce.</summary>
    private const string LegTwoKey = Space + "/z0";

    /// <summary>Leg 1: hash-routed (outside the ranged space), prepared before leg 2 refuses.</summary>
    private const string LegOneKey = "xq-left/acct1";

    private readonly Microsoft.Extensions.Logging.ILogger<IRaft> raftLogger;

    private readonly Microsoft.Extensions.Logging.ILogger<IKahuna> kahunaLogger;

    public TestSplitQuiesceAbortAtomicity(ITestOutputHelper outputHelper)
    {
        Microsoft.Extensions.Logging.ILoggerFactory loggerFactory = TestLogFactory.Create(outputHelper);
        raftLogger = Microsoft.Extensions.Logging.LoggerFactoryExtensions.CreateLogger<IRaft>(loggerFactory);
        kahunaLogger = Microsoft.Extensions.Logging.LoggerFactoryExtensions.CreateLogger<IKahuna>(loggerFactory);
    }

    private static byte[] V(string s) => Encoding.UTF8.GetBytes(s);

    private static async Task<(IRaft Raft, KahunaManager Kahuna)> LeaderOf(
        int partition, IRaft[] rafts, KahunaManager[] kahunas, CancellationToken ct)
    {
        while (true)
        {
            for (int i = 0; i < rafts.Length; i++)
                if (await rafts[i].AmILeaderIfHosted(partition, ct))
                    return (rafts[i], kahunas[i]);

            await Task.Delay(50, ct);
        }
    }

    private async Task<(IRaft[] Rafts, KahunaManager[] Kahunas, KahunaManager Driver)> Setup(CancellationToken ct)
    {
        (IRaft[] rafts, IKahuna[] kahunas) = await AssembleCluster(
            3, "memory", Partitions, raftLogger, kahunaLogger, replicationFactor: 0);

        KahunaManager[] managers = [.. kahunas.Cast<KahunaManager>()];

        foreach (KahunaManager kahuna in managers)
            kahuna.RegisterKeyRange(Space);

        (IRaft driverRaft, KahunaManager driver) = await LeaderOf(RangeMapStore.MetaPartitionId, rafts, managers, ct);

        // Seed the ranged space on a data partition the driver does not lead, mirroring the incident.
        int sourcePartition = 0;
        for (int partitionId = 1; partitionId <= Partitions; partitionId++)
        {
            if (!await driverRaft.AmILeaderIfHosted(partitionId, ct))
            {
                sourcePartition = partitionId;
                break;
            }
        }

        Assert.NotEqual(0, sourcePartition);

        bool seeded = await driver.RangeMapStore.MutateAsync(
            _ => [new RangeDescriptor { KeySpace = Space, PartitionId = sourcePartition, Generation = 1 }], ct);
        Assert.True(seeded);

        foreach (KahunaManager kahuna in managers)
            await WaitUntilAsync(
                () => kahuna.RangeMapStore.Current.Find(Space, Space + "/x")?.Generation == 1, timeoutMs: 30_000);

        // Seed both legs plus keys on both sides of the split point so the halves probe passes.
        foreach (string key in new[] { LegOneKey, Space + "/a0", Space + "/n0", Space + "/n1", LegTwoKey, Space + "/z1" })
        {
            (KeyValueResponseType type, _, _) = await RetryOnMustRetryAsync(
                () => driver.LocateAndTrySetKeyValue(
                    HLCTimestamp.Zero, key, V("base"), null, -1, KeyValueFlags.Set, 0,
                    KeyValueDurability.Persistent, ct),
                r => r.Item1);

            Assert.Equal(KeyValueResponseType.Set, type);
        }

        return (rafts, managers, driver);
    }

    private static async Task<(string? Value, long Revision)> ReadKey(
        KahunaManager manager, string key, CancellationToken ct)
    {
        (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await RetryOnMustRetryAsync(
            () => manager.LocateAndTryGetValue(
                HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct),
            r => r.Item1, timeoutMs: 30_000);

        Assert.Equal(KeyValueResponseType.Get, type);
        return (entry?.Value is null ? null : Encoding.UTF8.GetString(entry.Value), entry!.Revision);
    }

    /// <summary>
    /// The incident interleaving: a two-leg transaction runs inside the split's quiesce window, its
    /// second leg refused by the quiesced moving range. Whatever the transaction's outcome reports,
    /// leg 1 must hold its prior value and revision — immediately, after settlement drains, and
    /// after the cutover routes the moving half to the child partition.
    /// </summary>
    [Fact]
    public async Task Transaction_AbortedBySplitQuiesce_LeavesFirstLegUntouched()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft[] rafts, KahunaManager[] kahunas, KahunaManager driver) = await Setup(ct);

        (string? legOneBefore, long legOneRevBefore) = await ReadKey(driver, LegOneKey, ct);
        (string? legTwoBefore, long legTwoRevBefore) = await ReadKey(driver, LegTwoKey, ct);
        Assert.Equal("base", legOneBefore);
        Assert.Equal("base", legTwoBefore);

        byte[] script = Encoding.UTF8.GetBytes(
            $"BEGIN SET `{LegOneKey}` 'moved' SET `{LegTwoKey}` 'moved' COMMIT END");

        KeyValueTransactionResult? txResult = null;

        (_, driver) = await LeaderOf(RangeMapStore.MetaPartitionId, rafts, kahunas, ct);

        SplitOutcome outcome = await driver.ForceSplitAtKeyAsync(Space, SplitKey, async () =>
        {
            // Runs inside the quiesce window, right before the cutover: leg 1's prepare lands on
            // its own partition, leg 2's prepare meets the quiesced moving range and refuses.
            txResult = await driver.TryExecuteTransactionScript(script, null, null);
        }, ct);

        Assert.True(outcome.IsSuccess, $"Split failed: {outcome.Status}");
        Assert.NotNull(txResult);

        // The transaction must not have committed: the quiesce refuses leg 2's prepare.
        Assert.NotEqual(KeyValueResponseType.Set, txResult.Type);

        // Leg 1 unchanged — the aborted transaction's prepared first leg was discarded, not applied.
        (string? legOneAfter, long legOneRevAfter) = await ReadKey(driver, LegOneKey, ct);
        Assert.Equal("base", legOneAfter);
        Assert.Equal(legOneRevBefore, legOneRevAfter);

        // Leg 2 unchanged too, served now from the child range after cutover.
        (string? legTwoAfter, long legTwoRevAfter) = await ReadKey(driver, LegTwoKey, ct);
        Assert.Equal("base", legTwoAfter);
        Assert.Equal(legTwoRevBefore, legTwoRevAfter);

        // Settlement must drain the aborted intent, not materialize it: wait until no node holds an
        // intent for leg 1, then require the value and revision unchanged on every node.
        foreach (KahunaManager manager in kahunas)
        {
            KahunaManager observer = manager;
            await WaitUntilAsync(() => observer.DurablePreparedIntentStore.Get(LegOneKey) is null, timeoutMs: 30_000);
        }

        foreach (KahunaManager manager in kahunas)
        {
            (string? value, long revision) = await ReadKey(manager, LegOneKey, ct);
            Assert.Equal("base", value);
            Assert.Equal(legOneRevBefore, revision);
        }
    }

    /// <summary>
    /// The drain-shaped variant: transfers hammer their pairs with no pacing while split attempts
    /// run in a loop — each attempt holds the quiesce for its drain against the sustained load —
    /// and every node's prepared-intent recovery sweep runs concurrently, so presumed aborts race
    /// live finalizes exactly as they do in production once the abort drive routes. Whatever mix of
    /// commits, quiesce-refused aborts, and presumed aborts the run produces, each pair must end
    /// with equal values and equal revisions: an aborted transaction whose first leg was
    /// materialized off a wrong decision answer breaks the revision equality even when a later
    /// commit overwrites the value.
    /// </summary>
    [Fact]
    public async Task ConcurrentTransfers_HeldDrainWithRecoverySweeps_StayPairwiseAtomic()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft[] rafts, KahunaManager[] kahunas, KahunaManager driver) = await Setup(ct);

        const int pairs = 8;

        string[] legOne = [.. Enumerable.Range(0, pairs).Select(i => $"xq-left/dpair{i}")];
        string[] legTwo = [.. Enumerable.Range(0, pairs).Select(i => $"{Space}/z-dpair{i}")];

        foreach (string key in legOne.Concat(legTwo))
        {
            (KeyValueResponseType type, _, _) = await RetryOnMustRetryAsync(
                () => driver.LocateAndTrySetKeyValue(
                    HLCTimestamp.Zero, key, V("base"), null, -1, KeyValueFlags.Set, 0,
                    KeyValueDurability.Persistent, ct),
                r => r.Item1);
            Assert.Equal(KeyValueResponseType.Set, type);
        }

        bool stop = false;
        int totalCommits = 0;

        async Task Worker(int pair)
        {
            int attempt = 0;
            while (!Volatile.Read(ref stop))
            {
                attempt++;
                byte[] script = Encoding.UTF8.GetBytes(
                    $"BEGIN SET `{legOne[pair]}` 'd{pair}a{attempt}' SET `{legTwo[pair]}` 'd{pair}a{attempt}' COMMIT END");

                KeyValueTransactionResult result = await driver.TryExecuteTransactionScript(script, null, null);
                if (result.Type == KeyValueResponseType.Set)
                    Interlocked.Increment(ref totalCommits);
            }
        }

        // Every node's recovery sweep runs continuously — the presumed-abort pressure.
        async Task Sweeper(KahunaManager manager)
        {
            while (!Volatile.Read(ref stop))
            {
                try
                {
                    await manager.KeyValues.RecoverPreparedIntents(ct);
                }
                catch
                {
                    // The sweep is background machinery; a transient failure only skips one pass.
                }

                await Task.Delay(50, ct);
            }
        }

        Task[] workers = [.. Enumerable.Range(0, pairs).Select(Worker)];
        Task[] sweepers = [.. kahunas.Select(Sweeper)];

        // Split attempts in a loop for the whole stress window: refused attempts hold the drain
        // against the load; a successful attempt ends the loop with the range divided.
        SplitOutcome outcome = SplitOutcome.PartitionCreationFailed;
        System.Diagnostics.Stopwatch stress = System.Diagnostics.Stopwatch.StartNew();

        while (stress.Elapsed < TimeSpan.FromSeconds(10))
        {
            (_, driver) = await LeaderOf(RangeMapStore.MetaPartitionId, rafts, kahunas, ct);
            outcome = await driver.ForceSplitAtKeyAsync(Space, SplitKey, null, ct);
            if (outcome.IsSuccess)
                break;
            await Task.Delay(100, ct);
        }

        // Let the load run a moment past the split (or past the stress window when no attempt
        // landed), then stop everything.
        await Task.Delay(1_000, ct);
        Volatile.Write(ref stop, true);
        await Task.WhenAll(workers.Concat(sweepers));

        Assert.True(totalCommits > 0, "No transfer committed at all — the run says nothing about atomicity");

        foreach (string key in legOne.Concat(legTwo))
        {
            string k = key;
            foreach (KahunaManager manager in kahunas)
            {
                KahunaManager observer = manager;
                await WaitUntilAsync(() => observer.DurablePreparedIntentStore.Get(k) is null, timeoutMs: 30_000);
            }
        }

        for (int i = 0; i < pairs; i++)
        {
            (string? valueOne, long revisionOne) = await ReadKey(driver, legOne[i], ct);
            (string? valueTwo, long revisionTwo) = await ReadKey(driver, legTwo[i], ct);

            Assert.True(valueOne == valueTwo,
                $"pair {i}: legs diverged — '{valueOne}' vs '{valueTwo}' (a transaction applied one leg only)");
            Assert.True(revisionOne == revisionTwo,
                $"pair {i}: revision drift — {revisionOne} vs {revisionTwo} (an aborted transaction left a leg durable)");
        }
    }

    /// <summary>
    /// The sustained variant: pairs of accounts transfer continuously — one leg hash-routed, one in
    /// the moving half — while a split of the ranged space runs start to finish. Every transaction
    /// writes the same tag to both legs, so atomicity leaves each pair with equal values and equal
    /// revision counts whatever mix of commits and quiesce-refused aborts the run produced. A first
    /// leg that survives an abort (the incident signature) breaks the pair's revision equality even
    /// when a later commit overwrites the value.
    /// </summary>
    [Fact]
    public async Task ConcurrentTransfers_AcrossWholeSplit_StayPairwiseAtomic()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft[] rafts, KahunaManager[] kahunas, KahunaManager driver) = await Setup(ct);

        const int pairs = 6;

        // Leg 2 keys sit past the split point so they land in the moving half.
        string[] legOne = [.. Enumerable.Range(0, pairs).Select(i => $"xq-left/pair{i}")];
        string[] legTwo = [.. Enumerable.Range(0, pairs).Select(i => $"{Space}/z-pair{i}")];

        foreach (string key in legOne.Concat(legTwo))
        {
            (KeyValueResponseType type, _, _) = await RetryOnMustRetryAsync(
                () => driver.LocateAndTrySetKeyValue(
                    HLCTimestamp.Zero, key, V("base"), null, -1, KeyValueFlags.Set, 0,
                    KeyValueDurability.Persistent, ct),
                r => r.Item1);
            Assert.Equal(KeyValueResponseType.Set, type);
        }

        bool splitDone = false;
        int totalCommits = 0;

        async Task Worker(int pair)
        {
            int attempt = 0;
            int extraAfterSplit = 0;

            while (extraAfterSplit < 3)
            {
                if (Volatile.Read(ref splitDone))
                    extraAfterSplit++;

                attempt++;
                byte[] script = Encoding.UTF8.GetBytes(
                    $"BEGIN SET `{legOne[pair]}` 'p{pair}a{attempt}' SET `{legTwo[pair]}` 'p{pair}a{attempt}' COMMIT END");

                KeyValueTransactionResult result = await driver.TryExecuteTransactionScript(script, null, null);
                if (result.Type == KeyValueResponseType.Set)
                    Interlocked.Increment(ref totalCommits);

                await Task.Delay(25, ct);
            }
        }

        Task[] workers = [.. Enumerable.Range(0, pairs).Select(Worker)];

        // Run the split under the write load; retry retryable refusals exactly as the trigger would.
        SplitOutcome outcome = SplitOutcome.PartitionCreationFailed;
        for (int attempt = 0; attempt < 30; attempt++)
        {
            (_, driver) = await LeaderOf(RangeMapStore.MetaPartitionId, rafts, kahunas, ct);
            outcome = await driver.ForceSplitAtKeyAsync(Space, SplitKey, null, ct);
            if (outcome.IsSuccess)
                break;
            await Task.Delay(200, ct);
        }

        Volatile.Write(ref splitDone, true);
        await Task.WhenAll(workers);

        Assert.True(outcome.IsSuccess, $"Split did not land under the write load: {outcome.Status}");
        Assert.True(totalCommits > 0, "No transfer committed at all — the run says nothing about atomicity");

        // Let deferred settlement drain so revisions are final before reconciliation.
        foreach (string key in legOne.Concat(legTwo))
        {
            string k = key;
            foreach (KahunaManager manager in kahunas)
            {
                KahunaManager observer = manager;
                await WaitUntilAsync(() => observer.DurablePreparedIntentStore.Get(k) is null, timeoutMs: 30_000);
            }
        }

        for (int i = 0; i < pairs; i++)
        {
            (string? valueOne, long revisionOne) = await ReadKey(driver, legOne[i], ct);
            (string? valueTwo, long revisionTwo) = await ReadKey(driver, legTwo[i], ct);

            Assert.True(valueOne == valueTwo,
                $"pair {i}: legs diverged — '{valueOne}' vs '{valueTwo}' (a transaction applied one leg only)");
            Assert.True(revisionOne == revisionTwo,
                $"pair {i}: revision drift — {revisionOne} vs {revisionTwo} (an aborted transaction left a leg durable)");
        }
    }
}
