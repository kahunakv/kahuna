using System.Text;
using Google.Protobuf;
using Kahuna.Server.KeyValues;
using Kahuna.Shared.Communication.Grpc;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// Each range-scan page carries what it cost the serving node — entries inspected, dead entries skipped,
/// revision rows read on disk, revision-run seeks, intents merged and retries — so a consumer can attribute
/// read amplification to one scan instead of reading process-wide meters. These tests drive the public
/// <see cref="IKahuna"/> scan surface and assert the counts against data whose shape is known: live keys,
/// deleted keys, deep revision chains on RocksDB, refused attempts, pages without a record, and the same
/// page served locally and by a peer.
/// </summary>
public sealed class TestScanObservations : BaseCluster
{
    private readonly ILoggerFactory loggerFactory;
    private readonly ILogger<IRaft> raftLogger;
    private readonly ILogger<IKahuna> kahunaLogger;

    public TestScanObservations(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper);
        raftLogger = loggerFactory.CreateLogger<IRaft>();
        kahunaLogger = loggerFactory.CreateLogger<IKahuna>();
    }

    internal static async Task<EmbeddedKahunaNode> StartNode(ILoggerFactory loggerFactory, string storage, CancellationToken ct)
    {
        EmbeddedKahunaOptions options = new()
        {
            TimerInitialDelay = TimeSpan.FromMilliseconds(50),
            Storage = storage,
            StoragePath = storage == "rocksdb" ? TempDirectory() : "",
            StorageRevision = "v1",
            WalStorage = "memory",
            InitialPartitions = 1
        };

        EmbeddedKahunaNode node = new(options, loggerFactory);
        await node.StartAsync(ct);
        await node.WaitForLeaderForKeyAsync("obs/k0000", ct);
        return node;
    }

    private static string TempDirectory()
    {
        string dir = Path.Combine(Path.GetTempPath(), "kahuna_scanobs_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        return dir;
    }

    /// <summary>Writes <paramref name="value"/> and returns the commit timestamp of the written version, read
    /// back from the key so it is exactly the <c>LastModified</c> an as-of scan compares against.</summary>
    internal static async Task<HLCTimestamp> Set(IKahuna kahuna, string key, string value, KeyValueDurability durability, CancellationToken ct)
    {
        (KeyValueResponseType type, _, _) = await kahuna.LocateAndTrySetKeyValue(
            HLCTimestamp.Zero, key, Encoding.UTF8.GetBytes(value), null, -1, KeyValueFlags.Set, 0, durability, ct);
        Assert.Equal(KeyValueResponseType.Set, type);

        (KeyValueResponseType got, ReadOnlyKeyValueEntry? entry) = await kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, durability, ct);
        Assert.Equal(KeyValueResponseType.Get, got);
        Assert.NotNull(entry);
        Assert.False(entry.LastModified.IsNull());
        return entry.LastModified;
    }

    internal static async Task<List<(string Key, ReadOnlyKeyValueEntry Entry)>> Scan(
        IKahuna kahuna, string prefix, int pageSize, HLCTimestamp readTimestamp, KeyValueDurability durability,
        KeyValueScanObservationSink? sink, CancellationToken ct)
    {
        List<(string, ReadOnlyKeyValueEntry)> rows = [];
        await foreach ((string key, ReadOnlyKeyValueEntry entry) in kahuna.LocateAndScanRange(
            HLCTimestamp.Zero, prefix, null, true, null, false, pageSize, readTimestamp, durability, ct,
            observations: sink))
            rows.Add((key, entry));
        return rows;
    }

    [Theory]
    [InlineData(KeyValueDurability.Persistent)]
    [InlineData(KeyValueDurability.Ephemeral)]
    public async Task LiveKeys_SinglePage_InspectsEveryKeyOnce(KeyValueDurability durability)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, "memory", ct);

        const int keys = 20;
        for (int i = 0; i < keys; i++)
            await Set(node.Kahuna, $"obs/k{i:D4}", "v" + i, durability, ct);

        KeyValueScanObservationSink sink = new();
        List<(string Key, ReadOnlyKeyValueEntry Entry)> rows = await Scan(node.Kahuna, "obs", 100, HLCTimestamp.Zero, durability, sink, ct);

        Assert.Equal(keys, rows.Count);
        Assert.Equal(1, sink.Pages);
        Assert.Equal(0, sink.PagesUnobserved);
        Assert.Equal(keys, sink.Total.EntriesInspected);
        Assert.Equal(0, sink.Total.DeadEntriesSkipped);
        // The memory backend keeps no revision rows next to the heads.
        Assert.Equal(0, sink.Total.RevisionRowsExamined);
        Assert.Equal(0, sink.Total.RevisionRunSeeks);
        Assert.Equal(0, sink.Total.PageRetries);
        Assert.False(sink.Total.ServedRemotely);
        // Only the persistent path merges the resident entries with a disk read.
        Assert.Equal(durability == KeyValueDurability.Persistent, sink.Total.DiskFallback);
    }

    [Theory]
    [InlineData(KeyValueDurability.Persistent)]
    [InlineData(KeyValueDurability.Ephemeral)]
    public async Task LiveKeys_ManyPages_ReinspectOnlyEachPageSentinel(KeyValueDurability durability)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, "memory", ct);

        const int keys = 20;
        for (int i = 0; i < keys; i++)
            await Set(node.Kahuna, $"obs/k{i:D4}", "v" + i, durability, ct);

        KeyValueScanObservationSink sink = new();
        List<(string Key, ReadOnlyKeyValueEntry Entry)> rows = await Scan(node.Kahuna, "obs", 7, HLCTimestamp.Zero, durability, sink, ct);

        Assert.Equal(keys, rows.Count);
        Assert.Equal(3, sink.Pages);

        // Every page but the last evaluates one key past its limit to decide that more pages exist, and the
        // next page evaluates that key again as its first row: the walk touches it twice, and the count says so.
        Assert.Equal(keys + sink.Pages - 1, sink.Total.EntriesInspected);
        Assert.Equal(0, sink.Total.DeadEntriesSkipped);
    }

    [Theory]
    [InlineData(KeyValueDurability.Persistent)]
    [InlineData(KeyValueDurability.Ephemeral)]
    public async Task DeletedKeys_CountAsDeadEntriesSkipped(KeyValueDurability durability)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, "memory", ct);

        const int keys = 20;
        const int deleted = 5;
        for (int i = 0; i < keys; i++)
            await Set(node.Kahuna, $"obs/k{i:D4}", "v" + i, durability, ct);

        for (int i = 0; i < deleted; i++)
        {
            (KeyValueResponseType type, _, _) = await node.Kahuna.LocateAndTryDeleteKeyValue(
                HLCTimestamp.Zero, $"obs/k{i * 3:D4}", durability, ct);
            Assert.Equal(KeyValueResponseType.Deleted, type);
        }

        KeyValueScanObservationSink sink = new();
        List<(string Key, ReadOnlyKeyValueEntry Entry)> rows = await Scan(node.Kahuna, "obs", 100, HLCTimestamp.Zero, durability, sink, ct);

        Assert.Equal(keys - deleted, rows.Count);
        Assert.Equal(keys, sink.Total.EntriesInspected);
        Assert.Equal(deleted, sink.Total.DeadEntriesSkipped);
    }

    [Fact]
    public async Task RocksDb_KeyWrittenOnce_CostsItsOwnRevisionRow()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, "rocksdb", ct);

        const int keys = 12;
        for (int i = 0; i < keys; i++)
            await Set(node.Kahuna, $"obs/k{i:D4}", "v" + i, KeyValueDurability.Persistent, ct);

        await node.FlushAsync();

        KeyValueScanObservationSink sink = new();
        List<(string Key, ReadOnlyKeyValueEntry Entry)> rows = await Scan(node.Kahuna, "obs", 100, HLCTimestamp.Zero, KeyValueDurability.Persistent, sink, ct);

        Assert.Equal(keys, rows.Count);
        Assert.Equal(keys, sink.Total.EntriesInspected);
        Assert.Equal(0, sink.Total.DeadEntriesSkipped);
        // RocksDB stores every write's revision row next to its head, and the range walk steps over it: one
        // row per key even with no older history. A latest scan resolves no as-of image.
        Assert.Equal(keys, sink.Total.RevisionRowsExamined);
        Assert.Equal(0, sink.Total.RevisionRunSeeks);
    }

    [Fact]
    public async Task RocksDb_AsOfScanOverDeepChains_CountsRevisionRowsAndSeeks()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, "rocksdb", ct);

        (KeyValueScanObservationSink sink, int keys, int revisions, _) = await RunDeepChainAsOfScan(node, ct);

        // The disk walk reads at most the seek threshold of rows per key before it jumps the rest of the
        // block, and the as-of lookup reads at most the key's chain; neither reads fewer than one row per key.
        Assert.InRange(sink.Total.RevisionRowsExamined, keys, 2L * keys * revisions);
        Assert.InRange(sink.Total.RevisionRunSeeks, 1, keys);
        Assert.Equal(keys, sink.Total.EntriesInspected);
        Assert.Equal(0, sink.Total.DeadEntriesSkipped);
    }

    /// <summary>
    /// A bounded probe of one index key (<c>[key/, key0)</c>, the shape of an equality seek) inspects that
    /// key's entries and one sentinel per extra page, whatever the page size and however many other keys the
    /// index holds: the backend stops at the end key instead of filling the page from the keys past it.
    /// </summary>
    [Theory]
    [InlineData("memory")]
    [InlineData("rocksdb")]
    public async Task BoundedProbe_InspectsOnlyItsKeysEntries_WhateverThePageSize(string storage)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, storage, ct);
        IKahuna kahuna = node.Kahuna;

        const int indexKeys = 300;
        const int fanout = 11;
        const int wideKey = 150;
        const int wideFanout = 1500;

        List<KahunaSetKeyValueRequestItem> batch = [];
        byte[] value = "r"u8.ToArray();

        async Task Write()
        {
            List<KahunaSetKeyValueResponseItem> written = await kahuna.LocateAndTrySetManyKeyValue(batch, ct);
            Assert.All(written, item => Assert.Equal(KeyValueResponseType.Set, item.Type));
            batch.Clear();
        }

        for (int key = 0; key < indexKeys; key++)
        {
            int entries = key == wideKey ? wideFanout : fanout;
            for (int row = 0; row < entries; row++)
            {
                batch.Add(new()
                {
                    Key = $"idx/k{key:D4}/{row:D6}", Value = value, Flags = KeyValueFlags.Set, Durability = KeyValueDurability.Persistent
                });
                if (batch.Count == 500)
                    await Write();
            }
        }

        if (batch.Count > 0)
            await Write();

        await node.FlushAsync();

        static async Task<(int Rows, KeyValueScanObservationSink Sink)> Probe(IKahuna kahuna, int key, int pageSize, CancellationToken ct)
        {
            string start = $"idx/k{key:D4}/";
            string end = $"idx/k{key:D4}0";
            KeyValueScanObservationSink sink = new();
            int rows = 0;

            await foreach ((string scanned, ReadOnlyKeyValueEntry _) in kahuna.LocateAndScanRange(
                HLCTimestamp.Zero, "idx/", start, true, end, false, pageSize, HLCTimestamp.Zero,
                KeyValueDurability.Persistent, ct, observations: sink))
            {
                Assert.StartsWith(start, scanned, StringComparison.Ordinal);
                rows++;
            }

            return (rows, sink);
        }

        (int rows, KeyValueScanObservationSink sink) = await Probe(kahuna, 7, 512, ct);
        Assert.Equal(fanout, rows);
        Assert.Equal(1, sink.Pages);
        Assert.Equal(fanout, sink.Total.EntriesInspected);

        (rows, sink) = await Probe(kahuna, wideKey, 512, ct);
        Assert.Equal(wideFanout, rows);
        Assert.Equal(3, sink.Pages);
        Assert.Equal(wideFanout + sink.Pages - 1, sink.Total.EntriesInspected);

        (rows, sink) = await Probe(kahuna, wideKey, 32, ct);
        Assert.Equal(wideFanout, rows);
        Assert.Equal(47, sink.Pages);
        Assert.Equal(wideFanout + sink.Pages - 1, sink.Total.EntriesInspected);

        (rows, sink) = await Probe(kahuna, indexKeys - 1, 512, ct);
        Assert.Equal(fanout, rows);
        Assert.Equal(1, sink.Pages);
        Assert.Equal(fanout, sink.Total.EntriesInspected);
    }

    /// <summary>
    /// Writes <c>revisions</c> versions of each of a few keys, pins a snapshot after the first version,
    /// flushes, and scans as of that snapshot. Every row the scan returns must be the first version.
    /// </summary>
    internal static async Task<(KeyValueScanObservationSink Sink, int Keys, int Revisions, List<(string Key, ReadOnlyKeyValueEntry Entry)> Rows)> RunDeepChainAsOfScan(
        EmbeddedKahunaNode node, CancellationToken ct)
    {
        const int keys = 5;
        const int revisions = 20;

        HLCTimestamp snapshot = HLCTimestamp.Zero;
        for (int i = 0; i < keys; i++)
        {
            HLCTimestamp written = await Set(node.Kahuna, $"obs/k{i:D4}", "first", KeyValueDurability.Persistent, ct);
            if (written.CompareTo(snapshot) > 0)
                snapshot = written;
        }

        for (int r = 1; r < revisions; r++)
            for (int i = 0; i < keys; i++)
                await Set(node.Kahuna, $"obs/k{i:D4}", "v" + r, KeyValueDurability.Persistent, ct);

        await node.FlushAsync();

        KeyValueScanObservationSink sink = new();
        List<(string Key, ReadOnlyKeyValueEntry Entry)> rows = await Scan(node.Kahuna, "obs", 100, snapshot, KeyValueDurability.Persistent, sink, ct);

        Assert.Equal(keys, rows.Count);
        Assert.All(rows, row => Assert.Equal("first", Encoding.UTF8.GetString(row.Entry.Value!)));

        return (sink, keys, revisions, rows);
    }

    [Fact]
    public async Task RetriedPage_ReportsRetriesAndTheSumOverItsAttempts()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, "memory", ct);
        KahunaManager manager = (KahunaManager)node.Kahuna;

        const int keys = 10;
        for (int i = 0; i < keys; i++)
            await Set(node.Kahuna, $"obs/k{i:D4}", "v" + i, KeyValueDurability.Persistent, ct);

        // The first three attempts of page 0 are refused; each refusal reports that it inspected 4 entries.
        // The fourth attempt is the real page.
        const int refusals = 3;
        int attempts = 0;
        KeyValueScanObservation refusedCost = new(4, 1, 2, 0, 0, 0, false, true);

        manager.KeyValues.RoutedScans.TestScanPageInterceptor = (pageIndex, page) =>
            pageIndex == 0 && attempts++ < refusals
                ? new KeyValueGetByRangeResult(KeyValueResponseType.MustRetry, [], null, false, refusedCost)
                : page;

        try
        {
            KeyValueScanObservationSink sink = new();
            List<(string Key, ReadOnlyKeyValueEntry Entry)> rows = await Scan(node.Kahuna, "obs", 100, HLCTimestamp.Zero, KeyValueDurability.Persistent, sink, ct);

            Assert.Equal(keys, rows.Count);
            Assert.Equal(1, sink.Pages);
            Assert.Equal(refusals, sink.Total.PageRetries);
            Assert.Equal(keys + refusals * refusedCost.EntriesInspected, sink.Total.EntriesInspected);
            Assert.Equal(refusals * refusedCost.DeadEntriesSkipped, sink.Total.DeadEntriesSkipped);
            Assert.Equal(refusals * refusedCost.RevisionRowsExamined, sink.Total.RevisionRowsExamined);
        }
        finally
        {
            manager.KeyValues.RoutedScans.TestScanPageInterceptor = null;
        }
    }

    [Fact]
    public async Task PageWithoutRecord_IsUnknown_NeverZero()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, "memory", ct);
        KahunaManager manager = (KahunaManager)node.Kahuna;

        const int keys = 10;
        for (int i = 0; i < keys; i++)
            await Set(node.Kahuna, $"obs/k{i:D4}", "v" + i, KeyValueDurability.Persistent, ct);

        // A peer on an older build answers without the record: page 1 arrives with no observation.
        manager.KeyValues.RoutedScans.TestScanPageInterceptor = (pageIndex, page) =>
            pageIndex == 1 ? new KeyValueGetByRangeResult(page.Type, page.Items, page.NextCursor, page.HasMore) : page;

        try
        {
            KeyValueScanObservationSink sink = new();
            List<(string Key, ReadOnlyKeyValueEntry Entry)> rows = await Scan(node.Kahuna, "obs", 4, HLCTimestamp.Zero, KeyValueDurability.Persistent, sink, ct);

            Assert.Equal(keys, rows.Count);
            Assert.Equal(3, sink.Pages);
            Assert.Equal(1, sink.PagesUnobserved);

            // Pages 0 and 2 inspect 5 and 2 entries (page 0 includes its sentinel); page 1 is not in the total.
            Assert.Equal(7, sink.Total.EntriesInspected);
        }
        finally
        {
            manager.KeyValues.RoutedScans.TestScanPageInterceptor = null;
        }

        // On the wire, a response from a server that predates the record decodes to null, not to zeros.
        GrpcGetByRangeResponse legacy = GrpcGetByRangeResponse.Parser.ParseFrom(
            new GrpcGetByRangeResponse { Type = GrpcKeyValueResponseType.TypeGot, HasMore = false }.ToByteArray());
        Assert.Null(ScanObservationWire.FromGrpc(legacy.Observation));

        KeyValueScanObservation sent = new(9, 3, 40, 2, 1, 5, false, true);
        GrpcGetByRangeResponse current = GrpcGetByRangeResponse.Parser.ParseFrom(
            new GrpcGetByRangeResponse { Type = GrpcKeyValueResponseType.TypeGot, Observation = ScanObservationWire.ToGrpc(sent) }.ToByteArray());
        Assert.Equal(sent, ScanObservationWire.FromGrpc(current.Observation));
    }

    [Fact]
    public async Task ThreeNodeCluster_PeerServedPage_CarriesTheSameNumbersAsTheLocalPage()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft r1, IRaft r2, IRaft r3, IKahuna k1, IKahuna k2, IKahuna k3) =
            await AssembleThreNodeCluster("memory", 1, raftLogger, kahunaLogger);

        try
        {
            const int keys = 12;
            const int deleted = 3;

            HLCTimestamp snapshot = HLCTimestamp.Zero;
            for (int i = 0; i < keys; i++)
            {
                HLCTimestamp written = await Set(k1, $"obs/k{i:D4}", "first", KeyValueDurability.Persistent, ct);
                if (written.CompareTo(snapshot) > 0)
                    snapshot = written;
            }

            // Newer versions above the snapshot make the as-of scan resolve images through the backend.
            for (int i = 0; i < keys; i += 2)
                await Set(k1, $"obs/k{i:D4}", "second", KeyValueDurability.Persistent, ct);

            for (int i = 0; i < deleted; i++)
            {
                (KeyValueResponseType type, _, _) = await k1.LocateAndTryDeleteKeyValue(
                    HLCTimestamp.Zero, $"obs/k{i * 4 + 1:D4}", KeyValueDurability.Persistent, ct);
                Assert.Equal(KeyValueResponseType.Deleted, type);
            }

            foreach (IKahuna kahuna in (IKahuna[])[k1, k2, k3])
                await ((KahunaManager)kahuna).FlushPersistenceAsync();

            foreach (HLCTimestamp readTimestamp in (HLCTimestamp[])[HLCTimestamp.Zero, snapshot])
            {
                List<KeyValueScanObservation> observations = [];
                int remote = 0;

                foreach (IKahuna kahuna in (IKahuna[])[k1, k2, k3])
                {
                    KeyValueScanObservationSink sink = new();
                    List<(string Key, ReadOnlyKeyValueEntry Entry)> rows = await Scan(kahuna, "obs", 100, readTimestamp, KeyValueDurability.Persistent, sink, ct);

                    Assert.Equal(readTimestamp.IsNull() ? keys - deleted : keys, rows.Count);
                    Assert.Equal(0, sink.PagesUnobserved);

                    if (sink.Total.ServedRemotely)
                        remote++;

                    observations.Add(sink.Total);
                }

                // One node leads the partition and serves locally; the other two forward to it.
                Assert.Equal(2, remote);

                KeyValueScanObservation expected = Comparable(observations[0]);
                Assert.All(observations, o => Assert.Equal(expected, Comparable(o)));

                if (readTimestamp.IsNull())
                    Assert.Equal(deleted, expected.DeadEntriesSkipped);
                else
                    Assert.True(expected.RevisionRowsExamined > 0);
            }
        }
        finally
        {
            await LeaveCluster(r1, r2, r3);
        }
    }

    // The fields that must not depend on which node served the page or how often the caller retried.
    private static KeyValueScanObservation Comparable(KeyValueScanObservation o) =>
        o with { ServedRemotely = false, PageRetries = 0 };
}

/// <summary>
/// The per-page record and the process meter must report the same numbers. The meter is process-wide, so
/// the test runs alone.
/// </summary>
[Collection("ExclusiveAllocationMeasurement")]
public sealed class TestScanObservationMeterParity
{
    private readonly ILoggerFactory loggerFactory;

    public TestScanObservationMeterParity(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper);
    }

    [Fact]
    public async Task RevisionRunSeeks_RecordSumEqualsMeterTotal()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await TestScanObservations.StartNode(loggerFactory, "rocksdb", ct);

        using MetricCapture capture = new("", "kahuna.scan.revision_run_seeks_total");

        (KeyValueScanObservationSink sink, _, _, _) = await TestScanObservations.RunDeepChainAsOfScan(node, ct);

        Assert.True(sink.Total.RevisionRunSeeks > 0);
        Assert.Equal(sink.Total.RevisionRunSeeks, (long)capture.Total("kahuna.scan.revision_run_seeks_total"));
    }
}
