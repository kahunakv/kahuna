using System.Text;
using Kahuna;
using Kahuna.Server.KeyValues;
using Kahuna.Shared.KeyValue;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// A range scan whose page fails with a non-retryable response type must fail loudly instead of
/// ending the stream. Before the loud failure existed, a failed page silently terminated the
/// enumerable: a caller collecting the results received an empty or short collection that was
/// indistinguishable from a completed scan over an empty or small range. Consumers that treat an
/// empty enumerable as "this range holds nothing" then acted on the truncated answer and reported
/// success. The same silent truncation applied when a page returned an undecodable continuation
/// cursor while more items were pending. These tests drive both shapes through the real scan loop
/// via the page interceptor seam and assert the caller observes an exception, never a short result.
/// </summary>
public sealed class TestScanPageFailure
{
    private readonly ILoggerFactory loggerFactory;

    public TestScanPageFailure(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper);
    }

    private static async Task<EmbeddedKahunaNode> StartNode(ILoggerFactory loggerFactory, CancellationToken ct)
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

        EmbeddedKahunaNode node = new(options, loggerFactory);
        await node.StartAsync(ct);
        await node.WaitForLeaderForKeyAsync("scanfail/k00", ct);
        return node;
    }

    private static async Task SeedRows(IKahuna kahuna, string prefix, int count, CancellationToken ct)
    {
        for (int i = 0; i < count; i++)
        {
            (KeyValueResponseType set, _, _) = await kahuna.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, $"{prefix}/k{i:D2}", Encoding.UTF8.GetBytes($"v{i}"), null, -1,
                KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.Set, set);
        }
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
    public async Task ScanPage_ErroredOnFirstPage_ThrowsInsteadOfEmptyResult()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);

        KahunaManager manager = (KahunaManager)node.Kahuna;
        const string prefix = "scanfail";

        await SeedRows(node.Kahuna, prefix, 10, ct);

        manager.KeyValues.RoutedScans.TestScanPageInterceptor = (pageIndex, page) =>
            pageIndex == 0 ? new KeyValueGetByRangeResult(KeyValueResponseType.Errored, [], null, false) : page;

        KahunaServerException thrown = await Assert.ThrowsAsync<KahunaServerException>(() =>
            DrainScan(node.Kahuna.LocateAndScanRange(
                HLCTimestamp.Zero, prefix,
                null, true, null, false,
                pageSize: 4, HLCTimestamp.Zero,
                KeyValueDurability.Persistent, ct)));

        // The failure names the range and carries the response type so a caller can classify it
        // without parsing the message.
        Assert.Contains(prefix, thrown.Message);
        Assert.Contains(nameof(KeyValueResponseType.Errored), thrown.Message);
        Assert.Equal(KeyValueResponseType.Errored, thrown.ResponseType);

        // With the fault removed the same scan serves completely: the loud failure only reports a
        // failed page, it never fails a range that can serve.
        manager.KeyValues.RoutedScans.TestScanPageInterceptor = null;

        List<(string Key, ReadOnlyKeyValueEntry Entry)> rows = await DrainScan(node.Kahuna.LocateAndScanRange(
            HLCTimestamp.Zero, prefix,
            null, true, null, false,
            pageSize: 4, HLCTimestamp.Zero,
            KeyValueDurability.Persistent, ct));

        Assert.Equal(10, rows.Count);
    }

    [Fact]
    public async Task ScanPage_ErroredOnLaterPage_ThrowsInsteadOfShortResult()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);

        KahunaManager manager = (KahunaManager)node.Kahuna;
        const string prefix = "scanfail";

        await SeedRows(node.Kahuna, prefix, 10, ct);

        // Page size 2 over 10 rows produces 5 pages; failing page 2 means the caller has already
        // consumed rows from pages 0 and 1 when the failure lands. This is the dangerous shape: a
        // short result that looks like a small range.
        manager.KeyValues.RoutedScans.TestScanPageInterceptor = (pageIndex, page) =>
            pageIndex == 2 ? new KeyValueGetByRangeResult(KeyValueResponseType.Errored, [], null, false) : page;

        List<(string, ReadOnlyKeyValueEntry)> rows = [];
        KahunaServerException thrown = await Assert.ThrowsAsync<KahunaServerException>(async () =>
        {
            await foreach ((string key, ReadOnlyKeyValueEntry entry) in node.Kahuna.LocateAndScanRange(
                HLCTimestamp.Zero, prefix,
                null, true, null, false,
                pageSize: 2, HLCTimestamp.Zero,
                KeyValueDurability.Persistent, ct))
                rows.Add((key, entry));
        });

        // The stream yielded the healthy pages, then failed — it did not end as if the range were done.
        Assert.Equal(4, rows.Count);
        Assert.Equal(KeyValueResponseType.Errored, thrown.ResponseType);
        Assert.Contains(prefix, thrown.Message);
    }

    [Fact]
    public async Task ScanPage_AbortedPage_CarriesResponseTypeOnTheFailure()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);

        KahunaManager manager = (KahunaManager)node.Kahuna;
        const string prefix = "scanfail";

        await SeedRows(node.Kahuna, prefix, 4, ct);

        manager.KeyValues.RoutedScans.TestScanPageInterceptor = (_, _) =>
            new KeyValueGetByRangeResult(KeyValueResponseType.Aborted, [], null, false);

        KahunaServerException thrown = await Assert.ThrowsAsync<KahunaServerException>(() =>
            DrainScan(node.Kahuna.LocateAndScanRange(
                HLCTimestamp.Zero, prefix,
                null, true, null, false,
                pageSize: 4, HLCTimestamp.Zero,
                KeyValueDurability.Persistent, ct)));

        Assert.Equal(KeyValueResponseType.Aborted, thrown.ResponseType);
    }

    [Fact]
    public async Task ScanPage_UndecodableCursorWithMorePending_ThrowsInsteadOfTruncating()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);

        KahunaManager manager = (KahunaManager)node.Kahuna;
        const string prefix = "scanfail";

        await SeedRows(node.Kahuna, prefix, 10, ct);

        // A Get page that claims more items remain but whose continuation cursor cannot be decoded:
        // the scan cannot advance, so it must fail rather than end as if the range were exhausted.
        manager.KeyValues.RoutedScans.TestScanPageInterceptor = (pageIndex, page) =>
            pageIndex == 1 && page.HasMore
                ? new KeyValueGetByRangeResult(KeyValueResponseType.Get, page.Items, "not-a-cursor", true)
                : page;

        List<(string, ReadOnlyKeyValueEntry)> rows = [];
        KahunaServerException thrown = await Assert.ThrowsAsync<KahunaServerException>(async () =>
        {
            await foreach ((string key, ReadOnlyKeyValueEntry entry) in node.Kahuna.LocateAndScanRange(
                HLCTimestamp.Zero, prefix,
                null, true, null, false,
                pageSize: 4, HLCTimestamp.Zero,
                KeyValueDurability.Persistent, ct))
                rows.Add((key, entry));
        });

        // Pages 0 and 1 streamed their rows; the failure lands when the scan tries to advance.
        Assert.Equal(8, rows.Count);
        Assert.Contains("continuation cursor", thrown.Message);
    }
}
