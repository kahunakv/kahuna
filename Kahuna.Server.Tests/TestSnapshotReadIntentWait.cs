using System.Diagnostics;
using System.Text;
using Kahuna.Server.KeyValues;
using Kahuna.Shared.KeyValue;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// What a snapshot read pays for a live foreign write intent that is not yet prepared.
///
/// <para>The bound on such an intent is the stamp of the staged write on the key, or the plant time of a
/// lock that staged nothing, not the writer's transaction id (<c>KeyValueWriteIntentSafeTime</c>). A
/// writer that began before the read's snapshot but staged after it can never commit inside the
/// snapshot, so the read is served at once. A writer that staged before the snapshot holds the read, and
/// the read is served within the short poll cap of the intent clearing (<c>ReadWaitBackoff</c>), not at
/// the next power-of-two boundary of a one-second back-off. Both shapes are checked on the point read and
/// on the streamed range scan, the two reads a table scan and an index probe make.</para>
/// </summary>
public sealed class TestSnapshotReadIntentWait
{
    private const string Prefix = "intentwait";

    /// <summary>How long the writer holds its intent before the test clears it.</summary>
    private static readonly TimeSpan Hold = TimeSpan.FromMilliseconds(700);

    /// <summary>
    /// Slack over the hold for the served read: the poll cap, the actor round trips, and timer jitter. Far
    /// below the 1,023 ms the one-second back-off cost a 700 ms hold, so a regression to it fails here.
    /// </summary>
    private static readonly TimeSpan Slack = TimeSpan.FromMilliseconds(250);

    /// <summary>A read that does not wait finishes well inside this; one that waits takes at least the hold.</summary>
    private static readonly TimeSpan NoWait = TimeSpan.FromMilliseconds(300);

    private readonly ILoggerFactory loggerFactory;

    public TestSnapshotReadIntentWait(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper);
    }

    private static async Task<EmbeddedKahunaNode> StartNode(ILoggerFactory loggerFactory, CancellationToken ct)
    {
        EmbeddedKahunaNode node = new(new EmbeddedKahunaOptions
        {
            TimerInitialDelay = TimeSpan.FromMilliseconds(50),
            ReadIOThreads = 1,
            WriteIOThreads = 1,
            PartitionExecutorPoolSize = 1,
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 1
        }, loggerFactory);

        await node.StartAsync(ct);
        await node.WaitForLeaderForKeyAsync($"{Prefix}/k00", ct);
        return node;
    }

    /// <summary>Ten committed rows under the prefix, so a scan with a page size of four crosses pages.</summary>
    private static async Task SeedRows(EmbeddedKahunaNode node, CancellationToken ct)
    {
        for (int i = 0; i < 10; i++)
        {
            (KeyValueResponseType set, _, _) = await node.Kahuna.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, $"{Prefix}/k{i:D2}", Encoding.UTF8.GetBytes($"v{i}"), null, -1,
                KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.Set, set);
        }
    }

    private static HLCTimestamp Now(KahunaManager manager)
        => manager.Raft.HybridLogicalClock.TrySendOrLocalEvent(manager.Raft.GetLocalNodeId());

    private static async Task Stage(EmbeddedKahunaNode node, HLCTimestamp writer, string key, CancellationToken ct)
    {
        (KeyValueResponseType staged, _, _) = await node.Kahuna.LocateAndTrySetKeyValue(
            writer, key, "staged"u8.ToArray(), null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Set, staged);
    }

    private static async Task Lock(EmbeddedKahunaNode node, HLCTimestamp writer, string key, CancellationToken ct)
    {
        (KeyValueResponseType locked, _, _, _) = await node.Kahuna.LocateAndTryAcquireExclusiveLock(
            writer, key, 30_000, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Locked, locked);
    }

    /// <summary>Clears the writer's intent on the key, as a rollback would.</summary>
    private static async Task Release(KahunaManager manager, HLCTimestamp writer, string key, CancellationToken ct)
    {
        (KeyValueResponseType released, _) = await manager.KeyValues.LocateAndTryReleaseExclusiveLock(
            writer, key, KeyValueDurability.Persistent, ct);
        Assert.True(released is KeyValueResponseType.Unlocked or KeyValueResponseType.DoesNotExist, $"release answered {released}");
    }

    private static async Task<(TimeSpan Elapsed, string Value)> PointRead(EmbeddedKahunaNode node, string key, HLCTimestamp snapshot, CancellationToken ct)
    {
        long started = Stopwatch.GetTimestamp();
        (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await node.Kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, key, -1, snapshot, KeyValueDurability.Persistent, ct);
        TimeSpan elapsed = Stopwatch.GetElapsedTime(started);
        Assert.Equal(KeyValueResponseType.Get, type);
        Assert.NotNull(entry);
        return (elapsed, Encoding.UTF8.GetString(entry.Value!));
    }

    private static async Task<(TimeSpan Elapsed, List<string> Values)> Scan(EmbeddedKahunaNode node, HLCTimestamp snapshot, CancellationToken ct)
    {
        long started = Stopwatch.GetTimestamp();
        List<string> values = [];
        await foreach ((string _, ReadOnlyKeyValueEntry entry) in node.Kahuna.LocateAndScanRange(
            HLCTimestamp.Zero, Prefix, null, true, null, false, pageSize: 4, snapshot, KeyValueDurability.Persistent, ct))
            values.Add(Encoding.UTF8.GetString(entry.Value!));
        return (Stopwatch.GetElapsedTime(started), values);
    }

    private static Task ClearAfterHold(KahunaManager manager, HLCTimestamp writer, string key, CancellationToken ct)
        => Task.Run(async () =>
        {
            await Task.Delay(Hold, ct);
            await Release(manager, writer, key, ct);
        }, ct);

    /// <summary>
    /// The writer's transaction id is below the snapshot, its staged write is above it. The commit will carry
    /// at least the staged stamp, so it lands above the snapshot: neither the point read nor the scan waits,
    /// and both serve the committed value.
    /// </summary>
    [Fact]
    public async Task WriteStagedAfterTheSnapshot_ByAWriterThatBeganBefore_DoesNotBlock()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);
        KahunaManager manager = (KahunaManager)node.Kahuna;
        await SeedRows(node, ct);
        string key = $"{Prefix}/k06";

        HLCTimestamp writer = Now(manager);
        HLCTimestamp snapshot = Now(manager);
        await Stage(node, writer, key, ct);

        try
        {
            (TimeSpan pointElapsed, string value) = await PointRead(node, key, snapshot, ct);
            Assert.Equal("v6", value);
            Assert.True(pointElapsed < NoWait, $"the point read waited {pointElapsed.TotalMilliseconds:F0} ms on a write staged above its snapshot");

            (TimeSpan scanElapsed, List<string> values) = await Scan(node, snapshot, ct);
            Assert.Equal(10, values.Count);
            Assert.Equal("v6", values[6]);
            Assert.True(scanElapsed < NoWait, $"the scan waited {scanElapsed.TotalMilliseconds:F0} ms on a write staged above its snapshot");
        }
        finally
        {
            await Release(manager, writer, key, ct);
        }
    }

    /// <summary>
    /// The write is staged below the snapshot, so the read must wait for the writer's decision. The intent
    /// is cleared after the hold, and the read is served within the poll cap of that moment, with the
    /// committed value: it never costs the next boundary of a one-second back-off.
    /// </summary>
    [Fact]
    public async Task WriteStagedBeforeTheSnapshot_BlocksUntilTheIntentClears_AndIsServedWithinThePollCap()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);
        KahunaManager manager = (KahunaManager)node.Kahuna;
        await SeedRows(node, ct);
        string key = $"{Prefix}/k06";

        // The scan.
        HLCTimestamp writer = Now(manager);
        await Stage(node, writer, key, ct);
        HLCTimestamp snapshot = Now(manager);
        Task clear = ClearAfterHold(manager, writer, key, ct);

        (TimeSpan scanElapsed, List<string> values) = await Scan(node, snapshot, ct);
        await clear;

        Assert.Equal(10, values.Count);
        Assert.Equal("v6", values[6]);
        Assert.True(scanElapsed >= Hold, $"the scan was served after {scanElapsed.TotalMilliseconds:F0} ms, before the intent cleared");
        Assert.True(scanElapsed < Hold + Slack, $"the scan was served {scanElapsed.TotalMilliseconds:F0} ms after a {Hold.TotalMilliseconds:F0} ms hold");

        // The point read.
        writer = Now(manager);
        await Stage(node, writer, key, ct);
        snapshot = Now(manager);
        clear = ClearAfterHold(manager, writer, key, ct);

        (TimeSpan pointElapsed, string value) = await PointRead(node, key, snapshot, ct);
        await clear;

        Assert.Equal("v6", value);
        Assert.True(pointElapsed >= Hold, $"the point read was served after {pointElapsed.TotalMilliseconds:F0} ms, before the intent cleared");
        Assert.True(pointElapsed < Hold + Slack, $"the point read was served {pointElapsed.TotalMilliseconds:F0} ms after a {Hold.TotalMilliseconds:F0} ms hold");
    }

    /// <summary>
    /// A lock planted after the snapshot by a writer that began before it stages nothing on the key yet. Any
    /// write it stages later is stamped after the plant, so its commit lands above the snapshot: the read is
    /// served at once.
    /// </summary>
    [Fact]
    public async Task LockPlantedAfterTheSnapshot_ByAWriterThatBeganBefore_DoesNotBlock()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);
        KahunaManager manager = (KahunaManager)node.Kahuna;
        await SeedRows(node, ct);
        string key = $"{Prefix}/k06";

        HLCTimestamp writer = Now(manager);
        HLCTimestamp snapshot = Now(manager);
        await Lock(node, writer, key, ct);

        try
        {
            (TimeSpan pointElapsed, string value) = await PointRead(node, key, snapshot, ct);
            Assert.Equal("v6", value);
            Assert.True(pointElapsed < NoWait, $"the point read waited {pointElapsed.TotalMilliseconds:F0} ms on a lock planted above its snapshot");

            (TimeSpan scanElapsed, List<string> values) = await Scan(node, snapshot, ct);
            Assert.Equal(10, values.Count);
            Assert.True(scanElapsed < NoWait, $"the scan waited {scanElapsed.TotalMilliseconds:F0} ms on a lock planted above its snapshot");
        }
        finally
        {
            await Release(manager, writer, key, ct);
        }
    }

    /// <summary>
    /// A lock planted before the snapshot may still be followed by a write that commits inside it, so the
    /// read waits for the lock to clear, and is served within the poll cap of the release.
    /// </summary>
    [Fact]
    public async Task LockPlantedBeforeTheSnapshot_BlocksUntilReleased_AndIsServedWithinThePollCap()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);
        KahunaManager manager = (KahunaManager)node.Kahuna;
        await SeedRows(node, ct);
        string key = $"{Prefix}/k06";

        HLCTimestamp writer = Now(manager);
        await Lock(node, writer, key, ct);
        HLCTimestamp snapshot = Now(manager);
        Task clear = ClearAfterHold(manager, writer, key, ct);

        (TimeSpan elapsed, string value) = await PointRead(node, key, snapshot, ct);
        await clear;

        Assert.Equal("v6", value);
        Assert.True(elapsed >= Hold, $"the point read was served after {elapsed.TotalMilliseconds:F0} ms, before the lock cleared");
        Assert.True(elapsed < Hold + Slack, $"the point read was served {elapsed.TotalMilliseconds:F0} ms after a {Hold.TotalMilliseconds:F0} ms hold");
    }
}
