using Kahuna.Server.KeyValues.Watch;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace Kahuna.Server.Tests;

/// <summary>
/// The node-wide byte budget of the watch buffers: slow readers on distinct keys cannot together buffer past it, the
/// watch that holds the most ends first, a reader that keeps up is never the one ended, and the node's aggregate
/// returns to zero however a watch ends, including when its reader races the end.
/// </summary>
public sealed class TestKeyValueWatchNodeBudget
{
    private static readonly HLCTimestamp Expires = HLCTimestamp.Zero;

    private static readonly HLCTimestamp Commit = new(1, 1000, 1);

    private static readonly int[] PublisherPartitions = [1, 2];

    // The charge of one frame: the fixed overhead, the key in UTF-16 and the value.
    private static long FrameBytes(string key, int valueBytes) => 64 + key.Length * 2L + valueBytes;

    [Fact]
    public async Task SlowWatchesOnDistinctKeys_StayWithinTheBudget_AndAFastReaderSurvives()
    {
        const int valueBytes = 64 * 1024;
        const long budget = 4L * 1024 * 1024;
        const int slowCount = 20;
        const int rounds = 12;

        // The per-watch cap stays at its default, far above what any one watch reaches here: only the node budget acts.
        KeyValueWatchBroker broker = new(maxNodeBufferedBytes: budget);

        KeyValueWatch[] slow = new KeyValueWatch[slowCount];
        for (int i = 0; i < slowCount; i++)
            slow[i] = broker.Register(KeyValueWatchScope.ForKey($"ks/{i}"), keysOnly: false, capacity: 10_000, [1]);

        KeyValueWatch fast = broker.Register(KeyValueWatchScope.ForKey("fast"), keysOnly: false, capacity: 10_000, [1]);

        byte[] value = new byte[valueBytes];
        long revision = 0;
        int fastRead = 0;

        for (int round = 0; round < rounds; round++)
        {
            for (int i = 0; i < slowCount; i++)
            {
                broker.Publish(1, KeyValueWatchEventKind.Set, $"ks/{i}", value, ++revision, Expires, Commit, HLCTimestamp.Zero, logIndex: revision);
                Assert.True(broker.BufferedBytes <= budget, $"Buffered {broker.BufferedBytes} bytes past the budget of {budget}.");
            }

            broker.Publish(1, KeyValueWatchEventKind.Set, "fast", value, ++revision, Expires, Commit, HLCTimestamp.Zero, logIndex: revision);
            Assert.True(broker.BufferedBytes <= budget);

            // The fast reader takes every frame as it comes.
            fastRead += (await ReadAvailable(fast, 1)).Length;
        }

        Assert.False(fast.IsEnded);
        Assert.Equal(rounds, fastRead);
        Assert.Equal(0, fast.NodeShare);

        // Twenty slow readers would buffer 20 x 12 frames of 64 KiB (15 MiB); the budget holds 4 MiB, so some ended.
        KeyValueWatch[] ended = slow.Where(watch => watch.IsEnded).ToArray();
        int[] open = Enumerable.Range(0, slowCount).Where(i => !slow[i].IsEnded).ToArray();
        Assert.NotEmpty(ended);
        Assert.NotEmpty(open);

        foreach (KeyValueWatch watch in ended)
        {
            Assert.Equal(0, watch.NodeShare);
            KeyValueWatchFrame lagged = Assert.Single(await ReadToEnd(watch));
            Assert.Equal(KeyValueWatchFrameType.Lagged, lagged.Type);
            Assert.Equal(KeyValueWatchLagReason.Overflow, lagged.LagReason);
        }

        // The watches the budget kept still stream: each delivers what it buffered, in revision order.
        long expected = 0;
        foreach (int i in open)
        {
            KeyValueWatch watch = slow[i];
            long share = watch.NodeShare;
            Assert.True(share > 0);
            expected += share;

            int frames = (int)(share / FrameBytes($"ks/{i}", valueBytes));
            KeyValueWatchFrame[] read = await ReadAvailable(watch, frames);
            Assert.Equal(frames, read.Length);
            Assert.True(read.Select(frame => frame.Event!.Revision).SequenceEqual(read.Select(frame => frame.Event!.Revision).Order()));
            Assert.Equal(0, watch.NodeShare);
        }

        Assert.True(expected <= budget);
        Assert.Equal(0, broker.BufferedBytes);

        foreach (int i in open)
            await slow[i].DisposeAsync();

        await fast.DisposeAsync();
        Assert.Equal(0, broker.BufferedBytes);
        Assert.Equal(0, broker.OpenWatchCount);
    }

    [Fact]
    public async Task TheWatchThatHoldsTheMost_IsEndedFirst_NotTheOneThatArrives()
    {
        const int valueBytes = 1000;
        long frame = FrameBytes("a", valueBytes);
        long budget = frame * 10;

        KeyValueWatchBroker broker = new(maxNodeBufferedBytes: budget);
        KeyValueWatch large = broker.Register(KeyValueWatchScope.ForKey("a"), keysOnly: false, capacity: 100, [1]);
        await using KeyValueWatch small = broker.Register(KeyValueWatchScope.ForKey("b"), keysOnly: false, capacity: 100, [1]);

        byte[] value = new byte[valueBytes];

        for (int i = 1; i <= 8; i++)
            broker.Publish(1, KeyValueWatchEventKind.Set, "a", value, i, Expires, Commit, HLCTimestamp.Zero, logIndex: i);

        broker.Publish(1, KeyValueWatchEventKind.Set, "b", value, 9, Expires, Commit, HLCTimestamp.Zero, logIndex: 9);
        broker.Publish(1, KeyValueWatchEventKind.Set, "b", value, 10, Expires, Commit, HLCTimestamp.Zero, logIndex: 10);
        Assert.Equal(budget, broker.BufferedBytes);

        // The third frame of "b" passes the budget: "a" holds the most, so "a" ends and "b" keeps the frame.
        broker.Publish(1, KeyValueWatchEventKind.Set, "b", value, 11, Expires, Commit, HLCTimestamp.Zero, logIndex: 11);

        Assert.True(large.IsEnded);
        Assert.False(small.IsEnded);
        Assert.Equal(frame * 3, broker.BufferedBytes);
        Assert.Equal(KeyValueWatchLagReason.Overflow, Assert.Single(await ReadToEnd(large)).LagReason);
        Assert.Equal([9L, 10L, 11L], (await ReadAvailable(small, 3)).Select(f => f.Event!.Revision));
        Assert.Equal(0, broker.BufferedBytes);
    }

    [Fact]
    public async Task AWatchThatHoldsTheMost_EndsItself_WhenItsOwnFramePassesTheBudget()
    {
        const int valueBytes = 1000;
        long frame = FrameBytes("a", valueBytes);

        KeyValueWatchBroker broker = new(maxNodeBufferedBytes: frame * 4);
        KeyValueWatch watch = broker.Register(KeyValueWatchScope.ForKey("a"), keysOnly: false, capacity: 100, [1]);
        await using KeyValueWatch idle = broker.Register(KeyValueWatchScope.ForKey("b"), keysOnly: false, capacity: 100, [1]);

        byte[] value = new byte[valueBytes];

        for (int i = 1; i <= 5; i++)
            broker.Publish(1, KeyValueWatchEventKind.Set, "a", value, i, Expires, Commit, HLCTimestamp.Zero, logIndex: i);

        Assert.True(watch.IsEnded);
        Assert.False(idle.IsEnded);
        Assert.Equal(0, broker.BufferedBytes);
        Assert.Equal(KeyValueWatchLagReason.Overflow, Assert.Single(await ReadToEnd(watch)).LagReason);
    }

    [Fact]
    public async Task TheAggregateReturnsToZero_AfterEveryKindOfEnd()
    {
        KeyValueWatchBroker broker = new(maxBufferedBytes: 1024 * 1024, maxNodeBufferedBytes: 64L * 1024 * 1024);
        byte[] value = new byte[100];

        KeyValueWatch drained = broker.Register(KeyValueWatchScope.ForPrefix("ks/"), keysOnly: false, capacity: 100, [1]);
        KeyValueWatch closed = broker.Register(KeyValueWatchScope.ForPrefix("ks/"), keysOnly: false, capacity: 100, [1]);
        KeyValueWatch overflowed = broker.Register(KeyValueWatchScope.ForPrefix("ks/"), keysOnly: false, capacity: 3, [1]);
        KeyValueWatch partitionEnded = broker.Register(KeyValueWatchScope.ForPrefix("ks/"), keysOnly: false, capacity: 100, [1]);

        for (int i = 1; i <= 5; i++)
            broker.Publish(1, KeyValueWatchEventKind.Set, $"ks/{i}", value, i, Expires, Commit, HLCTimestamp.Zero, logIndex: i);

        Assert.True(overflowed.IsEnded);
        Assert.Equal(0, overflowed.NodeShare);
        Assert.Equal(3 * 5 * FrameBytes("ks/1", value.Length), broker.BufferedBytes);

        Assert.Equal(5, (await ReadAvailable(drained, 5)).Length);
        Assert.Equal(0, drained.NodeShare);

        await closed.DisposeAsync();
        Assert.Equal(0, closed.NodeShare);

        broker.EndPartition(1, KeyValueWatchLagReason.PartitionStateReplaced);
        Assert.Equal(0, partitionEnded.NodeShare);

        // The drained watch ended with the partition too; it held nothing.
        Assert.Equal(0, broker.BufferedBytes);
        Assert.Equal(0, broker.OpenWatchCount);
    }

    [Fact]
    public async Task HeldBackHeldBehindAKeyAndFilteredFrames_AreReleasedExactlyOnce()
    {
        KeyValueWatchBroker broker = new(maxNodeBufferedBytes: 64L * 1024 * 1024);
        byte[] value = new byte[10];
        long frame = FrameBytes("ks/a", value.Length);

        // A replaying partition holds its live frames back; the replay covers entries 1-2, so they are dropped.
        KeyValueWatch replaying = broker.TryRegister(KeyValueWatchScope.ForPrefix("ks/"), false, 100, [1], replayPartitions: [1])!;

        for (int i = 1; i <= 4; i++)
            broker.Publish(1, KeyValueWatchEventKind.Set, "ks/a", value, i, Expires, Commit, HLCTimestamp.Zero, logIndex: i);

        Assert.Equal(4 * frame, broker.BufferedBytes);

        replaying.CompleteReplay(1, replayedThrough: 2);
        Assert.Equal(2 * frame, broker.BufferedBytes);
        Assert.Equal([3L, 4L], (await ReadAvailable(replaying, 2)).Select(f => f.Event!.Revision));
        Assert.Equal(0, broker.BufferedBytes);
        await replaying.DisposeAsync();

        // A resume cursor at or above the commit drops the frame as it arrives. On its own partition: the progress
        // watermark sent here would refuse the later events at the same commit on partition 1.
        KeyValueWatch resumed = broker.TryRegister(
            KeyValueWatchScope.ForPrefix("ks/"), false, 100, [3],
            resumeCursors: new Dictionary<int, HLCTimestamp> { [3] = new(1, 2000, 1) })!;

        broker.Publish(3, KeyValueWatchEventKind.Set, "ks/a", value, 5, Expires, Commit, HLCTimestamp.Zero, logIndex: 5);
        broker.PublishProgress(3, new HLCTimestamp(1, 1500, 1), logIndex: 5);
        Assert.Equal(0, broker.BufferedBytes);
        await resumed.DisposeAsync();

        // A frame held behind a pending intent stays charged until the reader takes it after the release; a
        // transaction end the watch delivered no event of is dropped.
        KeyValueWatch holding = broker.Register(KeyValueWatchScope.ForPrefix("ks/"), keysOnly: false, capacity: 100, [1]);

        broker.Publish(1, KeyValueWatchEventKind.Set, "ks/a", value, 6, Expires, Commit, HLCTimestamp.Zero, logIndex: 6, heldBehindPendingIntent: true);
        broker.PublishTransactionEnd(1, new HLCTimestamp(1, 900, 1), logIndex: 6);
        Assert.Equal(frame, broker.BufferedBytes);

        broker.PublishIntentSettled(1, "ks/a", logIndex: 7);
        Assert.Equal(frame, broker.BufferedBytes);
        Assert.Equal(6, Assert.Single(await ReadAvailable(holding, 1)).Event!.Revision);
        Assert.Equal(0, broker.BufferedBytes);

        // Held behind a key at the end: the end releases it.
        broker.Publish(1, KeyValueWatchEventKind.Set, "ks/a", value, 8, Expires, Commit, HLCTimestamp.Zero, logIndex: 8, heldBehindPendingIntent: true);
        Assert.Equal(frame, broker.BufferedBytes);
        await holding.DisposeAsync();
        Assert.Equal(0, broker.BufferedBytes);
    }

    [Fact]
    public async Task AReplayWithNothingBuffered_EndsTheWatchThatHoldsTheBudget()
    {
        const int valueBytes = 1000;
        long frame = FrameBytes("a", valueBytes);

        KeyValueWatchBroker broker = new(maxNodeBufferedBytes: frame * 4);
        KeyValueWatch slow = broker.Register(KeyValueWatchScope.ForKey("a"), keysOnly: false, capacity: 100, [1]);
        await using KeyValueWatch replaying = broker.TryRegister(KeyValueWatchScope.ForKey("b"), false, 100, [2], replayPartitions: [2])!;

        byte[] value = new byte[valueBytes];
        for (int i = 1; i <= 4; i++)
            broker.Publish(1, KeyValueWatchEventKind.Set, "a", value, i, Expires, Commit, HLCTimestamp.Zero, logIndex: i);

        Assert.Equal(frame * 4, broker.BufferedBytes);

        KeyValueWatchEvent @event = new("b", KeyValueWatchEventKind.Set, value, 1, Expires, Commit, HLCTimestamp.Zero, 2, null);
        using CancellationTokenSource timeout = new(TimeSpan.FromSeconds(10));
        Assert.True(await replaying.DeliverReplayedAsync(KeyValueWatchFrame.ForEvent(@event), null, timeout.Token));

        Assert.True(slow.IsEnded);
        Assert.Equal(frame, broker.BufferedBytes);

        replaying.CompleteReplay(2, replayedThrough: 0);
        Assert.Single(await ReadAvailable(replaying, 1));
        Assert.Equal(0, broker.BufferedBytes);
    }

    [Fact]
    public async Task ReadersRacingTheEnd_LeaveTheAggregateAtZero()
    {
        const int iterations = 150;
        byte[] value = new byte[512];

        CancellationToken testToken = TestContext.Current.CancellationToken;

        for (int iteration = 0; iteration < iterations; iteration++)
        {
            // A budget a few watches fill, so node-budget ends race the readers and the other ends too.
            KeyValueWatchBroker broker = new(maxNodeBufferedBytes: 96 * 1024);
            using CancellationTokenSource stop = CancellationTokenSource.CreateLinkedTokenSource(testToken);
            stop.CancelAfter(TimeSpan.FromSeconds(30));

            KeyValueWatch[] watches = new KeyValueWatch[6];
            // Half on both partitions, so the partition end takes them; the rest stay open until the end of the run.
            for (int i = 0; i < watches.Length; i++)
                watches[i] = broker.Register(KeyValueWatchScope.ForPrefix("ks/"), keysOnly: false, capacity: 1_000, i % 2 == 0 ? [1, 2] : [1]);

            // Two readers drain to the end, one stops reading part way (a consumer that went away), three never read.
            Task[] readers =
            [
                Task.Run(() => Drain(watches[0], int.MaxValue, stop.Token), testToken),
                Task.Run(() => Drain(watches[1], int.MaxValue, stop.Token), testToken),
                Task.Run(() => Drain(watches[2], 50, stop.Token), testToken)
            ];

            Task[] publishers = PublisherPartitions.Select(partition => Task.Run(() =>
            {
                for (int i = 1; i <= 300; i++)
                    broker.Publish(partition, KeyValueWatchEventKind.Set, $"ks/{partition}/{i}", value, i, Expires, Commit, HLCTimestamp.Zero, logIndex: i);
            }, testToken)).ToArray();

            int seed = iteration;
            Task enders = Task.Run(async () =>
            {
                Random random = new(seed);
                await Task.Delay(random.Next(0, 3), testToken);
                await watches[0].DisposeAsync();
                await Task.Delay(random.Next(0, 3), testToken);
                broker.EndPartition(2, KeyValueWatchLagReason.PartitionStateReplaced);
            }, testToken);

            await Task.WhenAll(publishers);
            await enders;

            broker.EndAll(KeyValueWatchLagReason.Shutdown);
            await Task.WhenAll(readers);

            Assert.All(watches, watch => Assert.Equal(0, watch.NodeShare));
            Assert.Equal(0, broker.BufferedBytes);
            Assert.Equal(0, broker.OpenWatchCount);
        }
    }

    private static async Task Drain(KeyValueWatch watch, int limit, CancellationToken cancellationToken)
    {
        int read = 0;
        await foreach (KeyValueWatchFrame _ in watch.ReadAllAsync(cancellationToken))
        {
            if (++read >= limit)
                return;
        }
    }

    private static async Task<KeyValueWatchFrame[]> ReadAvailable(KeyValueWatch watch, int expected)
    {
        List<KeyValueWatchFrame> frames = [];
        if (expected == 0)
            return [];

        using CancellationTokenSource timeout = new(TimeSpan.FromSeconds(10));
        await foreach (KeyValueWatchFrame frame in watch.ReadAllAsync(timeout.Token))
        {
            frames.Add(frame);
            if (frames.Count == expected)
                break;
        }

        return [.. frames];
    }

    private static async Task<List<KeyValueWatchFrame>> ReadToEnd(KeyValueWatch watch)
    {
        using CancellationTokenSource timeout = new(TimeSpan.FromSeconds(10));
        List<KeyValueWatchFrame> frames = [];

        await foreach (KeyValueWatchFrame frame in watch.ReadAllAsync(timeout.Token))
            frames.Add(frame);

        return frames;
    }
}
