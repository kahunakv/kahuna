using System.Text;
using Kahuna.Server.KeyValues.Watch;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace Kahuna.Server.Tests;

/// <summary>
/// The watch broker and scope on their own: matching, the keys-only and delete shapes, the bounded buffer that
/// ends a slow reader with an overflow instead of growing, partition endings, and unregistration.
/// </summary>
public sealed class TestKeyValueWatchBroker
{
    private static readonly HLCTimestamp Expires = HLCTimestamp.Zero;

    private static readonly HLCTimestamp Commit = new(1, 1000, 1);

    [Fact]
    public void Scope_MatchesOrdinally()
    {
        KeyValueWatchScope key = KeyValueWatchScope.ForKey("ks/a");
        Assert.True(key.Matches("ks/a"));
        Assert.False(key.Matches("ks/A"));
        Assert.False(key.Matches("ks/ab"));

        KeyValueWatchScope prefix = KeyValueWatchScope.ForPrefix("db:t|");
        Assert.True(prefix.Matches("db:t|r/1"));
        Assert.True(prefix.Matches("db:t|i:pk/9"));
        Assert.False(prefix.Matches("db:u|r/1"));

        KeyValueWatchScope range = KeyValueWatchScope.ForRange("ks/", "ks/b", "ks/d");
        Assert.Equal("ks", range.Value);
        Assert.True(range.Matches("ks/b"));
        Assert.True(range.Matches("ks/c"));
        Assert.False(range.Matches("ks/d"));
        Assert.False(range.Matches("ks/a"));

        // A key of another key space is outside the range even when it sorts inside the bounds.
        Assert.False(range.Matches("ks/c/deeper"));

        Assert.Throws<ArgumentException>(() => KeyValueWatchScope.ForRange("ks", "ks/d", "ks/b"));
    }

    [Fact]
    public async Task Publish_DeliversToMatchingWatchesOnly_AndKeysOnlyDropsTheValue()
    {
        KeyValueWatchBroker broker = new();
        Assert.False(broker.HasSubscribers(1));

        await using KeyValueWatch full = broker.Register(KeyValueWatchScope.ForPrefix("ks/"), keysOnly: false, capacity: 16, [1]);
        await using KeyValueWatch keysOnly = broker.Register(KeyValueWatchScope.ForPrefix("ks/"), keysOnly: true, capacity: 16, [1]);
        await using KeyValueWatch other = broker.Register(KeyValueWatchScope.ForPrefix("zz/"), keysOnly: false, capacity: 16, [1]);
        await using KeyValueWatch otherPartition = broker.Register(KeyValueWatchScope.ForPrefix("ks/"), keysOnly: false, capacity: 16, [2]);

        Assert.True(broker.HasSubscribers(1));
        Assert.Equal(4, broker.RegistrationCount);

        byte[] value = Encoding.UTF8.GetBytes("v1");
        broker.Publish(1, KeyValueWatchEventKind.Set, "ks/a", value, 3, Expires, Commit, HLCTimestamp.Zero, logIndex: 1);
        broker.Publish(1, KeyValueWatchEventKind.Deleted, "ks/a", value, 4, Expires, Commit, HLCTimestamp.Zero, logIndex: 1);

        KeyValueWatchFrame[] fullFrames = await ReadAvailable(full, 2);
        Assert.Same(value, fullFrames[0].Event!.Value);
        Assert.Equal(3, fullFrames[0].Event!.Revision);
        Assert.Equal(KeyValueWatchEventKind.Deleted, fullFrames[1].Event!.Kind);
        Assert.Null(fullFrames[1].Event!.Value);

        KeyValueWatchFrame[] keysOnlyFrames = await ReadAvailable(keysOnly, 2);
        Assert.All(keysOnlyFrames, frame => Assert.Null(frame.Event!.Value));
        Assert.Equal("ks/a", keysOnlyFrames[0].Event!.Key);

        Assert.Empty(await ReadAvailable(other, 0));
        Assert.Empty(await ReadAvailable(otherPartition, 0));
    }

    [Fact]
    public async Task SlowReader_GetsOneOverflowFrame_AndTheBacklogIsDropped()
    {
        KeyValueWatchBroker broker = new();
        KeyValueWatch watch = broker.Register(KeyValueWatchScope.ForPrefix("ks/"), keysOnly: false, capacity: 3, [1, 2]);

        for (int i = 0; i < 10; i++)
            broker.Publish(i % 2 == 0 ? 1 : 2, KeyValueWatchEventKind.Set, $"ks/{i}", [1], i + 1, Expires, Commit, HLCTimestamp.Zero, logIndex: 1);

        Assert.True(watch.IsEnded);
        Assert.Equal(0, broker.RegistrationCount);
        Assert.False(broker.HasSubscribers(1));

        List<KeyValueWatchFrame> frames = await ReadToEnd(watch);
        KeyValueWatchFrame lagged = Assert.Single(frames);
        Assert.Equal(KeyValueWatchFrameType.Lagged, lagged.Type);
        Assert.Equal(KeyValueWatchLagReason.Overflow, lagged.LagReason);
    }

    [Fact]
    public async Task EndPartition_EndsOnlyTheWatchesOnThatPartition_AfterWhatTheyAlreadyRead()
    {
        KeyValueWatchBroker broker = new();
        KeyValueWatch onOne = broker.Register(KeyValueWatchScope.ForPrefix("ks/"), keysOnly: false, capacity: 16, [1, 2]);
        await using KeyValueWatch onThree = broker.Register(KeyValueWatchScope.ForPrefix("ks/"), keysOnly: false, capacity: 16, [3]);

        broker.Publish(1, KeyValueWatchEventKind.Set, "ks/a", [1], 1, Expires, Commit, HLCTimestamp.Zero, logIndex: 1);
        KeyValueWatchFrame[] read = await ReadAvailable(onOne, 1);
        Assert.Single(read);

        broker.EndPartition(2, KeyValueWatchLagReason.PartitionStateReplaced);

        List<KeyValueWatchFrame> rest = await ReadToEnd(onOne);
        KeyValueWatchFrame lagged = Assert.Single(rest);
        Assert.Equal(KeyValueWatchLagReason.PartitionStateReplaced, lagged.LagReason);
        Assert.Equal(2, lagged.PartitionId);

        Assert.False(onThree.IsEnded);
        Assert.Equal(1, broker.RegistrationCount);
    }

    [Fact]
    public async Task Dispose_Unregisters_AndTheReaderEndsWithoutAFrame()
    {
        KeyValueWatchBroker broker = new();
        KeyValueWatch watch = broker.Register(KeyValueWatchScope.ForKey("ks/a"), keysOnly: false, capacity: 16, [1]);

        await watch.DisposeAsync();
        await watch.DisposeAsync();

        Assert.Equal(0, broker.RegistrationCount);
        Assert.Empty(await ReadToEnd(watch));

        // Publishing after the close reaches nobody.
        broker.Publish(1, KeyValueWatchEventKind.Set, "ks/a", [1], 1, Expires, Commit, HLCTimestamp.Zero, logIndex: 1);
        Assert.Empty(await ReadToEnd(watch));
    }

    [Fact]
    public async Task ConcurrentPublishersAndRegistrations_DeliverEveryEventToAWatchThatStaysOpen()
    {
        KeyValueWatchBroker broker = new();
        const int perPartition = 2_000;
        await using KeyValueWatch watch = broker.Register(KeyValueWatchScope.ForPrefix("ks/"), keysOnly: true, capacity: perPartition * 4, [1, 2, 3, 4]);

        using CancellationTokenSource churnStop = new();
        Task churn = Task.Run(async () =>
        {
            // Registration changes race the publishers; they must never drop the open watch.
            while (!churnStop.IsCancellationRequested)
            {
                KeyValueWatch transient = broker.Register(KeyValueWatchScope.ForPrefix("other/"), keysOnly: true, capacity: 1, [1, 2, 3, 4]);
                await transient.DisposeAsync();
            }
        }, TestContext.Current.CancellationToken);

        Task[] publishers = Enumerable.Range(1, 4).Select(partition => Task.Run(() =>
        {
            for (int i = 0; i < perPartition; i++)
                broker.Publish(partition, KeyValueWatchEventKind.Set, $"ks/{partition}/{i}", null, i + 1, Expires, Commit, HLCTimestamp.Zero, logIndex: 1);
        }, TestContext.Current.CancellationToken)).ToArray();

        await Task.WhenAll(publishers);
        await churnStop.CancelAsync();
        await churn;

        KeyValueWatchFrame[] frames = await ReadAvailable(watch, perPartition * 4);
        Assert.Equal(perPartition * 4, frames.Length);

        // Each partition's events arrive in the order its single publisher sent them.
        foreach (IGrouping<int, KeyValueWatchFrame> partition in frames.GroupBy(frame => frame.PartitionId))
            Assert.Equal(Enumerable.Range(1, perPartition).Select(i => (long)i), partition.Select(frame => frame.Event!.Revision));
    }

    private static async Task<KeyValueWatchFrame[]> ReadAvailable(KeyValueWatch watch, int expected)
    {
        List<KeyValueWatchFrame> frames = [];
        if (expected == 0)
        {
            using CancellationTokenSource quiet = new(TimeSpan.FromMilliseconds(100));
            try
            {
                await foreach (KeyValueWatchFrame frame in watch.ReadAllAsync(quiet.Token))
                    frames.Add(frame);
            }
            catch (OperationCanceledException)
            {
            }

            return [.. frames];
        }

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
