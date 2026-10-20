using System.Collections.Concurrent;
using System.Text;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.KeyValues.Watch;
using Kahuna.Server.Replication;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Data;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// Key-value watches end to end on a three-node cluster, opened on every replica: a write made outside a
/// transaction is reported once per replica after its apply (leader and followers, resident or cold key); a
/// durable transaction's writes are reported once when they settle, never again for their materialization; the
/// registration is synchronous; and a watch ends with Lagged instead of going quiet when the reader falls
/// behind, an apply fails, or a range change moves its scope.
/// </summary>
public sealed class TestKeyValueWatch : BaseCluster
{
    private readonly ILogger<IRaft> raftLogger;

    private readonly ILogger<IKahuna> kahunaLogger;

    // How long a test waits for frames that must not arrive (a repeat, a materialization) after the ones that must.
    private const int QuietMs = 500;

    public TestKeyValueWatch(ITestOutputHelper outputHelper)
    {
        ILoggerFactory loggerFactory = TestLogFactory.Create(outputHelper, quietKommander: true);

        raftLogger = loggerFactory.CreateLogger<IRaft>();
        kahunaLogger = loggerFactory.CreateLogger<IKahuna>();
    }

    [Fact]
    public async Task DirectWrites_AreReportedOncePerReplica_InRevisionAndCommitOrder()
    {
        await WithCluster(async (nodes, ct) =>
        {
            string key = $"watch-direct/{Guid.NewGuid():N}";
            List<Collector> collectors = await OpenOnEveryNode(nodes, KeyValueWatchScope.ForKey(key));

            await Set(nodes[0], key, "v1", ct);
            await Set(nodes[1], key, "v2", ct);
            await Extend(nodes[2], key, ct);
            await Delete(nodes[0], key, ct);

            foreach (Collector collector in collectors)
            {
                List<KeyValueWatchEvent> events = await collector.WaitForEvents(4);
                await collector.AssertNoMoreEvents(4);

                Assert.Equal(
                    [KeyValueWatchEventKind.Set, KeyValueWatchEventKind.Set, KeyValueWatchEventKind.Extended, KeyValueWatchEventKind.Deleted],
                    events.Select(e => e.Kind));

                Assert.Equal("v1", Encoding.UTF8.GetString(events[0].Value!));
                Assert.Equal("v2", Encoding.UTF8.GetString(events[1].Value!));
                Assert.Null(events[2].Value);
                Assert.Null(events[3].Value);

                Assert.True(events[1].Revision > events[0].Revision);
                Assert.Equal(events[1].Revision, events[2].Revision);
                Assert.True(events[3].Revision >= events[2].Revision);

                for (int i = 1; i < events.Count; i++)
                    Assert.True(events[i].CommitTimestamp > events[i - 1].CommitTimestamp, $"event {i} is not after event {i - 1}");

                Assert.All(events, e => Assert.Equal(HLCTimestamp.Zero, e.TransactionId));
            }
        });
    }

    [Fact]
    public async Task Registration_IsSynchronous_OnEveryNode()
    {
        await WithCluster(async (nodes, ct) =>
        {
            foreach (KahunaManager node in nodes)
            {
                string key = $"watch-sync/{Guid.NewGuid():N}";

                (KeyValueResponseType type, KeyValueWatch? watch) = await node.WatchKeyValues(new(KeyValueWatchScope.ForKey(key)), ct);
                Assert.Equal(KeyValueResponseType.Get, type);

                await using Collector collector = new(watch!);
                await Set(node, key, "after-registration", ct);

                KeyValueWatchEvent written = Assert.Single(await collector.WaitForEvents(1));
                Assert.Equal(key, written.Key);
            }
        });
    }

    [Theory]
    [InlineData(true, false)]
    [InlineData(false, false)]
    [InlineData(false, true)]
    public async Task DurableTransaction_IsReportedOnceAtSettle_WithOneCommitTimestamp(bool materializeByReference, bool materializeOnResolve)
    {
        // By reference, the participant logs a record that names the intent; by value, a plain set record that
        // carries the transaction id; on resolve, no record at all. Each must report the write exactly once.
        await WithCluster(configure: c =>
        {
            c.DurableMaterializeByReference = materializeByReference;
            c.DurableMaterializeOnResolve = materializeOnResolve;
        }, body: async (nodes, ct) =>
        {
            string space = $"watch-tx-{Guid.NewGuid():N}";
            string a = space + "/a";
            string b = space + "/b";
            List<Collector> collectors = await OpenOnEveryNode(nodes, KeyValueWatchScope.ForPrefix(space + "/"));

            await RunScript(nodes[1], "BEGIN SET @a 'x' SET @b 'y' COMMIT END", ("@a", a), ("@b", b));

            foreach (Collector collector in collectors)
            {
                List<KeyValueWatchEvent> events = await collector.WaitForEvents(2);

                // The settle follows the materialization that made the values readable; neither the
                // materialization nor the removal of the intents may report the writes a second time.
                await collector.AssertNoMoreEvents(2);

                Assert.Equal([a, b], events.Select(e => e.Key).Order(StringComparer.Ordinal));
                Assert.All(events, e => Assert.Equal(KeyValueWatchEventKind.Set, e.Kind));
                Assert.NotEqual(HLCTimestamp.Zero, events[0].TransactionId);
                Assert.Equal(events[0].TransactionId, events[1].TransactionId);
                Assert.Equal(events[0].CommitTimestamp, events[1].CommitTimestamp);
                Assert.Equal("x", Encoding.UTF8.GetString(events.Single(e => e.Key == a).Value!));
            }
        });
    }

    [Fact]
    public async Task TransactionAcrossPartitions_IsReportedOnEach_WithTheSameIdentity()
    {
        await WithCluster(async (nodes, ct) =>
        {
            string marker = $"watch-xp-{Guid.NewGuid():N}";

            // Two key spaces whose placement groups hash to different partitions.
            string a = $"{marker}-0/k";
            string b = Enumerable.Range(1, 64).Select(i => $"{marker}-{i}/k").First(k => nodes[0].KeyValues.RouteKey(k) != nodes[0].KeyValues.RouteKey(a));

            // A prefix with no group separator can match many groups, so the watch covers every partition.
            List<Collector> collectors = await OpenOnEveryNode(nodes, KeyValueWatchScope.ForPrefix(marker));
            Assert.Equal(4, collectors[0].Watch.Partitions.Count);

            await RunScript(nodes[2], "BEGIN SET @a 'x' SET @b 'y' COMMIT END", ("@a", a), ("@b", b));

            foreach (Collector collector in collectors)
            {
                List<KeyValueWatchEvent> events = await collector.WaitForEvents(2);
                await collector.AssertNoMoreEvents(2);

                Assert.Equal(2, events.Select(e => e.PartitionId).Distinct().Count());
                Assert.Equal(events[0].TransactionId, events[1].TransactionId);
                Assert.Equal(events[0].CommitTimestamp, events[1].CommitTimestamp);
            }
        });
    }

    [Fact]
    public async Task GroupPrefix_CoversOnePartition_AndKeysOnlyDropsTheValue()
    {
        await WithCluster(async (nodes, ct) =>
        {
            string group = $"db{Guid.NewGuid():N}:t|";
            string key = group + "r/1";

            (KeyValueResponseType type, KeyValueWatch? watch) = await nodes[0].WatchKeyValues(new(KeyValueWatchScope.ForPrefix(group), KeysOnly: true), ct);
            Assert.Equal(KeyValueResponseType.Get, type);
            Assert.Equal([nodes[0].KeyValues.RouteKey(key)], watch!.Partitions);

            await using Collector collector = new(watch);
            await Set(nodes[1], key, "value", ct);

            KeyValueWatchEvent written = Assert.Single(await collector.WaitForEvents(1));
            Assert.Equal(key, written.Key);
            Assert.Null(written.Value);
        });
    }

    [Fact]
    public async Task ColdKeyOnAFollower_IsStillReported()
    {
        await WithCluster(async (nodes, ct) =>
        {
            string key = $"watch-cold/{Guid.NewGuid():N}";
            int partitionId = nodes[0].KeyValues.RouteKey(key);

            List<Collector> collectors = await OpenOnEveryNode(nodes, KeyValueWatchScope.ForKey(key));
            await Set(nodes[0], key, "v1", ct);

            foreach (Collector collector in collectors)
                await collector.WaitForEvents(1);

            // Drop the key from every follower's resident cache: a follower that only reported writes to resident
            // keys would miss the next one.
            foreach (KahunaManager node in nodes)
            {
                if (!await node.Raft.AmILeader(partitionId, ct))
                    await node.KeyValues.EvictPartitionEntriesAsync(partitionId);
            }

            await Set(nodes[0], key, "v2", ct);

            foreach (Collector collector in collectors)
            {
                List<KeyValueWatchEvent> events = await collector.WaitForEvents(2);
                Assert.Equal("v2", Encoding.UTF8.GetString(events[1].Value!));
            }
        });
    }

    [Fact]
    public async Task SlowReader_EndsWithOverflow()
    {
        await WithCluster(async (nodes, ct) =>
        {
            string space = $"watch-overflow-{Guid.NewGuid():N}";

            (KeyValueResponseType type, KeyValueWatch? watch) = await nodes[0].WatchKeyValues(new(KeyValueWatchScope.ForPrefix(space + "/"), Capacity: 2), ct);
            Assert.Equal(KeyValueResponseType.Get, type);

            for (int i = 0; i < 6; i++)
                await Set(nodes[0], $"{space}/{i}", "v", ct);

            await WaitUntilAsync(() => watch!.IsEnded);

            List<KeyValueWatchFrame> frames = await ReadToEnd(watch!);
            KeyValueWatchFrame lagged = Assert.Single(frames);
            Assert.Equal(KeyValueWatchLagReason.Overflow, lagged.LagReason);
        });
    }

    [Fact]
    public async Task EphemeralWatch_IsRefused()
    {
        await WithCluster(async (nodes, ct) =>
        {
            (KeyValueResponseType type, KeyValueWatch? watch) = await nodes[0].WatchKeyValues(
                new(KeyValueWatchScope.ForKey("watch-ephemeral/a"), KeyValueDurability.Ephemeral), ct);

            Assert.Equal(KeyValueResponseType.WatchUnsupportedOnEphemeral, type);
            Assert.Null(watch);
            Assert.Equal(0, nodes[0].KeyValues.WatchBroker.RegistrationCount);
        });
    }

    [Fact]
    public async Task FailedApply_EndsThePartitionsWatches()
    {
        await WithCluster(async (nodes, ct) =>
        {
            string key = $"watch-failed-apply/{Guid.NewGuid():N}";
            int partitionId = nodes[1].KeyValues.RouteKey(key);

            (KeyValueResponseType type, KeyValueWatch? watch) = await nodes[1].WatchKeyValues(new(KeyValueWatchScope.ForKey(key)), ct);
            Assert.Equal(KeyValueResponseType.Get, type);

            // An entry the replicator cannot decode: the apply reports failure, and Kommander would move past it.
            RaftLog undecodable = new() { Id = 0, LogType = ReplicationTypes.KeyValues, LogData = [0xFF, 0xFF, 0xFF, 0xFF] };
            Assert.False(await nodes[1].KeyValues.ReplicationDispatcher.OnReplicationReceived(partitionId, undecodable));

            KeyValueWatchFrame lagged = Assert.Single(await ReadToEnd(watch!));
            Assert.Equal(KeyValueWatchLagReason.ApplyDiscontinuity, lagged.LagReason);
            Assert.Equal(partitionId, lagged.PartitionId);
        });
    }

    [Fact]
    public async Task RangeChange_EndsAWatchWhoseKeysMoved()
    {
        await WithCluster(async (nodes, ct) =>
        {
            string space = $"watch-range-{Guid.NewGuid():N}";
            string key = space + "/0001";
            int hashPartition = nodes[0].KeyValues.RouteKey(key);

            List<Collector> collectors = await OpenOnEveryNode(nodes, KeyValueWatchScope.ForKey(key));

            foreach (KahunaManager node in nodes)
                node.RegisterKeyRange(space);

            int target = hashPartition == RangeMapStore.FirstDataPartitionId ? RangeMapStore.FirstDataPartitionId + 1 : RangeMapStore.FirstDataPartitionId;
            KahunaManager metaLeader = await LeaderOf(RangeMapStore.MetaPartitionId, nodes, ct);

            Assert.True(await metaLeader.RangeMapStore.MutateAsync(
                _ => [new RangeDescriptor { KeySpace = space, StartKey = null, EndKey = null, PartitionId = target, Generation = 1 }],
                ct));

            foreach (Collector collector in collectors)
            {
                await WaitUntilAsync(() => collector.Watch.IsEnded);
                KeyValueWatchFrame end = await collector.WaitForEnd();
                Assert.Equal(KeyValueWatchLagReason.RangeMapChanged, end.LagReason);
            }
        });
    }

    // ── helpers ──────────────────────────────────────────────────────────────────────────

    private async Task WithCluster(Func<KahunaManager[], CancellationToken, Task> body, Action<Configuration.KahunaConfiguration>? configure = null)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft r1, IRaft r2, IRaft r3, IKahuna k1, IKahuna k2, IKahuna k3) =
            await AssembleThreNodeCluster("memory", 4, raftLogger, kahunaLogger, configure);

        try
        {
            await body([(KahunaManager)k1, (KahunaManager)k2, (KahunaManager)k3], ct);
        }
        finally
        {
            await LeaveCluster(r1, r2, r3);
        }
    }

    private static async Task<List<Collector>> OpenOnEveryNode(KahunaManager[] nodes, KeyValueWatchScope scope)
    {
        List<Collector> collectors = [];

        foreach (KahunaManager node in nodes)
        {
            (KeyValueResponseType type, KeyValueWatch? watch) = await node.WatchKeyValues(new(scope), TestContext.Current.CancellationToken);
            Assert.Equal(KeyValueResponseType.Get, type);
            collectors.Add(new Collector(watch!));
        }

        return collectors;
    }

    private static async Task<KahunaManager> LeaderOf(int partitionId, KahunaManager[] nodes, CancellationToken ct)
    {
        while (true)
        {
            foreach (KahunaManager node in nodes)
            {
                if (await node.Raft.AmILeader(partitionId, ct))
                    return node;
            }

            await Task.Delay(50, ct);
        }
    }

    private static async Task Set(IKahuna node, string key, string value, CancellationToken ct)
    {
        (KeyValueResponseType type, _, _) = await RetryOnMustRetryAsync(
            () => node.LocateAndTrySetKeyValue(HLCTimestamp.Zero, key, Encoding.UTF8.GetBytes(value), null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct),
            r => r.Item1);

        Assert.Equal(KeyValueResponseType.Set, type);
    }

    private static async Task Extend(IKahuna node, string key, CancellationToken ct)
    {
        (KeyValueResponseType type, _, _) = await RetryOnMustRetryAsync(
            () => node.LocateAndTryExtendKeyValue(HLCTimestamp.Zero, key, 60_000, KeyValueDurability.Persistent, ct),
            r => r.Item1);

        Assert.Equal(KeyValueResponseType.Extended, type);
    }

    private static async Task Delete(IKahuna node, string key, CancellationToken ct)
    {
        (KeyValueResponseType type, _, _) = await RetryOnMustRetryAsync(
            () => node.LocateAndTryDeleteKeyValue(HLCTimestamp.Zero, key, KeyValueDurability.Persistent, ct),
            r => r.Item1);

        Assert.Equal(KeyValueResponseType.Deleted, type);
    }

    private static async Task RunScript(IKahuna node, string script, params (string Name, string Value)[] parameters)
    {
        List<KeyValueParameter> bound = parameters.Select(p => new KeyValueParameter { Key = p.Name, Value = p.Value }).ToList();
        KeyValueTransactionResult result = default!;

        for (int attempt = 0; attempt < 10; attempt++)
        {
            result = await node.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, bound);
            if (result.Type != KeyValueResponseType.MustRetry)
                break;

            await Task.Delay(100, TestContext.Current.CancellationToken);
        }

        Assert.True(
            result.Type is not (KeyValueResponseType.Aborted or KeyValueResponseType.MustRetry or KeyValueResponseType.Errored),
            $"{result.Type}: {result.Reason}");
    }

    private static async Task<List<KeyValueWatchFrame>> ReadToEnd(KeyValueWatch watch)
    {
        using CancellationTokenSource timeout = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        timeout.CancelAfter(TimeSpan.FromSeconds(10));

        List<KeyValueWatchFrame> frames = [];
        await foreach (KeyValueWatchFrame frame in watch.ReadAllAsync(timeout.Token))
            frames.Add(frame);

        return frames;
    }

    /// <summary>Reads a watch in the background into a list the test polls.</summary>
    private sealed class Collector : IAsyncDisposable
    {
        private readonly ConcurrentQueue<KeyValueWatchFrame> frames = new();

        private readonly CancellationTokenSource stop = new();

        private readonly Task pump;

        public Collector(KeyValueWatch watch)
        {
            Watch = watch;
            pump = Task.Run(async () =>
            {
                try
                {
                    await foreach (KeyValueWatchFrame frame in watch.ReadAllAsync(stop.Token))
                        frames.Enqueue(frame);
                }
                catch (OperationCanceledException)
                {
                }
            });
        }

        public KeyValueWatch Watch { get; }

        private List<KeyValueWatchEvent> Events() =>
            frames.Where(f => f.Type == KeyValueWatchFrameType.Event).Select(f => f.Event!).ToList();

        public async Task<List<KeyValueWatchEvent>> WaitForEvents(int count)
        {
            await WaitUntilAsync(() => Events().Count >= count || frames.Any(f => f.IsTerminal));

            List<KeyValueWatchEvent> events = Events();
            Assert.True(events.Count >= count, $"expected {count} events, got {events.Count}; terminal: {frames.FirstOrDefault(f => f.IsTerminal)?.LagReason}");
            return events;
        }

        public async Task AssertNoMoreEvents(int count)
        {
            await Task.Delay(QuietMs, TestContext.Current.CancellationToken);
            Assert.Equal(count, Events().Count);
            Assert.DoesNotContain(frames, f => f.IsTerminal);
        }

        public async Task<KeyValueWatchFrame> WaitForEnd()
        {
            await WaitUntilAsync(() => frames.Any(f => f.IsTerminal));
            return frames.First(f => f.IsTerminal);
        }

        public async ValueTask DisposeAsync()
        {
            await Watch.DisposeAsync();
            await stop.CancelAsync();
            await pump;
            stop.Dispose();
        }
    }
}
