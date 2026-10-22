using System.Collections.Concurrent;
using System.Text;
using Kahuna;
using Kahuna.Client;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.KeyValues.Watch;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// A watch that resumes from a progress watermark receives every event above it that it missed, from the retained
/// log, and then the live events with no gap: after a disconnect, on another replica, across concurrent writes, with
/// a transaction still pending at the start, and after the node restarts under a history hold. A resume the node cannot
/// prove complete ends with HistoryUnavailable, never a silent gap. The client resumes a stream on its own when the
/// stream loses continuity.
/// </summary>
public sealed class TestKeyValueWatchResume : BaseCluster
{
    private readonly ILoggerFactory loggerFactory;

    private readonly ILogger<IRaft> raftLogger;

    private readonly ILogger<IKahuna> kahunaLogger;

    public TestKeyValueWatchResume(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper, quietKommander: true);
        raftLogger = loggerFactory.CreateLogger<IRaft>();
        kahunaLogger = loggerFactory.CreateLogger<IKahuna>();
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    public async Task Resume_ReplaysEveryMissedEvent_ThenGoesLive(int resumeOn)
    {
        await WithCluster(lagMs: 1_000, async (nodes, ct) =>
        {
            string group = $"res{Guid.NewGuid():N}|";
            string key = group + "k";
            string other = group + "tx";

            HLCTimestamp cursor;
            await using (FrameCollector first = await Open(nodes[0], new(KeyValueWatchScope.ForPrefix(group)), ct))
            {
                for (int i = 1; i <= 3; i++)
                    await SetValue(nodes[1], key, $"v{i}", ct);

                await first.WaitUntil(c => c.Events(key).Count == 3);
                HLCTimestamp lastSeen = first.Events(key)[^1].CommitTimestamp;

                // The cursor is a watermark at or above everything this watch received.
                await first.WaitUntil(c => c.Watermarks().Any(w => w >= lastSeen), timeoutMs: 15_000);
                cursor = first.Watermarks()[^1];
            }

            // Missed while no watch was open: two direct writes and a transaction.
            await SetValue(nodes[2], key, "v4", ct);
            await SetValue(nodes[0], key, "v5", ct);
            KeyValueTransactionResult committed = await RetryOnMustRetry(nodes[1], Encoding.UTF8.GetBytes("BEGIN SET @k 'tx' COMMIT END"), null, [new KeyValueParameter { Key = "@k", Value = other }]);
            Assert.Equal(KeyValueResponseType.Set, committed.Type);

            // Settled everywhere before the resume opens, so the transaction's event can only come from the replay.
            foreach (KahunaManager node in nodes)
                await WaitUntilAsync(() => node.DurablePreparedIntentStore.Get(other) is null);

            await using FrameCollector resumed = await Open(nodes[resumeOn], new(KeyValueWatchScope.ForPrefix(group), ResumeFrom: cursor), ct);

            await resumed.WaitUntil(c => c.Events(key).Count >= 2 && c.Events(other).Count >= 1);

            await SetValue(nodes[2], key, "v6", ct);
            await resumed.WaitUntil(c => c.Events(key).Any(e => Encoding.UTF8.GetString(e.Value!) == "v6"));

            List<string> values = resumed.Events(key).Select(e => Encoding.UTF8.GetString(e.Value!)).Distinct().ToList();
            Assert.Equal(["v4", "v5", "v6"], values);
            Assert.All(resumed.Frames().Where(f => f.Event is not null), f => Assert.True(f.Event!.CommitTimestamp > cursor));
            Assert.DoesNotContain(resumed.Frames(), f => f.IsTerminal);
            resumed.AssertMutationsInOrder(key);
        });
    }

    [Fact]
    public async Task Resume_DuringConcurrentWrites_LosesNothing()
    {
        await WithCluster(lagMs: 1_000, async (nodes, ct) =>
        {
            string group = $"conc{Guid.NewGuid():N}|";
            string key = group + "k";

            HLCTimestamp cursor;
            await using (FrameCollector first = await Open(nodes[0], new(KeyValueWatchScope.ForPrefix(group)), ct))
            {
                await SetValue(nodes[1], key, "0", ct);
                await first.WaitUntil(c => c.Events(key).Count == 1);
                HLCTimestamp seen = first.Events(key)[0].CommitTimestamp;
                await first.WaitUntil(c => c.Watermarks().Any(w => w >= seen), timeoutMs: 15_000);
                cursor = first.Watermarks()[^1];
            }

            const int writes = 120;
            Task writer = Task.Run(async () =>
            {
                for (int i = 1; i <= writes; i++)
                    await SetValue(nodes[i % 3], key, i.ToString(), ct);
            }, ct);

            // Open the resume while the writes run, so some land in the replay, some in the hand-over, some live.
            await Task.Delay(150, ct);
            await using FrameCollector resumed = await Open(nodes[2], new(KeyValueWatchScope.ForPrefix(group), ResumeFrom: cursor), ct);
            await writer;

            await resumed.WaitUntil(c => c.Events(key).Any(e => Encoding.UTF8.GetString(e.Value!) == writes.ToString()), timeoutMs: 20_000);

            HashSet<string> delivered = resumed.Events(key).Select(e => Encoding.UTF8.GetString(e.Value!)).ToHashSet();
            for (int i = 1; i <= writes; i++)
                Assert.Contains(i.ToString(), delivered);

            Assert.DoesNotContain(resumed.Frames(), f => f.IsTerminal);
            resumed.AssertMutationsInOrder(key);
        });
    }

    [Fact]
    public async Task Resume_WithATransactionPendingAtTheStart_DeliversItsEventOnce()
    {
        await WithCluster(lagMs: 1_000, async (nodes, ct) =>
        {
            string group = $"pend{Guid.NewGuid():N}|";
            string held = group + "held";

            HLCTimestamp cursor;
            await using (FrameCollector first = await Open(nodes[0], new(KeyValueWatchScope.ForPrefix(group)), ct))
            {
                await SetValue(nodes[1], group + "seed", "s", ct);
                await first.WaitUntil(c => c.Watermarks().Count > 0, timeoutMs: 15_000);
            }

            using (ResolutionGate gate = new(nodes[0]))
            {
                KeyValueTransactionResult committed = await RetryOnMustRetry(nodes[0], Encoding.UTF8.GetBytes("BEGIN SET @k 'held' COMMIT END"), null, [new KeyValueParameter { Key = "@k", Value = held }]);
                Assert.Equal(KeyValueResponseType.Set, committed.Type);

                PreparedIntent pending = nodes[0].DurablePreparedIntentStore.Get(held)!;
                Assert.True(pending.IsPending);

                // A cursor below the pending commit: the replay sees the prepare, the settle comes later.
                cursor = new HLCTimestamp(pending.CommitTimestamp.N, pending.CommitTimestamp.L - 500, pending.CommitTimestamp.C);

                await using FrameCollector resumed = await Open(nodes[1], new(KeyValueWatchScope.ForPrefix(group), ResumeFrom: cursor), ct);
                await Task.Delay(500, ct);
                Assert.Empty(resumed.Events(held));

                gate.Dispose();

                await resumed.WaitUntil(c => c.Events(held).Count == 1);
                await Task.Delay(1_000, ct);

                Assert.Single(resumed.Events(held));
                Assert.DoesNotContain(resumed.Frames(), f => f.IsTerminal);
            }
        });
    }

    [Fact]
    public async Task Resume_AtOrBelowAnInstallBoundary_IsHistoryUnavailable()
    {
        await WithCluster(lagMs: 1_000, async (nodes, ct) =>
        {
            string group = $"inst{Guid.NewGuid():N}|";
            await SetValue(nodes[0], group + "k", "v", ct);
            int partitionId = nodes[0].KeyValues.LocateDurablePartition(group + "k").PartitionId;

            nodes[1].KeyValues.HistoryBoundsForTests.Record(partitionId, long.MaxValue / 2);

            await using FrameCollector resumed = await Open(nodes[1], new(KeyValueWatchScope.ForPrefix(group), ResumeFrom: new HLCTimestamp(1, 1, 0)), ct);
            KeyValueWatchFrame end = await resumed.WaitForEnd();

            Assert.Equal(KeyValueWatchFrameType.HistoryUnavailable, end.Type);
            Assert.Equal(partitionId, end.PartitionId);
        });
    }

    [Fact]
    public async Task Resume_WithoutTheWindow_IsHistoryUnavailable()
    {
        await WithCluster(lagMs: 0, async (nodes, ct) =>
        {
            string group = $"nowin{Guid.NewGuid():N}|";
            await SetValue(nodes[0], group + "k", "v", ct);

            await using FrameCollector resumed = await Open(nodes[0], new(KeyValueWatchScope.ForPrefix(group), ResumeFrom: new HLCTimestamp(1, 1, 0)), ct);
            Assert.Equal(KeyValueWatchFrameType.HistoryUnavailable, (await resumed.WaitForEnd()).Type);
        });
    }

    [Fact]
    public async Task ClientAutoResume_HidesALoss_AndReplaysWhatWasMissed()
    {
        await WithCluster(lagMs: 1_000, async (nodes, ct) =>
        {
            string group = $"auto{Guid.NewGuid():N}|";
            string key = group + "k";
            int partitionId = nodes[0].KeyValues.LocateDurablePartition(key).PartitionId;

            Dictionary<string, IKahuna> byUrl = new() { ["node-0"] = nodes[0], ["node-1"] = nodes[1], ["node-2"] = nodes[2] };
            KahunaClient client = new(["node-0", "node-1", "node-2"], communication: new InProcessKahunaCommunication(byUrl));

            await using KahunaWatch watch = await client.WatchKeyValues(new(KeyValueWatchScope.ForPrefix(group)), autoResume: true, ct);
            ConcurrentQueue<KeyValueWatchFrame> frames = new();
            using CancellationTokenSource stop = CancellationTokenSource.CreateLinkedTokenSource(ct);
            Task reader = Task.Run(async () =>
            {
                try
                {
                    await foreach (KeyValueWatchFrame frame in watch.WithCancellation(stop.Token))
                        frames.Enqueue(frame);
                }
                catch (OperationCanceledException)
                {
                }
            }, ct);

            await SetValue(nodes[1], key, "v1", ct);
            await WaitUntilAsync(() => frames.Any(f => f.Event?.Key == key));
            await WaitUntilAsync(() => watch.Watermarks.ContainsKey(partitionId), 15_000);

            // Every node's watches on the partition lose continuity at once; the client resumes from its watermark.
            foreach (KahunaManager node in nodes)
                node.KeyValues.WatchBroker.EndPartition(partitionId, KeyValueWatchLagReason.ApplyDiscontinuity);

            await SetValue(nodes[2], key, "v2", ct);
            await SetValue(nodes[0], key, "v3", ct);

            await WaitUntilAsync(() => frames.Any(f => f.Event is { } e && e.Key == key && Encoding.UTF8.GetString(e.Value!) == "v3"), 15_000);

            HashSet<string> values = frames.Where(f => f.Event?.Key == key).Select(f => Encoding.UTF8.GetString(f.Event!.Value!)).ToHashSet();
            Assert.Superset(new HashSet<string> { "v1", "v2", "v3" }, values);
            Assert.DoesNotContain(frames, f => f.IsTerminal);

            await stop.CancelAsync();
            await reader;
        });
    }

    [Fact]
    public async Task Resume_AfterARestart_UnderAHistoryHold_ReplaysEveryMissedEvent()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        string storagePath = CreateTempDir("kahuna-resume-store-");
        string walPath = CreateTempDir("kahuna-resume-wal-");
        string key = "restart|k";

        try
        {
            HLCTimestamp cursor;

            {
                await using EmbeddedKahunaNode node = new(PersistentOptions(storagePath, walPath), loggerFactory);
                await node.StartAsync(ct);
                await node.WaitForLeaderForKeyAsync(key, ct);

                KahunaManager kahuna = (KahunaManager)node.Kahuna;
                int partitionId = kahuna.KeyValues.LocateDurablePartition(key).PartitionId;

                await using (FrameCollector first = await Open(kahuna, new(KeyValueWatchScope.ForPrefix("restart|")), ct))
                {
                    await SetValue(kahuna, key, "v1", ct);
                    await first.WaitUntil(c => c.Events(key).Count == 1);
                    HLCTimestamp seen = first.Events(key)[0].CommitTimestamp;
                    await first.WaitUntil(c => c.Watermarks().Any(w => w >= seen), timeoutMs: 15_000);
                    cursor = first.Watermarks()[^1];
                }

                HistoryHoldAnswer hold = await RetryOnMustRetryAsync(
                    () => kahuna.HistoryHold(new HistoryHoldRequest(HistoryHoldOperation.Acquire, "cdc-restart", [partitionId], cursor, 600_000), ct), a => a.Type);
                Assert.Equal(KeyValueResponseType.Set, hold.Type);

                await SetValue(kahuna, key, "v2", ct);
            }

            {
                await using EmbeddedKahunaNode node = new(PersistentOptions(storagePath, walPath), loggerFactory);
                await node.StartAsync(ct);
                await node.WaitForLeaderForKeyAsync(key, ct);

                KahunaManager kahuna = (KahunaManager)node.Kahuna;
                await SetValue(kahuna, key, "v3", ct);

                await using FrameCollector resumed = await Open(kahuna, new(KeyValueWatchScope.ForPrefix("restart|"), ResumeFrom: cursor), ct);
                await resumed.WaitUntil(c => c.Events(key).Any(e => Encoding.UTF8.GetString(e.Value!) == "v3"), timeoutMs: 15_000);

                List<string> values = resumed.Events(key).Select(e => Encoding.UTF8.GetString(e.Value!)).Distinct().ToList();
                Assert.Equal(["v2", "v3"], values);
                Assert.DoesNotContain(resumed.Frames(), f => f.IsTerminal);
            }
        }
        finally
        {
            TryDeleteDir(storagePath);
            TryDeleteDir(walPath);
        }
    }

    [Fact]
    public async Task ClientWatermarks_AdvanceOnlyPastFramesTheReaderTook()
    {
        await WithCluster(1_000, async (nodes, ct) =>
        {
            string group = $"cursor{Guid.NewGuid():N}|";
            Dictionary<string, IKahuna> urls = new() { ["node"] = nodes[0] };
            KahunaClient client = new(["node"], communication: new InProcessKahunaCommunication(urls));
            await using KahunaWatch watch = await client.WatchKeyValues(new(KeyValueWatchScope.ForPrefix(group)), ct);

            await SetValue(nodes[0], group + "k", "unread", ct);

            // Progress arrives at the client, but the reader has not taken the event before it: no checkpoint yet.
            await Task.Delay(2_500, ct);
            Assert.Empty(watch.Watermarks);

            // Reading through the first progress frame hands the event over first, then the checkpoint moves.
            bool sawEvent = false;
            await foreach (KeyValueWatchFrame frame in watch.WithCancellation(ct))
            {
                if (frame.Event?.Key == group + "k")
                    sawEvent = true;

                if (frame.Type == KeyValueWatchFrameType.Progress)
                {
                    Assert.True(sawEvent, "a progress frame came before the event below it");
                    Assert.Equal(frame.Timestamp, watch.Watermarks[frame.PartitionId]);
                    break;
                }
            }
        });
    }

    [Fact]
    public async Task LiveTail_DeliversNothingAtOrBelowTheCursor()
    {
        await WithCluster(1_000, async (nodes, ct) =>
        {
            string group = $"future{Guid.NewGuid():N}|";
            await SetValue(nodes[0], group + "seed", "seed", ct);

            // A cursor ahead of this replica: the consumer already holds everything up to it (from another replica).
            HLCTimestamp cursor = new(1, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + 3_000, 0);
            await using FrameCollector watch = await Open(nodes[0], new(KeyValueWatchScope.ForPrefix(group), ResumeFrom: cursor), ct);

            await WaitUntilAsync(() => !watch.IsReplaying());
            await SetValue(nodes[0], group + "k", "below-cursor", ct);
            await Task.Delay(500, ct);

            Assert.DoesNotContain(watch.Frames(), f => f.Event is { } e && e.CommitTimestamp <= cursor);
            Assert.DoesNotContain(watch.Frames(), f => f.Type == KeyValueWatchFrameType.Progress && f.Timestamp <= cursor);
        });
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    public async Task DirectWrite_AfterADeferredCommit_ArrivesAfterTheCommitEvent(int watchOn)
    {
        await WithCluster(1_000, async (nodes, ct) =>
        {
            string group = $"order{Guid.NewGuid():N}|";
            string key = group + "k";
            await using FrameCollector watch = await Open(nodes[watchOn], new(KeyValueWatchScope.ForKey(key)), ct);

            // The transaction commits (its value is visible) but its settle waits; a write outside a transaction then
            // lands over the committed value before the settle reports the commit.
            using ResolutionGate gate = new(nodes[0]);
            KeyValueTransactionResult committed = await RetryOnMustRetry(nodes[0], Encoding.UTF8.GetBytes("BEGIN SET @k 'first' COMMIT END"), null, [new KeyValueParameter { Key = "@k", Value = key }]);
            Assert.Equal(KeyValueResponseType.Set, committed.Type);

            await SetValue(nodes[0], key, "second", ct);

            // Held behind the pending intent until it settles.
            await Task.Delay(300, ct);
            Assert.Empty(watch.Events(key));

            gate.Dispose();
            await watch.WaitUntil(c => c.Events(key).Count >= 2);

            List<KeyValueWatchEvent> events = watch.Events(key);
            Assert.Equal("first", Encoding.UTF8.GetString(events[0].Value!));
            Assert.Equal("second", Encoding.UTF8.GetString(events[1].Value!));
            watch.AssertMutationsInOrder(key);
        });
    }

    // ── helpers ──────────────────────────────────────────────────────────────────

    private async Task WithCluster(long lagMs, Func<KahunaManager[], CancellationToken, Task> body)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft r1, IRaft r2, IRaft r3, IKahuna k1, IKahuna k2, IKahuna k3) =
            await AssembleThreNodeCluster("memory", 4, raftLogger, kahunaLogger, c => c.CommitAdmissionLagMs = (int)lagMs);

        try
        {
            await body([(KahunaManager)k1, (KahunaManager)k2, (KahunaManager)k3], ct);
        }
        finally
        {
            await LeaveCluster(r1, r2, r3);
        }
    }

    private static async Task<FrameCollector> Open(KahunaManager node, KeyValueWatchRequest request, CancellationToken ct)
    {
        KeyValueWatchOpening opening = await RetryOnMustRetryAsync(() => node.WatchKeyValues(request, ct), o => o.Type);
        Assert.Equal(KeyValueResponseType.Get, opening.Type);
        return new FrameCollector(opening.Watch!);
    }

    private static async Task SetValue(IKahuna node, string key, string value, CancellationToken ct)
    {
        (KeyValueResponseType type, _, _) = await RetryOnMustRetryAsync(
            () => node.LocateAndTrySetKeyValue(HLCTimestamp.Zero, key, Encoding.UTF8.GetBytes(value), null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct),
            r => r.Item1);

        Assert.Equal(KeyValueResponseType.Set, type);
    }

    private static EmbeddedKahunaOptions PersistentOptions(string storagePath, string walPath) => new()
    {
        InitialPartitions = 1,
        Storage = "sqlite",
        StoragePath = storagePath,
        StorageRevision = "watch-resume",
        WalStorage = "sqlite",
        WalPath = walPath,
        WalRevision = "watch-resume-wal",
        WalSyncWrites = true,
        CommitAdmissionLagMs = 1_000,
        CollectionInterval = TimeSpan.FromMinutes(10)
    };

    private static string CreateTempDir(string prefix)
    {
        string path = Path.Combine(Path.GetTempPath(), prefix + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(path);
        return path;
    }

    private static void TryDeleteDir(string path)
    {
        try { if (Directory.Exists(path)) Directory.Delete(path, recursive: true); }
        catch { /* best-effort cleanup */ }
    }

    /// <summary>Holds every deferred resolution the node's coordinator schedules while installed. Disposal releases
    /// it.</summary>
    private sealed class ResolutionGate : IDisposable
    {
        private readonly DurableTransactionFinalizer finalizer;

        private readonly TaskCompletionSource open = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public ResolutionGate(KahunaManager manager)
        {
            finalizer = manager.TransactionCoordinator.DurableFinalizerForTests;
            finalizer.TestBeforeDeferredResolutionHook = ct => open.Task.WaitAsync(ct);
        }

        public void Dispose()
        {
            finalizer.TestBeforeDeferredResolutionHook = null;
            open.TrySetResult();
        }
    }

    /// <summary>Reads every frame of a watch in the background.</summary>
    private sealed class FrameCollector : IAsyncDisposable
    {
        private readonly ConcurrentQueue<KeyValueWatchFrame> frames = new();

        private readonly CancellationTokenSource stop = new();

        private readonly KeyValueWatch watch;

        private readonly Task pump;

        public FrameCollector(KeyValueWatch watch)
        {
            this.watch = watch;
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

        public List<KeyValueWatchFrame> Frames() => [.. frames];

        public List<KeyValueWatchEvent> Events(string key) =>
            frames.Where(f => f.Event?.Key == key).Select(f => f.Event!).ToList();

        public List<HLCTimestamp> Watermarks() =>
            frames.Where(f => f.Type == KeyValueWatchFrameType.Progress).Select(f => f.Timestamp).ToList();

        public async Task WaitUntil(Func<FrameCollector, bool> predicate, int timeoutMs = 10_000)
        {
            await WaitUntilAsync(() => predicate(this) || frames.Any(f => f.IsTerminal), timeoutMs);
            Assert.True(predicate(this), $"condition not met; terminal: {frames.FirstOrDefault(f => f.IsTerminal)?.Type} {frames.FirstOrDefault(f => f.IsTerminal)?.LagReason}");
        }

        public async Task<KeyValueWatchFrame> WaitForEnd()
        {
            await WaitUntilAsync(() => frames.Any(f => f.IsTerminal));
            return frames.First(f => f.IsTerminal);
        }

        /// <summary>The first delivery of each mutation of a key arrives in commit order; a repeat is ignored.</summary>
        public void AssertMutationsInOrder(string key)
        {
            KeyValueWatchEvent? last = null;
            HashSet<(long, HLCTimestamp)> seen = [];

            foreach (KeyValueWatchEvent e in Events(key))
            {
                if (!seen.Add((e.Revision, e.CommitTimestamp)))
                    continue;

                Assert.True(e.Supersedes(last), $"revision {e.Revision} at {e.CommitTimestamp} first arrived after revision {last?.Revision} at {last?.CommitTimestamp}");
                last = e;
            }
        }

        /// <summary>Whether the watch still replays any partition.</summary>
        public bool IsReplaying() => watch.Partitions.Any(watch.IsReplaying);

        public async ValueTask DisposeAsync()
        {
            await watch.DisposeAsync();
            await stop.CancelAsync();
            await pump;
            stop.Dispose();
        }
    }
}
