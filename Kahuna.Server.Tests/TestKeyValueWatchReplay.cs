using System.Reflection;

using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.KeyValues.Watch;
using Kahuna.Server.Replication;
using Kahuna.Server.Replication.Protos;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Data;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kahuna.Server.Tests;

/// <summary>
/// The watch delivery contract at the level of the store, the broker, the replay and the history bounds, with an
/// in-memory WAL: a replay classifies intents exactly as the live store does, ends transactions across its hand-over,
/// refuses a cursor an unbounded commit could undercut and stops when its watch ends; a write outside a transaction
/// waits behind a pending intent on its key; the history pin keeps what the replay reads; a history hold proposal never
/// overwrites the applied registry; the history bounds fail closed; and the node owns every watch limit.
/// </summary>
public sealed class TestKeyValueWatchReplay
{
    private static HLCTimestamp T(long value) => new(1, value, 0);

    private static PreparedIntent Intent(string key, long transaction = 9_000, long commit = 10_000) => new(
        TransactionId: T(transaction), Epoch: 1, Key: key, ManifestHash: 0, RecordAnchorKey: "p|a",
        CommitTimestamp: T(commit), State: KeyValueState.Set, Value: [1], Bucket: null,
        Revision: 1, Expires: HLCTimestamp.Zero, NoRevision: false,
        BaseRevision: PreparedIntent.UnknownBaseRevision, BaseState: KeyValueState.Undefined,
        RecoveryDeadline: HLCTimestamp.Zero, Resolution: PreparedIntentResolution.Pending);

    private static RaftLog Log(long id, params PreparedIntentCommand[] commands) => new()
    {
        Id = id, Type = RaftLogType.Committed, Time = T(10_000 + id),
        LogType = ReplicationTypes.PreparedIntent, LogData = [.. PreparedIntentStore.SerializeDelta(commands)]
    };

    private static InMemoryWAL Wal(params RaftLog[] logs)
    {
        InMemoryWAL wal = new(NullLogger<IRaft>.Instance);
        wal.Write([(1, logs.ToList())]);
        return wal;
    }

    private static KeyValueWatchReplayer Replayer(IWAL wal, long live, PartitionHistoryBounds? bounds = null) => new(
        new TestBackupService.StubRaft(wal, []), _ => live,
        bounds ?? new PartitionHistoryBounds(null, null, NullLogger<IKahuna>.Instance), true, NullLogger<IKahuna>.Instance);

    private static async Task WaitReplay(KeyValueWatch watch)
    {
        using CancellationTokenSource timeout = new(TimeSpan.FromSeconds(5));
        while (watch.IsReplaying(1) && !watch.IsEnded)
            await Task.Delay(5, timeout.Token);
    }

    private static async Task<List<KeyValueWatchFrame>> Drain(KeyValueWatch watch)
    {
        List<KeyValueWatchFrame> result = [];
        using CancellationTokenSource timeout = new(TimeSpan.FromMilliseconds(250));
        try
        {
            await foreach (KeyValueWatchFrame frame in watch.ReadAllAsync(timeout.Token))
                result.Add(frame);
        }
        catch (OperationCanceledException)
        {
        }

        return result;
    }

    private static KeyValueWatchFrame DirectWrite(string key, long revision, long commit) =>
        KeyValueWatchFrame.ForEvent(new KeyValueWatchEvent(key, KeyValueWatchEventKind.Set, [2], revision, HLCTimestamp.Zero, T(commit), HLCTimestamp.Zero, 1));

    // ── replay classification ───────────────────────────────────────────────────

    [Fact]
    public async Task Replay_KeepsTheResolutionAnImportedIntentCarries()
    {
        // A range hand-off replays an intent that is already committed as a prepare; the commit-resolve that follows is
        // idempotent. Neither the live store nor the replay reports it.
        PreparedIntent intent = Intent("p|a") with { Resolution = PreparedIntentResolution.Committed };
        RaftLog prepare = Log(1, new PrepareIntentCommand(intent));
        RaftLog resolve = Log(2, new ResolveIntentCommand(intent.TransactionId, 1, intent.Key, true));

        KeyValueWatchBroker broker = new();
        PreparedIntentStore store = new();
        store.AttachWatchBroker(broker);

        await using KeyValueWatch live = broker.Register(KeyValueWatchScope.ForKey(intent.Key), false, 100, [1]);
        store.ApplyDeltaAckPrepares(1, prepare);
        store.ApplyDeltaAckPrepares(1, resolve);
        Assert.DoesNotContain(await Drain(live), f => f.Event is not null);

        await using KeyValueWatch replay = broker.Register(KeyValueWatchScope.ForKey(intent.Key), false, 100, [1], replayPartitions: [1]);
        _ = Replayer(Wal(prepare, resolve), 2).Start(replay, 1, T(9_999));
        await WaitReplay(replay);
        Assert.DoesNotContain(await Drain(replay), f => f.Event is not null);
    }

    [Fact]
    public async Task Replay_RejectsAPrepareOverAnotherTransactionsIntent()
    {
        // H holds the key; X's prepare is rejected, so X's commit-resolve targets nothing and reports nothing.
        PreparedIntent holder = Intent("p|a", transaction: 9_000);
        PreparedIntent other = Intent("p|a", transaction: 9_100);

        KeyValueWatchBroker broker = new();
        await using KeyValueWatch watch = broker.Register(KeyValueWatchScope.ForKey("p|a"), false, 100, [1], replayPartitions: [1]);

        InMemoryWAL wal = Wal(
            Log(1, new PrepareIntentCommand(holder)),
            Log(2, new PrepareIntentCommand(other)),
            Log(3, new ResolveIntentCommand(other.TransactionId, 1, other.Key, true)),
            Log(4, new ResolveIntentCommand(holder.TransactionId, 1, holder.Key, true)));

        _ = Replayer(wal, 4).Start(watch, 1, T(9_999));
        await WaitReplay(watch);

        KeyValueWatchEvent only = Assert.Single(await Drain(watch), f => f.Event is not null).Event!;
        Assert.Equal(holder.TransactionId, only.TransactionId);
    }

    // ── transaction ends ────────────────────────────────────────────────────────

    [Fact]
    public async Task TransactionEnd_ReachesAWatchWhoseReplayHandedOver()
    {
        // T writes a and b on one partition. a settles before the watch opens and is replayed; b settles live after the
        // hand-over, and its end must still reach the watch, which only ever saw a.
        PreparedIntent a = Intent("p|a"), b = Intent("p|b");
        RaftLog prepare = Log(1, new PrepareIntentCommand(a, 5_000), new PrepareIntentCommand(b, 5_000));
        RaftLog settleA = Log(2, new ResolveIntentCommand(a.TransactionId, 1, a.Key, true));
        RaftLog settleB = Log(3, new ResolveIntentCommand(b.TransactionId, 1, b.Key, true));

        KeyValueWatchBroker broker = new() { TracksTransactionEnds = true };
        PreparedIntentStore store = new();
        store.ConfigureCommitAdmission(true);
        store.AttachWatchBroker(broker);
        store.ApplyDeltaAckPrepares(1, prepare);
        store.ApplyDeltaAckPrepares(1, settleA);

        await using KeyValueWatch watch = broker.Register(KeyValueWatchScope.ForKey(a.Key), false, 100, [1], replayPartitions: [1]);
        await using KeyValueWatch unrelated = broker.Register(KeyValueWatchScope.ForKey("p|c"), false, 100, [1]);

        _ = Replayer(Wal(prepare, settleA), 2).Start(watch, 1, T(9_999));
        await WaitReplay(watch);
        store.ApplyDeltaAckPrepares(1, settleB);

        List<KeyValueWatchFrame> frames = await Drain(watch);
        Assert.Single(frames, f => f.Event is not null);
        Assert.Single(frames, f => f.Type == KeyValueWatchFrameType.TransactionEnd && f.TransactionId == a.TransactionId);

        // A watch that received no event of the transaction receives no end.
        Assert.DoesNotContain(await Drain(unrelated), f => f.Type == KeyValueWatchFrameType.TransactionEnd);
    }

    // ── unbounded commits ───────────────────────────────────────────────────────

    [Fact]
    public async Task Replay_RefusesACursorBelowACommitTheWindowNeverBounded()
    {
        // A prepare admitted with no bound (lag 0) at entry time 1 000 carries a commit at 20 000; it settles at 2 000.
        // A replay from 15 000 starts at entry time 10 000 and would skip it.
        PreparedIntent intent = Intent("p|old", commit: 20_000);
        RaftLog prepare = Log(1, new PrepareIntentCommand(intent));
        prepare.Time = T(1_000);
        RaftLog resolve = Log(2, new ResolveIntentCommand(intent.TransactionId, 1, intent.Key, true));
        resolve.Time = T(2_000);
        RaftLog later = new() { Id = 3, Type = RaftLogType.Committed, Time = T(16_000) };

        // The live apply feeds the ceiling, as the manager wires it.
        PartitionHistoryBounds bounds = new(null, null, NullLogger<IKahuna>.Instance);
        PreparedIntentStore store = new();
        store.AttachCommitObserver(bounds.ObserveCommit);
        store.ApplyDeltaAckPrepares(1, prepare);
        store.ApplyDeltaAckPrepares(1, resolve);
        Assert.Equal(T(20_000), bounds.UnboundedCeiling(1));

        InMemoryWAL wal = Wal(prepare, resolve, later);
        KeyValueWatchBroker broker = new();

        await using KeyValueWatch below = broker.Register(KeyValueWatchScope.ForPrefix("p|"), false, 100, [1], replayPartitions: [1]);
        _ = Replayer(wal, 3, bounds).Start(below, 1, T(15_000));
        await WaitReplay(below);
        Assert.Contains(await Drain(below), f => f.Type == KeyValueWatchFrameType.HistoryUnavailable);

        // At or above the ceiling the commit is covered by the cursor, and the replay runs.
        await using KeyValueWatch above = broker.Register(KeyValueWatchScope.ForPrefix("p|"), false, 100, [1], replayPartitions: [1]);
        _ = Replayer(wal, 3, bounds).Start(above, 1, T(20_000));
        await WaitReplay(above);
        Assert.DoesNotContain(await Drain(above), f => f.IsTerminal);
    }

    [Fact]
    public void UnboundedCeiling_CountsOnlyCommitsThatLeadTheirEntryByTheBound()
    {
        PartitionHistoryBounds bounds = new(null, null, NullLogger<IKahuna>.Instance);

        bounds.ObserveCommit(1, T(1_000), T(5_999), 0);
        bounds.ObserveCommit(1, T(1_000), T(9_000), 1_000);
        Assert.Equal(HLCTimestamp.Zero, bounds.UnboundedCeiling(1));

        bounds.ObserveCommit(1, T(1_000), T(6_000), 0);
        Assert.Equal(T(6_000), bounds.UnboundedCeiling(1));
        Assert.Equal(HLCTimestamp.Zero, bounds.UnboundedCeiling(2));
    }

    // ── history bounds persistence ──────────────────────────────────────────────

    [Fact]
    public void HistoryBounds_RecordThrowsWhenItCannotPersist()
    {
        string absent = Path.Combine(Path.GetTempPath(), "absent-watch-" + Guid.NewGuid().ToString("N"));
        PartitionHistoryBounds bounds = new(absent, "test", NullLogger<IKahuna>.Instance);

        Assert.ThrowsAny<IOException>(() => bounds.Record(1, 100));

        // Memory keeps the conservative value even though the file did not take it.
        Assert.Equal(100, bounds.InstallBoundary(1));
    }

    [Fact]
    public void HistoryBounds_SurviveAReload_WithTheCeilingAndOldLines()
    {
        string dir = Directory.CreateTempSubdirectory("watch-bounds-").FullName;
        try
        {
            PartitionHistoryBounds first = new(dir, "r", NullLogger<IKahuna>.Instance);
            first.RecordInstall(1, 50, T(7_000));
            first.ObserveCommit(2, T(1_000), T(9_000), 0);
            first.Flush();

            PartitionHistoryBounds reloaded = new(dir, "r", NullLogger<IKahuna>.Instance);
            Assert.Equal(50, reloaded.InstallBoundary(1));
            Assert.Equal(T(7_000), reloaded.UnboundedCeiling(1));
            Assert.Equal(T(9_000), reloaded.UnboundedCeiling(2));

            // A file an earlier build wrote ("<partition> <boundary>") still loads.
            File.WriteAllText(Path.Combine(dir, "installboundaries_old.txt"), "3 40\n");
            Assert.Equal(40, new PartitionHistoryBounds(dir, "old", NullLogger<IKahuna>.Instance).InstallBoundary(3));
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public void HistoryBounds_ACorruptFile_FencesEveryUnrecordedPartitionAtItsLogEnd()
    {
        string dir = Directory.CreateTempSubdirectory("watch-bounds-").FullName;
        try
        {
            File.WriteAllText(Path.Combine(dir, "installboundaries_r.txt"), "1 30\nnot a line\n");

            PartitionHistoryBounds bounds = new(dir, "r", NullLogger<IKahuna>.Instance);
            bounds.AttachLogEnd(partitionId => partitionId * 1_000);

            Assert.Equal(1_000, bounds.InstallBoundary(1));
            Assert.Equal(2_000, bounds.InstallBoundary(2));

            // The fence is kept: a restart before a partition is asked about still fences it.
            PartitionHistoryBounds restarted = new(dir, "r", NullLogger<IKahuna>.Instance);
            restarted.AttachLogEnd(_ => 5_000);
            Assert.Equal(2_000, restarted.InstallBoundary(2));
            Assert.Equal(5_000, restarted.InstallBoundary(3));
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }

    // ── history pin and history holds ───────────────────────────────────────────

    [Fact]
    public async Task HistoryPin_KeepsTheEntryTheReplayReadsBeforeItsStart()
    {
        InMemoryWAL wal = Wal(
            new RaftLog { Id = 1, Type = RaftLogType.Committed, Time = T(1) },
            new RaftLog { Id = 2, Type = RaftLogType.Committed, Time = T(2) },
            new RaftLog { Id = 3, Type = RaftLogType.Committed, Time = T(6_000) },
            new RaftLog { Id = 4, Type = RaftLogType.Committed, Time = T(10_000) });

        using HistoryHoldPinner pinner = new(new TestBackupService.StubRaft(wal, []), null!, NullLogger<IKahuna>.Instance);

        // A resume from 10 000 starts at the first entry at or after 5 000 (index 3) and reads index 2 as proof.
        long pinned = pinner.IndexFor(1, T(10_000));
        Assert.Equal(2, pinned);

        wal.CompactLogsOlderThan(1, pinned, 100);

        KeyValueWatchBroker broker = new();
        await using KeyValueWatch watch = broker.Register(KeyValueWatchScope.ForPrefix("p|"), false, 100, [1], replayPartitions: [1]);
        _ = Replayer(wal, 4).Start(watch, 1, T(10_000));
        await WaitReplay(watch);
        Assert.DoesNotContain(await Drain(watch), f => f.Type == KeyValueWatchFrameType.HistoryUnavailable);
    }

    [Fact]
    public async Task HistoryHold_AProposalNeverOverwritesAnEntryAppliedMeanwhile()
    {
        ProposeOutcomeStubRaft raft = new();
        using HistoryHoldStore store = new(raft, null, null, NullLogger<IKahuna>.Instance, new(60_000, 100, 100));

        // While the acquire is in flight its own entry applies, and then another node's acquire applies after it.
        raft.BeforeProposeReturn = data =>
        {
            Assert.True(store.Replicate(0, new RaftLog { Id = 1, LogType = ReplicationTypes.HistoryHold, LogData = data }));

            HistoryHoldDeltaMessage delta = new();
            delta.Entries.Add(new HistoryHoldDeltaEntry
            {
                Hold = new HistoryHoldMessage
                {
                    HoldId = "newer", HolderId = "other", PartitionId = 1,
                    TimestampPhysical = 10_000, LeaseExpiryPhysical = long.MaxValue
                }
            });

            Assert.True(store.Replicate(0, new RaftLog { Id = 2, LogType = ReplicationTypes.HistoryHold, LogData = ReplicationSerializer.Serialize(delta) }));
            return Task.CompletedTask;
        };

        HistoryHoldAnswer answer = await store.AcquireAsync("first", [1], T(10_000), 30_000, CancellationToken.None);

        Assert.Equal(KeyValueResponseType.Set, answer.Type);
        Assert.Equal(2, store.List(null).Count);
        Assert.Contains(store.List(null), h => h.HoldId == "newer");
        Assert.Contains(store.List(null), h => h.HolderId == "first");
    }

    // ── per-key order behind a pending intent ───────────────────────────────────

    [Fact]
    public async Task DirectWrite_WaitsBehindAPendingIntent_UntilItCommits()
    {
        KeyValueWatchBroker broker = new();
        await using KeyValueWatch watch = broker.Register(KeyValueWatchScope.ForKey("p|a"), false, 100, [1]);

        watch.Deliver(DirectWrite("p|a", 2, 11_000), 5, holdKey: "p|a");
        Assert.Empty(await Drain(watch));

        // The intent's commit event goes first, then what waited behind it.
        broker.Publish(1, KeyValueWatchEventKind.Set, "p|a", [1], 1, HLCTimestamp.Zero, T(10_000), T(9_000), 6);

        List<KeyValueWatchFrame> frames = await Drain(watch);
        Assert.Equal([1L, 2L], frames.Select(f => f.Event!.Revision));
    }

    [Fact]
    public async Task DirectWrite_WaitsBehindAPendingIntent_UntilTheStoreReleasesIt()
    {
        PreparedIntent intent = Intent("p|a");
        KeyValueWatchBroker broker = new();
        PreparedIntentStore store = new();
        store.AttachWatchBroker(broker);
        store.ApplyDeltaAckPrepares(1, Log(1, new PrepareIntentCommand(intent)));

        await using KeyValueWatch watch = broker.Register(KeyValueWatchScope.ForKey("p|a"), false, 100, [1]);

        // The replicator tags a write outside a transaction that lands while the key has a pending intent.
        bool behind = store.Get("p|a") is { IsPending: true };
        Assert.True(behind);
        broker.Publish(1, KeyValueWatchEventKind.Set, "p|a", [2], 2, HLCTimestamp.Zero, T(11_000), HLCTimestamp.Zero, 2, heldBehindPendingIntent: behind);
        Assert.Empty(await Drain(watch));

        // The intent aborts: no commit event, and the held write follows the release.
        store.ApplyDeltaAckPrepares(1, Log(3, new ResolveIntentCommand(intent.TransactionId, 1, intent.Key, false)));

        KeyValueWatchFrame released = Assert.Single(await Drain(watch));
        Assert.Equal(2, released.Event!.Revision);
    }

    [Fact]
    public async Task Replay_HoldsADirectWriteBehindAPendingIntent_AndALiveReleaseFreesIt()
    {
        // The replay sees the prepare and the write but not the settle, which arrives live after the hand-over.
        PreparedIntent intent = Intent("p|a");
        RaftLog prepare = Log(1, new PrepareIntentCommand(intent));
        RaftLog write = new()
        {
            Id = 2, Type = RaftLogType.Committed, Time = T(10_002), LogType = ReplicationTypes.KeyValues,
            LogData = ReplicationSerializer.Serialize(new KeyValueMessage
            {
                Type = (int)KeyValueRequestType.TrySet, Key = "p|a", Value = Google.Protobuf.ByteString.CopyFrom(2), Revision = 2,
                LastModifiedNode = 1, LastModifiedPhysical = 11_000
            })
        };

        KeyValueWatchBroker broker = new();
        await using KeyValueWatch watch = broker.Register(KeyValueWatchScope.ForKey("p|a"), false, 100, [1], replayPartitions: [1]);

        _ = Replayer(Wal(prepare, write), 2).Start(watch, 1, T(9_999));
        await WaitReplay(watch);
        Assert.DoesNotContain(await Drain(watch), f => f.Event is not null);

        broker.Publish(1, KeyValueWatchEventKind.Set, "p|a", [1], 1, HLCTimestamp.Zero, T(10_000), T(9_000), 3);
        broker.PublishIntentSettled(1, "p|a", 3);

        Assert.Equal([1L, 2L], (await Drain(watch)).Where(f => f.Event is not null).Select(f => f.Event!.Revision));
    }

    // ── lifetime and limits ─────────────────────────────────────────────────────

    [Fact]
    public async Task Replay_StopsReadingWhenItsWatchEnds()
    {
        // A long retained log with nothing in the watch's scope, read slowly.
        List<RaftLog> logs = new(100_000);
        for (long id = 1; id <= 100_000; id++)
            logs.Add(new RaftLog { Id = id, Type = RaftLogType.Committed, Time = T(10_000 + id) });

        InMemoryWAL inner = new(NullLogger<IRaft>.Instance);
        inner.Write([(1, logs)]);

        IWAL wal = DispatchProxy.Create<IWAL, SlowReadWal>();
        SlowReadWal slow = (SlowReadWal)(object)wal;
        slow.Inner = inner;
        slow.DelayMs = 5;

        KeyValueWatchBroker broker = new();
        KeyValueWatch watch = broker.Register(KeyValueWatchScope.ForKey("nothing|matches"), false, 100, [1], replayPartitions: [1]);

        Task replay = Replayer(wal, 100_000).Start(watch, 1, T(10_000));
        await WaitUntilAsync(() => slow.Reads > 30);

        await watch.DisposeAsync();
        await replay.WaitAsync(TimeSpan.FromSeconds(2));

        int readsAtEnd = slow.Reads;
        await Task.Delay(200);
        Assert.Equal(readsAtEnd, slow.Reads);
        Assert.True(readsAtEnd < 150, $"the replay read {readsAtEnd} pages of 196 after its watch ended");
    }

    [Fact]
    public void Replayer_RefusesSlotsBeyondItsLimit()
    {
        KeyValueWatchReplayer replayer = new(
            new TestBackupService.StubRaft(new InMemoryWAL(NullLogger<IRaft>.Instance), []), _ => 0,
            new PartitionHistoryBounds(null, null, NullLogger<IKahuna>.Instance), true, NullLogger<IKahuna>.Instance, maxConcurrentReplays: 3);

        Assert.True(replayer.TryReserveSlots(2));
        Assert.False(replayer.TryReserveSlots(2));
        Assert.Equal(2, replayer.RunningReplays);

        replayer.ReleaseSlots(2);
        Assert.True(replayer.TryReserveSlots(3));
    }

    [Fact]
    public async Task Broker_RefusesARegistrationBeyondTheNodeLimit()
    {
        KeyValueWatchBroker broker = new(maxWatches: 2);

        KeyValueWatch? first = broker.TryRegister(KeyValueWatchScope.ForKey("a"), false, 10, [1]);
        KeyValueWatch? second = broker.TryRegister(KeyValueWatchScope.ForKey("b"), false, 10, [1, 2]);
        Assert.NotNull(first);
        Assert.NotNull(second);
        Assert.Null(broker.TryRegister(KeyValueWatchScope.ForKey("c"), false, 10, [1]));

        await first!.DisposeAsync();
        Assert.NotNull(broker.TryRegister(KeyValueWatchScope.ForKey("c"), false, 10, [1]));
    }

    [Fact]
    public async Task Watch_PastItsByteBudget_EndsWithAnOverflow()
    {
        KeyValueWatchBroker broker = new(maxBufferedBytes: 1_000);
        await using KeyValueWatch watch = broker.Register(KeyValueWatchScope.ForPrefix("p|"), false, 1_000, [1]);

        byte[] value = new byte[400];
        for (int i = 0; i < 3; i++)
            broker.Publish(1, KeyValueWatchEventKind.Set, "p|" + i, value, 1, HLCTimestamp.Zero, T(10_000 + i), HLCTimestamp.Zero, i + 1);

        KeyValueWatchFrame end = Assert.Single(await Drain(watch));
        Assert.Equal(KeyValueWatchFrameType.Lagged, end.Type);
        Assert.Equal(KeyValueWatchLagReason.Overflow, end.LagReason);
    }

    // ── event identity ──────────────────────────────────────────────────────────

    [Fact]
    public void Event_Supersedes_KeepsAnExtendAndDropsARepeat()
    {
        KeyValueWatchEvent set = new("k", KeyValueWatchEventKind.Set, [1], 4, HLCTimestamp.Zero, T(10_000), HLCTimestamp.Zero, 1);
        KeyValueWatchEvent extend = new("k", KeyValueWatchEventKind.Extended, null, 4, T(70_000), T(10_500), HLCTimestamp.Zero, 1);
        KeyValueWatchEvent repeat = new("k", KeyValueWatchEventKind.Set, [1], 4, HLCTimestamp.Zero, T(10_000), HLCTimestamp.Zero, 1);
        KeyValueWatchEvent recreated = new("k", KeyValueWatchEventKind.Set, [2], 0, HLCTimestamp.Zero, T(20_000), HLCTimestamp.Zero, 1);

        Assert.True(set.Supersedes(null));
        Assert.True(extend.Supersedes(set));
        Assert.False(repeat.Supersedes(set));
        Assert.False(set.Supersedes(extend));

        // A key re-created after its tombstone was purged can start its revisions again; its timestamp still orders it.
        Assert.True(recreated.Supersedes(extend));
    }

    /// <summary>An <see cref="IWAL"/> that forwards to another and slows and counts every range read.</summary>
    public class SlowReadWal : DispatchProxy
    {
        private int reads;

        public IWAL Inner { get; set; } = null!;

        public int DelayMs { get; set; }

        public int Reads => Volatile.Read(ref reads);

        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
        {
            if (targetMethod!.Name == nameof(IWAL.ReadLogsRange))
            {
                Interlocked.Increment(ref reads);
                Thread.Sleep(DelayMs);
            }

            try
            {
                return targetMethod.Invoke(Inner, args);
            }
            catch (TargetInvocationException ex) when (ex.InnerException is not null)
            {
                throw ex.InnerException;
            }
        }
    }

    private static async Task WaitUntilAsync(Func<bool> condition, int timeoutMs = 10_000)
    {
        long deadline = Environment.TickCount64 + timeoutMs;
        while (!condition())
        {
            Assert.True(Environment.TickCount64 < deadline, "condition not met in time");
            await Task.Delay(10);
        }
    }
}
