using System.Collections.Concurrent;
using System.Text;
using Google.Protobuf;
using Kahuna;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.KeyValues.Watch;
using Kahuna.Server.KeyValues.Writes;
using Kahuna.Server.Persistence.Pitr;
using Kahuna.Server.Replication;
using Kahuna.Server.Replication.Protos;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Data;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// The commit admission window and the watch watermark it makes sound. Each replica admits a prepare or a write made
/// outside a transaction only when its commit timestamp falls inside <c>(entry time − lag, entry time + 5 s)</c>; a
/// refused entry installs nothing anywhere and its writer gets a retryable answer. With the window on, every watch
/// receives progress watermarks that no later event goes below, a pending intent holds the watermark back until it
/// settles, an idle partition's watermark keeps moving, and a transaction's last event on a partition is followed by
/// its end.
/// </summary>
public sealed class TestCommitAdmission : BaseCluster
{
    private readonly ILogger<IRaft> raftLogger;

    private readonly ILogger<IKahuna> kahunaLogger;

    private readonly ILoggerFactory loggerFactory;

    public TestCommitAdmission(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper, quietKommander: true);
        raftLogger = loggerFactory.CreateLogger<IRaft>();
        kahunaLogger = loggerFactory.CreateLogger<IKahuna>();
    }

    // ── the verdict ──────────────────────────────────────────────────────────────

    [Fact]
    public void Judge_AdmitsOnlyCommitsInsideTheWindow()
    {
        HLCTimestamp tau = new(1, 100_000, 5);

        Assert.Equal(CommitAdmissionVerdict.Admitted, CommitAdmission.Judge(tau, new(1, 1, 0), 0));
        Assert.Equal(CommitAdmissionVerdict.Admitted, CommitAdmission.Judge(HLCTimestamp.Zero, new(1, 1, 0), 5_000));

        Assert.Equal(CommitAdmissionVerdict.BelowFloor, CommitAdmission.Judge(tau, new(1, 90_000, 0), 5_000));
        Assert.Equal(CommitAdmissionVerdict.BelowFloor, CommitAdmission.Judge(tau, new(1, 95_000, 5), 5_000));
        Assert.Equal(CommitAdmissionVerdict.Admitted, CommitAdmission.Judge(tau, new(1, 95_001, 0), 5_000));
        Assert.Equal(CommitAdmissionVerdict.Admitted, CommitAdmission.Judge(tau, new(1, 104_999, 0), 5_000));
        Assert.Equal(CommitAdmissionVerdict.AboveBound, CommitAdmission.Judge(tau, new(1, 105_000, 5), 5_000));
        Assert.Equal(CommitAdmissionVerdict.AboveBound, CommitAdmission.Judge(tau, new(1, 200_000, 0), 5_000));
    }

    [Fact]
    public void PrepareBound_SurvivesTheDeltaCodec()
    {
        PreparedIntent intent = MakeIntent("adm/codec", txPhysical: 1_000, commitPhysical: 1_001);
        byte[] delta = PreparedIntentStore.SerializeDelta([new PrepareIntentCommand(intent, 4_321), new PrepareIntentCommand(intent with { Key = "adm/codec2" })]);

        // A copy of the bytes, so the decoder runs instead of the proposer's cached commands.
        PreparedIntentCommand[] decoded = PreparedIntentStore.DecodeDelta([.. delta]);

        Assert.Equal(4_321, Assert.IsType<PrepareIntentCommand>(decoded[0]).AdmissionLagMs);
        Assert.Equal(0, Assert.IsType<PrepareIntentCommand>(decoded[1]).AdmissionLagMs);

        PreparedIntentDeltaMessage message = PreparedIntentDeltaMessage.Parser.ParseFrom(delta);
        Assert.Equal(4_321, Assert.IsType<PrepareIntentCommand>(PreparedIntentStore.ToCommand(message.Commands[0], message.Header)).AdmissionLagMs);
    }

    [Fact]
    public void PrepareOutsideTheWindow_InstallsNothing_AndIsRefusedAsAdmission()
    {
        PreparedIntentStore store = new();
        HLCTimestamp tau = new(1, 1_000_000, 0);

        // Ten seconds below the entry time: below the floor of a 5 s lag.
        PreparedIntent late = MakeIntent("adm/late", txPhysical: 989_000, commitPhysical: 990_000);
        Assert.False(store.ApplyDeltaAckPrepares(1, Log(tau, 1, new PrepareIntentCommand(late, 5_000))));
        Assert.Null(store.Get("adm/late"));
        Assert.True(store.TryTakePrepareRejection(late.TransactionId, late.Epoch, late.Key, out PrepareRejectionKind kind));
        Assert.Equal(PrepareRejectionKind.AdmissionRefused, kind);

        // Too far ahead of the entry time.
        PreparedIntent early = MakeIntent("adm/early", txPhysical: 1_000_000, commitPhysical: 1_006_000);
        Assert.False(store.ApplyDeltaAckPrepares(1, Log(tau, 2, new PrepareIntentCommand(early, 5_000))));
        Assert.Null(store.Get("adm/early"));

        // The same prepare with no bound in its entry (the window off, or a range move's copy) installs.
        PreparedIntent copied = late with { Key = "adm/copied" };
        Assert.True(store.ApplyDeltaAckPrepares(1, Log(tau, 3, new PrepareIntentCommand(copied))));
        Assert.NotNull(store.Get("adm/copied"));

        // Inside the window it installs.
        PreparedIntent inTime = MakeIntent("adm/in-time", txPhysical: 998_000, commitPhysical: 999_000);
        Assert.True(store.ApplyDeltaAckPrepares(1, Log(tau, 4, new PrepareIntentCommand(inTime, 5_000))));
        Assert.True(store.Get("adm/in-time")!.IsPending);
    }

    [Fact]
    public void SameIdentityRePrepare_KeepsItsAnswer_WhenItsEntryIsLate()
    {
        PreparedIntentStore store = new();
        PreparedIntent intent = MakeIntent("adm/redrive", txPhysical: 99_000, commitPhysical: 100_000);

        Assert.True(store.ApplyDeltaAckPrepares(1, Log(new HLCTimestamp(1, 100_500, 0), 1, new PrepareIntentCommand(intent, 5_000))));

        // A re-drive of the same attempt lands a minute later: it creates no new commit, so the window does not judge
        // it, and the intent the first copy installed stays pending.
        Assert.True(store.ApplyDeltaAckPrepares(1, Log(new HLCTimestamp(1, 160_000, 0), 2, new PrepareIntentCommand(intent, 5_000))));
        Assert.True(store.Get("adm/redrive")!.IsPending);
    }

    [Fact]
    public async Task PendingIndex_CapsTheWatermark_AndTheLastCommitEndsTheTransaction()
    {
        PreparedIntentStore store = new();
        store.ConfigureCommitAdmission(true);

        KeyValueWatchBroker broker = new() { TracksTransactionEnds = true };
        store.AttachWatchBroker(broker);
        KeyValueWatch watch = broker.Register(KeyValueWatchScope.ForPrefix("pend|"), keysOnly: false, capacity: 100, partitions: [1]);

        PreparedIntent a = MakeIntent("pend|a", txPhysical: 1_000, commitPhysical: 2_000);
        PreparedIntent b = a with { Key = "pend|b" };
        PreparedIntent lower = MakeIntent("pend|c", txPhysical: 500, commitPhysical: 1_500);
        PreparedIntent elsewhere = MakeIntent("other|d", txPhysical: 100, commitPhysical: 200);

        Assert.True(store.ApplyDeltaAckPrepares(1, Log(HLCTimestamp.Zero, 1, new PrepareIntentCommand(a), new PrepareIntentCommand(b))));
        Assert.True(store.ApplyDeltaAckPrepares(1, Log(HLCTimestamp.Zero, 2, new PrepareIntentCommand(lower))));
        Assert.True(store.ApplyDeltaAckPrepares(2, Log(HLCTimestamp.Zero, 1, new PrepareIntentCommand(elsewhere))));

        Assert.True(store.TryGetLowestPendingCommit(1, out HLCTimestamp lowest));
        Assert.Equal(lower.CommitTimestamp, lowest);
        Assert.True(store.TryGetLowestPendingCommit(2, out HLCTimestamp lowestElsewhere));
        Assert.Equal(elsewhere.CommitTimestamp, lowestElsewhere);
        Assert.True(store.TryGetLowestPendingCommit(out HLCTimestamp lowestOnNode));
        Assert.Equal(elsewhere.CommitTimestamp, lowestOnNode);

        // The lower transaction settles first; then the two keys of the other one, in two separate entries.
        Assert.True(store.ApplyDeltaAckPrepares(1, Log(HLCTimestamp.Zero, 3, new ResolveIntentCommand(lower.TransactionId, lower.Epoch, lower.Key, Commit: true))));
        Assert.True(store.TryGetLowestPendingCommit(1, out lowest));
        Assert.Equal(a.CommitTimestamp, lowest);

        Assert.True(store.ApplyDeltaAckPrepares(1, Log(HLCTimestamp.Zero, 4, new ResolveIntentCommand(a.TransactionId, a.Epoch, a.Key, Commit: true))));
        Assert.True(store.ApplyDeltaAckPrepares(1, Log(HLCTimestamp.Zero, 5, new ResolveIntentCommand(b.TransactionId, b.Epoch, b.Key, Commit: true))));
        Assert.False(store.TryGetLowestPendingCommit(1, out _));

        List<KeyValueWatchFrame> frames = await Take(watch, 5);

        Assert.Equal(
            [KeyValueWatchFrameType.Event, KeyValueWatchFrameType.TransactionEnd, KeyValueWatchFrameType.Event, KeyValueWatchFrameType.Event, KeyValueWatchFrameType.TransactionEnd],
            frames.Select(f => f.Type));

        Assert.Equal("pend|c", frames[0].Event!.Key);
        Assert.Equal(lower.TransactionId, frames[1].TransactionId);
        Assert.Equal(["pend|a", "pend|b"], new[] { frames[2].Event!.Key, frames[3].Event!.Key });
        Assert.Equal(a.TransactionId, frames[4].TransactionId);
        Assert.All(frames, f => Assert.Equal(1, f.PartitionId));

        await watch.DisposeAsync();
    }

    [Fact]
    public void RefusedDirectWrite_IsSkippedByTheRestoreAndThePitrReplay()
    {
        HLCTimestamp lastModified = new(1, 10_000, 0);

        KeyValueMessage message = new()
        {
            Type = KeyValueMessageDecoder.ToLoggedType(KeyValueRequestType.TrySet),
            Key = "adm/direct",
            Value = ByteString.CopyFromUtf8("v"),
            Revision = 1,
            LastModifiedNode = lastModified.N,
            LastModifiedPhysical = lastModified.L,
            LastModifiedCounter = lastModified.C,
            AdmissionLagMs = 5_000
        };

        byte[] data = ReplicationSerializer.Serialize(message);

        RaftLog late = new() { Id = 1, Term = 1, Type = RaftLogType.Committed, LogType = ReplicationTypes.KeyValues, LogData = data, Time = new(1, 20_000, 0) };
        RaftLog inTime = new() { Id = 2, Term = 1, Type = RaftLogType.Committed, LogType = ReplicationTypes.KeyValues, LogData = data, Time = new(1, 12_000, 0) };

        Assert.True(KeyValueReplicator.IsRefusedByAdmission(late, lastModified, 5_000));
        Assert.False(KeyValueReplicator.IsRefusedByAdmission(inTime, lastModified, 5_000));
        Assert.False(KeyValueReplicator.IsRefusedByAdmission(late, lastModified, 0));

        Assert.Null(RestoreEngine.ToRequestItem(WalSegmentEntry.From(late), [], Guid.NewGuid()));
        Assert.NotNull(RestoreEngine.ToRequestItem(WalSegmentEntry.From(inTime), [], Guid.NewGuid()));
    }

    // ── the refusal on a cluster ─────────────────────────────────────────────────

    [Fact]
    public async Task DelayedDirectWrite_IsRefusedOnEveryReplica_AndAnsweredMustRetry()
    {
        await WithCluster(lagMs: 50, proposeDelayMs: 200, body: async (nodes, ct) =>
        {
            string key = $"adm-direct/{Guid.NewGuid():N}";

            for (int attempt = 0; attempt < 3; attempt++)
            {
                (KeyValueResponseType type, _, _) = await nodes[attempt].LocateAndTrySetKeyValue(
                    HLCTimestamp.Zero, key, "late"u8.ToArray(), null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct);

                Assert.Equal(KeyValueResponseType.MustRetry, type);
            }

            foreach (KahunaManager node in nodes)
                Assert.Equal(KeyValueResponseType.DoesNotExist, await ReadLocal(node, key, ct));
        });
    }

    [Fact]
    public async Task DelayedTransaction_IsAbortedRetryably_AndInstallsNothing()
    {
        await WithCluster(lagMs: 50, proposeDelayMs: 200, body: async (nodes, ct) =>
        {
            string key = $"adm-tx/{Guid.NewGuid():N}";

            KeyValueTransactionResult result = await nodes[0].TryExecuteTransactionScript(
                Encoding.UTF8.GetBytes("BEGIN SET @k 'late' COMMIT END"), null, [new KeyValueParameter { Key = "@k", Value = key }]);

            Assert.Equal(KeyValueResponseType.MustRetry, result.Type);
            Assert.Equal(CommitRefusalClass.CommitTimestampOutsideWindow, result.Refusal);

            foreach (KahunaManager node in nodes)
            {
                await WaitUntilAsync(() => node.DurablePreparedIntentStore.Get(key) is null);
                Assert.Equal(KeyValueResponseType.DoesNotExist, await ReadLocal(node, key, ct));
            }
        });
    }

    [Fact]
    public async Task DelayedWrites_InsideALargeWindow_Commit()
    {
        await WithCluster(lagMs: 5_000, proposeDelayMs: 200, body: async (nodes, ct) =>
        {
            string direct = $"adm-ok/{Guid.NewGuid():N}";
            string transactional = $"adm-ok/{Guid.NewGuid():N}";

            (KeyValueResponseType type, _, _) = await RetryOnMustRetryAsync(
                () => nodes[1].LocateAndTrySetKeyValue(HLCTimestamp.Zero, direct, "v"u8.ToArray(), null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct),
                r => r.Item1);
            Assert.Equal(KeyValueResponseType.Set, type);

            KeyValueTransactionResult result = await RetryOnMustRetry(
                nodes[2], Encoding.UTF8.GetBytes("BEGIN SET @k 'v' COMMIT END"), null, [new KeyValueParameter { Key = "@k", Value = transactional }]);
            Assert.Equal(KeyValueResponseType.Set, result.Type);
        });
    }

    // ── the watermark ────────────────────────────────────────────────────────────

    [Fact]
    public async Task Watermark_StaysBelowAPendingIntent_UntilItSettles()
    {
        await WithCluster(lagMs: 1_000, proposeDelayMs: 0, body: async (nodes, ct) =>
        {
            string group = $"wm{Guid.NewGuid():N}|";
            string held = group + "held";
            string later = group + "later";

            List<FrameCollector> collectors = [];
            foreach (KahunaManager node in nodes)
            {
                (KeyValueResponseType opened, KeyValueWatch? watch) = await node.WatchKeyValues(new(KeyValueWatchScope.ForPrefix(group)), ct);
                Assert.Equal(KeyValueResponseType.Get, opened);
                collectors.Add(new FrameCollector(watch!));
            }

            HLCTimestamp heldCommit;
            HLCTimestamp heldTransaction;

            // The coordinator decides the commit, and the gate holds its settlement: the participant's intent stays
            // pending on every replica while later writes to the same partition commit.
            using (ResolutionGate gate = new(nodes[0]))
            {
                KeyValueTransactionResult committed = await RetryOnMustRetry(
                    nodes[0], Encoding.UTF8.GetBytes("BEGIN SET @k 'held' COMMIT END"), null, [new KeyValueParameter { Key = "@k", Value = held }]);
                Assert.Equal(KeyValueResponseType.Set, committed.Type);

                PreparedIntent pending = nodes[0].DurablePreparedIntentStore.Get(held)!;
                Assert.True(pending.IsPending);
                heldCommit = pending.CommitTimestamp;
                heldTransaction = pending.TransactionId;

                for (int i = 0; i < 3; i++)
                    await SetValue(nodes[1], later, $"v{i}", ct);

                foreach (FrameCollector collector in collectors)
                    await collector.WaitUntil(c => c.Events(later).Count == 3);

                // Two progress periods pass while the intent is pending: no watermark may reach it.
                await Task.Delay(2_500, ct);

                foreach (FrameCollector collector in collectors)
                {
                    Assert.Empty(collector.Events(held));
                    Assert.NotEmpty(collector.Watermarks());
                    Assert.All(collector.Watermarks(), w => Assert.True(w < heldCommit, $"watermark {w} reached the pending commit {heldCommit}"));
                }

                // The later writes committed above the held transaction and were delivered before it.
                Assert.Contains(collectors[0].Events(later), e => e.CommitTimestamp > heldCommit);
            }

            foreach (FrameCollector collector in collectors)
            {
                await collector.WaitUntil(c => c.Events(held).Count == 1);
                await collector.WaitUntil(c => c.Watermarks().Any(w => w >= heldCommit), timeoutMs: 15_000);

                collector.AssertNoEventBelowAnEarlierWatermark();
                collector.AssertWatermarksIncrease();

                // The held transaction's only event on the partition is followed by its end.
                List<KeyValueWatchFrame> frames = collector.Frames();
                int eventAt = frames.FindIndex(f => f.Event?.Key == held);
                int endAt = frames.FindIndex(f => f.Type == KeyValueWatchFrameType.TransactionEnd && f.TransactionId == heldTransaction);
                Assert.True(endAt > eventAt, "the transaction end must follow the transaction's event");

                Assert.DoesNotContain(frames, f => f.IsTerminal);
                await collector.DisposeAsync();
            }
        });
    }

    [Fact]
    public async Task IdlePartition_WatermarkAdvances_WithNoWrites()
    {
        await WithCluster(lagMs: 600, proposeDelayMs: 0, body: async (nodes, ct) =>
        {
            string group = $"idle{Guid.NewGuid():N}|";

            (KeyValueResponseType opened, KeyValueWatch? watch) = await nodes[2].WatchKeyValues(new(KeyValueWatchScope.ForPrefix(group)), ct);
            Assert.Equal(KeyValueResponseType.Get, opened);

            await using FrameCollector collector = new(watch!);
            await collector.WaitUntil(c => c.Watermarks().Count >= 3, timeoutMs: 15_000);

            collector.AssertWatermarksIncrease();
            Assert.Empty(collector.Frames().Where(f => f.Type == KeyValueWatchFrameType.Event));
        });
    }

    [Fact]
    public async Task WatermarkIsOff_WithoutTheWindow()
    {
        await WithCluster(lagMs: 0, proposeDelayMs: 0, body: async (nodes, ct) =>
        {
            string group = $"off{Guid.NewGuid():N}|";

            (KeyValueResponseType opened, KeyValueWatch? watch) = await nodes[0].WatchKeyValues(new(KeyValueWatchScope.ForPrefix(group)), ct);
            Assert.Equal(KeyValueResponseType.Get, opened);

            await using FrameCollector collector = new(watch!);
            await SetValue(nodes[1], group + "k", "v", ct);
            await collector.WaitUntil(c => c.Events(group + "k").Count == 1);
            await Task.Delay(1_000, ct);

            Assert.Empty(collector.Watermarks());
        });
    }

    // ── entry time ───────────────────────────────────────────────────────────────

    [Fact]
    public async Task EntryTime_NeverGoesDown_AcrossALeaderChange()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft r1, IRaft r2, IRaft r3, IKahuna k1, IKahuna k2, IKahuna k3) =
            await AssembleThreNodeCluster("memory", 4, raftLogger, kahunaLogger, c => c.CommitAdmissionLagMs = 5_000);

        IRaft[] rafts = [r1, r2, r3];
        KahunaManager[] nodes = [(KahunaManager)k1, (KahunaManager)k2, (KahunaManager)k3];

        try
        {
            string group = $"time{Guid.NewGuid():N}|";
            int partitionId = nodes[0].KeyValues.LocateDurablePartition(group + "k").PartitionId;

            for (int i = 0; i < 5; i++)
                await SetValue(nodes[i % 3], group + "k", $"before{i}", ct);

            int leader = await LeaderIndexOf(rafts, partitionId, ct);
            int successor = (leader + 1) % 3;
            Assert.Equal(RaftOperationStatus.Success, await rafts[leader].TransferLeadershipAsync(partitionId, $"localhost:{8001 + successor}", ct));
            await WaitUntilAsync(async () => await rafts[successor].AmILeader(partitionId, ct));

            for (int i = 0; i < 5; i++)
                await SetValue(nodes[i % 3], group + "k", $"after{i}", ct);

            await Task.Delay(500, ct);

            foreach (IRaft raft in rafts)
                AssertEntryTimesNeverGoDown(raft.WalAdapter.ReadLogsRange(partitionId, 1));
        }
        finally
        {
            await LeaveCluster(r1, r2, r3);
        }
    }

    [Fact]
    public async Task EntryTime_NeverGoesDown_AcrossARestart_AndARefusedWriteStaysRefused()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        string storagePath = CreateTempDir("kahuna-admission-store-");
        string walPath = CreateTempDir("kahuna-admission-wal-");

        try
        {
            // Before the restart: a write inside the window commits; a write held past the window is refused.
            {
                bool delay = false;
                await using EmbeddedKahunaNode node = new(
                    PersistentOptions(storagePath, walPath, inner => new GatedDelayExecutor(inner, 300, () => delay)), loggerFactory);
                await node.StartAsync(ct);
                await node.WaitForLeaderForKeyAsync("restart/kept", ct);

                Assert.Equal(KeyValueResponseType.Set, await TrySet(node.Kahuna, "restart/kept", "kept", ct));

                delay = true;
                Assert.Equal(KeyValueResponseType.MustRetry, await TrySet(node.Kahuna, "restart/refused", "refused", ct));
                delay = false;
            }

            // After the restart the retained log replays: the refused write still installs nothing.
            {
                await using EmbeddedKahunaNode node = new(PersistentOptions(storagePath, walPath, decorator: null), loggerFactory);
                await node.StartAsync(ct);
                await node.WaitForLeaderForKeyAsync("restart/kept", ct);

                Assert.Equal(KeyValueResponseType.Set, await TrySet(node.Kahuna, "restart/after", "after", ct));

                (KeyValueResponseType kept, _) = await node.Kahuna.LocateAndTryGetValue(HLCTimestamp.Zero, "restart/kept", -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
                Assert.Equal(KeyValueResponseType.Get, kept);

                (KeyValueResponseType refused, _) = await node.Kahuna.LocateAndTryGetValue(HLCTimestamp.Zero, "restart/refused", -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
                Assert.Equal(KeyValueResponseType.DoesNotExist, refused);

                int partitionId = ((KahunaManager)node.Kahuna).KeyValues.LocateDurablePartition("restart/kept").PartitionId;
                AssertEntryTimesNeverGoDown(node.Raft.WalAdapter.ReadLogsRange(partitionId, 1));
            }
        }
        finally
        {
            TryDeleteDir(storagePath);
            TryDeleteDir(walPath);
        }
    }

    // ── helpers ──────────────────────────────────────────────────────────────────

    private static void AssertEntryTimesNeverGoDown(List<RaftLog> logs)
    {
        HLCTimestamp previous = HLCTimestamp.Zero;
        int committed = 0;

        foreach (RaftLog log in logs.OrderBy(l => l.Id))
        {
            if (log.Type is not (RaftLogType.Committed or RaftLogType.CommittedCheckpoint))
                continue;

            committed++;
            Assert.True(log.Time >= previous, $"entry {log.Id} has time {log.Time}, below the previous entry's {previous}");
            previous = log.Time;
        }

        Assert.True(committed > 0, "no committed entry was read");
    }

    private async Task WithCluster(long lagMs, int proposeDelayMs, Func<KahunaManager[], CancellationToken, Task> body)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft r1, IRaft r2, IRaft r3, IKahuna k1, IKahuna k2, IKahuna k3) = await AssembleThreNodeCluster(
            "memory", 4, raftLogger, kahunaLogger,
            c => c.CommitAdmissionLagMs = (int)lagMs,
            proposeDelayMs > 0 ? _ => inner => new GatedDelayExecutor(inner, proposeDelayMs, () => true) : null);

        try
        {
            await body([(KahunaManager)k1, (KahunaManager)k2, (KahunaManager)k3], ct);
        }
        finally
        {
            await LeaveCluster(r1, r2, r3);
        }
    }

    private static async Task<int> LeaderIndexOf(IRaft[] rafts, int partitionId, CancellationToken ct)
    {
        while (true)
        {
            for (int i = 0; i < rafts.Length; i++)
            {
                if (await rafts[i].AmILeader(partitionId, ct))
                    return i;
            }

            await Task.Delay(50, ct);
        }
    }

    private static async Task SetValue(IKahuna node, string key, string value, CancellationToken ct)
    {
        (KeyValueResponseType type, _, _) = await RetryOnMustRetryAsync(
            () => node.LocateAndTrySetKeyValue(HLCTimestamp.Zero, key, Encoding.UTF8.GetBytes(value), null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct),
            r => r.Item1);

        Assert.Equal(KeyValueResponseType.Set, type);
    }

    private static async Task<KeyValueResponseType> TrySet(IKahuna node, string key, string value, CancellationToken ct)
    {
        (KeyValueResponseType type, _, _) = await node.LocateAndTrySetKeyValue(
            HLCTimestamp.Zero, key, Encoding.UTF8.GetBytes(value), null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct);

        return type;
    }

    // What this node's own replica holds for the key, without routing the read to the leader.
    private static async Task<KeyValueResponseType> ReadLocal(KahunaManager node, string key, CancellationToken ct)
    {
        await node.FlushPersistenceAsync();
        (KeyValueResponseType type, _) = await node.TryGetValue(HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent);
        return type;
    }

    private static async Task<List<KeyValueWatchFrame>> Take(KeyValueWatch watch, int count)
    {
        using CancellationTokenSource timeout = new(TimeSpan.FromSeconds(5));
        List<KeyValueWatchFrame> frames = [];

        await foreach (KeyValueWatchFrame frame in watch.ReadAllAsync(timeout.Token))
        {
            frames.Add(frame);
            if (frames.Count == count)
                break;
        }

        return frames;
    }

    private static PreparedIntent MakeIntent(string key, long txPhysical, long commitPhysical) => new(
        TransactionId: new HLCTimestamp(1, txPhysical, 0), Epoch: 1, Key: key,
        ManifestHash: 0, RecordAnchorKey: key,
        CommitTimestamp: new HLCTimestamp(1, commitPhysical, 0),
        State: KeyValueState.Set, Value: [1], Bucket: null,
        Revision: 1, Expires: HLCTimestamp.Zero, NoRevision: false,
        BaseRevision: PreparedIntent.UnknownBaseRevision, BaseState: KeyValueState.Undefined,
        RecoveryDeadline: HLCTimestamp.Zero, Resolution: PreparedIntentResolution.Pending);

    private static RaftLog Log(HLCTimestamp time, long id, params PreparedIntentCommand[] commands) =>
        new() { Id = id, Time = time, LogType = ReplicationTypes.PreparedIntent, LogData = [.. PreparedIntentStore.SerializeDelta(commands)] };

    private static EmbeddedKahunaOptions PersistentOptions(
        string storagePath, string walPath, Func<IPartitionBatchExecutor, IPartitionBatchExecutor>? decorator) => new()
    {
        InitialPartitions = 1,
        Storage = "sqlite",
        StoragePath = storagePath,
        StorageRevision = "commit-admission",
        WalStorage = "sqlite",
        WalPath = walPath,
        WalRevision = "commit-admission-wal",
        WalSyncWrites = true,
        CommitAdmissionLagMs = 100,
        CollectionInterval = TimeSpan.FromMinutes(10),
        WriteBatchExecutorDecorator = decorator
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

    /// <summary>Holds each batch before it reaches Raft while <c>enabled</c> answers true, so the entry time lands
    /// later than the writes' commit timestamps by at least the delay.</summary>
    private sealed class GatedDelayExecutor(IPartitionBatchExecutor inner, int delayMs, Func<bool> enabled) : IPartitionBatchExecutor
    {
        public async Task<RaftBatchReplicationResult> ReplicateAsync(int partitionId, IReadOnlyList<RaftProposalEntry> entries, CancellationToken cancellationToken)
        {
            if (enabled())
                await Task.Delay(delayMs, cancellationToken);

            return await inner.ReplicateAsync(partitionId, entries, cancellationToken);
        }
    }

    /// <summary>Holds every deferred resolution the node's coordinator schedules while installed, so a commit's
    /// decision→settlement window stays open. Disposal releases and uninstalls the hook.</summary>
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

    /// <summary>Reads every frame of a watch in the background, progress frames included.</summary>
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

        public Task WaitUntil(Func<FrameCollector, bool> predicate, int timeoutMs = 10_000) =>
            WaitUntilAsync(() => predicate(this) || frames.Any(f => f.IsTerminal), timeoutMs)
                .ContinueWith(_ => Assert.True(predicate(this), $"condition not met; terminal: {frames.FirstOrDefault(f => f.IsTerminal)?.LagReason}"));

        /// <summary>The watermark's promise: no event at or below a watermark the watch already received.</summary>
        public void AssertNoEventBelowAnEarlierWatermark()
        {
            HLCTimestamp promised = HLCTimestamp.Zero;

            foreach (KeyValueWatchFrame frame in frames)
            {
                if (frame.Type == KeyValueWatchFrameType.Progress)
                    promised = frame.Timestamp;
                else if (frame.Event is { } e)
                    Assert.True(e.CommitTimestamp > promised, $"event {e.Key} at {e.CommitTimestamp} arrived after watermark {promised}");
            }
        }

        public void AssertWatermarksIncrease()
        {
            List<HLCTimestamp> watermarks = Watermarks();
            for (int i = 1; i < watermarks.Count; i++)
                Assert.True(watermarks[i] > watermarks[i - 1], $"watermark {i} ({watermarks[i]}) is not above watermark {i - 1} ({watermarks[i - 1]})");
        }

        public async ValueTask DisposeAsync()
        {
            await watch.DisposeAsync();
            await stop.CancelAsync();
            await pump;
            stop.Dispose();
        }
    }
}
