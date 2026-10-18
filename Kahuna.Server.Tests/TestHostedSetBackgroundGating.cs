
using Kahuna;
using Kahuna.Server.Configuration;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Kommander;
using Kommander.Data;
using Kommander.System;
using Kommander.Time;
using Kommander.WAL;
using Kommander.WAL.IO;
using Microsoft.Extensions.Logging.Abstractions;
using Nixie;

namespace Kahuna.Server.Tests;

/// <summary>
/// Under per-partition replica placement, the background writer must scope its per-partition
/// machinery to the partitions this node actually hosts: the PITR horizon must not touch the WAL
/// retention of ranges hosted elsewhere, and every piece of per-partition bookkeeping (checkpoint
/// tracking, enqueued-HLC watermark, persisted-floor cache, durability-tracker state) must be
/// dropped once the committed map stops listing this node as a replica — a retained durability
/// floor is a WAL retention leak (or a stale voucher) if the range is ever hosted here again.
/// With full replication (every partition hosted) all of this must be inert.
/// </summary>
public sealed class TestHostedSetBackgroundGating
{
    private static KahunaConfiguration Config(TimeSpan pitrWindow = default) => ConfigurationValidator.Validate(new()
    {
        LocksWorkers = 1, KeyValueWorkers = 1, BackgroundWriterWorkers = 1, Storage = "memory",
        // Keep the periodic timer and the checkpoint path inert so the tests drive every
        // transition explicitly through Receive, with no concurrent mailbox activity.
        DirtyObjectsWriterDelay = 600_000,
        CheckpointInterval = TimeSpan.FromHours(1),
        PitrWindow = pitrWindow,
        BaseSnapshotInterval = pitrWindow == default ? default : TimeSpan.FromMinutes(1)
    });

    private static TestBackupService.StubRaft MakeRaft(params int[] partitionIds) => new(
        new InMemoryWAL(NullLogger<IRaft>.Instance),
        [.. partitionIds.Select(static id => new RaftPartitionRange { PartitionId = id, State = RaftPartitionState.Active })]);

    private static BackgroundWriteRequest KeyValueWrite(int partitionId, string key, long logIndex, long hlcL) => new(
        BackgroundWriteType.QueueStoreKeyValue, partitionId, key, [1, 2, 3], revision: 1,
        expires: HLCTimestamp.Zero, lastUsed: HLCTimestamp.Zero, lastModified: new HLCTimestamp(0, hlcL, 0),
        state: 1, noRevision: false, logIndex: logIndex);

    private sealed class WriterHarness : IDisposable
    {
        private readonly IDisposable actorLifetime;

        private readonly FairReadScheduler scheduler;

        public readonly PartitionDurabilityTracker Tracker = new();

        public readonly BackgroundWriterActor Writer;

        public WriterHarness(TestBackupService.StubRaft raft, KahunaConfiguration config)
        {
            actorLifetime = TestActorSystemLifetime.Create(out ActorSystem actorSystem);

            scheduler = new FairReadScheduler(NullLogger<IRaft>.Instance, 1, 1024);
            scheduler.Start();

            IActorRef<BackgroundWriterActor, BackgroundWriteRequest> bg = actorSystem.Spawn<BackgroundWriterActor, BackgroundWriteRequest>(
                "bg-hosted-gating", raft, scheduler, new MemoryPersistenceBackend(),
                null!, null!, null!, null!,
                config, NullLogger<IKahuna>.Instance, new FlushNotificationSink(), Tracker);

            Writer = (bg.Runner.Actor as BackgroundWriterActor)!;
        }

        public void Dispose()
        {
            actorLifetime.Dispose();
            scheduler.Stop();
            scheduler.Dispose();
        }
    }

    [Fact]
    public async Task PitrHorizon_TouchesOnlyPartitionsThisNodeHosts()
    {
        TestBackupService.StubRaft raft = MakeRaft(1, 2, 3);
        raft.HostsPartitionOverride = partitionId => partitionId != 2;

        using WriterHarness harness = new(raft, Config(pitrWindow: TimeSpan.FromHours(1)));

        await harness.Writer.Receive(new(BackgroundWriteType.Flush));

        Assert.Equal([1, 3], raft.MinRetainIndexByPartition.Keys.Order());
    }

    [Fact]
    public async Task PitrHorizon_LogEntirelyInsideTheWindow_AssertsAPositiveFloor()
    {
        // Every entry is younger than the boundary (now - 1h - 1m). The tick must protect the log
        // from its first committed index; a non-positive value would make Kommander drop the floor.
        TestBackupService.StubRaft raft = MakeRaft(1);
        long nowMs = (long)(DateTime.UtcNow - DateTime.UnixEpoch).TotalMilliseconds;
        raft.WalAdapter.Write([(1, [
            new RaftLog { Id = 1, Type = RaftLogType.Proposed, Time = new HLCTimestamp(0, nowMs, 0) },
            new RaftLog { Id = 2, Type = RaftLogType.Committed, Time = new HLCTimestamp(0, nowMs, 0) },
            new RaftLog { Id = 3, Type = RaftLogType.Committed, Time = new HLCTimestamp(0, nowMs, 0) }])]);

        using WriterHarness harness = new(raft, Config(pitrWindow: TimeSpan.FromHours(1)));

        await harness.Writer.Receive(new(BackgroundWriteType.Flush));

        Assert.Equal(2, raft.MinRetainIndexByPartition[1]);
    }

    [Fact]
    public async Task PitrHorizon_EmptyLog_AssertsTheFirstIndex()
    {
        TestBackupService.StubRaft raft = MakeRaft(1);

        using WriterHarness harness = new(raft, Config(pitrWindow: TimeSpan.FromHours(1)));

        await harness.Writer.Receive(new(BackgroundWriteType.Flush));

        Assert.Equal(1, raft.MinRetainIndexByPartition[1]);
    }

    [Fact]
    public async Task PitrHorizon_CoversEveryPartitionUnderFullReplication()
    {
        TestBackupService.StubRaft raft = MakeRaft(1, 2, 3);

        using WriterHarness harness = new(raft, Config(pitrWindow: TimeSpan.FromHours(1)));

        await harness.Writer.Receive(new(BackgroundWriteType.Flush));

        Assert.Equal([1, 2, 3], raft.MinRetainIndexByPartition.Keys.Order());
    }

    [Fact]
    public async Task PitrHorizon_PartitionsHostedAfterTheFirstTick_AreProtectedOnTheNextTick()
    {
        // The writer starts before the node joins its cluster, so the first tick sees no hosted
        // partition. The floor must land on the first tick that sees one, not one recompute interval
        // later (30 minutes at the defaults): until then compaction runs to the checkpoint and a
        // backup's parent range is gone within a minute under load.
        TestBackupService.StubRaft raft = MakeRaft(1, 2);
        raft.HostsPartitionOverride = static _ => false;

        using WriterHarness harness = new(raft, Config(pitrWindow: TimeSpan.FromHours(1)));

        await harness.Writer.Receive(new(BackgroundWriteType.Flush));
        Assert.Empty(raft.MinRetainIndexByPartition);

        raft.HostsPartitionOverride = null;
        await harness.Writer.Receive(new(BackgroundWriteType.Flush));

        Assert.Equal([1, 2], raft.MinRetainIndexByPartition.Keys.Order());
        Assert.Equal(1, raft.MinRetainIndexByPartition[1]);
        Assert.Equal(1, raft.MinRetainIndexByPartition[2]);
    }

    [Fact]
    public async Task PitrHorizon_FloorIsReassertedOnEveryTick_AndRecomputedOncePerInterval()
    {
        TestBackupService.StubRaft raft = MakeRaft(1);

        using WriterHarness harness = new(raft, Config(pitrWindow: TimeSpan.FromHours(1)));

        await harness.Writer.Receive(new(BackgroundWriteType.Flush));
        Assert.Equal(1, raft.MinRetainIndexByPartition[1]);

        // Entries older than the boundary: a recompute would protect from index 2, the last committed
        // entry at or before it. Within the recompute interval the stored index is re-asserted as is,
        // so Kommander's in-memory floor is re-established on every tick without paging the WAL.
        long oldMs = (long)(DateTime.UtcNow.AddHours(-3) - DateTime.UnixEpoch).TotalMilliseconds;
        raft.WalAdapter.Write([(1, [
            new RaftLog { Id = 1, Type = RaftLogType.Committed, Time = new HLCTimestamp(0, oldMs, 0) },
            new RaftLog { Id = 2, Type = RaftLogType.Committed, Time = new HLCTimestamp(0, oldMs, 0) }])]);
        raft.MinRetainIndexByPartition.Clear();

        await harness.Writer.Receive(new(BackgroundWriteType.Flush));

        Assert.Equal(1, raft.MinRetainIndexByPartition[1]);
    }

    [Fact]
    public async Task PitrHorizon_PartitionHostedAgain_IsComputedAfresh()
    {
        TestBackupService.StubRaft raft = MakeRaft(1);

        using WriterHarness harness = new(raft, Config(pitrWindow: TimeSpan.FromHours(1)));

        await harness.Writer.Receive(new(BackgroundWriteType.Flush));
        Assert.Equal(1, raft.MinRetainIndexByPartition[1]);
        Assert.True(harness.Writer.HasPartitionBookkeeping(1));

        // The replica moves away: the stored floor is bookkeeping for a range this node no longer
        // hosts and goes with the rest of it.
        raft.HostsPartitionOverride = static _ => false;
        raft.MinRetainIndexByPartition.Clear();
        await harness.Writer.Receive(new(BackgroundWriteType.Flush));

        Assert.Empty(raft.MinRetainIndexByPartition);
        Assert.False(harness.Writer.HasPartitionBookkeeping(1));

        // The range comes back with a log that starts elsewhere: its floor is computed from that log
        // on the first tick that sees it hosted again, not taken from the value stored before.
        long oldMs = (long)(DateTime.UtcNow.AddHours(-3) - DateTime.UnixEpoch).TotalMilliseconds;
        raft.WalAdapter.Write([(1, [
            new RaftLog { Id = 1, Type = RaftLogType.Committed, Time = new HLCTimestamp(0, oldMs, 0) },
            new RaftLog { Id = 2, Type = RaftLogType.Committed, Time = new HLCTimestamp(0, oldMs, 0) }])]);
        raft.HostsPartitionOverride = null;
        await harness.Writer.Receive(new(BackgroundWriteType.Flush));

        Assert.Equal(2, raft.MinRetainIndexByPartition[1]);
    }

    [Fact]
    public async Task LosingAReplica_DropsItsBookkeeping_AndKeepsTheHostedPartitionsIntact()
    {
        TestBackupService.StubRaft raft = MakeRaft(1, 2);

        using WriterHarness harness = new(raft, Config());
        BackgroundWriterActor writer = harness.Writer;
        PartitionDurabilityTracker tracker = harness.Tracker;

        // Both partitions hosted: applies register with the tracker, the writes flush, and the
        // floors advance — the full set of per-partition state a real node accumulates.
        tracker.RegisterPending(1, 5, DurabilityChannel.Flush);
        tracker.RegisterPending(2, 7, DurabilityChannel.Flush);

        await writer.Receive(KeyValueWrite(1, "kv/a", logIndex: 5, hlcL: 100));
        await writer.Receive(KeyValueWrite(2, "kv/b", logIndex: 7, hlcL: 200));
        await writer.Receive(new(BackgroundWriteType.Flush));

        Assert.True(writer.HasPartitionBookkeeping(1));
        Assert.True(writer.HasPartitionBookkeeping(2));
        Assert.Equal(5, tracker.GetWatermark(1));
        Assert.Equal(7, tracker.GetWatermark(2));

        // The committed map stops listing this node as a replica of partition 2.
        raft.HostsPartitionOverride = partitionId => partitionId != 2;

        await writer.Receive(new(BackgroundWriteType.ForgetUnhostedPartitions));

        Assert.False(writer.HasPartitionBookkeeping(2));
        Assert.Equal(HLCTimestamp.Zero, writer.GetMaxEnqueuedHlc(2));
        Assert.DoesNotContain(2, tracker.ObservedPartitions);
        Assert.Equal(-1, tracker.GetWatermark(2));

        // The partition still hosted is untouched.
        Assert.True(writer.HasPartitionBookkeeping(1));
        Assert.Contains(1, tracker.ObservedPartitions);
        Assert.Equal(5, tracker.GetWatermark(1));
    }

    [Fact]
    public async Task StragglerWrite_QueuedBeforeTheLoss_IsSweptByTheNextFlushCycle()
    {
        TestBackupService.StubRaft raft = MakeRaft(1, 2);

        using WriterHarness harness = new(raft, Config());
        BackgroundWriterActor writer = harness.Writer;

        await writer.Receive(KeyValueWrite(2, "kv/b", logIndex: 7, hlcL: 200));
        await writer.Receive(new(BackgroundWriteType.Flush));
        Assert.True(writer.HasPartitionBookkeeping(2));

        raft.HostsPartitionOverride = partitionId => partitionId != 2;
        await writer.Receive(new(BackgroundWriteType.ForgetUnhostedPartitions));
        Assert.False(writer.HasPartitionBookkeeping(2));

        // A write applied just before the loss re-creates the bookkeeping after the explicit
        // teardown ran; the periodic flush cycle must sweep it again on its own.
        await writer.Receive(KeyValueWrite(2, "kv/b2", logIndex: 9, hlcL: 300));
        await writer.Receive(new(BackgroundWriteType.Flush));

        Assert.False(writer.HasPartitionBookkeeping(2));
        Assert.DoesNotContain(2, harness.Tracker.ObservedPartitions);
    }

    [Fact]
    public void DurabilityTracker_ForgetDropsState_AndLateResolutionsDoNotResurrectIt()
    {
        PartitionDurabilityTracker tracker = new();

        tracker.RegisterPending(3, 10, DurabilityChannel.Flush);
        tracker.MarkApplied(3, 10, DurabilityChannel.Receipts);
        Assert.Contains(3, tracker.ObservedPartitions);

        tracker.Forget(3);
        Assert.DoesNotContain(3, tracker.ObservedPartitions);

        // A flush ack, a snapshot resolution or a resolve-side read landing after the partition was
        // forgotten must not make it observed again — that would let a later watermark advance over
        // an entry that was never resolved on the new copy.
        tracker.Resolve(3, 10);
        tracker.ResolveUpTo(3, DurabilityChannel.Receipts, 10);
        Assert.Equal(-1, tracker.GetHighestApplied(3, DurabilityChannel.Receipts));
        Assert.Equal(-1, tracker.GetWatermark(3));
        Assert.False(tracker.HasPendingSnapshotWork(3));
        Assert.DoesNotContain(3, tracker.ObservedPartitions);
    }

    [Fact]
    public void DurabilityTracker_RegistrationAfterForget_StartsFreshTrackingState()
    {
        PartitionDurabilityTracker tracker = new();

        tracker.RegisterPending(3, 10, DurabilityChannel.Flush);
        tracker.Resolve(3, 10);
        Assert.Equal(10, tracker.GetWatermark(3));

        tracker.Forget(3);

        // Hosting the partition again starts from a clean slate: the new registration is the only
        // opinion, with nothing inherited from the previous copy's floor.
        tracker.RegisterPending(3, 25, DurabilityChannel.Flush);
        Assert.Contains(3, tracker.ObservedPartitions);
        Assert.Equal(24, tracker.GetWatermark(3));

        tracker.Resolve(3, 25);
        Assert.Equal(25, tracker.GetWatermark(3));
    }
}
