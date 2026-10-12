
using System.Text;
using Kahuna;
using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Handlers;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Shared.KeyValue;
using Kahuna.Utils;
using Kommander;
using Kommander.Communication.Memory;
using Kommander.Discovery;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kahuna.Server.Tests;

/// <summary>
/// The published head records an actor keeps beside its store for off-actor latest reads: every change to a
/// resident head or to its intents retracts the live record before the change lands, and the end-of-turn flush
/// republishes a quiet <c>Set</c> head and nothing else. The checks drive the context and the handlers directly
/// and compare the table with the store after each step.
/// </summary>
public sealed class TestPublishedHeadTable : RaftTrackingTest
{
    [Fact]
    public void InsertedHead_PublishesOnlyAtFlush()
    {
        (KeyValueContext context, RaftManager raft) = CreateContext();
        HLCTimestamp now = Now(raft);

        KeyValueEntry entry = Insert(context, "k/1", "v1", 3, now);

        Assert.Equal(0, context.PublishedHeads.Count);
        Assert.Null(entry.Published);

        context.FlushHeadPublications();

        Assert.True(context.PublishedHeads.TryGet("k/1", out PublishedHead head));
        Assert.Same(head, entry.Published);
        Assert.False(head.IsRetracted);
        Assert.Equal(3L, head.Revision);
        Assert.Equal("v1", Encoding.UTF8.GetString(head.Value!));
        Assert.Equal(now, head.LastModified);
        Assert.Equal(KeyValueState.Set, head.State);
        Assert.False(head.MvccHeld);
        AssertTableMatchesStore(context);
    }

    [Fact]
    public void HeadChange_RetractsBeforeTheInstallCompletesAndRepublishesAtFlush()
    {
        (KeyValueContext context, RaftManager raft) = CreateContext();
        HLCTimestamp now = Now(raft);
        KeyValueEntry entry = Insert(context, "k/2", "v1", 3, now);
        context.FlushHeadPublications();
        PublishedHead first = entry.Published!;

        // The first field store of a multi-field install already retracts the record.
        entry.Value = Encoding.UTF8.GetBytes("v2");
        Assert.True(first.IsRetracted);
        Assert.Null(entry.Published);
        Assert.True(context.PublishedHeads.TryGet("k/2", out PublishedHead stillOld));
        Assert.Same(first, stillOld);

        entry.Revision = 4;
        entry.LastModified = Now(raft);
        context.FlushHeadPublications();

        Assert.True(context.PublishedHeads.TryGet("k/2", out PublishedHead second));
        Assert.NotSame(first, second);
        Assert.False(second.IsRetracted);
        Assert.Equal(4L, second.Revision);
        Assert.Equal("v2", Encoding.UTF8.GetString(second.Value!));
        AssertTableMatchesStore(context);
    }

    [Fact]
    public void WriteIntent_RetractsAndStaysOffUntilCleared()
    {
        (KeyValueContext context, RaftManager raft) = CreateContext();
        HLCTimestamp now = Now(raft);
        KeyValueEntry entry = Insert(context, "k/3", "v1", 1, now);
        context.FlushHeadPublications();
        PublishedHead live = entry.Published!;

        entry.WriteIntent = new KeyValueWriteIntent { TransactionId = now, Expires = now + 60_000 };
        Assert.True(live.IsRetracted);
        context.FlushHeadPublications();
        Assert.Null(entry.Published);
        Assert.True(context.PublishedHeads.TryGet("k/3", out PublishedHead kept) && kept.IsRetracted);
        AssertTableMatchesStore(context);

        entry.WriteIntent = null;
        context.FlushHeadPublications();
        Assert.NotNull(entry.Published);
        Assert.False(entry.Published!.IsRetracted);
        AssertTableMatchesStore(context);
    }

    [Fact]
    public void ReplicationIntentAndParkedHead_Retract()
    {
        (KeyValueContext context, RaftManager raft) = CreateContext();
        HLCTimestamp now = Now(raft);
        KeyValueEntry entry = Insert(context, "k/4", "v1", 1, now);
        context.FlushHeadPublications();
        PublishedHead live = entry.Published!;

        entry.ReplicationIntent = new KeyValueReplicationIntent { ProposalId = 7, Expires = now + 60_000 };
        Assert.True(live.IsRetracted);
        context.FlushHeadPublications();
        Assert.Null(entry.Published);

        entry.ReplicationIntent = null;
        context.FlushHeadPublications();
        PublishedHead relive = entry.Published!;
        Assert.False(relive.IsRetracted);

        entry.PendingCommittedHead = new InvalidateOrApplyData(2, Encoding.UTF8.GetBytes("v2"), HLCTimestamp.Zero, now, now, KeyValueState.Set);
        Assert.True(relive.IsRetracted);
        context.FlushHeadPublications();
        Assert.Null(entry.Published);

        entry.PendingCommittedHead = null;
        context.FlushHeadPublications();
        Assert.NotNull(entry.Published);
        AssertTableMatchesStore(context);
    }

    [Fact]
    public void MvccDictionary_FlagsTheRecordHeld()
    {
        (KeyValueContext context, RaftManager raft) = CreateContext();
        HLCTimestamp now = Now(raft);
        KeyValueEntry entry = Insert(context, "k/5", "v1", 1, now);
        context.FlushHeadPublications();
        PublishedHead free = entry.Published!;
        Assert.False(free.MvccHeld);

        entry.MvccEntries ??= new();
        Assert.True(free.IsRetracted);
        context.FlushHeadPublications();
        Assert.True(entry.Published!.MvccHeld);

        // A second pin added to the existing dictionary changes nothing the record must learn.
        PublishedHead held = entry.Published!;
        entry.MvccEntries.Add(now, new KeyValueMvccEntry { Revision = 1, State = KeyValueState.Set });
        Assert.False(held.IsRetracted);

        entry.MvccEntries = null;
        Assert.True(held.IsRetracted);
        context.FlushHeadPublications();
        Assert.False(entry.Published!.MvccHeld);
        AssertTableMatchesStore(context);
    }

    [Fact]
    public void Tombstone_DropsTheRecord_AndRemovalDropsIt()
    {
        (KeyValueContext context, RaftManager raft) = CreateContext();
        HLCTimestamp now = Now(raft);
        KeyValueEntry deleted = Insert(context, "k/6", "v1", 1, now);
        KeyValueEntry removed = Insert(context, "k/7", "v1", 1, now);
        context.FlushHeadPublications();
        PublishedHead deletedLive = deleted.Published!;
        PublishedHead removedLive = removed.Published!;

        deleted.State = KeyValueState.Deleted;
        deleted.Value = null;
        Assert.True(deletedLive.IsRetracted);
        context.FlushHeadPublications();
        Assert.False(context.PublishedHeads.TryGet("k/6", out _));

        Assert.True(context.RemoveStoreEntry("k/7"));
        Assert.True(removedLive.IsRetracted);
        Assert.False(context.PublishedHeads.TryGet("k/7", out _));
        Assert.Null(removed.Owner);

        // A later change to the evicted object neither republishes nor reaches the table.
        removed.Revision = 9;
        context.FlushHeadPublications();
        Assert.False(context.PublishedHeads.TryGet("k/7", out _));
        AssertTableMatchesStore(context);
    }

    [Fact]
    public void PendingApplyNewerThanTheHead_BlocksPublicationUntilCleared()
    {
        (KeyValueContext context, RaftManager raft) = CreateContext();
        HLCTimestamp now = Now(raft);
        KeyValueEntry entry = Insert(context, "k/8", "v1", 1, now);

        HLCTimestamp later = Now(raft);
        context.PublishedHeads.NotePendingApply("k/8", 2, later);
        context.FlushHeadPublications();
        Assert.False(context.PublishedHeads.TryGet("k/8", out _));
        Assert.True(context.PublishedHeads.HasNewerPendingApply("k/8", 1, now));

        // The newest pending head wins over an older note, and a stale replay does not lower it.
        context.PublishedHeads.NotePendingApply("k/8", 1, now);
        Assert.True(context.PublishedHeads.HasNewerPendingApply("k/8", 1, now));
        context.PublishedHeads.ClearPendingApply("k/8", 1, now);
        Assert.True(context.PublishedHeads.HasNewerPendingApply("k/8", 1, now));

        // The apply lands: the head advances past the pending note, which is cleared with it.
        entry.Value = Encoding.UTF8.GetBytes("v2");
        entry.Revision = 2;
        entry.LastModified = later;
        context.PublishedHeads.ClearPendingApply("k/8", 2, later);
        Assert.Equal(0, context.PublishedHeads.PendingApplyCount);
        context.FlushHeadPublications();
        Assert.True(context.PublishedHeads.TryGet("k/8", out PublishedHead head));
        Assert.Equal(2L, head.Revision);
        Assert.False(context.PublishedHeads.HasNewerPendingApply("k/8", 2, later));
        AssertTableMatchesStore(context);
    }

    [Fact]
    public void PrefixLockPresence_FollowsTheLockTable()
    {
        (KeyValueContext context, RaftManager raft) = CreateContext();
        HLCTimestamp now = Now(raft);

        context.FlushHeadPublications();
        Assert.False(context.PublishedHeads.PrefixLocksHeld);

        context.LocksByPrefix.Add("k", new KeyValueWriteIntent { TransactionId = now, Expires = now + 60_000 });
        context.FlushHeadPublications();
        Assert.True(context.PublishedHeads.PrefixLocksHeld);

        context.LocksByPrefix.Remove("k");
        context.FlushHeadPublications();
        Assert.False(context.PublishedHeads.PrefixLocksHeld);
    }

    [Fact]
    public void FlagOff_PublishesNothing()
    {
        KahunaConfiguration config = CreateConfiguration();
        config.PublishedHeadReads = false;
        (KeyValueContext context, RaftManager raft) = CreateContext(config);

        Insert(context, "k/9", "v1", 1, Now(raft));
        context.FlushHeadPublications();

        Assert.Equal(0, context.PublishedHeads.Count);
    }

    [Fact]
    public void Collect_EvictsColdCleanEntries_AndDropsTheirRecords()
    {
        KahunaConfiguration config = CreateConfiguration();
        config.MaxEntriesPerActor = 2;
        (KeyValueContext context, RaftManager raft) = CreateContext(config);
        TryCollectHandler collector = new(context);
        HLCTimestamp stale = new(1, 1, 0);

        for (int i = 0; i < 6; i++)
            Insert(context, $"c/{i}", "v", 1, stale, lastUsed: stale);
        context.FlushHeadPublications();
        Assert.Equal(6, context.PublishedHeads.Count);

        collector.Execute();
        context.FlushHeadPublications();

        Assert.True(context.Store.Count <= 2);
        Assert.Equal(context.Store.Count, context.PublishedHeads.Count);
        AssertTableMatchesStore(context);
    }

    [Fact]
    public void Collect_KeepsAKeyThatWasReadOffTheActor()
    {
        KahunaConfiguration config = CreateConfiguration();
        config.CacheEntryTtl = TimeSpan.FromMinutes(5);
        (KeyValueContext context, RaftManager raft) = CreateContext(config);
        TryCollectHandler collector = new(context);
        HLCTimestamp now = Now(raft);
        HLCTimestamp stale = new(1, 1, 0);

        KeyValueEntry idle = Insert(context, "i/idle", "v", 1, stale, lastUsed: stale);
        KeyValueEntry readOffActor = Insert(context, "i/read", "v", 1, stale, lastUsed: stale);
        context.FlushHeadPublications();

        // An off-actor read stamps the record, not the entry: the entry still looks idle in the LRU list.
        readOffActor.Published!.Touch(now.L);

        collector.Execute();
        context.FlushHeadPublications();

        Assert.False(context.Store.ContainsKey("i/idle"));
        Assert.True(context.Store.ContainsKey("i/read"));
        Assert.Equal(now.L, readOffActor.LastUsed.L);
        Assert.Same(context.LruHead, readOffActor);
        Assert.Null(idle.Owner);
        AssertTableMatchesStore(context);
    }

    [Fact]
    public void DropLeaderState_RepublishesTheCommittedHeadWithoutItsStagedState()
    {
        (KeyValueContext context, RaftManager raft) = CreateContext();
        DropLeaderStateHandler drop = new(context);
        HLCTimestamp now = Now(raft);

        KeyValueEntry entry = Insert(context, "d/1", "v1", 1, now);
        entry.WriteIntent = new KeyValueWriteIntent { TransactionId = now, Expires = now + 60_000 };
        entry.MvccEntries ??= new();
        context.FlushHeadPublications();
        Assert.Null(entry.Published);

        drop.Execute(new KeyValueRequest(KeyValueRequestType.DropLeaderState, HLCTimestamp.Zero, HLCTimestamp.Zero, "d/1",
            null, null, -1, KeyValueFlags.None, 0, HLCTimestamp.Zero, KeyValueDurability.Persistent, 0, OwnerOf(context, raft, "d/1"), default));
        context.FlushHeadPublications();

        Assert.Null(entry.WriteIntent);
        Assert.Null(entry.MvccEntries);
        Assert.NotNull(entry.Published);
        Assert.False(entry.Published!.MvccHeld);
        AssertTableMatchesStore(context);
    }

    [Fact]
    public void EvictPartition_DropsEveryRecordOfThePartition()
    {
        (KeyValueContext context, RaftManager raft) = CreateContext();
        EvictPartitionHandler evict = new(context);
        HLCTimestamp now = Now(raft);

        Insert(context, "e/1", "v1", 1, now);
        Insert(context, "e/2", "v1", 1, now);
        context.FlushHeadPublications();
        Assert.Equal(2, context.PublishedHeads.Count);

        evict.Execute(new KeyValueRequest(KeyValueRequestType.EvictPartition, HLCTimestamp.Zero, HLCTimestamp.Zero, "",
            null, null, -1, KeyValueFlags.None, 0, HLCTimestamp.Zero, KeyValueDurability.Persistent, 0, OwnerOf(context, raft, "e/1"), default));
        context.FlushHeadPublications();

        Assert.Equal(0, context.Store.Count);
        Assert.Equal(0, context.PublishedHeads.Count);
    }

    [Fact]
    public void ReadPin_PinsTheRevisionTheReadObserved_AndKeepsAnEarlierPin()
    {
        (KeyValueContext context, RaftManager raft) = CreateContext();
        RecordReadPinHandler pins = new(context);
        HLCTimestamp now = Now(raft);
        HLCTimestamp tx = Now(raft);

        KeyValueEntry entry = Insert(context, "p/1", "v1", 1, now);
        context.FlushHeadPublications();
        PublishedHead observed = entry.Published!;

        // The head moved after the read was answered: the pin still records what the read saw.
        entry.Value = Encoding.UTF8.GetBytes("v2");
        entry.Revision = 2;
        pins.Execute(PinRequest("p/1", tx, observed));

        Assert.NotNull(entry.MvccEntries);
        Assert.True(entry.MvccEntries!.TryGetValue(tx, out KeyValueMvccEntry? pin));
        Assert.Equal(1L, pin!.Revision);
        Assert.Equal("v1", Encoding.UTF8.GetString(pin.Value!));

        // A later pin for the same transaction does not replace the earlier, stricter one.
        context.FlushHeadPublications();
        pins.Execute(PinRequest("p/1", tx, entry.Published!));
        Assert.Equal(1L, entry.MvccEntries[tx].Revision);
        Assert.Single(entry.MvccEntries);
    }

    [Fact]
    public void ReadPin_OnAnEvictedEntry_TakesTheKeyFromTheTransaction()
    {
        (KeyValueContext context, RaftManager raft) = CreateContext();
        RecordReadPinHandler pins = new(context);
        HLCTimestamp now = Now(raft);
        HLCTimestamp tx = Now(raft);

        KeyValueEntry entry = Insert(context, "p/2", "v1", 1, now);
        context.FlushHeadPublications();
        PublishedHead observed = entry.Published!;
        context.RemoveStoreEntry("p/2");

        pins.Execute(PinRequest("p/2", tx, observed));

        Assert.True(context.HasYieldedIntent("p/2", tx));
        Assert.False(context.Store.ContainsKey("p/2"));
    }

    // ── helpers ──────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Every resident entry has a live record exactly when its head is a quiet <c>Set</c> head, that record is
    /// the one in the table, and the table holds no live record for a key that is not resident.
    /// </summary>
    private static void AssertTableMatchesStore(KeyValueContext context)
    {
        foreach (KeyValuePair<string, KeyValueEntry> kv in context.Store.GetItems())
        {
            KeyValueEntry entry = kv.Value;

            bool quiet = entry.State == KeyValueState.Set
                && entry.ReplicationIntent is null
                && entry.WriteIntent is null
                && entry.PendingCommittedHead is null
                && !context.PublishedHeads.HasNewerPendingApply(kv.Key, entry.Revision, entry.LastModified);

            bool published = context.PublishedHeads.TryGet(kv.Key, out PublishedHead head) && !head.IsRetracted;

            Assert.True(quiet == published, $"{kv.Key}: quiet={quiet} published={published}");
            if (!published)
            {
                Assert.Null(entry.Published);
                continue;
            }

            Assert.Same(head, entry.Published);
            Assert.Equal(entry.Revision, head.Revision);
            Assert.Equal(entry.LastModified, head.LastModified);
            Assert.Equal(entry.Expires, head.Expires);
            Assert.Equal(entry.State, head.State);
            Assert.Equal(entry.Value, head.Value);
            Assert.Equal(entry.MvccEntries is not null, head.MvccHeld);
        }
    }

    private static KeyValueRequest PinRequest(string key, HLCTimestamp tx, PublishedHead head) =>
        new KeyValueRequest(KeyValueRequestType.RecordReadPin, tx, HLCTimestamp.Zero, key,
            null, null, -1, KeyValueFlags.None, 0, HLCTimestamp.Zero, KeyValueDurability.Persistent, 0, 0, default)
        { ReadPin = head };

    private static int OwnerOf(KeyValueContext context, RaftManager raft, string key) =>
        PartitionDataEnumerator.OwnerOfKey(context.RangeMapStore.Current, key, raft.Configuration.InitialPartitions);

    private static HLCTimestamp Now(RaftManager raft) => raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

    private static KeyValueEntry Insert(KeyValueContext context, string key, string value, long revision, HLCTimestamp lastModified, HLCTimestamp? lastUsed = null)
    {
        KeyValueEntry entry = new()
        {
            Bucket = KeyValueKeySpace.OfKey(key),
            Value = Encoding.UTF8.GetBytes(value),
            Revision = revision,
            FlushedRevision = revision,
            LastModified = lastModified,
            LastUsed = lastUsed ?? lastModified,
            State = KeyValueState.Set
        };
        context.InsertStoreEntry(key, entry);
        return entry;
    }

    private static KahunaConfiguration CreateConfiguration() => ConfigurationValidator.Validate(new()
    {
        LocksWorkers = 1,
        KeyValueWorkers = 1,
        BackgroundWriterWorkers = 1,
        Storage = "memory",
        CacheEntryTtl = TimeSpan.FromMinutes(5),
        CacheEntriesToRemove = 1000,
        MaxEntriesPerActor = 50_000,
        MaxBytesPerActor = 256L * 1024 * 1024,
        CollectBatchMax = 1000,
        RevisionRetention = 16
    });

    private (KeyValueContext, RaftManager) CreateContext(KahunaConfiguration? config = null)
    {
        config ??= CreateConfiguration();
        ILogger<IKahuna> logger = NullLogger<IKahuna>.Instance;
        ILogger<IRaft> raftLogger = NullLogger<IRaft>.Instance;

        RaftManager raft = Track(new RaftManager(
            new RaftConfiguration
            {
                NodeName = "published-head-table",
                NodeId = 1,
                Host = "localhost",
                Port = 0,
                InitialPartitions = 1,
                EnableQuiescence = false,
                PartitionExecutorPoolSize = 1
            },
            new StaticDiscovery([]),
            new InMemoryWAL(raftLogger),
            new InMemoryCommunication(),
            new HybridLogicalClock(),
            raftLogger));

        KeyValueContext context = new(
            null!,
            new BTree<string, KeyValueEntry>(32),
            new Dictionary<string, KeyValueWriteIntent>(),
            new Dictionary<string, List<KeyValueRangeLock>>(),
            new Dictionary<int, KeyValueProposal>(),
            null!,
            null!,
            new MemoryPersistenceBackend(),
            raft,
            raft.ReadScheduler,
            new KeySpaceRegistry(),
            new RangeMapStore(raft, null, null, logger),
            config,
            logger);

        return (context, raft);
    }
}
