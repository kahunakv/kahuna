
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
/// The published head records an actor keeps beside its store for off-actor point reads: every change to a
/// resident head, to its intents, or to its MVCC table retracts the live record before the change lands, and
/// the end-of-turn flush republishes a <c>Set</c> head that no direct write and no parked head covers, with a
/// copy of its write intent's owner and of its MVCC table. The checks drive the context and the handlers
/// directly and compare the table with the store after each step.
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
    public void WriteIntent_RetractsAndRepublishesTheHeadWithItsOwner()
    {
        (KeyValueContext context, RaftManager raft) = CreateContext();
        HLCTimestamp now = Now(raft);
        KeyValueEntry entry = Insert(context, "k/3", "v1", 1, now);
        context.FlushHeadPublications();
        PublishedHead live = entry.Published!;
        Assert.False(live.WriteIntentHeld);

        entry.WriteIntent = new KeyValueWriteIntent { TransactionId = now, Expires = now + 60_000 };
        Assert.True(live.IsRetracted);
        context.FlushHeadPublications();

        // The committed head below the intent stays published, and the record names the owner.
        PublishedHead held = entry.Published!;
        Assert.False(held.IsRetracted);
        Assert.True(held.WriteIntentHeld);
        Assert.Equal(now, held.WriteIntentOwner);
        Assert.Equal(1L, held.Revision);
        AssertTableMatchesStore(context);

        // Another owner is another intent object: the record follows it.
        HLCTimestamp other = Now(raft);
        entry.WriteIntent = new KeyValueWriteIntent { TransactionId = other, Expires = other + 60_000 };
        Assert.True(held.IsRetracted);
        context.FlushHeadPublications();
        Assert.Equal(other, entry.Published!.WriteIntentOwner);
        AssertTableMatchesStore(context);

        entry.WriteIntent = null;
        context.FlushHeadPublications();
        Assert.NotNull(entry.Published);
        Assert.False(entry.Published!.IsRetracted);
        Assert.False(entry.Published.WriteIntentHeld);
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
        Assert.Equal(PublishedReadFallback.Retracted, live.RetractCause);
        context.FlushHeadPublications();
        Assert.Null(entry.Published);

        // The retracted record stays in the table and says why no new one replaced it.
        Assert.True(context.PublishedHeads.TryGet("k/4", out PublishedHead kept));
        Assert.Same(live, kept);
        Assert.Equal(PublishedReadFallback.ReplicationIntent, kept.RetractCause);

        entry.ReplicationIntent = null;
        context.FlushHeadPublications();
        PublishedHead relive = entry.Published!;
        Assert.False(relive.IsRetracted);

        entry.PendingCommittedHead = new InvalidateOrApplyData(2, Encoding.UTF8.GetBytes("v2"), HLCTimestamp.Zero, now, now, KeyValueState.Set);
        Assert.True(relive.IsRetracted);
        context.FlushHeadPublications();
        Assert.Null(entry.Published);
        Assert.Equal(PublishedReadFallback.ParkedHead, relive.RetractCause);

        entry.PendingCommittedHead = null;
        context.FlushHeadPublications();
        Assert.NotNull(entry.Published);
        AssertTableMatchesStore(context);
    }

    [Fact]
    public void MvccTable_IsCopiedIntoTheRecord_AndEveryChangeOfItRetracts()
    {
        (KeyValueContext context, RaftManager raft) = CreateContext();
        HLCTimestamp now = Now(raft);
        KeyValueEntry entry = Insert(context, "k/5", "v1", 1, now);
        context.FlushHeadPublications();
        PublishedHead free = entry.Published!;
        Assert.False(free.MvccHeld);

        HLCTimestamp reader = Now(raft);
        HLCTimestamp writer = Now(raft);
        HLCTimestamp late = Now(raft);
        Assert.Equal(PublishedMvccState.None, free.MvccStateOf(reader));

        // The table itself.
        entry.MvccEntries ??= new();
        Assert.True(free.IsRetracted);
        PublishedHead current = Republished(context, entry);
        Assert.True(current.MvccHeld);
        Assert.Equal(PublishedMvccState.None, current.MvccStateOf(reader));

        // A read pin at the head.
        KeyValueMvccEntry pin = new() { Revision = 1, LastModified = now, State = KeyValueState.Set };
        entry.MvccEntries.Add(reader, pin);
        Assert.True(current.IsRetracted);
        current = Republished(context, entry);
        Assert.Equal(PublishedMvccState.PinAtHead, current.MvccStateOf(reader));
        Assert.Equal(PublishedMvccState.None, current.MvccStateOf(writer));

        // A second entry, through the indexer: a pin at another revision than the head.
        entry.MvccEntries[late] = new KeyValueMvccEntry { Revision = 0, LastModified = now, State = KeyValueState.Set };
        Assert.True(current.IsRetracted);
        current = Republished(context, entry);
        Assert.Equal(PublishedMvccState.PinMoved, current.MvccStateOf(late));
        Assert.Equal(PublishedMvccState.PinAtHead, current.MvccStateOf(reader));

        // A pin at the head's revision with another stamp is not at the head: an extend moved the head.
        entry.MvccEntries[late] = new KeyValueMvccEntry { Revision = 1, LastModified = late, State = KeyValueState.Set };
        current = Republished(context, entry);
        Assert.Equal(PublishedMvccState.PinMoved, current.MvccStateOf(late));

        // A staged write: the pin's revision and stamp change in place, then it is marked staged. Each of the
        // three retracts on its own.
        KeyValueMvccEntry staged = new() { Revision = 1, LastModified = now, State = KeyValueState.Set };
        entry.MvccEntries.Add(writer, staged);
        current = Republished(context, entry);
        Assert.Equal(PublishedMvccState.PinAtHead, current.MvccStateOf(writer));

        staged.Revision = 2;
        Assert.True(current.IsRetracted);
        current = Republished(context, entry);
        Assert.Equal(PublishedMvccState.PinMoved, current.MvccStateOf(writer));

        staged.LastModified = Now(raft);
        Assert.True(current.IsRetracted);
        current = Republished(context, entry);

        staged.StagedUnder = new KeyValueWriteIntent { TransactionId = writer, Expires = writer + 60_000 };
        Assert.True(current.IsRetracted);
        current = Republished(context, entry);
        Assert.Equal(PublishedMvccState.Staged, current.MvccStateOf(writer));
        AssertTableMatchesStore(context);

        // A removal.
        Assert.True(entry.MvccEntries.Remove(late, out KeyValueMvccEntry? removed));
        Assert.True(current.IsRetracted);
        current = Republished(context, entry);
        Assert.Equal(PublishedMvccState.None, current.MvccStateOf(late));

        // An entry that left the table no longer reaches the record.
        removed!.Revision = 7;
        Assert.False(current.IsRetracted);

        entry.MvccEntries = null;
        Assert.True(current.IsRetracted);
        current = Republished(context, entry);
        Assert.False(current.MvccHeld);
        Assert.Equal(PublishedMvccState.None, current.MvccStateOf(reader));
        AssertTableMatchesStore(context);
    }

    [Fact]
    public void MoreMvccEntriesThanARecordCarries_PublishesNoneOfThem()
    {
        (KeyValueContext context, RaftManager raft) = CreateContext();
        HLCTimestamp now = Now(raft);
        KeyValueEntry entry = Insert(context, "k/5b", "v1", 1, now);
        entry.MvccEntries = new();

        HLCTimestamp first = Now(raft);
        entry.MvccEntries.Add(first, new KeyValueMvccEntry { Revision = 1, LastModified = now, State = KeyValueState.Set });
        for (int i = 1; i < PublishedHead.MaxPins; i++)
            entry.MvccEntries.Add(Now(raft), new KeyValueMvccEntry { Revision = 1, LastModified = now, State = KeyValueState.Set });

        PublishedHead full = Republished(context, entry);
        Assert.Equal(PublishedHead.MaxPins, full.Pins!.Length);
        Assert.Equal(PublishedMvccState.PinAtHead, full.MvccStateOf(first));

        // One more: the record says only that the key holds MVCC entries, also for a transaction with none.
        HLCTimestamp extra = Now(raft);
        entry.MvccEntries.Add(extra, new KeyValueMvccEntry { Revision = 1, LastModified = now, State = KeyValueState.Set });
        PublishedHead over = Republished(context, entry);
        Assert.True(over.MvccHeld);
        Assert.Null(over.Pins);
        Assert.Equal(PublishedMvccState.Unknown, over.MvccStateOf(first));
        Assert.Equal(PublishedMvccState.Unknown, over.MvccStateOf(Now(raft)));
        AssertTableMatchesStore(context);

        Assert.True(entry.MvccEntries.Remove(extra, out _));
        Assert.Equal(PublishedMvccState.PinAtHead, Republished(context, entry).MvccStateOf(first));
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
        Assert.True(entry.Published!.WriteIntentHeld);
        Assert.True(entry.Published.MvccHeld);

        drop.Execute(new KeyValueRequest(KeyValueRequestType.DropLeaderState, HLCTimestamp.Zero, HLCTimestamp.Zero, "d/1",
            null, null, -1, KeyValueFlags.None, 0, HLCTimestamp.Zero, KeyValueDurability.Persistent, 0, OwnerOf(context, raft, "d/1"), default));
        context.FlushHeadPublications();

        Assert.Null(entry.WriteIntent);
        Assert.Null(entry.MvccEntries);
        Assert.NotNull(entry.Published);
        Assert.False(entry.Published!.MvccHeld);
        Assert.False(entry.Published.WriteIntentHeld);
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

        // The record of the moved head shows the pin behind it.
        context.FlushHeadPublications();
        Assert.Equal(PublishedMvccState.PinMoved, entry.Published!.MvccStateOf(tx));

        // A later pin for the same transaction does not replace the earlier, stricter one.
        pins.Execute(PinRequest("p/1", tx, entry.Published!));
        Assert.Equal(1L, entry.MvccEntries[tx].Revision);
        Assert.Single(entry.MvccEntries);
    }

    [Fact]
    public void ReadPin_WhenItLands_EndsItsRegistration_OnEveryPath()
    {
        (KeyValueContext context, RaftManager raft) = CreateContext();
        RecordReadPinHandler pins = new(context);
        HLCTimestamp now = Now(raft);
        HLCTimestamp tx = Now(raft);

        KeyValueEntry entry = Insert(context, "p/3", "v1", 1, now);
        context.FlushHeadPublications();
        PublishedHead observed = entry.Published!;

        // A second registration of the same transaction and key finds the first, with its record.
        Assert.True(context.PublishedHeads.TryRegisterPin(tx, "p/3", observed, out _));
        Assert.False(context.PublishedHeads.TryRegisterPin(tx, "p/3", new PublishedHead(null, 9, HLCTimestamp.Zero, now, KeyValueState.Set, 0), out PublishedHead first));
        Assert.Same(observed, first);
        Assert.Equal(1, context.PublishedHeads.PinsInFlightCount);

        // The pin is added and the record of the key retracted before the registration ends, so the
        // transaction shows in one of the two at any time.
        pins.Execute(PinRequest("p/3", tx, observed));
        Assert.Equal(0, context.PublishedHeads.PinsInFlightCount);
        Assert.True(observed.IsRetracted);
        context.FlushHeadPublications();
        Assert.Equal(PublishedMvccState.PinAtHead, entry.Published!.MvccStateOf(tx));

        // A pin that finds the transaction already pinned.
        PublishedHead again = entry.Published!;
        Assert.True(context.PublishedHeads.TryRegisterPin(tx, "p/3", again, out _));
        pins.Execute(PinRequest("p/3", tx, again));
        Assert.Equal(0, context.PublishedHeads.PinsInFlightCount);

        // A pin that finds the entry evicted.
        HLCTimestamp other = Now(raft);
        Assert.True(context.PublishedHeads.TryRegisterPin(other, "p/3", again, out _));
        context.RemoveStoreEntry("p/3");
        pins.Execute(PinRequest("p/3", other, again));
        Assert.Equal(0, context.PublishedHeads.PinsInFlightCount);

        // A registration ends only with the pin it was made for.
        Assert.True(context.PublishedHeads.TryRegisterPin(tx, "p/4", observed, out _));
        context.PublishedHeads.ForgetPin(tx, "p/4", again);
        Assert.Equal(1, context.PublishedHeads.PinsInFlightCount);
        context.PublishedHeads.ForgetPin(tx, "p/4", observed);
        Assert.Equal(0, context.PublishedHeads.PinsInFlightCount);
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
    /// Every resident entry has a live record exactly when its head is a <c>Set</c> head with no direct write in
    /// flight, no parked head and no newer head pending. That record is the one in the table, and it states the
    /// entry's head, its write intent and its MVCC table as they are now.
    /// </summary>
    private static void AssertTableMatchesStore(KeyValueContext context)
    {
        foreach (KeyValuePair<string, KeyValueEntry> kv in context.Store.GetItems())
        {
            KeyValueEntry entry = kv.Value;

            bool publishable = entry.State == KeyValueState.Set
                && entry.ReplicationIntent is null
                && entry.PendingCommittedHead is null
                && !context.PublishedHeads.HasNewerPendingApply(kv.Key, entry.Revision, entry.LastModified);

            bool published = context.PublishedHeads.TryGet(kv.Key, out PublishedHead head) && !head.IsRetracted;

            Assert.True(publishable == published, $"{kv.Key}: publishable={publishable} published={published}");
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
            Assert.Equal(entry.WriteIntent is not null, head.WriteIntentHeld);
            Assert.Equal(entry.WriteIntent?.TransactionId ?? HLCTimestamp.Zero, head.WriteIntentOwner);
            Assert.Equal(entry.MvccEntries is not null, head.MvccHeld);

            if (entry.MvccEntries is not { } mvcc)
                continue;

            if (mvcc.Count > PublishedHead.MaxPins)
            {
                Assert.Null(head.Pins);
                continue;
            }

            Assert.Equal(mvcc.Count, head.Pins!.Length);

            foreach ((HLCTimestamp transactionId, KeyValueMvccEntry own) in mvcc)
            {
                PublishedMvccState expected =
                    own.StagedUnder is not null ? PublishedMvccState.Staged
                    : own.Revision == entry.Revision && own.LastModified == entry.LastModified ? PublishedMvccState.PinAtHead
                    : PublishedMvccState.PinMoved;

                Assert.Equal(expected, head.MvccStateOf(transactionId));
            }
        }
    }

    /// <summary>Runs the end-of-turn flush and returns the live record of <paramref name="entry"/>.</summary>
    private static PublishedHead Republished(KeyValueContext context, KeyValueEntry entry)
    {
        context.FlushHeadPublications();
        Assert.NotNull(entry.Published);
        Assert.False(entry.Published!.IsRetracted);
        return entry.Published;
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
            new KeyValueStore(),
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
