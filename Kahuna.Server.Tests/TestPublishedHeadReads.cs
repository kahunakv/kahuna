
using System.Text;
using Kahuna;
using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Communication.Memory;
using Kommander.Discovery;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Nixie;

namespace Kahuna.Server.Tests;

/// <summary>
/// The off-actor point read: a resident key is answered from the owning actor's published head record with no
/// actor message, and every state the actor path treats specially sends the read back to the actor: a direct
/// write in flight, a prefix lock, an expired or absent head, a newer head still on its way to the actor, a
/// durable intent the head does not include, a transaction that staged a write or whose pin is not at the head,
/// a read that must wait for a write intent. Driven against a real spawned actor through the reader, and end to
/// end through an embedded node.
/// </summary>
public sealed class TestPublishedHeadReads : RaftTrackingTest
{
    private readonly ILoggerFactory loggerFactory;

    public TestPublishedHeadReads(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper);
    }

    // ── against a real actor ─────────────────────────────────────────────────────────────

    [Fact]
    public async Task QuietResidentKey_IsServedWithoutAnActorMessage_AndEqualsTheActorAnswer()
    {
        await using ActorRig rig = await ActorRig.CreateAsync(this);
        const string key = "pub/quiet";

        KeyValueResponse? set = await rig.AskAsync(Set(key, "v1"));
        Assert.Equal(KeyValueResponseType.Set, set!.Type);

        KeyValueEntry resident = rig.Context.Store.Get(key)!;
        HLCTimestamp lastUsedBefore = resident.LastUsed;

        Assert.True(rig.Reader.TryRead(HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Ephemeral, KeyValueReadMode.Default, out ReadOnlyKeyValueEntry? published));
        Assert.Equal("v1", Encoding.UTF8.GetString(published!.Value!));

        // The actor was not involved: its own entry was not touched.
        Assert.Equal(lastUsedBefore, resident.LastUsed);

        KeyValueResponse? viaActor = await rig.AskAsync(Get(key));
        Assert.Equal(KeyValueResponseType.Get, viaActor!.Type);
        AssertSameHead(viaActor.Entry!, published);
    }

    [Fact]
    public async Task ReadKindsTheRecordCannotAnswer_GoToTheActor()
    {
        await using ActorRig rig = await ActorRig.CreateAsync(this);
        const string key = "pub/kinds";
        HLCTimestamp before = rig.Now();
        await rig.AskAsync(Set(key, "v1"));
        HLCTimestamp now = rig.Now();

        Assert.False(rig.Reader.TryRead(HLCTimestamp.Zero, key, 0, HLCTimestamp.Zero, KeyValueDurability.Ephemeral, KeyValueReadMode.Default, out _), "by revision");
        Assert.False(rig.Reader.TryRead(HLCTimestamp.Zero, key, -1, before, KeyValueDurability.Ephemeral, KeyValueReadMode.Default, out _), "snapshot below the head");
        Assert.False(rig.Reader.TryRead(rig.Now(), key, -1, now, KeyValueDurability.Ephemeral, KeyValueReadMode.Default, out _), "transactional snapshot in the default mode");
        Assert.False(rig.Reader.TryRead(HLCTimestamp.Zero, "pub/absent", -1, HLCTimestamp.Zero, KeyValueDurability.Ephemeral, KeyValueReadMode.Default, out _), "absent");
        Assert.False(rig.Reader.TryRead(HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, KeyValueReadMode.Default, out _), "other ring");
    }

    [Fact]
    public async Task SnapshotReadAtOrAboveTheHead_IsServedFromTheRecord_AndEqualsTheActorAnswer()
    {
        await using ActorRig rig = await ActorRig.CreateAsync(this);
        const string key = "pub/snapshot";
        await rig.AskAsync(Set(key, "v1"));
        HLCTimestamp afterFirst = rig.Now();

        Assert.True(rig.Reader.TryRead(HLCTimestamp.Zero, key, -1, afterFirst, KeyValueDurability.Ephemeral, KeyValueReadMode.Default, out ReadOnlyKeyValueEntry? published));
        Assert.Equal("v1", Encoding.UTF8.GetString(published!.Value!));

        KeyValueResponse? viaActor = await rig.AskAsync(SnapshotGet(key, afterFirst));
        Assert.Equal(KeyValueResponseType.Get, viaActor!.Type);
        AssertSameHead(viaActor.Entry!, published);

        // A snapshot exactly at the head's commit time reads the head.
        Assert.True(rig.Reader.TryRead(HLCTimestamp.Zero, key, -1, published.LastModified, KeyValueDurability.Ephemeral, KeyValueReadMode.Default, out ReadOnlyKeyValueEntry? atHead));
        AssertSameHead(published, atHead!);

        // Once the head moves past the snapshot the record no longer holds the answer: the archive does.
        await rig.AskAsync(Set(key, "v2"));
        Assert.False(rig.Reader.TryRead(HLCTimestamp.Zero, key, -1, afterFirst, KeyValueDurability.Ephemeral, KeyValueReadMode.Default, out _), "snapshot below the new head");

        KeyValueResponse? archived = await rig.AskAsync(SnapshotGet(key, afterFirst));
        Assert.Equal(KeyValueResponseType.Get, archived!.Type);
        Assert.Equal("v1", Encoding.UTF8.GetString(archived.Entry!.Value!));

        HLCTimestamp afterSecond = rig.Now();
        Assert.True(rig.Reader.TryRead(HLCTimestamp.Zero, key, -1, afterSecond, KeyValueDurability.Ephemeral, KeyValueReadMode.Default, out ReadOnlyKeyValueEntry? second));
        Assert.Equal("v2", Encoding.UTF8.GetString(second!.Value!));
    }

    [Fact]
    public async Task SnapshotRead_UnderAWriteIntent_GoesToTheActor()
    {
        await using ActorRig rig = await ActorRig.CreateAsync(this);
        const string key = "pub/snapshot-intent";
        await rig.AskAsync(Set(key, "v1"));
        HLCTimestamp writer = rig.Now();

        KeyValueResponse? locked = await rig.AskAsync(Request(KeyValueRequestType.TryAcquireExclusiveLock, key, writer, expiresMs: 60_000));
        Assert.Equal(KeyValueResponseType.Locked, locked!.Type);

        // The actor decides what a snapshot above a staged write does (wait, or push the commit above it).
        Assert.False(rig.Reader.TryRead(HLCTimestamp.Zero, key, -1, rig.Now(), KeyValueDurability.Ephemeral, KeyValueReadMode.Default, default, out _, out PublishedReadFallback fallback));
        Assert.Equal(PublishedReadFallback.WriteIntent, fallback);

        // A latest read of the same key is not held back by the intent.
        Assert.True(rig.Reader.TryRead(HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Ephemeral, KeyValueReadMode.Default, out _));

        await rig.AskAsync(Request(KeyValueRequestType.TryReleaseExclusiveLock, key, writer));
        Assert.True(rig.Reader.TryRead(HLCTimestamp.Zero, key, -1, rig.Now(), KeyValueDurability.Ephemeral, KeyValueReadMode.Default, out _));
    }

    [Fact]
    public async Task SnapshotRead_AtOrAheadOfTheNodeClock_GoesToTheActor_AndLeavesTheClockBehindIt()
    {
        await using ActorRig rig = await ActorRig.CreateAsync(this);
        const string key = "pub/snapshot-clock";
        await rig.AskAsync(Set(key, "v1"));

        // A timestamp minted on a node whose clock runs ahead of this one. Serving it here would let a commit
        // staged afterwards be stamped below a snapshot that has already been answered, so the record declines
        // and the actor, which folds the timestamp into the clock or refuses it, decides.
        HLCTimestamp ahead = rig.Now() + 60_000;
        Assert.False(rig.Reader.TryRead(HLCTimestamp.Zero, key, -1, ahead, KeyValueDurability.Ephemeral, KeyValueReadMode.Default, out _), "snapshot ahead of the clock");

        // Declining did not move the clock up to the timestamp: that is the actor's call, not the record's.
        Assert.True(rig.Now().CompareTo(ahead) < 0);

        // A timestamp the clock has passed is served.
        HLCTimestamp passed = rig.Now();
        Assert.True(rig.Reader.TryRead(HLCTimestamp.Zero, key, -1, passed, KeyValueDurability.Ephemeral, KeyValueReadMode.Default, out ReadOnlyKeyValueEntry? served));
        Assert.Equal("v1", Encoding.UTF8.GetString(served!.Value!));
    }

    [Fact]
    public async Task PersistentSnapshotRead_BelowInstalledHistory_GoesToTheActor()
    {
        InstalledHistoryFence installed = new(null, null, NullLogger<IKahuna>.Instance);
        await using ActorRig rig = await ActorRig.CreateAsync(this, persistent: true, installedHistory: installed);
        const string key = "pub/snapshot-installed";
        HLCTimestamp written = rig.Now();
        rig.InsertResident(key, "v1", 1, written);

        // No install on this node: a snapshot above the head is served.
        HLCTimestamp beforeInstall = rig.Now();
        Assert.True(rig.Reader.TryRead(HLCTimestamp.Zero, key, -1, beforeInstall, KeyValueDurability.Persistent, KeyValueReadMode.Default, out _));

        // An install leaves a partition complete only from its own timestamp on. A snapshot below that bound may
        // need history this node does not hold, and only the actor knows the key's partition, so it decides.
        HLCTimestamp completeFrom = rig.Now();
        installed.Record(1, completeFrom, []);
        Assert.False(rig.Reader.TryRead(HLCTimestamp.Zero, key, -1, beforeInstall, KeyValueDurability.Persistent, KeyValueReadMode.Default, out _), "snapshot below the installed bound");

        // At or above the bound the node holds every revision, and the record answers again.
        HLCTimestamp afterInstall = rig.Now();
        Assert.True(rig.Reader.TryRead(HLCTimestamp.Zero, key, -1, afterInstall, KeyValueDurability.Persistent, KeyValueReadMode.Default, out ReadOnlyKeyValueEntry? served));
        Assert.Equal(1L, served!.Revision);

        // A latest read asks for no history and is not held back by the bound.
        Assert.True(rig.Reader.TryRead(HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, KeyValueReadMode.Default, out _));
    }

    [Fact]
    public async Task StatementRead_IsServedFromTheRecord_LeavesNoPin_AndIsNotBoundByAPin()
    {
        await using ActorRig rig = await ActorRig.CreateAsync(this);
        const string key = "pub/statement";
        await rig.AskAsync(Set(key, "v1"));
        HLCTimestamp tx = rig.Now();
        HLCTimestamp statement = rig.Now();

        Assert.True(rig.Reader.TryRead(tx, key, -1, statement, KeyValueDurability.Ephemeral, KeyValueReadMode.StatementSnapshot, out ReadOnlyKeyValueEntry? published));
        Assert.Equal("v1", Encoding.UTF8.GetString(published!.Value!));

        KeyValueRequest viaActorRequest = SnapshotGet(key, statement, tx);
        viaActorRequest.ReadMode = KeyValueReadMode.StatementSnapshot;
        KeyValueResponse? viaActor = await rig.AskAsync(viaActorRequest);
        Assert.Equal(KeyValueResponseType.Get, viaActor!.Type);
        AssertSameHead(viaActor.Entry!, published);

        // Neither path left a pin: the entry has no MVCC table and the record still says so.
        KeyValueEntry resident = rig.Context.Store.Get(key)!;
        Assert.Null(resident.MvccEntries);
        Assert.True(rig.Context.PublishedHeads.TryGet(key, out PublishedHead live) && !live.IsRetracted && !live.MvccHeld);
        Assert.Equal(0, rig.Context.PublishedHeads.PinsInFlightCount);

        // A read pin does not bind a statement read, on the actor or here: not the pin of another transaction, and
        // not a pin an earlier latest read of the same transaction left.
        HLCTimestamp pinner = rig.Now();
        Assert.True(rig.Reader.TryRead(pinner, key, -1, HLCTimestamp.Zero, KeyValueDurability.Ephemeral, KeyValueReadMode.Default, out _));
        await WaitUntil(() => rig.MvccStateOf(key, pinner) == PublishedMvccState.PinAtHead);

        Assert.True(rig.Reader.TryRead(tx, key, -1, rig.Now(), KeyValueDurability.Ephemeral, KeyValueReadMode.StatementSnapshot, out _), "pin of another transaction");
        Assert.True(rig.Reader.TryRead(pinner, key, -1, rig.Now(), KeyValueDurability.Ephemeral, KeyValueReadMode.StatementSnapshot, out _), "own pin");
        Assert.True(rig.Reader.TryRead(HLCTimestamp.Zero, key, -1, rig.Now(), KeyValueDurability.Ephemeral, KeyValueReadMode.Default, out _), "plain snapshot read");
        Assert.Single(resident.MvccEntries!);
    }

    [Fact]
    public async Task PrefixLock_RoutesTheReadToTheActor_UntilReleased()
    {
        await using ActorRig rig = await ActorRig.CreateAsync(this);
        const string key = "lk/key";
        await rig.AskAsync(Set(key, "v1"));
        HLCTimestamp tx = rig.Now();

        Assert.True(rig.Reader.TryRead(HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Ephemeral, KeyValueReadMode.Default, out _));

        KeyValueResponse? prefixLocked = await rig.AskAsync(Request(KeyValueRequestType.TryAcquireExclusivePrefixLock, "lk", tx, expiresMs: 60_000));
        Assert.Equal(KeyValueResponseType.Locked, prefixLocked!.Type);
        Assert.True(rig.Context.PublishedHeads.PrefixLocksHeld);
        Assert.False(rig.Reader.TryRead(HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Ephemeral, KeyValueReadMode.Default, default, out _, out PublishedReadFallback fallback), "prefix lock");
        Assert.Equal(PublishedReadFallback.PrefixLock, fallback);

        await rig.AskAsync(Request(KeyValueRequestType.TryReleaseExclusivePrefixLock, "lk", tx));
        Assert.False(rig.Context.PublishedHeads.PrefixLocksHeld);
        Assert.True(rig.Reader.TryRead(HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Ephemeral, KeyValueReadMode.Default, out _), "prefix lock released");
    }

    /// <summary>
    /// A write intent of another transaction does not hold back a latest read on the actor, so it does not hold
    /// back the record either: a plain read and a transaction's first read are answered with the committed head
    /// below the intent, with the bytes the actor answers.
    /// </summary>
    [Fact]
    public async Task LatestRead_UnderAForeignWriteIntent_IsServedFromTheRecord_AndEqualsTheActorAnswer()
    {
        await using ActorRig rig = await ActorRig.CreateAsync(this);
        const string key = "wi/key";
        await rig.AskAsync(Set(key, "v1"));
        HLCTimestamp writer = rig.Now();
        HLCTimestamp reader = rig.Now();

        KeyValueResponse? staged = await rig.AskAsync(TxSet(key, "staged", writer));
        Assert.Equal(KeyValueResponseType.Set, staged!.Type);

        Assert.True(rig.Context.PublishedHeads.TryGet(key, out PublishedHead flagged) && !flagged.IsRetracted);
        Assert.True(flagged.WriteIntentHeld);
        Assert.Equal(writer, flagged.WriteIntentOwner);
        Assert.Equal(PublishedMvccState.Staged, flagged.MvccStateOf(writer));

        // A plain read: the committed head, not the staged write.
        Assert.True(rig.Reader.TryRead(HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Ephemeral, KeyValueReadMode.Default, out ReadOnlyKeyValueEntry? plain));
        Assert.Equal("v1", Encoding.UTF8.GetString(plain!.Value!));
        KeyValueResponse? plainViaActor = await rig.AskAsync(Get(key));
        AssertSameHead(plainViaActor!.Entry!, plain);

        // The first read of another transaction: the same head, and a pin at it.
        Assert.True(rig.Reader.TryRead(reader, key, -1, HLCTimestamp.Zero, KeyValueDurability.Ephemeral, KeyValueReadMode.Default, out ReadOnlyKeyValueEntry? first));
        AssertSameHead(plain, first!);
        await WaitUntil(() => rig.MvccStateOf(key, reader) == PublishedMvccState.PinAtHead);

        KeyValueResponse? firstViaActor = await rig.AskAsync(Get(key, reader));
        Assert.Equal(KeyValueResponseType.Get, firstViaActor!.Type);
        AssertSameHead(firstViaActor.Entry!, first!);

        // The writer reads its own staged write, and only the actor has it.
        Assert.False(rig.Reader.TryRead(writer, key, -1, HLCTimestamp.Zero, KeyValueDurability.Ephemeral, KeyValueReadMode.Default, default, out _, out PublishedReadFallback fallback));
        Assert.Equal(PublishedReadFallback.MvccStaged, fallback);
        KeyValueResponse? own = await rig.AskAsync(Get(key, writer));
        Assert.Equal("staged", Encoding.UTF8.GetString(own!.Entry!.Value!));

        // The release of the intent publishes the head again without the flag.
        await rig.AskAsync(Request(KeyValueRequestType.TryReleaseExclusiveLock, key, writer));
        Assert.True(rig.Context.PublishedHeads.TryGet(key, out PublishedHead released) && !released.IsRetracted && !released.WriteIntentHeld);
    }

    /// <summary>
    /// The first read of a key by a transaction that asked to wait parks on the actor behind a live write intent
    /// of another transaction, so the record leaves that read to the actor. The owner of the intent, a
    /// transaction that already holds a pin on the key, a read that did not ask to wait, and a server with the
    /// wait turned off are not parked, and the record answers them.
    /// </summary>
    [Fact]
    public async Task ReadThatAsksToWait_UnderAForeignWriteIntent_GoesToTheActor()
    {
        await using ActorRig rig = await ActorRig.CreateAsync(this);
        const string key = "wait/key";
        await rig.AskAsync(Set(key, "v1"));
        KeyValueIntentWait wait = new(100, false);

        // A transaction pins the key while it is free of intents.
        HLCTimestamp pinned = rig.Now();
        Assert.True(rig.Reader.TryRead(pinned, key, -1, HLCTimestamp.Zero, KeyValueDurability.Ephemeral, KeyValueReadMode.Default, wait, out _, out _));
        await WaitUntil(() => rig.MvccStateOf(key, pinned) == PublishedMvccState.PinAtHead);

        HLCTimestamp writer = rig.Now();
        KeyValueResponse? locked = await rig.AskAsync(Request(KeyValueRequestType.TryAcquireExclusiveLock, key, writer, expiresMs: 60_000));
        Assert.Equal(KeyValueResponseType.Locked, locked!.Type);

        HLCTimestamp waiter = rig.Now();
        Assert.False(rig.Reader.TryRead(waiter, key, -1, HLCTimestamp.Zero, KeyValueDurability.Ephemeral, KeyValueReadMode.Default, wait, out _, out PublishedReadFallback fallback));
        Assert.Equal(PublishedReadFallback.IntentWait, fallback);
        Assert.Equal(0, rig.Context.PublishedHeads.PinsInFlightCount);

        Assert.True(rig.Reader.TryRead(pinned, key, -1, HLCTimestamp.Zero, KeyValueDurability.Ephemeral, KeyValueReadMode.Default, wait, out _, out _), "a transaction with a pin does not park");
        Assert.True(rig.Reader.TryRead(writer, key, -1, HLCTimestamp.Zero, KeyValueDurability.Ephemeral, KeyValueReadMode.Default, wait, out _, out _), "the owner of the intent");
        Assert.True(rig.Reader.TryRead(rig.Now(), key, -1, HLCTimestamp.Zero, KeyValueDurability.Ephemeral, KeyValueReadMode.Default, default, out _, out _), "a read that did not ask to wait");
        Assert.True(rig.Reader.TryRead(HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Ephemeral, KeyValueReadMode.Default, wait, out _, out _), "a plain read");

        PublishedHeadReader noWaitServer = rig.NewReader(maxIntentWaitMs: 0);
        Assert.True(noWaitServer.TryRead(rig.Now(), key, -1, HLCTimestamp.Zero, KeyValueDurability.Ephemeral, KeyValueReadMode.Default, wait, out _, out _), "the server parks no read");
    }

    [Fact]
    public async Task ExpiredHeadAndTombstone_RouteTheReadToTheActor()
    {
        await using ActorRig rig = await ActorRig.CreateAsync(this);

        await rig.AskAsync(Set("exp/key", "v1", expiresMs: 1));
        await Task.Delay(30, TestContext.Current.CancellationToken);
        Assert.False(rig.Reader.TryRead(HLCTimestamp.Zero, "exp/key", -1, HLCTimestamp.Zero, KeyValueDurability.Ephemeral, KeyValueReadMode.Default, out _), "expired");
        KeyValueResponse? expired = await rig.AskAsync(Get("exp/key"));
        Assert.Equal(KeyValueResponseType.DoesNotExist, expired!.Type);

        await rig.AskAsync(Set("del/key", "v1"));
        Assert.True(rig.Reader.TryRead(HLCTimestamp.Zero, "del/key", -1, HLCTimestamp.Zero, KeyValueDurability.Ephemeral, KeyValueReadMode.Default, out _));
        KeyValueResponse? deleted = await rig.AskAsync(Request(KeyValueRequestType.TryDelete, "del/key", HLCTimestamp.Zero));
        Assert.Equal(KeyValueResponseType.Deleted, deleted!.Type);
        Assert.False(rig.Reader.TryRead(HLCTimestamp.Zero, "del/key", -1, HLCTimestamp.Zero, KeyValueDurability.Ephemeral, KeyValueReadMode.Default, out _), "tombstone");
        Assert.False(rig.Context.PublishedHeads.TryGet("del/key", out _));
    }

    /// <summary>
    /// A transaction's first read pins the head. Its next read of the key finds its own pin at the head of the
    /// record and is answered from the record, with the bytes the actor answers and no second pin. A head that
    /// moved since the pin goes to the actor, which answers <c>Aborted</c>.
    /// </summary>
    [Fact]
    public async Task TransactionalRead_PinsOnce_RepeatReadIsServedFromTheRecord_AndAHeadThatMovedGoesToTheActor()
    {
        await using ActorRig rig = await ActorRig.CreateAsync(this);
        const string key = "tx/key";
        await rig.AskAsync(Set(key, "v1"));
        HLCTimestamp tx = rig.Now();
        HLCTimestamp otherTx = rig.Now();

        Assert.True(rig.Reader.TryRead(tx, key, -1, HLCTimestamp.Zero, KeyValueDurability.Ephemeral, KeyValueReadMode.Default, out ReadOnlyKeyValueEntry? first));
        Assert.Equal("v1", Encoding.UTF8.GetString(first!.Value!));

        // The pin lands on the actor as a control message, and the next record shows it.
        await WaitUntil(() => rig.MvccStateOf(key, tx) == PublishedMvccState.PinAtHead);
        Assert.Equal(0, rig.Context.PublishedHeads.PinsInFlightCount);
        KeyValueEntry resident = rig.Context.Store.Get(key)!;
        Assert.True(resident.MvccEntries!.ContainsKey(tx));
        Assert.Equal(first.Revision, resident.MvccEntries[tx].Revision);

        // The repeat read is answered from the record, with no message to the actor: no pin is sent and the
        // record is not replaced.
        Assert.True(rig.Context.PublishedHeads.TryGet(key, out PublishedHead pinnedRecord));
        Assert.True(rig.Reader.TryRead(tx, key, -1, HLCTimestamp.Zero, KeyValueDurability.Ephemeral, KeyValueReadMode.Default, out ReadOnlyKeyValueEntry? repeat), "own repeat read");
        AssertSameHead(first, repeat!);
        Assert.Equal(0, rig.Context.PublishedHeads.PinsInFlightCount);
        Assert.True(rig.Context.PublishedHeads.TryGet(key, out PublishedHead stillPinnedRecord));
        Assert.Same(pinnedRecord, stillPinnedRecord);

        KeyValueResponse? repeatViaActor = await rig.AskAsync(Get(key, tx));
        Assert.Equal(KeyValueResponseType.Get, repeatViaActor!.Type);
        AssertSameHead(repeatViaActor.Entry!, repeat!);
        Assert.Single(resident.MvccEntries);

        // The first read of another transaction is not held back by that pin, and leaves its own.
        Assert.True(rig.Reader.TryRead(otherTx, key, -1, HLCTimestamp.Zero, KeyValueDurability.Ephemeral, KeyValueReadMode.Default, out _), "other transaction");
        await WaitUntil(() => rig.MvccStateOf(key, otherTx) == PublishedMvccState.PinAtHead);
        Assert.Equal(2, resident.MvccEntries.Count);
        Assert.True(rig.Reader.TryRead(HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Ephemeral, KeyValueReadMode.Default, out _), "plain read");

        await rig.AskAsync(Set(key, "v2"));

        // The head moved past the pin: the record says so and the actor answers Aborted.
        Assert.Equal(PublishedMvccState.PinMoved, rig.MvccStateOf(key, tx));
        Assert.False(rig.Reader.TryRead(tx, key, -1, HLCTimestamp.Zero, KeyValueDurability.Ephemeral, KeyValueReadMode.Default, default, out _, out PublishedReadFallback fallback));
        Assert.Equal(PublishedReadFallback.MvccPinMoved, fallback);

        KeyValueResponse? afterWrite = await rig.AskAsync(Get(key, tx));
        Assert.Equal(KeyValueResponseType.Aborted, afterWrite!.Type);
    }

    /// <summary>
    /// A transaction that staged a write on a key reads its own write, which only the actor holds. The staging
    /// turn replaces the record, so the read that follows the write's answer cannot find the old one.
    /// </summary>
    [Fact]
    public async Task TransactionWithAStagedWrite_GoesToTheActor_AndReadsItsWrite()
    {
        await using ActorRig rig = await ActorRig.CreateAsync(this);
        const string key = "tx/staged";
        await rig.AskAsync(Set(key, "v1"));
        HLCTimestamp tx = rig.Now();

        // The transaction reads the key, then writes it: the pin becomes a staged write.
        Assert.True(rig.Reader.TryRead(tx, key, -1, HLCTimestamp.Zero, KeyValueDurability.Ephemeral, KeyValueReadMode.Default, out _));
        await WaitUntil(() => rig.MvccStateOf(key, tx) == PublishedMvccState.PinAtHead);

        KeyValueResponse? staged = await rig.AskAsync(TxSet(key, "mine", tx));
        Assert.Equal(KeyValueResponseType.Set, staged!.Type);
        Assert.Equal(PublishedMvccState.Staged, rig.MvccStateOf(key, tx));

        Assert.False(rig.Reader.TryRead(tx, key, -1, HLCTimestamp.Zero, KeyValueDurability.Ephemeral, KeyValueReadMode.Default, default, out _, out PublishedReadFallback fallback));
        Assert.Equal(PublishedReadFallback.MvccStaged, fallback);

        KeyValueResponse? own = await rig.AskAsync(Get(key, tx));
        Assert.Equal(KeyValueResponseType.Get, own!.Type);
        Assert.Equal("mine", Encoding.UTF8.GetString(own.Entry!.Value!));

        // A statement read of the transaction also reads the staged write, so it goes to the actor too.
        Assert.False(rig.Reader.TryRead(tx, key, -1, rig.Now(), KeyValueDurability.Ephemeral, KeyValueReadMode.StatementSnapshot, out _));
    }

    /// <summary>
    /// Two reads of a key by one transaction, the second before the pin of the first landed. The second is
    /// answered from the same head, and one pin is sent for the two.
    /// </summary>
    [Fact]
    public async Task SecondReadBeforeThePinLands_IsServedFromTheSameHead_WithOnePin()
    {
        await using ActorRig rig = await ActorRig.CreateAsync(this);
        const string key = "tx/early";
        await rig.AskAsync(Set(key, "v1"));
        HLCTimestamp tx = rig.Now();

        HoldTurn hold = await rig.HoldActorAsync(key);

        Assert.True(rig.Reader.TryRead(tx, key, -1, HLCTimestamp.Zero, KeyValueDurability.Ephemeral, KeyValueReadMode.Default, out ReadOnlyKeyValueEntry? first));
        Assert.Equal(1, rig.Context.PublishedHeads.PinsInFlightCount);

        Assert.True(rig.Reader.TryRead(tx, key, -1, HLCTimestamp.Zero, KeyValueDurability.Ephemeral, KeyValueReadMode.Default, out ReadOnlyKeyValueEntry? second));
        AssertSameHead(first!, second!);
        Assert.Equal(1, rig.Context.PublishedHeads.PinsInFlightCount);

        hold.Release.SetResult();
        await hold.Turn;

        await WaitUntil(() => rig.MvccStateOf(key, tx) == PublishedMvccState.PinAtHead);
        Assert.Equal(0, rig.Context.PublishedHeads.PinsInFlightCount);
        KeyValueEntry resident = rig.Context.Store.Get(key)!;
        Assert.Single(resident.MvccEntries!);
    }

    /// <summary>
    /// A transaction read a head and its pin has not landed. The head then moves, and the new record does not show
    /// the transaction. Its next read must not be answered from that record: it would read two heads with no
    /// revision check. The registration of the pin sends the read to the actor, which processes the pin first and
    /// answers <c>Aborted</c>.
    /// </summary>
    [Fact]
    public async Task ReadWhileAPinFromAnotherHeadIsInFlight_GoesToTheActor_WhichAnswersAborted()
    {
        await using ActorRig rig = await ActorRig.CreateAsync(this);
        const string key = "tx/late";
        await rig.AskAsync(Set(key, "v1"));
        HLCTimestamp tx = rig.Now();

        // The first read is served and its pin is registered, and the actor is held so the pin cannot land.
        // The message that moves the head is already in the mailbox, ahead of nothing the pin can pass: both
        // wait for the held turn, and the head change is delivered here by hand before the pin.
        Assert.True(rig.Context.PublishedHeads.TryGet(key, out PublishedHead observed));
        Assert.True(rig.Context.PublishedHeads.TryRegisterPin(tx, key, observed, out _));

        await rig.AskAsync(Set(key, "v2"));
        Assert.True(rig.Context.PublishedHeads.TryGet(key, out PublishedHead moved) && !moved.IsRetracted);
        Assert.Equal(PublishedMvccState.None, moved.MvccStateOf(tx));

        Assert.False(rig.Reader.TryRead(tx, key, -1, HLCTimestamp.Zero, KeyValueDurability.Ephemeral, KeyValueReadMode.Default, default, out _, out PublishedReadFallback fallback));
        Assert.Equal(PublishedReadFallback.PinInFlight, fallback);

        // The pin lands late, at the revision the transaction read.
        rig.Ring.Send(KeyValueRequestPool.RentReadPin(key, tx, KeyValueDurability.Ephemeral, observed));
        await WaitUntil(() => rig.MvccStateOf(key, tx) == PublishedMvccState.PinMoved);
        Assert.Equal(0, rig.Context.PublishedHeads.PinsInFlightCount);

        KeyValueResponse? viaActor = await rig.AskAsync(Get(key, tx));
        Assert.Equal(KeyValueResponseType.Aborted, viaActor!.Type);
    }

    /// <summary>
    /// A record is judged against guards that are read after the record itself. A head installed in between
    /// retracts the record, and what announced that head can be gone again by the time the guards are read. The
    /// record is checked once more after the guards, so a record that was replaced after it was taken is not
    /// served.
    /// </summary>
    [Fact]
    public async Task RecordReplacedAfterItWasTaken_IsNotServed()
    {
        await using ActorRig rig = await ActorRig.CreateAsync(this);
        const string key = "tx/replaced";
        await rig.AskAsync(Set(key, "v1"));

        Assert.True(rig.Context.PublishedHeads.TryGet(key, out PublishedHead taken) && !taken.IsRetracted);

        // Served while it is the head.
        Assert.True(rig.Reader.TryServe(rig.Ring, rig.Context.PublishedHeads, taken, HLCTimestamp.Zero, key, HLCTimestamp.Zero,
            KeyValueDurability.Ephemeral, default, rig.Now(), 0, out ReadOnlyKeyValueEntry? served, out _));
        Assert.Equal("v1", Encoding.UTF8.GetString(served!.Value!));

        // The head moves. Every other guard is clear again: nothing is pending, no intent, no lock.
        await rig.AskAsync(Set(key, "v2"));
        Assert.Equal(0, rig.Context.PublishedHeads.PendingApplyCount);

        Assert.False(rig.Reader.TryServe(rig.Ring, rig.Context.PublishedHeads, taken, HLCTimestamp.Zero, key, HLCTimestamp.Zero,
            KeyValueDurability.Ephemeral, default, rig.Now(), 0, out _, out PublishedReadFallback fallback));
        Assert.Equal(PublishedReadFallback.Retracted, fallback);

        // A transaction's first read of the replaced record is refused too. Its pin was registered before the
        // second look, so it is still sent, at the head the record held, and the registration ends with it. The
        // actor then answers the read as it answers any read that was concurrent with the write.
        HLCTimestamp tx = rig.Now();
        Assert.False(rig.Reader.TryServe(rig.Ring, rig.Context.PublishedHeads, taken, tx, key, HLCTimestamp.Zero,
            KeyValueDurability.Ephemeral, default, rig.Now(), 0, out _, out _));
        await WaitUntil(() => rig.MvccStateOf(key, tx) == PublishedMvccState.PinMoved);
        Assert.Equal(0, rig.Context.PublishedHeads.PinsInFlightCount);

        KeyValueResponse? viaActor = await rig.AskAsync(Get(key, tx));
        Assert.Equal(KeyValueResponseType.Aborted, viaActor!.Type);
    }

    /// <summary>
    /// A durable prepared intent on the key. An intent that is not decided, or that is committed and not installed
    /// in the head yet, is the actor's to answer: it waits for the decision or serves the intent's value. An
    /// intent whose write the head already includes stays in the store until its settlement removes it, and the
    /// actor answers such a key with the bytes of the head, so the record answers it too.
    /// </summary>
    [Fact]
    public async Task PersistentKey_PreparedIntent_IsServedOnlyWhenTheHeadIncludesIt()
    {
        await using ActorRig rig = await ActorRig.CreateAsync(this, persistent: true);
        const string key = "pi/key";
        HLCTimestamp now = rig.Now();
        rig.InsertResident(key, "v1", 1, now);

        HLCTimestamp writer = rig.Now();
        HLCTimestamp commitAt = rig.Now();
        PreparedIntent intent = Intent(key, writer, "v2", revision: 2, commitAt);

        // Prepared, not decided.
        rig.Intents.Apply(new PrepareIntentCommand(intent));
        Assert.False(rig.Reader.TryRead(HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, KeyValueReadMode.Default, default, out _, out PublishedReadFallback fallback), "undecided");
        Assert.Equal(PublishedReadFallback.PreparedIntent, fallback);
        KeyValueResponse? undecided = await rig.AskAsync(Get(key, durability: KeyValueDurability.Persistent));
        Assert.Equal(KeyValueResponseType.WaitingForReplication, undecided!.Type);

        // Committed, and the head does not hold it yet: the actor serves the intent's value.
        rig.Intents.Apply(new ResolveIntentCommand(writer, intent.Epoch, key, Commit: true));
        Assert.False(rig.Reader.TryRead(HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, KeyValueReadMode.Default, out _), "committed, not installed");
        KeyValueResponse? fromIntent = await rig.AskAsync(Get(key, durability: KeyValueDurability.Persistent));
        Assert.Equal(KeyValueResponseType.Get, fromIntent!.Type);
        Assert.Equal("v2", Encoding.UTF8.GetString(fromIntent.Entry!.Value!));

        // The commit reaches the head, as the settlement installs it: the intent's revision and commit timestamp.
        // The intent is still in the store.
        rig.Ring.NotePendingApply(key, 2, commitAt);
        rig.Ring.Send(KeyValueRequestPool.RentInvalidateOrApply(
            key, 2, "v2"u8.ToArray(), HLCTimestamp.Zero, commitAt, commitAt, KeyValueState.Set,
            forceResident: false, transactionId: writer, partitionId: 0, noRevision: false,
            isRollback: false, returnToPoolOnReceive: true));
        await WaitUntil(() => rig.Context.PublishedHeads.TryGet(key, out PublishedHead h) && !h.IsRetracted && h.Revision == 2);
        Assert.NotNull(rig.Intents.Get(key));

        Assert.True(rig.Reader.TryRead(HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, KeyValueReadMode.Default, out ReadOnlyKeyValueEntry? included), "the head includes the intent");
        Assert.Equal("v2", Encoding.UTF8.GetString(included!.Value!));
        KeyValueResponse? includedViaActor = await rig.AskAsync(Get(key, durability: KeyValueDurability.Persistent));
        Assert.Equal(KeyValueResponseType.Get, includedViaActor!.Type);
        AssertSameHead(includedViaActor.Entry!, included);

        // A transaction's first read pins that head on both paths.
        HLCTimestamp reader = rig.Now();
        Assert.True(rig.Reader.TryRead(reader, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, KeyValueReadMode.Default, out ReadOnlyKeyValueEntry? pinned));
        AssertSameHead(included, pinned!);
        await WaitUntil(() => rig.MvccStateOf(key, reader) == PublishedMvccState.PinAtHead);
        KeyValueResponse? pinnedViaActor = await rig.AskAsync(Get(key, reader, KeyValueDurability.Persistent));
        Assert.Equal(KeyValueResponseType.Get, pinnedViaActor!.Type);
        AssertSameHead(pinnedViaActor.Entry!, pinned!);

        // The transaction that owns the intent is left to the actor.
        Assert.False(rig.Reader.TryRead(writer, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, KeyValueReadMode.Default, out _), "own intent");

        // A later intent on the key is ahead of the head again.
        rig.Intents.Apply(new RemoveIntentCommand(writer, intent.Epoch, key));
        Assert.Null(rig.Intents.Get(key));
        HLCTimestamp next = rig.Now();
        rig.Intents.Apply(new PrepareIntentCommand(Intent(key, next, "v3", revision: 3, rig.Now())));
        Assert.False(rig.Reader.TryRead(HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, KeyValueReadMode.Default, out _), "a newer intent");
    }

    /// <summary>
    /// A committed delete keeps the revision of the value it deletes. At that revision the head does not include
    /// the delete, whatever its stamp, so the read goes to the actor, which answers that the key does not exist.
    /// </summary>
    [Fact]
    public async Task PersistentKey_CommittedDeleteIntentAtTheHeadRevision_GoesToTheActor()
    {
        await using ActorRig rig = await ActorRig.CreateAsync(this, persistent: true);
        const string key = "pi/deleted";
        HLCTimestamp now = rig.Now();
        rig.InsertResident(key, "v1", 1, now);

        HLCTimestamp writer = rig.Now();
        PreparedIntent delete = Intent(key, writer, null, revision: 1, rig.Now()) with { State = KeyValueState.Deleted };
        rig.Intents.Apply(new PrepareIntentCommand(delete));
        rig.Intents.Apply(new ResolveIntentCommand(writer, delete.Epoch, key, Commit: true));

        Assert.False(rig.Reader.TryRead(HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, KeyValueReadMode.Default, default, out _, out PublishedReadFallback fallback));
        Assert.Equal(PublishedReadFallback.PreparedIntent, fallback);

        KeyValueResponse? viaActor = await rig.AskAsync(Get(key, durability: KeyValueDurability.Persistent));
        Assert.Equal(KeyValueResponseType.DoesNotExist, viaActor!.Type);
    }

    [Fact]
    public async Task PersistentKey_NewerUnflushedWriteOrPendingApply_RoutesTheReadToTheActor()
    {
        await using ActorRig rig = await ActorRig.CreateAsync(this, persistent: true);
        const string key = "ovl/key";
        HLCTimestamp now = rig.Now();
        rig.InsertResident(key, "v1", 1, now);

        Assert.True(rig.Reader.TryRead(HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, KeyValueReadMode.Default, out ReadOnlyKeyValueEntry? e));
        Assert.Equal(1L, e!.Revision);

        // An older or equal head in the overlay is not newer than the record.
        rig.Unflushed.Record(key, "v1"u8.ToArray(), 1, HLCTimestamp.Zero, now, now, KeyValueState.Set, false);
        Assert.True(rig.Reader.TryRead(HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, KeyValueReadMode.Default, out _));

        HLCTimestamp later = rig.Now();
        rig.Unflushed.Record(key, "v2"u8.ToArray(), 2, HLCTimestamp.Zero, later, later, KeyValueState.Set, false);
        Assert.False(rig.Reader.TryRead(HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, KeyValueReadMode.Default, out _), "newer unflushed write");
        rig.Unflushed.RemoveFlushed(key, 2, later);
        Assert.True(rig.Reader.TryRead(HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, KeyValueReadMode.Default, out _));

        rig.Ring.NotePendingApply(key, 2, later);
        Assert.False(rig.Reader.TryRead(HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, KeyValueReadMode.Default, out _), "pending apply");
        rig.Context.PublishedHeads.ClearPendingApply(key, 2, later);
        Assert.True(rig.Reader.TryRead(HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, KeyValueReadMode.Default, out _));
    }

    /// <summary>
    /// The ordering hazard the pending-apply note closes: the replicator records a committed head and sends it
    /// to the actor, the applied frontier moves, and a read that passed the leadership check arrives while the
    /// actor has not processed the notification yet. The read must not be answered from the published record.
    /// The actor is held inside a turn so the window stays open for the assertion.
    /// </summary>
    [Fact]
    public async Task CommittedApplyPendingOnAHeldActor_RoutesTheReadToTheActor_WhichAnswersTheNewHead()
    {
        await using ActorRig rig = await ActorRig.CreateAsync(this, persistent: true);
        const string key = "hazard/key";
        HLCTimestamp now = rig.Now();
        rig.InsertResident(key, "v1", 1, now);
        Assert.True(rig.Reader.TryRead(HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, KeyValueReadMode.Default, out _));

        HoldTurn hold = new();
        KeyValueRequest turnRequest = Request(KeyValueRequestType.RunActorTurn, key, HLCTimestamp.Zero);
        turnRequest.Turn = hold;
        Task<KeyValueResponse?> turn = rig.Ring.Ask(turnRequest);
        await hold.Started.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

        // What KeyValueReplicator.ApplyCommittedMutation does for a committed entry, in its order: note the
        // pending apply, record the unflushed write, send the notification. The frontier would move next.
        HLCTimestamp later = rig.Now();
        rig.Ring.NotePendingApply(key, 2, later);
        rig.Unflushed.Record(key, "v2"u8.ToArray(), 2, HLCTimestamp.Zero, later, later, KeyValueState.Set, false);
        rig.Ring.Send(KeyValueRequestPool.RentInvalidateOrApply(
            key, 2, "v2"u8.ToArray(), HLCTimestamp.Zero, later, later, KeyValueState.Set,
            forceResident: false, transactionId: HLCTimestamp.Zero, partitionId: 0, noRevision: false,
            isRollback: false, returnToPoolOnReceive: true));

        // The record still says v1 and is live; both guards send the read to the actor.
        Assert.True(rig.Context.PublishedHeads.TryGet(key, out PublishedHead stale) && !stale.IsRetracted && stale.Revision == 1);
        Assert.False(rig.Reader.TryRead(HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, KeyValueReadMode.Default, out _), "while the apply is pending");

        // Even with the overlay already flushed (the flush raced the actor), the pending note alone blocks.
        rig.Unflushed.RemoveFlushed(key, 2, later);
        Assert.False(rig.Reader.TryRead(HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, KeyValueReadMode.Default, out _), "pending note alone");

        hold.Release.SetResult();
        await turn;

        KeyValueResponse? viaActor = await rig.AskAsync(Get(key, durability: KeyValueDurability.Persistent));
        Assert.Equal(KeyValueResponseType.Get, viaActor!.Type);
        Assert.Equal(2L, viaActor.Entry!.Revision);

        Assert.Equal(0, rig.Context.PublishedHeads.PendingApplyCount);
        Assert.True(rig.Reader.TryRead(HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, KeyValueReadMode.Default, out ReadOnlyKeyValueEntry? fresh));
        Assert.Equal(2L, fresh!.Revision);
        Assert.Equal("v2", Encoding.UTF8.GetString(fresh.Value!));
    }

    // ── end to end through an embedded node ──────────────────────────────────────────────

    [Fact]
    public async Task EmbeddedNode_SetThenGet_IsServedFromThePublishedRecord()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = new(Options(), loggerFactory);
        await node.StartAsync(ct);
        string key = "e2e/pub/" + Guid.NewGuid().ToString("N")[..8];
        await node.WaitForLeaderForKeyAsync(key, ct);

        using MetricCapture capture = new("path", "kahuna.kv.read.stage");

        (KeyValueResponseType setType, long revision, _) = await node.Kahuna.LocateAndTrySetKeyValue(
            HLCTimestamp.Zero, key, "hello"u8.ToArray(), null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Set, setType);

        PublishedHeadTable table = ((KahunaManager)node.Kahuna).KeyValueRouters.Persistent.PublishedHeadsFor(key);
        Assert.True(table.TryGet(key, out PublishedHead head) && !head.IsRetracted);
        Assert.Equal(revision, head.Revision);

        int before = capture.Samples("kahuna.kv.read.stage", "published").Count;

        (KeyValueResponseType getType, ReadOnlyKeyValueEntry? entry) = await node.Kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Get, getType);
        Assert.Equal("hello"u8.ToArray(), entry!.Value);
        Assert.Equal(revision, entry.Revision);
        Assert.Equal(head.LastModified, entry.LastModified);

        Assert.True(capture.Samples("kahuna.kv.read.stage", "published").Count > before);

        // A delete drops the record; the read of the tombstone comes from the actor.
        (KeyValueResponseType deleteType, _, _) = await node.Kahuna.LocateAndTryDeleteKeyValue(HLCTimestamp.Zero, key, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Deleted, deleteType);
        Assert.False(table.TryGet(key, out PublishedHead afterDelete) && !afterDelete.IsRetracted);

        (KeyValueResponseType gone, _) = await node.Kahuna.LocateAndTryGetValue(HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.DoesNotExist, gone);
    }

    [Fact]
    public async Task EmbeddedNode_TransactionalRead_PinsAndAbortsAfterAConcurrentWrite()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = new(Options(), loggerFactory);
        await node.StartAsync(ct);
        string key = "e2e/tx/" + Guid.NewGuid().ToString("N")[..8];
        await node.WaitForLeaderForKeyAsync(key, ct);

        await node.Kahuna.LocateAndTrySetKeyValue(HLCTimestamp.Zero, key, "one"u8.ToArray(), null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct);
        HLCTimestamp tx = node.Raft.HybridLogicalClock.TrySendOrLocalEvent(node.Raft.GetLocalNodeId());

        (KeyValueResponseType t1, ReadOnlyKeyValueEntry? e1) = await node.Kahuna.LocateAndTryGetValue(tx, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Get, t1);
        Assert.Equal("one"u8.ToArray(), e1!.Value);

        PublishedHeadTable table = ((KahunaManager)node.Kahuna).KeyValueRouters.Persistent.PublishedHeadsFor(key);
        await WaitUntil(() => table.TryGet(key, out PublishedHead h) && !h.IsRetracted && h.MvccHeld);

        (KeyValueResponseType t2, ReadOnlyKeyValueEntry? e2) = await node.Kahuna.LocateAndTryGetValue(tx, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Get, t2);
        AssertSameHead(e1, e2!);

        (KeyValueResponseType setType, _, _) = await node.Kahuna.LocateAndTrySetKeyValue(
            HLCTimestamp.Zero, key, "two"u8.ToArray(), null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Set, setType);

        (KeyValueResponseType t3, _) = await node.Kahuna.LocateAndTryGetValue(tx, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Aborted, t3);

        (KeyValueResponseType t4, ReadOnlyKeyValueEntry? e4) = await node.Kahuna.LocateAndTryGetValue(HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Get, t4);
        Assert.Equal("two"u8.ToArray(), e4!.Value);
    }

    /// <summary>
    /// One transaction on an embedded node, through the routed read path: its first read and its repeat read of a
    /// key are answered from the published record, and after it wrote the key its read takes the actor, which
    /// returns the staged value. The actor-path sample names the guard that refused the record.
    /// </summary>
    [Fact]
    public async Task EmbeddedNode_RepeatReadOfATransaction_IsServedFromThePublishedRecord_AndAnActorReadNamesItsReason()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = new(Options(), loggerFactory);
        await node.StartAsync(ct);
        string key = "e2e/repeat/" + Guid.NewGuid().ToString("N")[..8];
        await node.WaitForLeaderForKeyAsync(key, ct);

        using MetricCapture paths = new("path", "kahuna.kv.read.stage");
        using MetricCapture reasons = new("reason", "kahuna.kv.read.fallback", "kahuna.kv.read.stage");

        await node.Kahuna.LocateAndTrySetKeyValue(HLCTimestamp.Zero, key, "one"u8.ToArray(), null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct);

        (KeyValueResponseType started, TransactionHandle handle) = await node.Kahuna.LocateAndStartTransaction(
            new KeyValueTransactionOptions { CoordinatorKey = key, Locking = KeyValueTransactionLocking.Optimistic, Timeout = 10_000 }, ct);
        Assert.Equal(KeyValueResponseType.Set, started);

        (KeyValueResponseType t1, ReadOnlyKeyValueEntry? e1) = await node.Kahuna.LocateAndTryGetValue(
            handle.TransactionId, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct,
            coordinatorKey: handle.CoordinatorKey, operationId: TransactionOperationId.NewRandom());
        Assert.Equal(KeyValueResponseType.Get, t1);
        Assert.Equal("one"u8.ToArray(), e1!.Value);

        PublishedHeadTable table = ((KahunaManager)node.Kahuna).KeyValueRouters.Persistent.PublishedHeadsFor(key);
        await WaitUntil(() => table.TryGet(key, out PublishedHead h) && !h.IsRetracted && h.MvccStateOf(handle.TransactionId) == PublishedMvccState.PinAtHead);

        int publishedBefore = paths.Samples("kahuna.kv.read.stage", "published").Count;

        (KeyValueResponseType t2, ReadOnlyKeyValueEntry? e2) = await node.Kahuna.LocateAndTryGetValue(
            handle.TransactionId, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct,
            coordinatorKey: handle.CoordinatorKey, operationId: TransactionOperationId.NewRandom());
        Assert.Equal(KeyValueResponseType.Get, t2);
        AssertSameHead(e1, e2!);
        Assert.True(paths.Samples("kahuna.kv.read.stage", "published").Count > publishedBefore);
        Assert.Equal(0, table.PinsInFlightCount);

        (KeyValueResponseType setType, _, _) = await node.Kahuna.LocateAndTrySetKeyValue(
            handle.TransactionId, key, "two"u8.ToArray(), null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct,
            coordinatorKey: handle.CoordinatorKey, operationId: TransactionOperationId.NewRandom());
        Assert.Equal(KeyValueResponseType.Set, setType);

        int stagedBefore = reasons.Samples("kahuna.kv.read.fallback", "mvcc_staged").Count;

        (KeyValueResponseType t3, ReadOnlyKeyValueEntry? e3) = await node.Kahuna.LocateAndTryGetValue(
            handle.TransactionId, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct,
            coordinatorKey: handle.CoordinatorKey, operationId: TransactionOperationId.NewRandom());
        Assert.Equal(KeyValueResponseType.Get, t3);
        Assert.Equal("two"u8.ToArray(), e3!.Value);
        Assert.True(reasons.Samples("kahuna.kv.read.fallback", "mvcc_staged").Count > stagedBefore);

        // The stage series keeps its two tags: an actor-path sample carries no reason there.
        Assert.Empty(reasons.Samples("kahuna.kv.read.stage", "mvcc_staged"));

        // Another reader still gets the committed value while the write is staged.
        (KeyValueResponseType t4, ReadOnlyKeyValueEntry? e4) = await node.Kahuna.LocateAndTryGetValue(HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Get, t4);
        Assert.Equal("one"u8.ToArray(), e4!.Value);

        TransactionCommitAnswer commit = await node.Kahuna.LocateAndCommitTransaction(handle, ct);
        Assert.Equal(KeyValueResponseType.Committed, commit.Type);

        // After the commit answer every read returns the new value, from the record or from the actor.
        for (int i = 0; i < 20; i++)
        {
            (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await node.Kahuna.LocateAndTryGetValue(HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.Get, type);
            Assert.Equal("two"u8.ToArray(), entry!.Value);
        }
    }

    [Fact]
    public async Task EmbeddedNode_LockFreeProbe_AnswersPublishedAndActorKeysAlike_AndPinsATransaction()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = new(Options(), loggerFactory);
        await node.StartAsync(ct);
        string space = "e2e/probe-" + Guid.NewGuid().ToString("N")[..8];
        string[] keys = [$"{space}/a", $"{space}/absent", $"{space}/b", $"{space}/gone"];
        await node.WaitForLeaderForKeyAsync(keys[0], ct);

        foreach (string key in (string[])[keys[0], keys[2], keys[3]])
            await node.Kahuna.LocateAndTrySetKeyValue(HLCTimestamp.Zero, key, "one"u8.ToArray(), null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct);
        await node.Kahuna.LocateAndTryDeleteKeyValue(HLCTimestamp.Zero, keys[3], KeyValueDurability.Persistent, ct);

        // Two keys have a live record and two (never written, deleted) have none: one probe answers both kinds,
        // each as a single read of the key does, in input order.
        PublishedHeadTable table = ((KahunaManager)node.Kahuna).KeyValueRouters.Persistent.PublishedHeadsFor(keys[0]);
        Assert.True(table.TryGet(keys[0], out PublishedHead headA) && !headA.IsRetracted);
        Assert.True(table.TryGet(keys[2], out PublishedHead headB) && !headB.IsRetracted);

        KeyValueProbeResult[] probed = await node.Kahuna.LocateAndTryProbeMany(
            HLCTimestamp.Zero, HLCTimestamp.Zero, keys, KeyValueProbeLock.None, 0, KeyValueDurability.Persistent, ct);

        Assert.Equal(keys.Length, probed.Length);
        for (int i = 0; i < keys.Length; i++)
        {
            (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await node.Kahuna.LocateAndTryGetValue(
                HLCTimestamp.Zero, keys[i], -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
            Assert.Equal(type, probed[i].Type);
            Assert.Equal(entry?.Revision, probed[i].Entry?.Revision);
            Assert.Equal(entry?.Value, probed[i].Entry?.Value);
            Assert.False(probed[i].Locked);
        }

        Assert.Equal(KeyValueResponseType.Get, probed[0].Type);
        Assert.Equal(KeyValueResponseType.Get, probed[2].Type);

        // A transactional probe pins what it read, as a transactional single read does: a write that lands after
        // it aborts the transaction's next read of the key.
        (KeyValueResponseType started, TransactionHandle handle) = await node.Kahuna.LocateAndStartTransaction(
            new KeyValueTransactionOptions { CoordinatorKey = keys[0], Locking = KeyValueTransactionLocking.Optimistic, Timeout = 10_000 }, ct);
        Assert.Equal(KeyValueResponseType.Set, started);

        string[] live = [keys[0], keys[2]];
        KeyValueProbeResult[] pinned = await node.Kahuna.LocateAndTryProbeMany(
            handle.TransactionId, HLCTimestamp.Zero, live, KeyValueProbeLock.None, 0, KeyValueDurability.Persistent, ct,
            handle.CoordinatorKey, TransactionOperationId.NewRandom());
        Assert.All(pinned, answer => Assert.Equal(KeyValueResponseType.Get, answer.Type));

        await WaitUntil(() => table.TryGet(keys[0], out PublishedHead a) && a.MvccHeld && table.TryGet(keys[2], out PublishedHead b) && b.MvccHeld);

        await node.Kahuna.LocateAndTrySetKeyValue(HLCTimestamp.Zero, keys[2], "two"u8.ToArray(), null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct);

        KeyValueProbeResult[] after = await node.Kahuna.LocateAndTryProbeMany(
            handle.TransactionId, HLCTimestamp.Zero, live, KeyValueProbeLock.None, 0, KeyValueDurability.Persistent, ct,
            handle.CoordinatorKey, TransactionOperationId.NewRandom());
        Assert.Equal(KeyValueResponseType.Get, after[0].Type);
        Assert.Equal(KeyValueResponseType.Aborted, after[1].Type);
    }

    [Fact]
    public async Task EmbeddedNode_PrefixLock_RoutesTheReadToTheActor()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = new(Options(), loggerFactory);
        await node.StartAsync(ct);
        string prefix = "e2e-lock-" + Guid.NewGuid().ToString("N")[..8];
        string key = prefix + "/row";
        await node.WaitForLeaderForKeyAsync(key, ct);

        await node.Kahuna.LocateAndTrySetKeyValue(HLCTimestamp.Zero, key, "v"u8.ToArray(), null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct);
        HLCTimestamp tx = node.Raft.HybridLogicalClock.TrySendOrLocalEvent(node.Raft.GetLocalNodeId());

        Assert.Equal(KeyValueResponseType.Locked, await node.Kahuna.LocateAndTryAcquireExclusivePrefixLock(tx, prefix, 60_000, KeyValueDurability.Persistent, ct));

        (KeyValueResponseType blocked, _) = await node.Kahuna.LocateAndTryGetValue(HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.MustRetry, blocked);

        Assert.Equal(KeyValueResponseType.Unlocked, await node.Kahuna.LocateAndTryReleaseExclusivePrefixLock(tx, prefix, KeyValueDurability.Persistent, ct));

        (KeyValueResponseType served, ReadOnlyKeyValueEntry? entry) = await node.Kahuna.LocateAndTryGetValue(HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Get, served);
        Assert.Equal("v"u8.ToArray(), entry!.Value);
    }

    [Fact]
    public async Task EmbeddedNode_DeferredDurableCommit_ReadsItsOwnWriteBeforeSettlement()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        EmbeddedKahunaOptions options = Options();
        options.DurableDeferredSettlement = true;
        await using EmbeddedKahunaNode node = new(options, loggerFactory);
        await node.StartAsync(ct);
        string key = "e2e/def/" + Guid.NewGuid().ToString("N")[..8];
        await node.WaitForLeaderForKeyAsync(key, ct);

        // A published record for the pre-commit head exists before the durable commit.
        await node.Kahuna.LocateAndTrySetKeyValue(HLCTimestamp.Zero, key, "old"u8.ToArray(), null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct);
        (KeyValueResponseType warm, _) = await node.Kahuna.LocateAndTryGetValue(HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Get, warm);

        KeyValueTransactionResult result = await node.Kahuna.TryExecuteTransactionScript(
            Encoding.UTF8.GetBytes($"BEGIN SET `{key}` 'new' COMMIT END"), null, null);
        Assert.Equal(KeyValueResponseType.Set, result.Type);

        for (int i = 0; i < 20; i++)
        {
            (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await node.Kahuna.LocateAndTryGetValue(HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.Get, type);
            Assert.Equal("new"u8.ToArray(), entry!.Value);
        }
    }

    [Fact]
    public async Task EmbeddedNode_FlagOff_PublishesNothing()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        EmbeddedKahunaOptions options = Options();
        options.PublishedHeadReads = false;
        await using EmbeddedKahunaNode node = new(options, loggerFactory);
        await node.StartAsync(ct);
        string key = "e2e/off/" + Guid.NewGuid().ToString("N")[..8];
        await node.WaitForLeaderForKeyAsync(key, ct);

        await node.Kahuna.LocateAndTrySetKeyValue(HLCTimestamp.Zero, key, "v"u8.ToArray(), null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct);
        (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await node.Kahuna.LocateAndTryGetValue(HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Get, type);
        Assert.Equal("v"u8.ToArray(), entry!.Value);

        PublishedHeadTable table = ((KahunaManager)node.Kahuna).KeyValueRouters.Persistent.PublishedHeadsFor(key);
        Assert.False(table.TryGet(key, out _));
    }

    // ── helpers ──────────────────────────────────────────────────────────────────────────

    private static EmbeddedKahunaOptions Options() => new()
    {
        Storage = "memory",
        WalStorage = "memory",
        InitialPartitions = 1
    };

    private static void AssertSameHead(ReadOnlyKeyValueEntry expected, ReadOnlyKeyValueEntry actual)
    {
        Assert.Equal(expected.Value, actual.Value);
        Assert.Equal(expected.Revision, actual.Revision);
        Assert.Equal(expected.Expires, actual.Expires);
        Assert.Equal(expected.LastModified, actual.LastModified);
        Assert.Equal(expected.State, actual.State);
    }

    private static async Task WaitUntil(Func<bool> predicate, int timeoutMs = 5000)
    {
        long deadline = Environment.TickCount64 + timeoutMs;
        while (Environment.TickCount64 < deadline)
        {
            if (predicate()) return;
            await Task.Delay(5);
        }
        Assert.True(predicate(), "condition not met in time");
    }

    private static KeyValueRequest Request(KeyValueRequestType type, string key, HLCTimestamp transactionId, int expiresMs = 0, KeyValueDurability durability = KeyValueDurability.Ephemeral) =>
        new(type, transactionId, HLCTimestamp.Zero, key, null, null, -1, KeyValueFlags.None, expiresMs, HLCTimestamp.Zero, durability, 0, 0, default);

    private static KeyValueRequest Set(string key, string value, int expiresMs = 0) =>
        new(KeyValueRequestType.TrySet, HLCTimestamp.Zero, HLCTimestamp.Zero, key, Encoding.UTF8.GetBytes(value), null, -1,
            KeyValueFlags.Set, expiresMs, HLCTimestamp.Zero, KeyValueDurability.Ephemeral, 0, 0, default);

    private static KeyValueRequest TxSet(string key, string value, HLCTimestamp transactionId) =>
        new(KeyValueRequestType.TrySet, transactionId, HLCTimestamp.Zero, key, Encoding.UTF8.GetBytes(value), null, -1,
            KeyValueFlags.Set, 0, HLCTimestamp.Zero, KeyValueDurability.Ephemeral, 0, 0, default);

    private static PreparedIntent Intent(string key, HLCTimestamp transactionId, string? value, long revision, HLCTimestamp commitTimestamp) =>
        new(
            TransactionId: transactionId, Epoch: 1, Key: key, ManifestHash: 0, RecordAnchorKey: key,
            CommitTimestamp: commitTimestamp,
            State: KeyValueState.Set, Value: value is null ? null : Encoding.UTF8.GetBytes(value), Bucket: null, Revision: revision,
            Expires: HLCTimestamp.Zero, NoRevision: false, BaseRevision: revision - 1, BaseState: KeyValueState.Set,
            RecoveryDeadline: new HLCTimestamp(0, long.MaxValue, 0), Resolution: PreparedIntentResolution.Pending);

    private static KeyValueRequest Get(string key, HLCTimestamp transactionId = default, KeyValueDurability durability = KeyValueDurability.Ephemeral) =>
        new(KeyValueRequestType.TryGet, transactionId, HLCTimestamp.Zero, key, null, null, -1, KeyValueFlags.None, 0, HLCTimestamp.Zero, durability, 0, 0, default);

    private static KeyValueRequest SnapshotGet(string key, HLCTimestamp readTimestamp, HLCTimestamp transactionId = default)
    {
        KeyValueRequest request = Get(key, transactionId);
        request.ReadTimestamp = readTimestamp;
        return request;
    }

    /// <summary>A turn that parks the actor until released, so a test can open a window on its mailbox.</summary>
    private sealed class HoldTurn : IKeyValueActorTurn
    {
        public readonly TaskCompletionSource Started = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public readonly TaskCompletionSource Release = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>The ask that carries this turn, when a rig started it.</summary>
        public Task<KeyValueResponse?> Turn = Task.FromResult<KeyValueResponse?>(null);

        public async ValueTask RunAsync(IKeyValueInlineDispatcher actor)
        {
            Started.TrySetResult();
            await Release.Task;
        }
    }

    /// <summary>
    /// One real key-value actor behind a one-actor ring, with the reader wired to that ring's table, a bare
    /// unflushed-writes overlay, and a prepared-intent store that the actor and the reader share. The same actor
    /// serves as both rings so a read of the wrong durability finds a table with nothing in it for the key.
    /// </summary>
    private sealed class ActorRig : IAsyncDisposable
    {
        public KeyValueActorRing Ring = null!;
        public KeyValueContext Context = null!;
        public PublishedHeadReader Reader = null!;
        public UnflushedKeyValueWritesIndex Unflushed = new();
        public PreparedIntentStore Intents = new();
        private RaftManager raft = null!;
        private IDisposable actorSystemLifetime = null!;
        private bool persistent;
        private KeyValueActorRing otherRing = null!;
        private InstalledHistoryFence? installed;
        private int maxIntentWaitMs;

        public static async Task<ActorRig> CreateAsync(TestPublishedHeadReads owner, bool persistent = false, InstalledHistoryFence? installedHistory = null)
        {
            ActorRig rig = new() { persistent = persistent };

            KahunaConfiguration config = ConfigurationValidator.Validate(new()
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

            ILogger<IRaft> raftLogger = NullLogger<IRaft>.Instance;
            ILogger<IKahuna> logger = NullLogger<IKahuna>.Instance;

            rig.raft = owner.Track(new RaftManager(
                new RaftConfiguration
                {
                    NodeName = "published-head-reads",
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

            rig.actorSystemLifetime = TestActorSystemLifetime.Create(out ActorSystem actorSystem);

            IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> actorRef =
                actorSystem.Spawn<KeyValueActor, KeyValueRequest, KeyValueResponse>(
                    "published-head-kv",
                    null!,
                    null!,
                    new MemoryPersistenceBackend(),
                    rig.raft,
                    rig.raft.ReadScheduler,
                    new KeySpaceRegistry(),
                    new RangeMapStore(rig.raft, null, null, logger),
                    config,
                    logger,
                    null!,
                    null!,
                    rig.Intents,
                    null!);

            KeyValueActor? instance = null;
            for (int i = 0; i < 500 && instance is null; i++)
            {
                instance = ((ActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse>)actorRef).Runner.Actor as KeyValueActor;
                if (instance is null)
                    await Task.Delay(10);
            }

            rig.Context = instance!.GetContext();
            rig.Ring = new KeyValueActorRing([actorRef], [rig.Context.PublishedHeads]);

            // An empty table stands in for the ring the test does not use.
            rig.otherRing = new([actorRef], [new PublishedHeadTable()]);
            rig.installed = installedHistory;
            rig.maxIntentWaitMs = config.MaxIntentWaitMs;
            rig.Reader = rig.NewReader(config.MaxIntentWaitMs);

            return rig;
        }

        /// <summary>A reader over this rig's rings and stores, for a server whose cap on an intent wait is
        /// <paramref name="maxIntentWaitMs"/>.</summary>
        public PublishedHeadReader NewReader(int maxIntentWaitMs) =>
            new(
                persistent ? otherRing : Ring,
                persistent ? Ring : otherRing,
                Unflushed,
                Intents,
                raft,
                enabled: true,
                installedHistory: installed,
                maxIntentWaitMs: maxIntentWaitMs);

        /// <summary>What the live record of <paramref name="key"/> says <paramref name="transactionId"/> holds
        /// on it, or <see cref="PublishedMvccState.Unknown"/> when the key has no live record.</summary>
        public PublishedMvccState MvccStateOf(string key, HLCTimestamp transactionId) =>
            Context.PublishedHeads.TryGet(key, out PublishedHead head) && !head.IsRetracted
                ? head.MvccStateOf(transactionId)
                : PublishedMvccState.Unknown;

        /// <summary>Parks the actor inside a turn until the returned hold is released.</summary>
        public async Task<HoldTurn> HoldActorAsync(string key)
        {
            HoldTurn hold = new();
            KeyValueRequest turnRequest = Request(KeyValueRequestType.RunActorTurn, key, HLCTimestamp.Zero);
            turnRequest.Turn = hold;
            hold.Turn = Ring.Ask(turnRequest);
            await hold.Started.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
            return hold;
        }

        public HLCTimestamp Now() => raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

        public Task<KeyValueResponse?> AskAsync(KeyValueRequest request) => Ring.Ask(request);

        /// <summary>Makes a clean head resident and published, as a hydration on the actor would, while the actor is idle.</summary>
        public void InsertResident(string key, string value, long revision, HLCTimestamp lastModified)
        {
            Context.InsertStoreEntry(key, new KeyValueEntry
            {
                Bucket = KeyValueKeySpace.OfKey(key),
                Value = Encoding.UTF8.GetBytes(value),
                Revision = revision,
                FlushedRevision = revision,
                LastModified = lastModified,
                LastUsed = lastModified,
                State = KeyValueState.Set
            });
            Context.FlushHeadPublications();
        }

        public ValueTask DisposeAsync()
        {
            actorSystemLifetime.Dispose();
            return ValueTask.CompletedTask;
        }
    }
}
