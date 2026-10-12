
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
/// The off-actor latest read: a quiet resident key is answered from the owning actor's published head record
/// with no actor message, and every state the actor path treats specially — an intent, a prefix lock, an
/// expired or absent head, a newer head still on its way to the actor, a durable intent, a transaction that
/// already holds a snapshot — sends the read back to the actor. Driven against a real spawned actor through the
/// reader, and end to end through an embedded node.
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

        Assert.True(rig.Reader.TryRead(HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Ephemeral, out ReadOnlyKeyValueEntry? published));
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
        await rig.AskAsync(Set(key, "v1"));
        HLCTimestamp now = rig.Now();

        Assert.False(rig.Reader.TryRead(HLCTimestamp.Zero, key, 0, HLCTimestamp.Zero, KeyValueDurability.Ephemeral, out _), "by revision");
        Assert.False(rig.Reader.TryRead(HLCTimestamp.Zero, key, -1, now, KeyValueDurability.Ephemeral, out _), "snapshot");
        Assert.False(rig.Reader.TryRead(HLCTimestamp.Zero, "pub/absent", -1, HLCTimestamp.Zero, KeyValueDurability.Ephemeral, out _), "absent");
        Assert.False(rig.Reader.TryRead(HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, out _), "other ring");
    }

    [Fact]
    public async Task WriteIntentAndPrefixLock_RouteTheReadToTheActor_UntilReleased()
    {
        await using ActorRig rig = await ActorRig.CreateAsync(this);
        const string key = "lk/key";
        await rig.AskAsync(Set(key, "v1"));
        HLCTimestamp tx = rig.Now();

        Assert.True(rig.Reader.TryRead(HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Ephemeral, out _));

        KeyValueResponse? locked = await rig.AskAsync(Request(KeyValueRequestType.TryAcquireExclusiveLock, key, tx, expiresMs: 60_000));
        Assert.Equal(KeyValueResponseType.Locked, locked!.Type);
        Assert.False(rig.Reader.TryRead(HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Ephemeral, out _), "write intent");

        await rig.AskAsync(Request(KeyValueRequestType.TryReleaseExclusiveLock, key, tx));
        Assert.True(rig.Reader.TryRead(HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Ephemeral, out _), "released");

        KeyValueResponse? prefixLocked = await rig.AskAsync(Request(KeyValueRequestType.TryAcquireExclusivePrefixLock, "lk", tx, expiresMs: 60_000));
        Assert.Equal(KeyValueResponseType.Locked, prefixLocked!.Type);
        Assert.True(rig.Context.PublishedHeads.PrefixLocksHeld);
        Assert.False(rig.Reader.TryRead(HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Ephemeral, out _), "prefix lock");

        await rig.AskAsync(Request(KeyValueRequestType.TryReleaseExclusivePrefixLock, "lk", tx));
        Assert.False(rig.Context.PublishedHeads.PrefixLocksHeld);
        Assert.True(rig.Reader.TryRead(HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Ephemeral, out _), "prefix lock released");
    }

    [Fact]
    public async Task ExpiredHeadAndTombstone_RouteTheReadToTheActor()
    {
        await using ActorRig rig = await ActorRig.CreateAsync(this);

        await rig.AskAsync(Set("exp/key", "v1", expiresMs: 1));
        await Task.Delay(30, TestContext.Current.CancellationToken);
        Assert.False(rig.Reader.TryRead(HLCTimestamp.Zero, "exp/key", -1, HLCTimestamp.Zero, KeyValueDurability.Ephemeral, out _), "expired");
        KeyValueResponse? expired = await rig.AskAsync(Get("exp/key"));
        Assert.Equal(KeyValueResponseType.DoesNotExist, expired!.Type);

        await rig.AskAsync(Set("del/key", "v1"));
        Assert.True(rig.Reader.TryRead(HLCTimestamp.Zero, "del/key", -1, HLCTimestamp.Zero, KeyValueDurability.Ephemeral, out _));
        KeyValueResponse? deleted = await rig.AskAsync(Request(KeyValueRequestType.TryDelete, "del/key", HLCTimestamp.Zero));
        Assert.Equal(KeyValueResponseType.Deleted, deleted!.Type);
        Assert.False(rig.Reader.TryRead(HLCTimestamp.Zero, "del/key", -1, HLCTimestamp.Zero, KeyValueDurability.Ephemeral, out _), "tombstone");
        Assert.False(rig.Context.PublishedHeads.TryGet("del/key", out _));
    }

    [Fact]
    public async Task TransactionalRead_PinsOnce_ThenAnswersFromTheActor_AndAbortsAfterAWrite()
    {
        await using ActorRig rig = await ActorRig.CreateAsync(this);
        const string key = "tx/key";
        await rig.AskAsync(Set(key, "v1"));
        HLCTimestamp tx = rig.Now();
        HLCTimestamp otherTx = rig.Now();

        Assert.True(rig.Reader.TryRead(tx, key, -1, HLCTimestamp.Zero, KeyValueDurability.Ephemeral, out ReadOnlyKeyValueEntry? first));
        Assert.Equal("v1", Encoding.UTF8.GetString(first!.Value!));

        // The pin lands on the actor as a control message and flags the record as held.
        await WaitUntil(() => rig.Context.PublishedHeads.TryGet(key, out PublishedHead h) && !h.IsRetracted && h.MvccHeld);
        KeyValueEntry resident = rig.Context.Store.Get(key)!;
        Assert.True(resident.MvccEntries!.ContainsKey(tx));
        Assert.Equal(first.Revision, resident.MvccEntries[tx].Revision);

        // Transactional reads now go to the actor; non-transactional reads keep the record.
        Assert.False(rig.Reader.TryRead(tx, key, -1, HLCTimestamp.Zero, KeyValueDurability.Ephemeral, out _), "own repeat read");
        Assert.False(rig.Reader.TryRead(otherTx, key, -1, HLCTimestamp.Zero, KeyValueDurability.Ephemeral, out _), "other transaction");
        Assert.True(rig.Reader.TryRead(HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Ephemeral, out _), "plain read");

        KeyValueResponse? repeat = await rig.AskAsync(Get(key, tx));
        Assert.Equal(KeyValueResponseType.Get, repeat!.Type);
        Assert.Equal(first.Revision, repeat.Entry!.Revision);
        Assert.Single(resident.MvccEntries);

        await rig.AskAsync(Set(key, "v2"));

        KeyValueResponse? afterWrite = await rig.AskAsync(Get(key, tx));
        Assert.Equal(KeyValueResponseType.Aborted, afterWrite!.Type);
    }

    [Fact]
    public async Task PersistentKey_NewerUnflushedWriteOrPendingApply_RoutesTheReadToTheActor()
    {
        await using ActorRig rig = await ActorRig.CreateAsync(this, persistent: true);
        const string key = "ovl/key";
        HLCTimestamp now = rig.Now();
        rig.InsertResident(key, "v1", 1, now);

        Assert.True(rig.Reader.TryRead(HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, out ReadOnlyKeyValueEntry? e));
        Assert.Equal(1L, e!.Revision);

        // An older or equal head in the overlay is not newer than the record.
        rig.Unflushed.Record(key, "v1"u8.ToArray(), 1, HLCTimestamp.Zero, now, now, KeyValueState.Set, false);
        Assert.True(rig.Reader.TryRead(HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, out _));

        HLCTimestamp later = rig.Now();
        rig.Unflushed.Record(key, "v2"u8.ToArray(), 2, HLCTimestamp.Zero, later, later, KeyValueState.Set, false);
        Assert.False(rig.Reader.TryRead(HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, out _), "newer unflushed write");
        rig.Unflushed.RemoveFlushed(key, 2, later);
        Assert.True(rig.Reader.TryRead(HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, out _));

        rig.Ring.NotePendingApply(key, 2, later);
        Assert.False(rig.Reader.TryRead(HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, out _), "pending apply");
        rig.Context.PublishedHeads.ClearPendingApply(key, 2, later);
        Assert.True(rig.Reader.TryRead(HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, out _));
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
        Assert.True(rig.Reader.TryRead(HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, out _));

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
        Assert.False(rig.Reader.TryRead(HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, out _), "while the apply is pending");

        // Even with the overlay already flushed (the flush raced the actor), the pending note alone blocks.
        rig.Unflushed.RemoveFlushed(key, 2, later);
        Assert.False(rig.Reader.TryRead(HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, out _), "pending note alone");

        hold.Release.SetResult();
        await turn;

        KeyValueResponse? viaActor = await rig.AskAsync(Get(key, durability: KeyValueDurability.Persistent));
        Assert.Equal(KeyValueResponseType.Get, viaActor!.Type);
        Assert.Equal(2L, viaActor.Entry!.Revision);

        Assert.Equal(0, rig.Context.PublishedHeads.PendingApplyCount);
        Assert.True(rig.Reader.TryRead(HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, out ReadOnlyKeyValueEntry? fresh));
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

    private static KeyValueRequest Get(string key, HLCTimestamp transactionId = default, KeyValueDurability durability = KeyValueDurability.Ephemeral) =>
        new(KeyValueRequestType.TryGet, transactionId, HLCTimestamp.Zero, key, null, null, -1, KeyValueFlags.None, 0, HLCTimestamp.Zero, durability, 0, 0, default);

    /// <summary>A turn that parks the actor until released, so a test can open a window on its mailbox.</summary>
    private sealed class HoldTurn : IKeyValueActorTurn
    {
        public readonly TaskCompletionSource Started = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public readonly TaskCompletionSource Release = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public async ValueTask RunAsync(IKeyValueInlineDispatcher actor)
        {
            Started.TrySetResult();
            await Release.Task;
        }
    }

    /// <summary>
    /// One real key-value actor behind a one-actor ring, with the reader wired to that ring's table, a bare
    /// unflushed-writes overlay, and an empty prepared-intent store. The same actor serves as both rings so a
    /// read of the wrong durability finds a table with nothing in it for the key.
    /// </summary>
    private sealed class ActorRig : IAsyncDisposable
    {
        public KeyValueActorRing Ring = null!;
        public KeyValueContext Context = null!;
        public PublishedHeadReader Reader = null!;
        public UnflushedKeyValueWritesIndex Unflushed = new();
        private RaftManager raft = null!;
        private IDisposable actorSystemLifetime = null!;
        private bool persistent;

        public static async Task<ActorRig> CreateAsync(TestPublishedHeadReads owner, bool persistent = false)
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
                    logger);

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
            KeyValueActorRing other = new([actorRef], [new PublishedHeadTable()]);
            rig.Reader = new PublishedHeadReader(
                persistent ? other : rig.Ring,
                persistent ? rig.Ring : other,
                rig.Unflushed,
                new PreparedIntentStore(),
                rig.raft,
                enabled: true);

            return rig;
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
