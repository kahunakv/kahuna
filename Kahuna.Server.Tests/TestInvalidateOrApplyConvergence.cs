using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Handlers;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Shared.KeyValue;
using Kahuna.Utils;
using Kommander;
using Kommander.Communication.Memory;
using Kommander.Discovery;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.Logging.Abstractions;
using Nixie;

namespace Kahuna.Server.Tests;

/// <summary>
/// Convergence coverage for the <c>InvalidateOrApply</c> apply paths, driven at the handler level so the
/// exact interleavings that wedged hot keys in the Caraxes soaks are deterministic here:
///
/// <list type="bullet">
///   <item>A replicated materialization whose transaction owns the entry's write intent must APPLY, not
///   defer: the routed force-resident apply reaches only the partition leader at resolution time, so on any
///   other replica still holding the intent this notification is the only signal that ever clears it.
///   Deferring froze the entry at the superseded revision while the staged-base fence's committed head
///   advanced — every later read-modify-write refused forever at a frozen validated/head pair.</item>
///   <item>A late force-resident commit-apply on a non-resident key must not install its (possibly
///   superseded) mutation over a newer durable row. The handler itself must do no backend I/O (an in-actor
///   await parks the mailbox and expires request batches), so it answers MustRetry un-hydrated and applies
///   against the sender's off-actor point read on the second ask.</item>
///   <item>The full wedge sequence must converge: a commit whose materialization never reached the entry
///   yields one truthful fence refusal, the settle-observer repair re-drives the mutation, and the next
///   validated-base prepare (after the client's re-read) is acknowledged.</item>
/// </list>
/// </summary>
public sealed class TestInvalidateOrApplyConvergence : RaftTrackingTest
{
    private static HLCTimestamp Ts(long physical) => new(0, physical, 0);

    private RaftManager BuildRaft(string name)
    {
        return Track(new RaftManager(
            new RaftConfiguration
            {
                NodeName = name,
                NodeId = 1,
                Host = "localhost",
                Port = 0,
                InitialPartitions = 2,
                EnableQuiescence = false, PartitionExecutorPoolSize = 1
            },
            new StaticDiscovery([]),
            new InMemoryWAL(NullLogger<IRaft>.Instance),
            new InMemoryCommunication(),
            new HybridLogicalClock(),
            NullLogger<IRaft>.Instance));
    }

    private static KahunaConfiguration BuildConfig()
    {
        return ConfigurationValidator.Validate(new()
        {
            LocksWorkers = 1,
            KeyValueWorkers = 1,
            BackgroundWriterWorkers = 1,
            Storage = "memory",
            RevisionRetention = 4,
            MaxEntriesPerActor = 50_000,
            MaxBytesPerActor = 256L * 1024 * 1024,
            CacheEntriesToRemove = 1000,
            CollectBatchMax = 1000,
            CacheEntryTtl = TimeSpan.FromMinutes(5)
        });
    }

    private sealed record Harness(
        KeyValueContext Context,
        InvalidateOrApplyHandler Handler,
        BTree<string, KeyValueEntry> Store,
        RaftManager Raft);

    private Harness BuildHarness(
        string raftName,
        IPersistenceBackend? backend = null,
        IActorRef<BackgroundWriterActor, BackgroundWriteRequest>? backgroundWriter = null,
        CompletionReceiptStore? receiptStore = null)
    {
        RaftManager raft = BuildRaft(raftName);
        BTree<string, KeyValueEntry> store = new(32);

        KeyValueContext context = new(
            actorContext: null!,
            store: store,
            locksByPrefix: [],
            locksByRange: [],
            proposals: [],
            backgroundWriter: backgroundWriter!,
            writeAggregator: null!,
            persistenceBackend: backend!,
            raft: raft,
            // Deliberately null: the handler's contract forbids any backend I/O inside the actor loop, so no
            // path in these tests may ever reach the read scheduler — a violation fails loudly here instead
            // of silently re-introducing the mailbox-parking regression.
            backendReadScheduler: null!,
            keySpaceRegistry: new(),
            rangeMapStore: new(raft, null, null, NullLogger<IKahuna>.Instance),
            configuration: BuildConfig(),
            logger: NullLogger<IKahuna>.Instance,
            completionReceiptStore: receiptStore);

        return new(context, new InvalidateOrApplyHandler(context), store, raft);
    }

    /// <summary>
    /// Installs a resident entry at revision 5 carrying <paramref name="intentOwner"/>'s staged write intent
    /// and MVCC snapshot — the state a partition leader holds for a key mid-2PC.
    ///
    /// <para>The intent is stamped from the harness clock, so it is live under the session-owned liveness
    /// ceiling. Pass <paramref name="orphaned"/> to age that stamp past the ceiling instead, which is the
    /// holder whose session can no longer exist — the policy drops such a holder on sight.</para>
    /// </summary>
    private static KeyValueEntry SeedEntryWithIntent(
        KeyValueContext context, string key, HLCTimestamp intentOwner, bool orphaned = false)
    {
        HLCTimestamp now = context.Raft.HybridLogicalClock.TrySendOrLocalEvent(context.Raft.GetLocalNodeId());
        HLCTimestamp acquiredAt = orphaned ? now - context.SessionOwnedIntentCeilingMs - 1_000 : now;

        KeyValueEntry entry = new()
        {
            Bucket = null,
            Value = "v5"u8.ToArray(),
            Revision = 5,
            FlushedRevision = 5,
            State = KeyValueState.Set,
            LastModified = Ts(5_000),
            CachedBytes = 100_000,
            WriteIntent = new() { TransactionId = intentOwner, Expires = HLCTimestamp.Zero, AcquiredAt = acquiredAt },
            MvccEntries = new()
            {
                [intentOwner] = new KeyValueMvccEntry
                {
                    Value = "v6"u8.ToArray(),
                    Revision = 6,
                    LastModified = Ts(6_000),
                    State = KeyValueState.Set
                }
            }
        };

        context.InsertStoreEntry(key, entry);
        return entry;
    }

    private static KeyValueRequest MaterializationOf(
        string key, HLCTimestamp transactionId, long revision, byte[] value, HLCTimestamp lastModified, bool forceResident,
        bool backendHydrated = false, ReadOnlyKeyValueEntry? hydratedEntry = null)
    {
        return KeyValueRequestPool.RentInvalidateOrApply(
            key,
            revision,
            value,
            expires: HLCTimestamp.Zero,
            lastUsed: lastModified,
            lastModified: lastModified,
            state: KeyValueState.Set,
            forceResident: forceResident,
            transactionId: transactionId,
            partitionId: 1,
            noRevision: false,
            isRollback: false,
            returnToPoolOnReceive: false,
            backendHydrated: backendHydrated,
            hydratedEntry: hydratedEntry);
    }

    [Fact]
    public void OwnTransactionNotification_AppliesCommittedMaterialization()
    {
        Harness h = BuildHarness("inv-own-apply");
        HLCTimestamp tx = Ts(1_000);
        KeyValueEntry entry = SeedEntryWithIntent(h.Context, "acct/a", tx);

        KeyValueResponse? response = h.Handler.Execute(
            MaterializationOf("acct/a", tx, revision: 6, "v6"u8.ToArray(), Ts(6_000), forceResident: false));

        Assert.Null(response);
        Assert.Equal(6, entry.Revision);
        Assert.Equal("v6"u8.ToArray(), entry.Value);
        Assert.Null(entry.WriteIntent);
        Assert.True(entry.MvccEntries is null || !entry.MvccEntries.ContainsKey(tx));
        Assert.Equal(tx, entry.LastAppliedTransactionId);
        Assert.True(h.Context.WasCommittedHere(tx));

        // The superseded revision was archived exactly once.
        Assert.NotNull(entry.Revisions);
        Assert.True(entry.Revisions!.TryGetValue(5, out KeyValueRevisionEntry archived));
        Assert.Equal("v5"u8.ToArray(), archived.Value);

        // A replay of the same notification is an idempotent no-op.
        h.Handler.Execute(
            MaterializationOf("acct/a", tx, revision: 6, "v6"u8.ToArray(), Ts(6_000), forceResident: false));
        Assert.Equal(6, entry.Revision);
        Assert.Equal(1, entry.Revisions!.Count);

        // The routed force-resident apply arriving second degrades to an idempotent no-op too.
        KeyValueResponse? forced = h.Handler.Execute(
            MaterializationOf("acct/a", tx, revision: 6, "v6"u8.ToArray(), Ts(6_000), forceResident: true));
        Assert.Equal(KeyValueResponseType.Committed, forced?.Type);
        Assert.Equal(6, entry.Revision);
        Assert.Equal(1, entry.Revisions!.Count);
    }

    /// <summary>
    /// A committed head strictly above the resident entry, arriving while a LIVE foreign intent holds
    /// the key, must apply and clear the intent — not defer. The committed history moving past the
    /// staged base proves the intent's owner can never commit here (the streak-triggered coherence
    /// reconcile makes exactly this call from the durable row), and the notification is single-shot:
    /// deferring it left the resident entry behind the node's own applied committed state, which a
    /// read gated by a quorum-confirmed leadership check then served as a stale snapshot.
    /// </summary>
    [Fact]
    public void ForeignLiveIntentNotification_StrictlyNewerAppliesAndClearsTheIntent()
    {
        Harness h = BuildHarness("inv-foreign-newer");
        HLCTimestamp holder = Ts(1_000);
        HLCTimestamp other = Ts(2_000);
        KeyValueEntry entry = SeedEntryWithIntent(h.Context, "acct/b", holder);

        KeyValueResponse? response = h.Handler.Execute(
            MaterializationOf("acct/b", other, revision: 6, "v6"u8.ToArray(), Ts(6_000), forceResident: false));

        Assert.Null(response);
        Assert.Equal(6, entry.Revision);
        Assert.Equal("v6"u8.ToArray(), entry.Value);
        Assert.Null(entry.WriteIntent);
        Assert.True(entry.MvccEntries is null || !entry.MvccEntries.ContainsKey(holder));

        // The superseded revision was archived, so snapshot readers can still resolve it.
        Assert.NotNull(entry.Revisions);
        Assert.True(entry.Revisions!.TryGetValue(5, out KeyValueRevisionEntry archived));
        Assert.Equal("v5"u8.ToArray(), archived.Value);
    }

    /// <summary>
    /// A replay at-or-below the resident head keeps the defer: the guards below would no-op it
    /// anyway, and the live intent may belong to an active transaction on this very leader.
    /// </summary>
    [Fact]
    public void ForeignLiveIntentNotification_ReplayStillDefers()
    {
        Harness h = BuildHarness("inv-foreign-defer");
        HLCTimestamp holder = Ts(1_000);
        HLCTimestamp other = Ts(2_000);
        KeyValueEntry entry = SeedEntryWithIntent(h.Context, "acct/b2", holder);

        KeyValueResponse? response = h.Handler.Execute(
            MaterializationOf("acct/b2", other, revision: 4, "v4"u8.ToArray(), Ts(4_000), forceResident: false));

        Assert.Null(response);
        Assert.Equal(5, entry.Revision);
        Assert.NotNull(entry.WriteIntent);
        Assert.Equal(holder, entry.WriteIntent!.TransactionId);
    }

    /// <summary>
    /// The same notification behind a holder whose session can no longer exist must not defer. Deferring
    /// froze the entry for the life of the process: reads kept serving the superseded revision while the
    /// staged-base fence's committed head advanced on the replicated settle, refusing every later
    /// read-modify-write of the key. The single-shot notification is the only signal that clears the intent
    /// on a replica that lost leadership mid-transaction, so once the holder is provably gone the apply must
    /// go through — that is the wedge healing instead of forming.
    /// </summary>
    [Fact]
    public void ForeignOrphanedIntentNotification_AppliesAndClearsTheIntent()
    {
        Harness h = BuildHarness("inv-foreign-orphan");
        HLCTimestamp holder = Ts(1_000);
        HLCTimestamp other = Ts(2_000);
        KeyValueEntry entry = SeedEntryWithIntent(h.Context, "acct/c", holder, orphaned: true);

        KeyValueResponse? response = h.Handler.Execute(
            MaterializationOf("acct/c", other, revision: 6, "v6"u8.ToArray(), Ts(6_000), forceResident: false));

        Assert.Null(response);
        Assert.Equal(6, entry.Revision);
        Assert.Equal("v6"u8.ToArray(), entry.Value);
        Assert.Null(entry.WriteIntent);
    }

    [Fact]
    public void NonResidentUnhydratedApply_AnswersMustRetry_WithoutTouchingTheStore()
    {
        Harness h = BuildHarness("inv-unhydrated");

        // First step of the two-step hydration protocol: the actor answers MustRetry so the SENDER performs
        // the backend read off the actor — the handler itself must do no I/O and install nothing.
        KeyValueResponse? response = h.Handler.Execute(
            MaterializationOf("acct/u", Ts(1_000), revision: 6, "v6"u8.ToArray(), Ts(6_000), forceResident: true));

        Assert.Equal(KeyValueResponseType.MustRetry, response?.Type);
        Assert.False(h.Store.TryGetValue("acct/u", out _));
    }

    [Fact]
    public void LateForceResidentApply_DoesNotShadowNewerDurableRow()
    {
        Harness h = BuildHarness("inv-late-apply");

        // The durable truth: the key's committed row is already at revision 7 (a whole-partition install or a
        // flush landed it), while the actor-resident entry was evicted. The sender's off-actor point read
        // found that row and hands it in as the hydrated base.
        ReadOnlyKeyValueEntry persisted = new("v7"u8.ToArray(), 7, HLCTimestamp.Zero, Ts(7_000), Ts(7_000), KeyValueState.Set);

        // A commit-apply for the SUPERSEDED revision 6 arrives late (a stalled resolution leg finally
        // landing). It must adopt the hydrated row and no-op instead of installing revision 6 as the head.
        KeyValueResponse? response = h.Handler.Execute(
            MaterializationOf("acct/c", Ts(1_000), revision: 6, "v6"u8.ToArray(), Ts(6_000), forceResident: true,
                backendHydrated: true, hydratedEntry: persisted));

        Assert.Equal(KeyValueResponseType.Committed, response?.Type);
        Assert.True(h.Store.TryGetValue("acct/c", out KeyValueEntry? resident));
        Assert.Equal(7, resident!.Revision);
        Assert.Equal("v7"u8.ToArray(), resident.Value);
    }

    [Fact]
    public void HydratedApplyOnAFreshKey_InstallsTheCommittedMutation()
    {
        using IDisposable lifetime = TestActorSystemLifetime.Create(out ActorSystem actorSystem);

        RaftManager writerRaft = BuildRaft("inv-fresh-writer");
        IActorRef<BackgroundWriterActor, BackgroundWriteRequest> writer =
            actorSystem.Spawn<BackgroundWriterActor, BackgroundWriteRequest>(
                "inv-fresh-bg", writerRaft, writerRaft.ReadScheduler, new MemoryPersistenceBackend(),
                null!, null!, new TransactionRecordStore(), new PreparedIntentStore(),
                BuildConfig(), NullLogger<IKahuna>.Instance, new FlushNotificationSink(), null!);

        Harness h = BuildHarness("inv-fresh-install", backgroundWriter: writer);

        // The sender's off-actor read found no persisted row: a genuinely fresh key (the seeding shape).
        // The hydrated apply must install the committed mutation as the visible head.
        KeyValueResponse? response = h.Handler.Execute(
            MaterializationOf("acct/new", Ts(1_000), revision: 0, "v0"u8.ToArray(), Ts(1_100), forceResident: true,
                backendHydrated: true, hydratedEntry: null));

        Assert.Equal(KeyValueResponseType.Committed, response?.Type);
        Assert.True(h.Store.TryGetValue("acct/new", out KeyValueEntry? resident));
        Assert.Equal(0, resident!.Revision);
        Assert.Equal("v0"u8.ToArray(), resident.Value);
        Assert.Equal(KeyValueState.Set, resident.State);
    }

    /// <summary>
    /// The deterministic wedge: a key's commit settles (the fence's committed head advances) while its
    /// materialization never reaches the visible entry. Read-modify-write attempts against the frozen entry
    /// must produce one truthful refusal — not an unbounded storm — and the settle-observer repair must
    /// converge the entry so the next attempt (validating the re-read, moved base) is acknowledged.
    /// </summary>
    [Fact]
    public void WedgedKey_RefusalThenRepair_Converges()
    {
        using IDisposable lifetime = TestActorSystemLifetime.Create(out ActorSystem actorSystem);

        MemoryPersistenceBackend backend = new();
        TransactionRecordStore records = new();
        PreparedIntentStore intents = new();
        CompletionReceiptStore receipts = new();

        RaftManager writerRaft = BuildRaft("inv-wedge-writer");
        IActorRef<BackgroundWriterActor, BackgroundWriteRequest> writer =
            actorSystem.Spawn<BackgroundWriterActor, BackgroundWriteRequest>(
                "inv-wedge-bg", writerRaft, writerRaft.ReadScheduler, backend,
                null!, null!, records, intents,
                BuildConfig(), NullLogger<IKahuna>.Instance, new FlushNotificationSink(), null!);

        Harness h = BuildHarness("inv-wedge", backend, writer, receipts);

        // The wedged replica's state: entry frozen at revision 5, still carrying T's staged intent.
        HLCTimestamp tx = Ts(1_000);
        KeyValueEntry entry = SeedEntryWithIntent(h.Context, "acct/w", tx);

        // T's commit settles through the intent store — the committed head reaches revision 6 on the
        // replicated path — but no materialization ever touches the entry. Capture what the settle
        // observer hands the repair wiring.
        List<PreparedIntent> observed = [];
        intents.AttachCommittedSettleObserver(observed.Add);

        PreparedIntent committed = new(
            TransactionId: tx, Epoch: 1, Key: "acct/w",
            ManifestHash: 0, RecordAnchorKey: "acct/w",
            CommitTimestamp: Ts(6_000),
            State: KeyValueState.Set, Value: "v6"u8.ToArray(), Bucket: null,
            Revision: 6, Expires: HLCTimestamp.Zero, NoRevision: false,
            BaseRevision: 5, BaseState: KeyValueState.Set,
            RecoveryDeadline: HLCTimestamp.Zero, Resolution: PreparedIntentResolution.Pending);

        Assert.Equal(TransactionApplyOutcome.Applied, intents.Apply(new PrepareIntentCommand(committed)).Outcome);
        Assert.Equal(TransactionApplyOutcome.Applied, intents.Apply(new ResolveIntentCommand(tx, 1, "acct/w", Commit: true)).Outcome);
        Assert.Equal(TransactionApplyOutcome.Applied, intents.Apply(new RemoveIntentCommand(tx, 1, "acct/w")).Outcome);

        Assert.NotEmpty(observed);
        Assert.Equal(6, observed[0].Revision);

        // A client that read the frozen entry validates base 5; the fence must refuse it (head is 6) —
        // truthfully, once, not as an unbounded storm on an unchanged pair.
        PreparedIntent staleAttempt = committed with { TransactionId = Ts(2_000), CommitTimestamp = Ts(7_000), Revision = 6 };
        PreparedIntentApplyResult refused = intents.Apply(new PrepareIntentCommand(staleAttempt));
        Assert.True(refused.StaleBase, "a prepare validated against the frozen entry must be refused");
        Assert.Equal(TransactionApplyOutcome.Applied, refused.Outcome);
        intents.Apply(new ResolveIntentCommand(staleAttempt.TransactionId, 1, "acct/w", Commit: false));
        intents.Apply(new RemoveIntentCommand(staleAttempt.TransactionId, 1, "acct/w"));

        // No completion receipt exists for T here, so the repair wiring re-drives the committed mutation.
        // The entry is resident (it still holds T's staged intent), so the apply is the single-ask sync
        // fast path — no hydration round trip.
        Assert.False(receipts.Contains(tx, "acct/w", KeyValueDurability.Persistent));
        KeyValueResponse? repaired = h.Handler.Execute(
            MaterializationOf("acct/w", tx, revision: 6, "v6"u8.ToArray(), Ts(6_000), forceResident: true));

        Assert.Equal(KeyValueResponseType.Committed, repaired?.Type);
        Assert.Equal(6, entry.Revision);
        Assert.Equal("v6"u8.ToArray(), entry.Value);
        Assert.Null(entry.WriteIntent);
        Assert.True(receipts.Contains(tx, "acct/w", KeyValueDurability.Persistent));

        // The client's next attempt re-reads the converged entry (revision 6) and passes the fence.
        PreparedIntent freshAttempt = committed with
        {
            TransactionId = Ts(3_000), CommitTimestamp = Ts(8_000),
            Revision = 7, BaseRevision = 6
        };
        PreparedIntentApplyResult acknowledged = intents.Apply(new PrepareIntentCommand(freshAttempt));
        Assert.Equal(TransactionApplyOutcome.Applied, acknowledged.Outcome);
        Assert.False(acknowledged.StaleBase, "after convergence the re-read base must be acknowledged");
    }

    private static KeyValueRequest ReconcileOf(string key, ReadOnlyKeyValueEntry? row)
    {
        return KeyValueRequestPool.RentInvalidateOrApply(
            key,
            row?.Revision ?? 0,
            row?.Value,
            expires: row?.Expires ?? HLCTimestamp.Zero,
            lastUsed: row?.LastModified ?? HLCTimestamp.Zero,
            lastModified: row?.LastModified ?? HLCTimestamp.Zero,
            state: row?.State ?? KeyValueState.Undefined,
            forceResident: false,
            transactionId: HLCTimestamp.Zero,
            partitionId: 1,
            noRevision: false,
            isRollback: false,
            returnToPoolOnReceive: false,
            backendHydrated: true,
            hydratedEntry: row,
            reconcile: true);
    }

    [Fact]
    public void Reconcile_AdoptsNewerDurableRow_ClearingALiveOrphanIntent()
    {
        Harness h = BuildHarness("inv-reconcile-adopt");

        // The run-S kernel state: the entry is frozen one revision behind the node's own durable row, held
        // there by a LIVE session-owned write intent orphaned by a superseded leadership (its cleanup was
        // routed to the then-current leader, never here; a zero-duration lease never expires on its own).
        HLCTimestamp orphan = Ts(500);
        KeyValueEntry entry = SeedEntryWithIntent(h.Context, "acct/r", orphan);
        Assert.NotNull(entry.WriteIntent);

        ReadOnlyKeyValueEntry durableRow = new("v6"u8.ToArray(), 6, HLCTimestamp.Zero, Ts(6_000), Ts(6_000), KeyValueState.Set);

        KeyValueResponse? response = h.Handler.Execute(ReconcileOf("acct/r", durableRow));

        Assert.Null(response);
        Assert.Equal(6, entry.Revision);
        Assert.Equal("v6"u8.ToArray(), entry.Value);
        Assert.Null(entry.WriteIntent);
        Assert.True(entry.MvccEntries is null || !entry.MvccEntries.ContainsKey(orphan));

        // The superseded revision was archived, so snapshot readers can still resolve it.
        Assert.NotNull(entry.Revisions);
        Assert.True(entry.Revisions!.TryGetValue(5, out KeyValueRevisionEntry archived));
        Assert.Equal("v5"u8.ToArray(), archived.Value);
    }

    [Fact]
    public void Reconcile_NoOps_WhenTheEntryIsCurrentOrAhead()
    {
        Harness h = BuildHarness("inv-reconcile-noop");
        HLCTimestamp holder = Ts(500);
        KeyValueEntry entry = SeedEntryWithIntent(h.Context, "acct/n", holder);

        // Row equal to the entry: nothing to converge; the live intent must be left alone (its owner may be
        // an active transaction on this very leader).
        ReadOnlyKeyValueEntry equalRow = new("v5"u8.ToArray(), 5, HLCTimestamp.Zero, Ts(5_000), Ts(5_000), KeyValueState.Set);
        h.Handler.Execute(ReconcileOf("acct/n", equalRow));
        Assert.Equal(5, entry.Revision);
        Assert.NotNull(entry.WriteIntent);

        // Row behind the entry: same.
        ReadOnlyKeyValueEntry olderRow = new("v4"u8.ToArray(), 4, HLCTimestamp.Zero, Ts(4_000), Ts(4_000), KeyValueState.Set);
        h.Handler.Execute(ReconcileOf("acct/n", olderRow));
        Assert.Equal(5, entry.Revision);
        Assert.NotNull(entry.WriteIntent);

        // No durable row at all: the entry is the only truth.
        h.Handler.Execute(ReconcileOf("acct/n", null));
        Assert.Equal(5, entry.Revision);
        Assert.NotNull(entry.WriteIntent);
    }

    [Fact]
    public void Reconcile_InstallsTheDurableRow_WhenTheKeyIsNotResident()
    {
        Harness h = BuildHarness("inv-reconcile-install");

        ReadOnlyKeyValueEntry durableRow = new("v9"u8.ToArray(), 9, HLCTimestamp.Zero, Ts(9_000), Ts(9_000), KeyValueState.Set);
        h.Handler.Execute(ReconcileOf("acct/m", durableRow));

        Assert.True(h.Store.TryGetValue("acct/m", out KeyValueEntry? resident));
        Assert.Equal(9, resident!.Revision);
        Assert.Equal("v9"u8.ToArray(), resident.Value);
        Assert.Equal(9, resident.FlushedRevision);
    }

    /// <summary>
    /// The run-S wedge, end to end: the head-advancing commit's ONE coherence notification never reaches
    /// the resident entry (a strictly-newer notification now applies over a live foreign intent, so the
    /// frozen state is seeded directly — it stands in for a signal lost before delivery, e.g. the entry
    /// was not resident and was later loaded from a lagging durable row); its settle advances the fence's
    /// committed head; every later read-modify-write validates one behind and is refused. The refusal
    /// streak must trigger the wedge-repair hook within a handful of refusals — not 157,036 — and the
    /// reconcile it drives must converge the entry from the node's own durable row so the next attempt
    /// (validating the re-read, moved base) is acknowledged.
    /// </summary>
    [Fact]
    public void RunSKernel_DroppedNotificationBehindLiveIntent_HealsViaStreakReconcile()
    {
        Harness h = BuildHarness("inv-runs-kernel");
        PreparedIntentStore intents = new();

        List<(string Key, long ValidatedBase, long CommittedHead)> repairRequests = [];
        intents.AttachFenceWedgeRepairer((key, validatedBase, committedHead) =>
            repairRequests.Add((key, validatedBase, committedHead)));

        // The healed old leader's state: entry at revision 5 with T0's session lock, whose cleanup went to
        // the then-current leader. The onset commit T (revision 6, base 5) elsewhere never reached this
        // entry; its settle applies below — the fence's committed head reaches 6 while the entry stays 5.
        HLCTimestamp holder = Ts(500);
        KeyValueEntry entry = SeedEntryWithIntent(h.Context, "acct/k", holder);

        HLCTimestamp tx = Ts(1_000);
        Assert.Equal(5, entry.Revision); // frozen: the entry never saw T's materialization

        PreparedIntent committed = new(
            TransactionId: tx, Epoch: 1, Key: "acct/k",
            ManifestHash: 0, RecordAnchorKey: "acct/k",
            CommitTimestamp: Ts(6_000),
            State: KeyValueState.Set, Value: "v6"u8.ToArray(), Bucket: null,
            Revision: 6, Expires: HLCTimestamp.Zero, NoRevision: false,
            BaseRevision: 5, BaseState: KeyValueState.Set,
            RecoveryDeadline: HLCTimestamp.Zero, Resolution: PreparedIntentResolution.Pending);

        intents.Apply(new PrepareIntentCommand(committed));
        intents.Apply(new ResolveIntentCommand(tx, 1, "acct/k", Commit: true));
        intents.Apply(new RemoveIntentCommand(tx, 1, "acct/k"));

        // The storm: each client attempt reads the frozen entry (5), validates base 5, is refused (head 6),
        // and its abort resolution rolls the installed intent back — then the next attempt repeats. The
        // repair hook must fire within the streak threshold.
        int attempts = 0;
        while (repairRequests.Count == 0 && attempts < 10)
        {
            attempts++;
            PreparedIntent staleAttempt = committed with { TransactionId = Ts(2_000 + attempts), CommitTimestamp = Ts(7_000 + attempts) };
            PreparedIntentApplyResult refused = intents.Apply(new PrepareIntentCommand(staleAttempt));
            Assert.True(refused.StaleBase, $"attempt {attempts} validated the frozen base and must be refused");
            intents.Apply(new ResolveIntentCommand(staleAttempt.TransactionId, 1, "acct/k", Commit: false));
            intents.Apply(new RemoveIntentCommand(staleAttempt.TransactionId, 1, "acct/k"));
        }

        Assert.Single(repairRequests);
        Assert.Equal("acct/k", repairRequests[0].Key);
        Assert.Equal(5, repairRequests[0].ValidatedBase);
        Assert.Equal(6, repairRequests[0].CommittedHead);
        Assert.True(attempts <= 6, $"the repair must trigger within a handful of refusals, took {attempts}");

        // The repair's reconcile: the node's own durable row has revision 6 (the replicator recorded and
        // flushed it before the notification was dropped). Adopting it clears the intent and converges.
        ReadOnlyKeyValueEntry durableRow = new("v6"u8.ToArray(), 6, HLCTimestamp.Zero, Ts(6_000), Ts(6_000), KeyValueState.Set);
        h.Handler.Execute(ReconcileOf("acct/k", durableRow));

        Assert.Equal(6, entry.Revision);
        Assert.Equal("v6"u8.ToArray(), entry.Value);
        Assert.Null(entry.WriteIntent);

        // The client's next attempt re-reads the converged entry and passes the fence.
        PreparedIntent freshAttempt = committed with
        {
            TransactionId = Ts(9_000), CommitTimestamp = Ts(9_100),
            Revision = 7, BaseRevision = 6
        };
        PreparedIntentApplyResult acknowledged = intents.Apply(new PrepareIntentCommand(freshAttempt));
        Assert.Equal(TransactionApplyOutcome.Applied, acknowledged.Outcome);
        Assert.False(acknowledged.StaleBase, "after the reconcile the re-read base must be acknowledged");
    }

    [Fact]
    public void LocalMaterializationGate_TrustsOnlyTheOverlayWitness()
    {
        PreparedIntent intent = new(
            TransactionId: Ts(1_000), Epoch: 1, Key: "gate/k",
            ManifestHash: 0, RecordAnchorKey: "gate/k",
            CommitTimestamp: Ts(1_100),
            State: KeyValueState.Set, Value: [1], Bucket: null,
            Revision: 9, Expires: HLCTimestamp.Zero, NoRevision: false,
            BaseRevision: 8, BaseState: KeyValueState.Set,
            RecoveryDeadline: HLCTimestamp.Zero, Resolution: PreparedIntentResolution.Pending);

        // No overlay configured: absence cannot be proven either way — the repair must run.
        Assert.True(KeyValueReplicator.LocalMaterializationMissing(null, intent));

        // Overlay has no entry for the key: this node's replicator never processed the commit's record.
        UnflushedKeyValueWritesIndex overlay = new();
        Assert.True(KeyValueReplicator.LocalMaterializationMissing(overlay, intent));

        // Overlay holds an OLDER head: the record's apply still never ran here.
        overlay.Record("gate/k", [0], 8, HLCTimestamp.Zero, Ts(900), Ts(900), KeyValueState.Set, noRevision: false);
        Assert.True(KeyValueReplicator.LocalMaterializationMissing(overlay, intent));

        // Overlay at the commit's revision: the replicator ran — the local durable read path serves it.
        overlay.Record("gate/k", [1], 9, HLCTimestamp.Zero, Ts(1_100), Ts(1_100), KeyValueState.Set, noRevision: false);
        Assert.False(KeyValueReplicator.LocalMaterializationMissing(overlay, intent));

        // Overlay already past the commit (a newer write queued): equally proven.
        overlay.Record("gate/k", [2], 10, HLCTimestamp.Zero, Ts(1_200), Ts(1_200), KeyValueState.Set, noRevision: false);
        Assert.False(KeyValueReplicator.LocalMaterializationMissing(overlay, intent));
    }

    [Fact]
    public void PendingCommitRepairRegistry_StaysArmedUntilConfirmed()
    {
        // Registry semantics only: the drive itself fails immediately against the null router and must leave
        // the parked mutation armed — the property run U lacked (a one-shot repair lost in the pause window
        // left the key read-only to run end).
        KeyValueReplicator replicator = new(
            null!, null!, null!, null!, null!, null!, NullLogger<IKahuna>.Instance);

        PreparedIntent MakeParked(long revision) => new(
            TransactionId: Ts(1_000 + revision), Epoch: 1, Key: "park/k",
            ManifestHash: 0, RecordAnchorKey: "park/k",
            CommitTimestamp: Ts(2_000 + revision),
            State: KeyValueState.Set, Value: [(byte)revision], Bucket: null,
            Revision: revision, Expires: HLCTimestamp.Zero, NoRevision: false,
            BaseRevision: revision - 1, BaseState: KeyValueState.Set,
            RecoveryDeadline: HLCTimestamp.Zero, Resolution: PreparedIntentResolution.Pending);

        // Parked before the drive, so even an instantly-failing drive leaves the mutation armed.
        replicator.ScheduleDurableCommitRepair(1, MakeParked(6));
        Assert.Equal(6, replicator.TryGetPendingCommitRepair("park/k")?.Revision);

        // An older arrival never downgrades the parked mutation; a newer one supersedes it.
        replicator.ScheduleDurableCommitRepair(1, MakeParked(5));
        Assert.Equal(6, replicator.TryGetPendingCommitRepair("park/k")?.Revision);
        replicator.ScheduleDurableCommitRepair(1, MakeParked(7));
        Assert.Equal(7, replicator.TryGetPendingCommitRepair("park/k")?.Revision);

        // A proven settle at a lower revision discards nothing; at or above, it releases the parked bytes.
        replicator.DiscardPendingCommitRepair("park/k", upToRevision: 6);
        Assert.Equal(7, replicator.TryGetPendingCommitRepair("park/k")?.Revision);
        replicator.DiscardPendingCommitRepair("park/k", upToRevision: 7);
        Assert.Null(replicator.TryGetPendingCommitRepair("park/k"));

        // With nothing armed, the streak hook's re-drive reports so and falls through to the reconcile.
        Assert.False(replicator.RetryPendingCommitRepair(1, "park/k"));
    }

    private static async Task WaitUntilUnparked(KeyValueReplicator replicator, string key, int timeoutMs = 5_000)
    {
        long deadline = Environment.TickCount64 + timeoutMs;
        while (replicator.TryGetPendingCommitRepair(key) is not null)
        {
            if (Environment.TickCount64 >= deadline)
                Assert.Fail($"repair for '{key}' was not released within {timeoutMs} ms");
            await Task.Delay(10, TestContext.Current.CancellationToken);
        }
    }

    [Fact]
    public async Task ScheduledRepair_ResolvesSilently_WhenTheOverlayProvesTheRowDurable()
    {
        // The common benign race: the settle-time witness missed, but the overlay (re-checked off the actor
        // by the repair task) holds the row — flushed-not-missing. The repair must release without driving.
        UnflushedKeyValueWritesIndex overlay = new();
        overlay.Record("verify/a", [6], 6, HLCTimestamp.Zero, Ts(6_000), Ts(6_000), KeyValueState.Set, noRevision: false);

        KeyValueReplicator replicator = new(
            null!, null!, null!, null!, null!, null!, NullLogger<IKahuna>.Instance,
            unflushedWrites: overlay);

        replicator.ScheduleDurableCommitRepair(1, new PreparedIntent(
            TransactionId: Ts(1_000), Epoch: 1, Key: "verify/a",
            ManifestHash: 0, RecordAnchorKey: "verify/a",
            CommitTimestamp: Ts(6_000),
            State: KeyValueState.Set, Value: [6], Bucket: null,
            Revision: 6, Expires: HLCTimestamp.Zero, NoRevision: false,
            BaseRevision: 5, BaseState: KeyValueState.Set,
            RecoveryDeadline: HLCTimestamp.Zero, Resolution: PreparedIntentResolution.Pending));

        await WaitUntilUnparked(replicator, "verify/a");
    }

    [Fact]
    public async Task ScheduledRepair_ResolvesSilently_WhenTheBackendProvesTheRowDurable()
    {
        // The overlay entry was removed by a landed flush; the hydration read finds the flushed row at (or
        // past) the intent's revision. Still flushed-not-missing: release without driving.
        KeyValueEntry flushedRow = new() { Revision = 7, Value = [7], State = KeyValueState.Set, LastModified = Ts(7_000) };

        KeyValueReplicator replicator = new(
            null!, null!, null!, null!, null!, null!, NullLogger<IKahuna>.Instance,
            hydrateFromBackend: (_, _) => Task.FromResult<KeyValueEntry?>(flushedRow));

        replicator.ScheduleDurableCommitRepair(1, new PreparedIntent(
            TransactionId: Ts(1_000), Epoch: 1, Key: "verify/b",
            ManifestHash: 0, RecordAnchorKey: "verify/b",
            CommitTimestamp: Ts(6_000),
            State: KeyValueState.Set, Value: [6], Bucket: null,
            Revision: 6, Expires: HLCTimestamp.Zero, NoRevision: false,
            BaseRevision: 5, BaseState: KeyValueState.Set,
            RecoveryDeadline: HLCTimestamp.Zero, Resolution: PreparedIntentResolution.Pending));

        await WaitUntilUnparked(replicator, "verify/b");
    }

    [Fact]
    public void CommittedSettleObserver_FiresOnCommit_NotOnAbort()
    {
        PreparedIntentStore intents = new();
        List<PreparedIntent> observed = [];
        intents.AttachCommittedSettleObserver(observed.Add);

        PreparedIntent committed = new(
            TransactionId: Ts(1_000), Epoch: 1, Key: "obs/commit",
            ManifestHash: 0, RecordAnchorKey: "obs/commit",
            CommitTimestamp: Ts(1_100),
            State: KeyValueState.Set, Value: [1], Bucket: null,
            Revision: 3, Expires: HLCTimestamp.Zero, NoRevision: false,
            BaseRevision: 2, BaseState: KeyValueState.Set,
            RecoveryDeadline: HLCTimestamp.Zero, Resolution: PreparedIntentResolution.Pending);

        intents.Apply(new PrepareIntentCommand(committed));
        Assert.Empty(observed);

        intents.Apply(new ResolveIntentCommand(committed.TransactionId, 1, "obs/commit", Commit: true));
        Assert.Single(observed);
        Assert.Equal(3, observed[0].Revision);

        intents.Apply(new RemoveIntentCommand(committed.TransactionId, 1, "obs/commit"));
        Assert.Equal(2, observed.Count); // the removal of a committed intent re-fires; the repair is idempotent

        observed.Clear();
        PreparedIntent aborted = committed with { TransactionId = Ts(2_000), Key = "obs/abort", RecordAnchorKey = "obs/abort" };
        intents.Apply(new PrepareIntentCommand(aborted));
        intents.Apply(new ResolveIntentCommand(aborted.TransactionId, 1, "obs/abort", Commit: false));
        intents.Apply(new RemoveIntentCommand(aborted.TransactionId, 1, "obs/abort"));
        Assert.Empty(observed);
    }
}
