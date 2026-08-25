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

    /// <summary>Installs a resident entry at revision 5 carrying <paramref name="intentOwner"/>'s staged
    /// write intent and MVCC snapshot — the state a partition leader holds for a key mid-2PC.</summary>
    private static KeyValueEntry SeedEntryWithIntent(KeyValueContext context, string key, HLCTimestamp intentOwner)
    {
        KeyValueEntry entry = new()
        {
            Bucket = null,
            Value = "v5"u8.ToArray(),
            Revision = 5,
            FlushedRevision = 5,
            State = KeyValueState.Set,
            LastModified = Ts(5_000),
            CachedBytes = 100_000,
            WriteIntent = new() { TransactionId = intentOwner, Expires = HLCTimestamp.Zero },
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

    [Fact]
    public void ForeignLiveIntentNotification_StillDefers()
    {
        Harness h = BuildHarness("inv-foreign-defer");
        HLCTimestamp holder = Ts(1_000);
        HLCTimestamp other = Ts(2_000);
        KeyValueEntry entry = SeedEntryWithIntent(h.Context, "acct/b", holder);

        KeyValueResponse? response = h.Handler.Execute(
            MaterializationOf("acct/b", other, revision: 6, "v6"u8.ToArray(), Ts(6_000), forceResident: false));

        Assert.Null(response);
        Assert.Equal(5, entry.Revision);
        Assert.NotNull(entry.WriteIntent);
        Assert.Equal(holder, entry.WriteIntent!.TransactionId);
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
