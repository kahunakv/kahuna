using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Handlers;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kahuna.Utils;
using Kommander;
using Kommander.Communication.Memory;
using Kommander.Discovery;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kahuna.Server.Tests;

/// <summary>
/// Coverage for the stale-hydration refusal: a persistent cache-miss load whose row (or absence) sits
/// strictly below the staged-base fence's committed-head memory is provably missing committed history —
/// the state a freshly promoted leader with a cold cache serves right before a read-modify-write silently
/// discards the missing writes. The read paths must refuse such a row (MustRetry) and schedule the
/// convergence repair instead of installing it as the key's base.
///
/// <para>Driven at the store and helper level so the decision logic is deterministic; the handler call
/// sites are thin guards over <see cref="BaseHandler.HydratedRowProvablyStale(KeyValueContext, string, KeyValueEntry?)"/>.</para>
/// </summary>
public sealed class TestStaleHydrationFence : RaftTrackingTest
{
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

    private KeyValueContext BuildContext(string raftName, PreparedIntentStore? intentStore)
    {
        RaftManager raft = BuildRaft(raftName);

        return new(
            actorContext: null!,
            store: new BTree<string, KeyValueEntry>(32),
            locksByPrefix: [],
            locksByRange: [],
            proposals: [],
            backgroundWriter: null!,
            writeAggregator: null!,
            persistenceBackend: null!,
            raft: raft,
            backendReadScheduler: null!,
            keySpaceRegistry: new(),
            rangeMapStore: new(raft, null, null, NullLogger<IKahuna>.Instance),
            configuration: ConfigurationValidator.Validate(new()
            {
                LocksWorkers = 1,
                KeyValueWorkers = 1,
                BackgroundWriterWorkers = 1,
                Storage = "memory"
            }),
            logger: NullLogger<IKahuna>.Instance,
            preparedIntentStore: intentStore);
    }

    private static PreparedIntent MakeIntent(string key, long txPhysical, long revision) => new(
        TransactionId: new HLCTimestamp(0, txPhysical, 0), Epoch: 1, Key: key,
        ManifestHash: 0, RecordAnchorKey: key,
        CommitTimestamp: new HLCTimestamp(0, txPhysical + 1, 0),
        State: KeyValueState.Set, Value: [1, 2, 3], Bucket: null,
        Revision: revision, Expires: HLCTimestamp.Zero, NoRevision: false,
        BaseRevision: revision - 1, BaseState: KeyValueState.Set,
        RecoveryDeadline: HLCTimestamp.Zero, Resolution: PreparedIntentResolution.Pending);

    /// <summary>Runs one commit lifecycle through the store so its head is remembered by the fence memory.</summary>
    private static void CommitThroughStore(PreparedIntentStore store, PreparedIntent intent)
    {
        Assert.Equal(TransactionApplyOutcome.Applied, store.Apply(new PrepareIntentCommand(intent)).Outcome);
        Assert.Equal(TransactionApplyOutcome.Applied,
            store.Apply(new ResolveIntentCommand(intent.TransactionId, intent.Epoch, intent.Key, Commit: true)).Outcome);
        Assert.Equal(TransactionApplyOutcome.Applied,
            store.Apply(new RemoveIntentCommand(intent.TransactionId, intent.Epoch, intent.Key)).Outcome);
    }

    [Fact]
    public void TryGetCommittedHead_ReturnsTheSettledCommit_AndNothingForUnknownKeys()
    {
        PreparedIntentStore store = new();

        CommitThroughStore(store, MakeIntent("k/head", txPhysical: 1_000, revision: 6));

        Assert.True(store.TryGetCommittedHead("k/head", out long revision, out KeyValueState state));
        Assert.Equal(6, revision);
        Assert.Equal(KeyValueState.Set, state);

        Assert.False(store.TryGetCommittedHead("k/unknown", out _, out _));
    }

    [Fact]
    public void RequestConvergenceRepair_FiresTheWiredHook_OnlyBelowTheHead()
    {
        PreparedIntentStore store = new();
        CommitThroughStore(store, MakeIntent("k/repair", txPhysical: 1_000, revision: 6));

        List<(string Key, long Observed, long Head)> repairs = [];
        store.AttachFenceWedgeRepairer((key, observed, head) => repairs.Add((key, observed, head)));

        // Observed at or above the head: benign, no repair.
        Assert.False(store.RequestConvergenceRepair("k/repair", observedRevision: 6));
        Assert.False(store.RequestConvergenceRepair("k/repair", observedRevision: 9));
        Assert.Empty(repairs);

        // Observed strictly below the head: the repair is requested with both revisions.
        Assert.True(store.RequestConvergenceRepair("k/repair", observedRevision: 2));
        (string, long, long) repair = Assert.Single(repairs);
        Assert.Equal(("k/repair", 2L, 6L), repair);

        // A key with no remembered head proves nothing and requests nothing.
        Assert.False(store.RequestConvergenceRepair("k/unknown", observedRevision: -1));
        Assert.Single(repairs);
    }

    [Fact]
    public void HydratedRow_AtOrAboveTheHead_IsNotStale()
    {
        PreparedIntentStore store = new();
        CommitThroughStore(store, MakeIntent("k/current", txPhysical: 1_000, revision: 6));
        KeyValueContext context = BuildContext("stale-hyd-current", store);

        Assert.False(BaseHandler.HydratedRowProvablyStale(context, "k/current",
            new KeyValueEntry { Revision = 6, State = KeyValueState.Set }));
        Assert.False(BaseHandler.HydratedRowProvablyStale(context, "k/current",
            new KeyValueEntry { Revision = 7, State = KeyValueState.Set }));

        // No remembered head: absence is not proof of staleness.
        Assert.False(BaseHandler.HydratedRowProvablyStale(context, "k/unknown",
            new KeyValueEntry { Revision = 1, State = KeyValueState.Set }));
        Assert.False(BaseHandler.HydratedRowProvablyStale(context, "k/unknown", null));
    }

    [Fact]
    public void HydratedRow_BelowTheHead_IsRefused_CountsAndSchedulesTheRepair()
    {
        PreparedIntentStore store = new();
        CommitThroughStore(store, MakeIntent("k/stale", txPhysical: 1_000, revision: 6));
        KeyValueContext context = BuildContext("stale-hyd-below", store);

        List<(string Key, long Observed, long Head)> repairs = [];
        store.AttachFenceWedgeRepairer((key, observed, head) => repairs.Add((key, observed, head)));

        long refusedBefore = DurableTransactionMetrics.StaleHydrationsRefusedCount;

        Assert.True(BaseHandler.HydratedRowProvablyStale(context, "k/stale",
            new KeyValueEntry { Revision = 3, State = KeyValueState.Set }));

        Assert.True(DurableTransactionMetrics.StaleHydrationsRefusedCount >= refusedBefore + 1);
        Assert.Contains(("k/stale", 3L, 6L), repairs);
    }

    [Fact]
    public void AbsentRow_IsStaleAgainstASetHead_ButNotAgainstADelete()
    {
        PreparedIntentStore store = new();
        CommitThroughStore(store, MakeIntent("k/vanished", txPhysical: 1_000, revision: 6));
        KeyValueContext context = BuildContext("stale-hyd-absent", store);

        // The head says a committed value exists; hydrating nothing means the local durable state
        // lost it — stale.
        Assert.True(BaseHandler.HydratedRowProvablyStale(context, "k/vanished", null));

        // A committed transactional delete legitimately hydrates as absent (its tombstone may have
        // been pruned) — not stale.
        PreparedIntent delete = MakeIntent("k/gone", txPhysical: 2_000, revision: 4) with
        {
            State = KeyValueState.Deleted, Value = null
        };
        CommitThroughStore(store, delete);

        Assert.False(BaseHandler.HydratedRowProvablyStale(context, "k/gone", null));
    }

    [Fact]
    public void ContextWithoutAnIntentStore_NeverRefuses()
    {
        KeyValueContext context = BuildContext("stale-hyd-nostore", intentStore: null);

        Assert.False(BaseHandler.HydratedRowProvablyStale(context, "k/any",
            new KeyValueEntry { Revision = 0, State = KeyValueState.Set }));
        Assert.False(BaseHandler.HydratedRowProvablyStale(context, "k/any", null));
    }
}
