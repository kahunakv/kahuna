using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Handlers;
using Kahuna.Server.KeyValues.Transactions;
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
/// The answer of the durable commit apply on the actor that owns the key. It says the transaction's own intent
/// was released only when the apply found that intent and removed it with the transaction's MVCC entry. The
/// committer leaves such a key out of its working-set release, so every other <c>Committed</c> answer must say
/// that nothing was released: on those the transaction can still hold an MVCC entry on the key.
/// </summary>
public sealed class TestCommitApplyReportsOwnIntentRelease : RaftTrackingTest
{
    private const string Key = "acct/k";

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

    private sealed record Harness(KeyValueContext Context, InvalidateOrApplyHandler Invalidate, TryReleaseExclusiveLockHandler Release);

    private Harness BuildHarness(string name, ActorSystem actorSystem)
    {
        RaftManager writerRaft = BuildRaft(name + "-writer");
        IActorRef<BackgroundWriterActor, BackgroundWriteRequest> writer =
            actorSystem.Spawn<BackgroundWriterActor, BackgroundWriteRequest>(
                name + "-bg", writerRaft, writerRaft.ReadScheduler, new MemoryPersistenceBackend(),
                null!, null!, new TransactionRecordStore(), new PreparedIntentStore(),
                BuildConfig(), NullLogger<IKahuna>.Instance, new FlushNotificationSink(), null!);

        RaftManager raft = BuildRaft(name);

        KeyValueContext context = new(
            actorContext: null!,
            store: new KeyValueStore(),
            locksByPrefix: [],
            locksByRange: [],
            proposals: [],
            backgroundWriter: writer,
            writeAggregator: null!,
            persistenceBackend: new MemoryPersistenceBackend(),
            raft: raft,
            backendReadScheduler: null!,
            keySpaceRegistry: new(),
            rangeMapStore: new(raft, null, null, NullLogger<IKahuna>.Instance),
            configuration: BuildConfig(),
            logger: NullLogger<IKahuna>.Instance);

        return new(context, new InvalidateOrApplyHandler(context), new TryReleaseExclusiveLockHandler(context));
    }

    private static HLCTimestamp Now(Harness h) =>
        h.Context.Raft.HybridLogicalClock.TrySendOrLocalEvent(h.Context.Raft.GetLocalNodeId());

    /// <summary>
    /// A resident entry at <paramref name="revision"/> that <paramref name="reader"/> has read (an MVCC entry at
    /// that revision) and that <paramref name="intentOwner"/> holds a live write intent on, when one is given.
    /// </summary>
    private static KeyValueEntry Seed(Harness h, long revision, HLCTimestamp reader, HLCTimestamp? intentOwner)
    {
        HLCTimestamp now = Now(h);

        KeyValueEntry entry = new()
        {
            Bucket = null,
            Value = "base"u8.ToArray(),
            Revision = revision,
            FlushedRevision = revision,
            State = KeyValueState.Set,
            LastModified = new(0, 5_000, 0),
            WriteIntent = intentOwner is { } owner
                ? new() { TransactionId = owner, Expires = now + 60_000, AcquiredAt = now }
                : null,
            MvccEntries = new()
            {
                [reader] = new()
                {
                    Value = "base"u8.ToArray(),
                    Revision = revision,
                    LastModified = new(0, 5_000, 0),
                    State = KeyValueState.Set
                }
            }
        };

        h.Context.InsertStoreEntry(Key, entry);
        return entry;
    }

    /// <summary>The apply of <paramref name="committer"/>'s value at revision 6, stamped 6,000.</summary>
    private static KeyValueRequest CommitApplyOf(HLCTimestamp committer) =>
        KeyValueRequestPool.RentInvalidateOrApply(
            Key,
            6,
            "v6"u8.ToArray(),
            expires: HLCTimestamp.Zero,
            lastUsed: new(0, 6_000, 0),
            lastModified: new(0, 6_000, 0),
            state: KeyValueState.Set,
            forceResident: true,
            transactionId: committer,
            partitionId: 1,
            noRevision: false,
            isRollback: false,
            returnToPoolOnReceive: false);

    private static KeyValueRequest ReleaseOf(HLCTimestamp transactionId) =>
        KeyValueRequestPool.Rent(
            KeyValueRequestType.TryReleaseExclusiveLock,
            transactionId,
            HLCTimestamp.Zero,
            Key,
            null,
            null,
            -1,
            KeyValueFlags.None,
            0,
            HLCTimestamp.Zero,
            KeyValueDurability.Persistent,
            0,
            0,
            default);

    private static bool HoldsNothing(KeyValueEntry entry, HLCTimestamp transactionId) =>
        (entry.WriteIntent is null || entry.WriteIntent.TransactionId != transactionId)
        && (entry.MvccEntries is null || !entry.MvccEntries.ContainsKey(transactionId));

    /// <summary>
    /// The committer owns the intent: the apply installs the value, removes the intent and the MVCC entry, and
    /// says so. A release of the key for the committer then changes nothing.
    /// </summary>
    [Fact]
    public async Task ApplyOverTheCommittersOwnIntent_ReportsTheRelease_AndLeavesNothingForARelease()
    {
        using IDisposable lifetime = TestActorSystemLifetime.Create(out ActorSystem actorSystem);
        Harness h = BuildHarness("apply-own-intent", actorSystem);
        HLCTimestamp committer = Now(h);
        KeyValueEntry entry = Seed(h, revision: 5, reader: committer, intentOwner: committer);

        KeyValueResponse? applied = h.Invalidate.Execute(CommitApplyOf(committer));

        Assert.Equal(KeyValueResponseType.Committed, applied?.Type);
        Assert.True(applied!.OwnIntentReleased);
        Assert.Equal(6, entry.Revision);
        Assert.Null(entry.WriteIntent);
        Assert.True(HoldsNothing(entry, committer));

        // What a release would do now: answer, and change nothing.
        KeyValueResponse released = await h.Release.Execute(ReleaseOf(committer));
        Assert.True(TransactionCoordinator.IsReleaseAcked(released.Type));
        Assert.Equal(6, entry.Revision);
        Assert.Null(entry.WriteIntent);
        Assert.Null(entry.MvccEntries);
    }

    /// <summary>
    /// A takeover record for the committer on the key is removed by the same apply, as a release removes it, so
    /// the reported release leaves no record behind.
    /// </summary>
    [Fact]
    public void ApplyOverTheCommittersOwnIntent_ForgetsATakeoverRecord()
    {
        using IDisposable lifetime = TestActorSystemLifetime.Create(out ActorSystem actorSystem);
        Harness h = BuildHarness("apply-forgets-takeover", actorSystem);
        HLCTimestamp committer = Now(h);
        Seed(h, revision: 5, reader: committer, intentOwner: committer);
        h.Context.RecordYieldedIntent(Key, committer, Now(h) + 60_000);

        KeyValueResponse? applied = h.Invalidate.Execute(CommitApplyOf(committer));

        Assert.True(applied!.OwnIntentReleased);
        Assert.False(h.Context.HasYieldedIntent(Key, committer));
        Assert.Equal(0, h.Context.YieldedIntentCount);
    }

    /// <summary>
    /// No intent on the key (a leader change or a lapsed lease dropped it): the apply installs the value, and
    /// reports no release, because it did not find the committer's intent.
    /// </summary>
    [Fact]
    public void ApplyWithNoIntentOnTheKey_ReportsNoRelease()
    {
        using IDisposable lifetime = TestActorSystemLifetime.Create(out ActorSystem actorSystem);
        Harness h = BuildHarness("apply-no-intent", actorSystem);
        HLCTimestamp committer = Now(h);
        KeyValueEntry entry = Seed(h, revision: 5, reader: committer, intentOwner: null);

        KeyValueResponse? applied = h.Invalidate.Execute(CommitApplyOf(committer));

        Assert.Equal(KeyValueResponseType.Committed, applied?.Type);
        Assert.False(applied!.OwnIntentReleased);
        Assert.Equal(6, entry.Revision);
    }

    /// <summary>
    /// Another transaction holds the intent: the apply installs the value, keeps that intent, and reports no
    /// release.
    /// </summary>
    [Fact]
    public void ApplyUnderAForeignIntent_ReportsNoRelease_AndKeepsTheIntent()
    {
        using IDisposable lifetime = TestActorSystemLifetime.Create(out ActorSystem actorSystem);
        Harness h = BuildHarness("apply-foreign-intent", actorSystem);
        HLCTimestamp committer = Now(h);
        HLCTimestamp holder = Now(h);
        KeyValueEntry entry = Seed(h, revision: 5, reader: holder, intentOwner: holder);

        KeyValueResponse? applied = h.Invalidate.Execute(CommitApplyOf(committer));

        Assert.Equal(KeyValueResponseType.Committed, applied?.Type);
        Assert.False(applied!.OwnIntentReleased);
        Assert.Equal(6, entry.Revision);
        Assert.Equal(holder, entry.WriteIntent?.TransactionId);
    }

    /// <summary>
    /// The head is already past the committed revision and the committer does not own the intent. The apply
    /// answers <c>Committed</c> and changes nothing, so the committer's MVCC entry is still on the key: the
    /// answer must not report a release, or that entry would stay until it is trimmed.
    /// </summary>
    [Fact]
    public async Task ApplyUnderANewerHead_ReportsNoRelease_AndTheCommittersEntryIsStillThere()
    {
        using IDisposable lifetime = TestActorSystemLifetime.Create(out ActorSystem actorSystem);
        Harness h = BuildHarness("apply-newer-head", actorSystem);
        HLCTimestamp committer = Now(h);
        KeyValueEntry entry = Seed(h, revision: 9, reader: committer, intentOwner: null);
        entry.LastModified = new(0, 9_000, 0);

        KeyValueResponse? applied = h.Invalidate.Execute(CommitApplyOf(committer));

        Assert.Equal(KeyValueResponseType.Committed, applied?.Type);
        Assert.False(applied!.OwnIntentReleased);
        Assert.Equal(9, entry.Revision);
        Assert.False(HoldsNothing(entry, committer));

        // The release is what removes the entry.
        KeyValueResponse released = await h.Release.Execute(ReleaseOf(committer));
        Assert.True(TransactionCoordinator.IsReleaseAcked(released.Type));
        Assert.True(HoldsNothing(entry, committer));
    }

    /// <summary>
    /// The head is this commit already, applied by the actor before: a replay. The apply answers
    /// <c>Committed</c> and changes nothing, so it reports no release.
    /// </summary>
    [Fact]
    public void ReplayOfAnAppliedCommit_ReportsNoRelease()
    {
        using IDisposable lifetime = TestActorSystemLifetime.Create(out ActorSystem actorSystem);
        Harness h = BuildHarness("apply-replay", actorSystem);
        HLCTimestamp committer = Now(h);
        KeyValueEntry entry = Seed(h, revision: 5, reader: committer, intentOwner: committer);

        Assert.True(h.Invalidate.Execute(CommitApplyOf(committer))!.OwnIntentReleased);

        // The committer reads the key again before the replay arrives; the replay must not claim that entry.
        entry.MvccEntries = new()
        {
            [committer] = new() { Value = entry.Value, Revision = entry.Revision, LastModified = entry.LastModified, State = entry.State }
        };

        KeyValueResponse? replayed = h.Invalidate.Execute(CommitApplyOf(committer));

        Assert.Equal(KeyValueResponseType.Committed, replayed?.Type);
        Assert.False(replayed!.OwnIntentReleased);
        Assert.Equal(6, entry.Revision);
        Assert.False(HoldsNothing(entry, committer));
    }
}
