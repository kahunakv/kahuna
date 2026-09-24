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
/// A committed value applied to a key that another transaction has locked and read in the meantime (the
/// deferred-settlement window: the committing transaction released its own intent before its value
/// materialized). The apply advances the entry, but it must not erase the lock holder's state: the holder's MVCC
/// entry is the record of the base it read, and it is what makes the holder's next read or write of the key
/// answer <c>Aborted</c>. Deleting it let the holder re-pin at the new head and write a value computed from the
/// superseded one over the commit, with every check passing.
/// </summary>
public sealed class TestCommittedApplyKeepsLockHolderState : RaftTrackingTest
{
    private static readonly HLCTimestamp Committer = new(0, 1_000, 0);

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

    private sealed record Harness(KeyValueContext Context, InvalidateOrApplyHandler Invalidate, TryGetHandler Get);

    private Harness BuildHarness(string raftName, IActorRef<BackgroundWriterActor, BackgroundWriteRequest>? backgroundWriter = null)
    {
        RaftManager raft = BuildRaft(raftName);

        KeyValueContext context = new(
            actorContext: null!,
            store: new BTree<string, KeyValueEntry>(32),
            locksByPrefix: [],
            locksByRange: [],
            proposals: [],
            backgroundWriter: backgroundWriter!,
            writeAggregator: null!,
            persistenceBackend: new MemoryPersistenceBackend(),
            raft: raft,
            backendReadScheduler: null!,
            keySpaceRegistry: new(),
            rangeMapStore: new(raft, null, null, NullLogger<IKahuna>.Instance),
            configuration: BuildConfig(),
            logger: NullLogger<IKahuna>.Instance);

        return new(context, new InvalidateOrApplyHandler(context), new TryGetHandler(context));
    }

    private static HLCTimestamp Now(Harness h) =>
        h.Context.Raft.HybridLogicalClock.TrySendOrLocalEvent(h.Context.Raft.GetLocalNodeId());

    /// <summary>
    /// A resident entry at revision 5 locked by a live transaction that has also read it: its MVCC entry pins
    /// revision 5. The committer's value (revision 6) has not reached the entry yet. The holder's id is a current
    /// HLC timestamp, so the MVCC trim that runs on every commit apply treats the holder as live.
    /// </summary>
    private static (KeyValueEntry Entry, HLCTimestamp Holder) SeedLockedAndPinned(Harness h, string key)
    {
        HLCTimestamp now = Now(h);
        HLCTimestamp holder = now;

        KeyValueEntry entry = new()
        {
            Bucket = null,
            Value = "v5"u8.ToArray(),
            Revision = 5,
            FlushedRevision = 5,
            State = KeyValueState.Set,
            LastModified = new(0, 5_000, 0),
            WriteIntent = new()
            {
                TransactionId = holder,
                Expires = now + 60_000,
                AcquiredAt = now
            },
            MvccEntries = new()
            {
                [holder] = new()
                {
                    Value = "v5"u8.ToArray(),
                    Revision = 5,
                    LastModified = new(0, 5_000, 0),
                    State = KeyValueState.Set
                }
            }
        };

        h.Context.InsertStoreEntry(key, entry);
        return (entry, holder);
    }

    private static KeyValueRequest CommittedApplyOf(string key, bool forceResident) =>
        KeyValueRequestPool.RentInvalidateOrApply(
            key,
            6,
            "v6"u8.ToArray(),
            expires: HLCTimestamp.Zero,
            lastUsed: new(0, 6_000, 0),
            lastModified: new(0, 6_000, 0),
            state: KeyValueState.Set,
            forceResident: forceResident,
            transactionId: Committer,
            partitionId: 1,
            noRevision: false,
            isRollback: false,
            returnToPoolOnReceive: false);

    private static KeyValueRequest HolderReadOf(string key, HLCTimestamp holder) =>
        KeyValueRequestPool.Rent(
            KeyValueRequestType.TryGet,
            holder,
            HLCTimestamp.Zero,
            key,
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

    /// <summary>
    /// The replication notice of the commit reaches the actor while the holder's lock is live. The entry advances
    /// and the lock is cleared as before, but the holder's pin stays, so the holder's next read aborts instead of
    /// re-pinning at revision 6.
    /// </summary>
    [Fact]
    public async Task ReplicationNotice_OverLiveForeignLock_KeepsTheHoldersPin()
    {
        Harness h = BuildHarness("apply-notice-keeps-pin");
        (KeyValueEntry entry, HLCTimestamp holder) = SeedLockedAndPinned(h, "acct/n");

        Assert.Null(h.Invalidate.Execute(CommittedApplyOf("acct/n", forceResident: false)));

        Assert.Equal(6, entry.Revision);
        Assert.Equal("v6"u8.ToArray(), entry.Value);
        Assert.True(entry.MvccEntries?.ContainsKey(holder), "the notice deleted the lock holder's MVCC entry");

        KeyValueResponse read = await h.Get.Execute(HolderReadOf("acct/n", holder));
        Assert.Equal(KeyValueResponseType.Aborted, read.Type);
    }

    /// <summary>
    /// The durable-intent apply of the commit runs on the leader after the committer released its intent, while
    /// the holder's lock is live. It advances the entry and keeps both the holder's lock and its pin.
    /// </summary>
    [Fact]
    public async Task DurableCommitApply_ByTransactionThatDoesNotOwnTheLock_KeepsTheHoldersLockAndPin()
    {
        using IDisposable lifetime = TestActorSystemLifetime.Create(out ActorSystem actorSystem);

        RaftManager writerRaft = BuildRaft("apply-durable-writer");
        IActorRef<BackgroundWriterActor, BackgroundWriteRequest> writer =
            actorSystem.Spawn<BackgroundWriterActor, BackgroundWriteRequest>(
                "apply-durable-bg", writerRaft, writerRaft.ReadScheduler, new MemoryPersistenceBackend(),
                null!, null!, new TransactionRecordStore(), new PreparedIntentStore(),
                BuildConfig(), NullLogger<IKahuna>.Instance, new FlushNotificationSink(), null!);

        Harness h = BuildHarness("apply-durable-keeps-lock", writer);
        (KeyValueEntry entry, HLCTimestamp holder) = SeedLockedAndPinned(h, "acct/d");

        KeyValueResponse? applied = h.Invalidate.Execute(CommittedApplyOf("acct/d", forceResident: true));
        Assert.Equal(KeyValueResponseType.Committed, applied?.Type);

        Assert.Equal(6, entry.Revision);
        Assert.Equal(holder, entry.WriteIntent?.TransactionId);
        Assert.True(entry.MvccEntries?.ContainsKey(holder), "the durable apply deleted the lock holder's MVCC entry");

        KeyValueResponse read = await h.Get.Execute(HolderReadOf("acct/d", holder));
        Assert.Equal(KeyValueResponseType.Aborted, read.Type);
    }
}
