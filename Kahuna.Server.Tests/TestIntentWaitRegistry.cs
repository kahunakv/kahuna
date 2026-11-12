using System.Text;
using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Handlers;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Communication.Memory;
using Kommander.Discovery;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kahuna.Server.Tests;

/// <summary>
/// A read parked behind a live write intent (<see cref="IntentWaitRegistry"/>) is woken by every path that takes the
/// intent away, whichever handler does it, and by nothing else: the write intent setter queues the entry for the
/// end-of-turn flush, which wakes the reads of an intent that is gone or now belongs to another transaction; an entry
/// that leaves the store or is replaced wakes its reads at once. A wake is accepted once, for the park it was armed for.
///
/// <para>These tests drive the context and the handlers without an actor, and record the wake messages a parked read
/// sends instead of delivering them.</para>
/// </summary>
public sealed class TestIntentWaitRegistry : RaftTrackingTest
{
    private static readonly HLCTimestamp Holder = new(1, 1_000, 0);
    private static readonly HLCTimestamp OtherHolder = new(1, 2_000, 0);

    [Fact]
    public void IntentCleared_WakesTheRead_AtTheEndOfTheTurn()
    {
        (KeyValueContext context, RaftManager raft) = CreateContext();
        KeyValueEntry entry = InsertWithIntent(context, "w/a", Holder, Now(raft));
        (ParkedIntentRead read, List<KeyValueRequest> wakes) = Park(context, entry, raft);

        entry.WriteIntent = null;
        Assert.Empty(wakes);

        context.FlushHeadPublications();

        KeyValueRequest wake = Assert.Single(wakes);
        Assert.Equal(KeyValueRequestType.ResumeIntentWait, wake.Type);
        Assert.Same(read, wake.ParkedRead);
        Assert.Equal(0, context.IntentWaits.ParkedCount);
        Assert.Null(entry.ParkedReads);
    }

    [Fact]
    public void IntentOfTheSameHolderReplaced_DoesNotWake()
    {
        (KeyValueContext context, RaftManager raft) = CreateContext();
        HLCTimestamp now = Now(raft);
        KeyValueEntry entry = InsertWithIntent(context, "w/b", Holder, now);
        (_, List<KeyValueRequest> wakes) = Park(context, entry, raft);

        // The holder's prepare plants a new intent object of the same transaction.
        entry.WriteIntent = new KeyValueWriteIntent { TransactionId = Holder, Expires = now + 60_000, CommitTimestamp = now };
        context.FlushHeadPublications();

        Assert.Empty(wakes);
        Assert.Equal(1, context.IntentWaits.ParkedCount);
    }

    [Fact]
    public void IntentTakenOverByAnotherTransaction_Wakes()
    {
        (KeyValueContext context, RaftManager raft) = CreateContext();
        HLCTimestamp now = Now(raft);
        KeyValueEntry entry = InsertWithIntent(context, "w/c", Holder, now);
        (_, List<KeyValueRequest> wakes) = Park(context, entry, raft);

        entry.WriteIntent = new KeyValueWriteIntent { TransactionId = OtherHolder, Expires = now + 60_000 };
        context.FlushHeadPublications();

        Assert.Single(wakes);
    }

    [Fact]
    public void LeaderStateDrop_Wakes()
    {
        (KeyValueContext context, RaftManager raft) = CreateContext();
        KeyValueEntry entry = InsertWithIntent(context, "w/d", Holder, Now(raft));
        (_, List<KeyValueRequest> wakes) = Park(context, entry, raft);

        new DropLeaderStateHandler(context).Execute(Control(KeyValueRequestType.DropLeaderState, "w/d", OwnerOf(context, raft, "w/d")));
        context.FlushHeadPublications();

        Assert.Single(wakes);
    }

    [Fact]
    public void PartitionEviction_WakesAtOnce()
    {
        (KeyValueContext context, RaftManager raft) = CreateContext();
        KeyValueEntry entry = InsertWithIntent(context, "w/e", Holder, Now(raft));
        (_, List<KeyValueRequest> wakes) = Park(context, entry, raft);

        new EvictPartitionHandler(context).Execute(Control(KeyValueRequestType.EvictPartition, "", OwnerOf(context, raft, "w/e")));

        Assert.Single(wakes);
        Assert.Equal(0, context.IntentWaits.ParkedCount);
    }

    [Fact]
    public void EntryReplacedByANewObject_WakesAtOnce()
    {
        (KeyValueContext context, RaftManager raft) = CreateContext();
        HLCTimestamp now = Now(raft);
        KeyValueEntry entry = InsertWithIntent(context, "w/f", Holder, now);
        (_, List<KeyValueRequest> wakes) = Park(context, entry, raft);

        context.InsertStoreEntry("w/f", new KeyValueEntry { Value = "x"u8.ToArray(), Revision = 2, LastModified = now, State = KeyValueState.Set });

        Assert.Single(wakes);
    }

    [Fact]
    public void ResumeIsAcceptedOnce_ForTheParkItWasArmedFor()
    {
        (KeyValueContext context, RaftManager raft) = CreateContext();
        HLCTimestamp now = Now(raft);
        KeyValueEntry entry = InsertWithIntent(context, "w/g", Holder, now);
        (ParkedIntentRead read, _) = Park(context, entry, raft);

        long first = read.Generation;
        Assert.False(context.IntentWaits.TryAcceptResume(read, first + 1));

        // A timer's resume takes the read off the entry it still waits on.
        Assert.True(context.IntentWaits.TryAcceptResume(read, first));
        Assert.Null(entry.ParkedReads);
        Assert.Equal(0, context.IntentWaits.ParkedCount);
        Assert.False(context.IntentWaits.TryAcceptResume(read, first));

        // Parked again: only the new park's resume is accepted.
        context.IntentWaits.Park(read, entry, now);
        Assert.False(context.IntentWaits.TryAcceptResume(read, first));
        Assert.True(context.IntentWaits.TryAcceptResume(read, read.Generation));
    }

    [Fact]
    public void KeyBound_IsReported()
    {
        KahunaConfiguration config = CreateConfiguration();
        config.MaxParkedReadsPerKey = 2;
        (KeyValueContext context, RaftManager raft) = CreateContext(config);
        KeyValueEntry entry = InsertWithIntent(context, "w/h", Holder, Now(raft));

        Assert.True(context.IntentWaits.HasRoom(entry));
        Park(context, entry, raft);
        Park(context, entry, raft);
        Assert.False(context.IntentWaits.HasRoom(entry));
    }

    // ── helpers ──────────────────────────────────────────────────────────────────────────

    private static (ParkedIntentRead Read, List<KeyValueRequest> Wakes) Park(KeyValueContext context, KeyValueEntry entry, RaftManager raft)
    {
        List<KeyValueRequest> wakes = [];
        KeyValueRequest request = new(
            KeyValueRequestType.TryGet, OtherHolder, HLCTimestamp.Zero, entry.StoreKey!, null, null, -1,
            KeyValueFlags.None, 0, HLCTimestamp.Zero, KeyValueDurability.Persistent, 0, 0, default);

        ParkedIntentRead read = new(request, default, Environment.TickCount64 + 60_000, wakes.Add);
        context.IntentWaits.Park(read, entry, Now(raft));
        return (read, wakes);
    }

    private static KeyValueEntry InsertWithIntent(KeyValueContext context, string key, HLCTimestamp holder, HLCTimestamp now)
    {
        KeyValueEntry entry = new()
        {
            Bucket = KeyValueKeySpace.OfKey(key),
            Value = Encoding.UTF8.GetBytes("v"),
            Revision = 1,
            FlushedRevision = 1,
            LastModified = now,
            LastUsed = now,
            State = KeyValueState.Set
        };
        context.InsertStoreEntry(key, entry);
        entry.WriteIntent = new KeyValueWriteIntent { TransactionId = holder, Expires = now + 60_000 };
        context.FlushHeadPublications();
        return entry;
    }

    private static KeyValueRequest Control(KeyValueRequestType type, string key, int partitionId) =>
        new(type, HLCTimestamp.Zero, HLCTimestamp.Zero, key, null, null, -1, KeyValueFlags.None, 0, HLCTimestamp.Zero,
            KeyValueDurability.Persistent, 0, partitionId, default);

    private static int OwnerOf(KeyValueContext context, RaftManager raft, string key) =>
        PartitionDataEnumerator.OwnerOfKey(context.RangeMapStore.Current, key, raft.Configuration.InitialPartitions);

    private static HLCTimestamp Now(RaftManager raft) => raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

    private static KahunaConfiguration CreateConfiguration() => ConfigurationValidator.Validate(new()
    {
        LocksWorkers = 1,
        KeyValueWorkers = 1,
        BackgroundWriterWorkers = 1,
        Storage = "memory",
        MaxEntriesPerActor = 50_000,
        MaxBytesPerActor = 256L * 1024 * 1024,
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
                NodeName = "intent-wait",
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
            new KeyValueStore(KeyValueStore.DefaultOrder, config.MaxEntriesPerActor),
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
