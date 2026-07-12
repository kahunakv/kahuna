
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
/// Handler-level tests for <see cref="TryPrepareMutationsHandler"/> focused on stamping the
/// transaction record anchor into the participant-side write intent. The write intent is transient,
/// per-node, in-memory prepare state with no external observability, so the only faithful test is to
/// drive the handler directly against a seeded MVCC entry and inspect <see cref="KeyValueEntry.WriteIntent"/>.
/// Ephemeral durability is used so preparation stops at write-intent creation and never touches the
/// Raft proposal path.
/// </summary>
[Collection("ClusterTests")]
public sealed class TestTryPrepareMutationsHandler
{
    [Fact]
    public async Task Prepare_CreatesWriteIntent_StampsRecordAnchorKey()
    {
        (TryPrepareMutationsHandler handler, KeyValueContext context, RaftManager raft) = CreateHandler();

        HLCTimestamp txId = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());
        HLCTimestamp commitId = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

        SeedPreparableEntry(context, "prepkey", txId, revision: 5, lastModified: txId);

        KeyValueResponse resp = await handler.Execute(
            MakePrepare("prepkey", txId, commitId, KeyValueDurability.Ephemeral, recordAnchorKey: "anchor-key"));

        Assert.Equal(KeyValueResponseType.Prepared, resp.Type);

        Assert.True(context.Store.TryGetValue("prepkey", out KeyValueEntry? entry));
        Assert.NotNull(entry!.WriteIntent);
        Assert.Equal(txId, entry.WriteIntent!.TransactionId);
        Assert.Equal("anchor-key", entry.WriteIntent.RecordAnchorKey);
    }

    [Fact]
    public async Task Prepare_ExistingIntentWithoutAnchor_GetsStamped()
    {
        (TryPrepareMutationsHandler handler, KeyValueContext context, RaftManager raft) = CreateHandler();

        HLCTimestamp txId = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());
        HLCTimestamp commitId = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

        KeyValueEntry entry = SeedPreparableEntry(context, "prepkey", txId, revision: 5, lastModified: txId);
        // A plain pre-prepare lock the same transaction already holds carries no anchor yet.
        entry.WriteIntent = new()
        {
            TransactionId   = txId,
            Expires         = txId + 15000,
            CommitTimestamp = txId,
            RecordAnchorKey = null
        };

        await handler.Execute(
            MakePrepare("prepkey", txId, commitId, KeyValueDurability.Ephemeral, recordAnchorKey: "anchor-key"));

        Assert.Equal("anchor-key", entry.WriteIntent.RecordAnchorKey);
    }

    [Fact]
    public async Task Prepare_NoAnchorSupplied_LeavesWriteIntentAnchorNull()
    {
        (TryPrepareMutationsHandler handler, KeyValueContext context, RaftManager raft) = CreateHandler();

        HLCTimestamp txId = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());
        HLCTimestamp commitId = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

        SeedPreparableEntry(context, "prepkey", txId, revision: 5, lastModified: txId);

        await handler.Execute(
            MakePrepare("prepkey", txId, commitId, KeyValueDurability.Ephemeral, recordAnchorKey: null));

        Assert.True(context.Store.TryGetValue("prepkey", out KeyValueEntry? entry));
        Assert.NotNull(entry!.WriteIntent);
        Assert.Null(entry.WriteIntent!.RecordAnchorKey);
    }

    // ── Helpers ───────────────────────────────────────────────────────────────────────────

    private static KeyValueEntry SeedPreparableEntry(
        KeyValueContext context, string key, HLCTimestamp txId, long revision, HLCTimestamp lastModified)
    {
        KeyValueMvccEntry mvcc = new()
        {
            Value = [1, 2, 3],
            Revision = revision,
            State = KeyValueState.Set,
            LastModified = lastModified,
            LastUsed = lastModified
        };

        KeyValueEntry entry = new()
        {
            Bucket = null,
            Value = [1, 2, 3],
            Revision = revision,
            FlushedRevision = revision,
            LastModified = lastModified,
            LastUsed = lastModified,
            State = KeyValueState.Set,
            MvccEntries = new() { [txId] = mvcc }
        };

        context.InsertStoreEntry(key, entry);
        return entry;
    }

    private static KeyValueRequest MakePrepare(
        string key,
        HLCTimestamp txId,
        HLCTimestamp commitId,
        KeyValueDurability durability,
        string? recordAnchorKey)
    {
        return new KeyValueRequest(
            KeyValueRequestType.TryPrepareMutations,
            txId,
            commitId,
            key,
            null,
            null,
            -1,
            KeyValueFlags.None,
            0,
            HLCTimestamp.Zero,
            durability,
            0,
            0,
            null
        ) { RecordAnchorKey = recordAnchorKey };
    }

    private static (TryPrepareMutationsHandler, KeyValueContext, RaftManager) CreateHandler()
    {
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

        BTree<string, KeyValueEntry> store = new(32);
        ILogger<IKahuna> logger = NullLogger<IKahuna>.Instance;
        ILogger<IRaft> raftLogger = NullLogger<IRaft>.Instance;

        RaftManager raft = new(
            new RaftConfiguration
            {
                NodeName = "tryprepare-test",
                NodeId = 1,
                Host = "localhost",
                Port = 0,
                InitialPartitions = 1,
                EnableQuiescence = false
            },
            new StaticDiscovery([]),
            new InMemoryWAL(raftLogger),
            new InMemoryCommunication(),
            new HybridLogicalClock(),
            raftLogger
        );

        MemoryPersistenceBackend backend = new();

        KeyValueContext context = new(
            null!,   // actorContext — not exercised on the ephemeral prepare path
            store,
            new Dictionary<string, KeyValueWriteIntent>(),
            new Dictionary<string, List<KeyValueRangeLock>>(),
            new Dictionary<int, KeyValueProposal>(),
            null!,   // backgroundWriter
            null!,   // proposalRouter
            backend,
            raft,
            new KeySpaceRegistry(),
            new RangeMapStore(raft, null, null, logger),
            config,
            logger
        );

        return (new TryPrepareMutationsHandler(context), context, raft);
    }
}
