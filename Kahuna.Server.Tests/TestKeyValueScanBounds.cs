
using Kahuna;
using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Handlers;
using Kahuna.Server.KeyValues.Ranges;
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
/// The synchronous ephemeral scans must bound how many entries they *inspect*, not merely how many
/// they *return*: a run of interleaved tombstones must not make the mailbox-thread walk the whole
/// resident range. The non-paginated prefix scan truncates (documented contract); the paginated range
/// scan resumes past the tombstone wall from its cursor, so no live entry is lost across pages.
/// </summary>
public sealed class TestKeyValueScanBounds
{
    private const int PrefixInspectionBudget =
        KeyValueScanLimits.MaxPrefixScanResults + KeyValueScanLimits.MaxScanInspectionSlack;

    [Fact]
    public async Task EphemeralPrefixScan_WithManyTombstones_IsInspectionBounded()
    {
        (KeyValueContext context, RaftManager raft) = CreateContext(CreateConfiguration());
        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

        // A wall of tombstones taller than the inspection budget, then live entries beyond it.
        int wall = PrefixInspectionBudget + 8;
        for (int i = 0; i < wall; i++)
            InsertTombstone(context, $"k/{i:D8}", now);

        InsertLive(context, "k/z0000000", now);
        InsertLive(context, "k/z0000001", now);
        InsertLive(context, "k/z0000002", now);

        TryScanByPrefixHandler scan = new(context);
        KeyValueResponse response = await scan.Execute(BuildRequest("k", limit: 0));

        Assert.Equal(KeyValueResponseType.Get, response.Type);
        // The scan stopped on the inspection budget before reaching the far live entries — an unbounded
        // walk would have skipped every tombstone and returned all three.
        Assert.Empty(response.Items!);
    }

    [Fact]
    public async Task EphemeralRangeScan_WithManyTombstones_ResumesWithoutLosingEntries()
    {
        (KeyValueContext context, RaftManager raft) = CreateContext(CreateConfiguration());
        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

        const int limit = 5;
        int rangeBudget = limit + 1 + KeyValueScanLimits.MaxScanInspectionSlack;

        int wall = rangeBudget + 4;
        for (int i = 0; i < wall; i++)
            InsertTombstone(context, $"k/{i:D8}", now);

        HashSet<string> liveKeys = ["k/z0000000", "k/z0000001", "k/z0000002"];
        foreach (string key in liveKeys)
            InsertLive(context, key, now);

        TryGetByRangeHandler scan = new(context);

        // First page can only walk `rangeBudget` entries — all tombstones — so it returns no items but
        // must still signal HasMore with a cursor so the caller can page past the wall.
        KeyValueResponse first = await scan.Execute(BuildRangeRequest("k", limit, startKey: null));
        KeyValueGetByRangeResult firstPage = first.RangeResult!;
        Assert.Empty(firstPage.Items);
        Assert.True(firstPage.HasMore);
        Assert.NotNull(firstPage.NextCursor);

        // Page through to exhaustion; every live entry must surface exactly once and paging must
        // terminate in a bounded number of pages (each advances by ~the inspection budget).
        List<string> collected = [];
        string? cursor = firstPage.NextCursor;
        int pages = 1;

        while (cursor is not null)
        {
            Assert.True(KeyValueRangeCursor.TryDecode(cursor, out string lastKey, out _, out _, out _));

            KeyValueResponse next = await scan.Execute(BuildRangeRequest("k", limit, startKey: lastKey));
            KeyValueGetByRangeResult page = next.RangeResult!;
            pages++;

            foreach ((string key, ReadOnlyKeyValueEntry _) in page.Items)
                collected.Add(key);

            cursor = page.HasMore ? page.NextCursor : null;
            Assert.True(pages <= 6, "paging past the tombstone wall must terminate quickly");
        }

        Assert.Equal(liveKeys.OrderBy(k => k), collected.OrderBy(k => k));
    }

    // ── helpers ──────────────────────────────────────────────────────────────────────────

    private static void InsertTombstone(KeyValueContext context, string key, HLCTimestamp now)
    {
        int slash = key.LastIndexOf('/');
        context.InsertStoreEntry(key, new KeyValueEntry
        {
            State = KeyValueState.Deleted,
            Bucket = slash == -1 ? null : key[..slash],
            Revision = 0,
            FlushedRevision = 0,
            LastUsed = now,
            LastModified = now,
            Expires = HLCTimestamp.Zero
        });
    }

    private static void InsertLive(KeyValueContext context, string key, HLCTimestamp now)
    {
        int slash = key.LastIndexOf('/');
        context.InsertStoreEntry(key, new KeyValueEntry
        {
            Value = [1],
            State = KeyValueState.Set,
            Bucket = slash == -1 ? null : key[..slash],
            Revision = 0,
            FlushedRevision = 0,
            LastUsed = now,
            LastModified = now,
            Expires = HLCTimestamp.Zero
        });
    }

    private static KeyValueRequest BuildRequest(string key, int limit)
    {
        KeyValueRequest request = new(
            KeyValueRequestType.ScanByPrefix, HLCTimestamp.Zero, HLCTimestamp.Zero, key,
            null, null, -1, KeyValueFlags.None, 0, HLCTimestamp.Zero, KeyValueDurability.Ephemeral, 0, 0, null);
        request.Limit = limit;
        return request;
    }

    private static KeyValueRequest BuildRangeRequest(string key, int limit, string? startKey)
    {
        KeyValueRequest request = new(
            KeyValueRequestType.GetByRange, HLCTimestamp.Zero, HLCTimestamp.Zero, key,
            null, null, -1, KeyValueFlags.None, 0, HLCTimestamp.Zero, KeyValueDurability.Ephemeral, 0, 0, null);
        request.Limit = limit;
        request.StartKey = startKey;
        request.StartInclusive = false;
        return request;
    }

    private static (KeyValueContext, RaftManager) CreateContext(KahunaConfiguration config)
    {
        BTree<string, KeyValueEntry> store = new(32);
        ILogger<IKahuna> logger = NullLogger<IKahuna>.Instance;
        ILogger<IRaft> raftLogger = NullLogger<IRaft>.Instance;

        RaftManager raft = new(
            new RaftConfiguration
            {
                NodeName = "scan-bounds-test",
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

        KeyValueContext context = new(
            null!,
            store,
            new Dictionary<string, KeyValueWriteIntent>(),
            new Dictionary<string, List<KeyValueRangeLock>>(),
            new Dictionary<int, KeyValueProposal>(),
            null!,
            null!,
            null!,
            raft,
            new KeySpaceRegistry(),
            new RangeMapStore(raft, null, null, logger),
            config,
            logger
        );

        return (context, raft);
    }

    private static KahunaConfiguration CreateConfiguration()
    {
        return ConfigurationValidator.Validate(new()
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
    }
}
