
using System.Text;
using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Handlers;
using Kahuna.Server.KeyValues.Ranges;
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
using RocksDbSharp;

namespace Kahuna.Server.Tests;

/// <summary>
/// Tests for the two scan-cost/scan-visibility defects of version-heavy stores:
///
/// <para>1. The RocksDB kv family stores one row per retained revision next to each key's
/// ~CURRENT row, so a range scan used to step over every revision ever written — O(total
/// revisions), not O(logical keys). Scans now seek past a deep revision block, gated by the
/// tilde-key registry: keys may legally contain '~', and a key named "X~&lt;something&gt;" has its
/// whole row block INSIDE the seek window of block X, so the seek is taken only when the
/// registry proves no such sibling can exist. These tests pin the correctness contract of
/// that seek: identical results with and without siblings, across restarts, and on legacy
/// stores where the registry is not authoritative.</para>
///
/// <para>2. A snapshot range scan silently dropped (or served stale) a resident row whose
/// in-memory revision archive was gapped by a head jump while the skipped revisions' flushes
/// were still queued. Point reads fail closed on exactly that state; the scan evaluation now
/// does too.</para>
/// </summary>
public sealed class TestRevisionRunSeek : RaftTrackingTest
{
    private static PersistenceRequestItem MakeItem(string key, long revision) =>
        new(key,
            Encoding.UTF8.GetBytes("val" + revision),
            revision: revision,
            expiresNode: 0, expiresPhysical: 0, expiresCounter: 0,
            lastUsedNode: 0, lastUsedPhysical: 0, lastUsedCounter: 0,
            lastModifiedNode: 0, lastModifiedPhysical: revision * 1_000,
            lastModifiedCounter: 0,
            state: (int)KeyValueState.Set);

    private static string RocksDbTempPath()
    {
        string dir = Path.Combine(Path.GetTempPath(), "kahuna_rocksdb_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        return dir;
    }

    /// <summary>Stores revisions 1..count of one key, one batch per revision, so every
    /// revision lands as a retained history row under the advancing ~CURRENT row.</summary>
    private static void StoreDeepKey(RocksDbPersistenceBackend backend, string key, int count, int firstRevision = 1)
    {
        for (int revision = firstRevision; revision < firstRevision + count; revision++)
            Assert.True(backend.StoreKeyValues([MakeItem(key, revision)]));
    }

    // ── Seek-skip correctness ────────────────────────────────────────────────────────────

    [Fact]
    public void RangeScan_DeepRevisionBlocks_ReturnsOneCurrentRowPerKey()
    {
        string path = RocksDbTempPath();
        using RocksDbPersistenceBackend backend = new(path, "v1");

        for (int i = 1; i <= 6; i++)
            StoreDeepKey(backend, $"acct/{i:D4}", 40);

        List<(string, ReadOnlyKeyValueEntry)> items = backend.GetKeyValueByRange("acct", null, 100);

        Assert.Equal(6, items.Count);
        for (int i = 0; i < items.Count; i++)
        {
            Assert.Equal($"acct/{i + 1:D4}", items[i].Item1);
            Assert.Equal(40, items[i].Item2.Revision);
            Assert.Equal("val40", Encoding.UTF8.GetString(items[i].Item2.Value!));
        }
    }

    [Fact]
    public void RangeScan_TildeSiblingInsideDeepBlock_IsNeverSkipped()
    {
        string path = RocksDbTempPath();
        using RocksDbPersistenceBackend backend = new(path, "v1");

        // The sibling's whole row block sorts between acct/0001's revision rows and
        // acct/0001~CURRENT ("05x" &lt; "CURRENT" ordinally). A blind seek would jump it.
        StoreDeepKey(backend, "acct/0001", 40);
        StoreDeepKey(backend, "acct/0001~05x", 2);
        StoreDeepKey(backend, "acct/0002", 40);

        List<(string, ReadOnlyKeyValueEntry)> items = backend.GetKeyValueByRange("acct", null, 100);

        // Items arrive in physical marker-row order, and a tilde-extension sibling's ~CURRENT
        // row sorts BEFORE its base key's marker ("05x~CURRENT" < "CURRENT") — so assert
        // membership and values, not position. What matters here is that the sibling is
        // present at all: a blind seek over the deep block would have dropped it.
        Assert.Equal(3, items.Count);

        Dictionary<string, string> byKey = items.ToDictionary(
            static i => i.Item1, static i => Encoding.UTF8.GetString(i.Item2.Value!));

        Assert.Equal("val40", byKey["acct/0001"]);
        Assert.Equal("val2", byKey["acct/0001~05x"]);
        Assert.Equal("val40", byKey["acct/0002"]);
    }

    [Fact]
    public void RangeScan_TildeRegistrationSurvivesReopen()
    {
        string path = RocksDbTempPath();

        using (RocksDbPersistenceBackend backend = new(path, "v1"))
        {
            StoreDeepKey(backend, "acct/0001", 20);
            StoreDeepKey(backend, "acct/0001~05x", 1);
        }

        using (RocksDbPersistenceBackend reopened = new(path, "v1"))
        {
            // New deep revisions after the reopen: the loaded registry must still block the
            // seek in this bucket, or the sibling written before the restart disappears.
            StoreDeepKey(reopened, "acct/0001", 20, firstRevision: 21);

            List<(string, ReadOnlyKeyValueEntry)> items = reopened.GetKeyValueByRange("acct", null, 100);

            Assert.Equal(2, items.Count);

            Dictionary<string, string> byKey = items.ToDictionary(
                static i => i.Item1, static i => Encoding.UTF8.GetString(i.Item2.Value!));

            Assert.Equal("val40", byKey["acct/0001"]);
            Assert.Equal("val1", byKey["acct/0001~05x"]);
        }
    }

    [Fact]
    public void RangeScan_LegacyStoreWithoutSentinel_StaysCorrectAndNeverClaimsAuthority()
    {
        string path = RocksDbTempPath();

        using (RocksDbPersistenceBackend backend = new(path, "v1"))
        {
            StoreDeepKey(backend, "acct/0001", 40);
            StoreDeepKey(backend, "acct/0001~05x", 1);
            StoreDeepKey(backend, "acct/0002", 40);
        }

        // Simulate a store written by a build that predates the registry: strip the sentinel
        // and every registry row, leaving only user rows behind.
        DbOptions options = new DbOptions().SetCreateIfMissing(false).SetCreateMissingColumnFamilies(false);
        ColumnFamilies families = new() { { "kv", new ColumnFamilyOptions() }, { "locks", new ColumnFamilyOptions() } };

        using (RocksDb raw = RocksDb.Open(options, $"{path}/v1", families))
        {
            ColumnFamilyHandle kv = raw.GetColumnFamily("kv");
            raw.Remove("\0tilde_registry"u8.ToArray(), cf: kv);
            raw.Remove(Encoding.UTF8.GetBytes("\0tilde/acct/"), cf: kv);
        }

        using (RocksDbPersistenceBackend reopened = new(path, "v1"))
        {
            List<(string, ReadOnlyKeyValueEntry)> items = reopened.GetKeyValueByRange("acct", null, 100);

            Assert.Equal(3, items.Count);

            HashSet<string> keys = items.Select(static i => i.Item1).ToHashSet();
            Assert.Contains("acct/0001", keys);
            Assert.Contains("acct/0001~05x", keys);
            Assert.Contains("acct/0002", keys);
        }

        // The store held user rows at open, so the reopen must not have re-minted the
        // sentinel: authority claimed over unregistered historical tilde keys would let a
        // later scan seek over them.
        using RocksDb rawAgain = RocksDb.Open(options, $"{path}/v1", families);
        Assert.Null(rawAgain.Get("\0tilde_registry"u8.ToArray(), cf: rawAgain.GetColumnFamily("kv")));
    }

    [Fact]
    public void PrefixScan_DeepRevisionBlocks_ReturnsAllCurrentRows()
    {
        string path = RocksDbTempPath();
        using RocksDbPersistenceBackend backend = new(path, "v1");

        for (int i = 1; i <= 5; i++)
            StoreDeepKey(backend, $"idx/{i:D4}", 30);

        List<(string, ReadOnlyKeyValueEntry)> items = backend.GetKeyValueByPrefix("idx/");

        Assert.Equal(5, items.Count);
        for (int i = 0; i < items.Count; i++)
            Assert.Equal(30, items[i].Item2.Revision);
    }

    [Fact]
    public void ScanKeyValues_DeepRevisionBlocks_PagesEveryCurrentRow()
    {
        string path = RocksDbTempPath();
        using RocksDbPersistenceBackend backend = new(path, "v1");

        for (int i = 1; i <= 7; i++)
            StoreDeepKey(backend, $"acct/{i:D4}", 25);

        List<string> seen = [];
        string? cursor = null;

        while (true)
        {
            KeyValueScanPage page = backend.ScanKeyValues(cursor, limit: 3);
            foreach ((string key, ReadOnlyKeyValueEntry entry) in page.Items)
            {
                seen.Add(key);
                Assert.Equal(25, entry.Revision);
            }

            if (page.NextCursor is null)
                break;
            cursor = page.NextCursor;
        }

        Assert.Equal(7, seen.Count);
        Assert.Equal("acct/0001", seen[0]);
        Assert.Equal("acct/0007", seen[^1]);
    }

    [Fact]
    public void RevisionReads_UnaffectedByDeepBlockSeeks()
    {
        string path = RocksDbTempPath();
        using RocksDbPersistenceBackend backend = new(path, "v1");

        StoreDeepKey(backend, "acct/0001", 40);

        // As-of read below the head must still walk/probe the revision rows the scan skips.
        KeyValueEntry? snapshot = backend.GetKeyValueRevisionAtOrBefore("acct/0001", 39, new HLCTimestamp(0, 17_500, 0));

        Assert.NotNull(snapshot);
        Assert.Equal(17, snapshot!.Revision);
        Assert.Equal("val17", Encoding.UTF8.GetString(snapshot.Value!));
    }

    // ── Snapshot scan fail-closed on an unflushed archive gap ────────────────────────────

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
                EnableQuiescence = false,
                PartitionExecutorPoolSize = 1
            },
            new StaticDiscovery([]),
            new InMemoryWAL(NullLogger<IRaft>.Instance),
            new InMemoryCommunication(),
            new HybridLogicalClock(),
            NullLogger<IRaft>.Instance));
    }

    private KeyValueContext BuildContext(string raftName)
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
            persistenceBackend: new MemoryPersistenceBackend(),
            raft: raft,
            backendReadScheduler: null!,
            keySpaceRegistry: new(),
            rangeMapStore: new(raft, null, null, NullLogger<IKahuna>.Instance),
            configuration: ConfigurationValidator.Validate(new()
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
            }),
            logger: NullLogger<IKahuna>.Instance);
    }

    /// <summary>A resident head advanced by a revision jump whose skipped revisions are not
    /// all flushed: the archive cannot answer for the gap, and neither can disk yet.</summary>
    private static KeyValueEntry GappedEntry(long flushedRevision) => new()
    {
        Value = "v8"u8.ToArray(),
        Revision = 8,
        FlushedRevision = flushedRevision,
        State = KeyValueState.Set,
        LastModified = Ts(8_000),
        ArchiveGapStart = Ts(5_000),
        ArchiveGapEnd = Ts(8_000),
        ArchiveGapEndRevision = 8
    };

    private static KeyValueResponse? RunSnapshotScan(KeyValueContext context, KeyValueEntry entry, HLCTimestamp snapshotTs)
    {
        TaskCompletionSource<KeyValueResponse?> promise = new();

        RangeScanContinuation continuation = new(
            prefix: "acct/",
            limit: 10,
            KeyValueDurability.Persistent,
            partitionId: 1,
            startInclusive: true,
            startKey: null,
            endKey: null,
            endInclusive: true,
            transactionId: HLCTimestamp.Zero,
            snapshotTs: snapshotTs,
            currentTime: Ts(9_000),
            isSnapshotRead: true,
            memItems: [("acct/gap", entry)],
            memEnd: null,
            memEndInclusive: false,
            memBatch: 512,
            memMaybeMore: false,
            diskCursor: "acct/",
            promise: promise)
        {
            // An exhausted disk page: the key's snapshot revision is not in the projection.
            RangeScanPage = new RangeDiskPage([], false, null)
        };

        continuation.Execute(context);

        Assert.True(promise.Task.IsCompleted);
        return promise.Task.Result;
    }

    [Fact]
    public void SnapshotScan_UnflushedArchiveGap_FailsClosedWithMustRetry()
    {
        KeyValueContext context = BuildContext("scan-gap-unflushed");

        // Snapshot inside the jump window, flushes still behind the jump target: the true
        // as-of revision is invisible to both the archive and disk. The scan must retry the
        // page, never silently omit the row (the old behavior).
        KeyValueResponse? response = RunSnapshotScan(context, GappedEntry(flushedRevision: 4), Ts(6_000));

        Assert.NotNull(response);
        Assert.Equal(KeyValueResponseType.MustRetry, response!.Type);
    }

    [Fact]
    public void SnapshotScan_FlushedArchiveGap_DoesNotRetry()
    {
        KeyValueContext context = BuildContext("scan-gap-flushed");

        // Everything below the jump target is flushed: the durable history is authoritative
        // for the window, so the scan proceeds to the disk fallback instead of retrying.
        KeyValueResponse? response = RunSnapshotScan(context, GappedEntry(flushedRevision: 7), Ts(6_000));

        Assert.NotNull(response);
        Assert.Equal(KeyValueResponseType.Get, response!.Type);
    }
}
