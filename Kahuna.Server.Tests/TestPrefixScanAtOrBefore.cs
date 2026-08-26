using System.Text;

using Kahuna.Server.KeyValues;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Kommander.Time;

namespace Kahuna.Server.Tests;

/// <summary>
/// Tests for <c>GetKeyValueByPrefixAtOrBefore</c> across all three persistence backends — the
/// single-pass snapshot prefix scan that replaced the per-key revision-history walk.
///
/// Memory and SQLite serve the interface's default composition (prefix scan plus one indexed
/// at-or-before lookup per stale key). RocksDB overrides it with one sequential pass over the
/// physical range, because its head and history rows interleave there and a per-key walk re-reads
/// the range once per stale key. The parity assertions pin the override to the composition's
/// semantics; the sibling-key cases guard the RocksDB row-attribution rules ('~' may appear in
/// key names, so the last delimiter splits the owning key from its revision suffix).
/// </summary>
public sealed class TestPrefixScanAtOrBefore
{
    private static PersistenceRequestItem Rev(string key, long revision, long physical, KeyValueState state = KeyValueState.Set) =>
        new(key,
            Encoding.UTF8.GetBytes("val" + revision),
            revision: revision,
            expiresNode: 0, expiresPhysical: 0, expiresCounter: 0,
            lastUsedNode: 0, lastUsedPhysical: 0, lastUsedCounter: 0,
            lastModifiedNode: 0, lastModifiedPhysical: physical, lastModifiedCounter: 0,
            state: (int)state);

    // Stores revisions 0..count-1 for key, revision r carrying lastModifiedPhysical (r + 1) * baseStep.
    private static void StoreRevisions(IPersistenceBackend backend, string key, int count, long baseStep = 100)
    {
        for (int rev = 0; rev < count; rev++)
            backend.StoreKeyValues([Rev(key, rev, (rev + 1) * baseStep)]);
    }

    private static HLCTimestamp Ts(long physical) => new(0, physical, 0u);

    private static string TempPath()
    {
        string dir = Path.Combine(Path.GetTempPath(), "kahuna_pfxasof_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        return dir;
    }

    // ─── As-of projection: head-served, history-served, and not-yet-existing keys ─────────────────

    private static void AssertAsOfProjection(IPersistenceBackend backend)
    {
        StoreRevisions(backend, "acct/old", 1);              // head rev 0 @ 100 — at-or-before the snapshot
        StoreRevisions(backend, "acct/hot", 5);              // head rev 4 @ 500 — as-of is rev 1 @ 200
        StoreRevisions(backend, "acct/late", 3, 1000);       // head rev 2 @ 3000 — did not exist at the snapshot

        // SQLite shards rows by the key's bucket (the part before the last '/'), so a prefix scan
        // must use the exact bucket string to reach the right shard.
        List<(string Key, ReadOnlyKeyValueEntry Current, ReadOnlyKeyValueEntry? Snapshot)> page =
            backend.GetKeyValueByPrefixAtOrBefore("acct", Ts(250));

        Assert.Equal(3, page.Count);

        // Row order follows the inner scan and is not part of the contract; normalize it here.
        page.Sort(static (a, b) => string.CompareOrdinal(a.Key, b.Key));
        Assert.Equal(["acct/hot", "acct/late", "acct/old"], page.Select(p => p.Key).ToArray());

        (_, ReadOnlyKeyValueEntry hotCurrent, ReadOnlyKeyValueEntry? hotSnapshot) = page[0];
        Assert.Equal(4, hotCurrent.Revision);
        Assert.NotNull(hotSnapshot);
        Assert.Equal(1, hotSnapshot!.Revision);
        Assert.Equal("val1", Encoding.UTF8.GetString(hotSnapshot.Value!));

        (_, ReadOnlyKeyValueEntry lateCurrent, ReadOnlyKeyValueEntry? lateSnapshot) = page[1];
        Assert.Equal(2, lateCurrent.Revision);
        Assert.Null(lateSnapshot);

        (_, ReadOnlyKeyValueEntry oldCurrent, ReadOnlyKeyValueEntry? oldSnapshot) = page[2];
        Assert.Equal(0, oldCurrent.Revision);
        Assert.NotNull(oldSnapshot);
        Assert.Equal(0, oldSnapshot!.Revision);
    }

    [Fact]
    public void TestMemoryAsOfProjection()
    {
        using MemoryPersistenceBackend backend = new();
        AssertAsOfProjection(backend);
    }

    [Fact]
    public void TestSqliteAsOfProjection()
    {
        using SqlitePersistenceBackend backend = new(TempPath(), "v1");
        AssertAsOfProjection(backend);
    }

    [Fact]
    public void TestRocksDbAsOfProjection()
    {
        using RocksDbPersistenceBackend backend = new(TempPath(), "v1");
        AssertAsOfProjection(backend);
    }

    // ─── A tombstone at the snapshot is returned with its state, not silently dropped ─────────────

    private static void AssertDeletedAtSnapshotKept(IPersistenceBackend backend)
    {
        backend.StoreKeyValues([Rev("doc/x", 0, 100)]);
        backend.StoreKeyValues([Rev("doc/x", 1, 200, KeyValueState.Deleted)]);
        backend.StoreKeyValues([Rev("doc/x", 2, 300)]);

        List<(string Key, ReadOnlyKeyValueEntry Current, ReadOnlyKeyValueEntry? Snapshot)> page =
            backend.GetKeyValueByPrefixAtOrBefore("doc", Ts(250));

        (string key, ReadOnlyKeyValueEntry current, ReadOnlyKeyValueEntry? snapshot) = Assert.Single(page);
        Assert.Equal("doc/x", key);
        Assert.Equal(2, current.Revision);
        Assert.NotNull(snapshot);
        Assert.Equal(1, snapshot!.Revision);
        Assert.Equal(KeyValueState.Deleted, snapshot.State);
    }

    [Fact]
    public void TestMemoryDeletedAtSnapshotKept()
    {
        using MemoryPersistenceBackend backend = new();
        AssertDeletedAtSnapshotKept(backend);
    }

    [Fact]
    public void TestSqliteDeletedAtSnapshotKept()
    {
        using SqlitePersistenceBackend backend = new(TempPath(), "v1");
        AssertDeletedAtSnapshotKept(backend);
    }

    [Fact]
    public void TestRocksDbDeletedAtSnapshotKept()
    {
        using RocksDbPersistenceBackend backend = new(TempPath(), "v1");
        AssertDeletedAtSnapshotKept(backend);
    }

    // ─── Sibling keys containing '~' must be attributed to their own logical key ──────────────────

    private static void AssertSiblingKeysAttributedIndependently(IPersistenceBackend backend)
    {
        // "m/metric~-arch" rows sort before "m/metric"'s digit rows; "m/metric~2024" rows nest
        // inside them. The single pass must attribute every row to the key before its LAST '~'
        // and must not break a key's accumulation on a sibling's rows. All three keys share the
        // bucket "m" so the SQLite sharded scan can see them on one shard.
        StoreRevisions(backend, "m/metric", 5);
        StoreRevisions(backend, "m/metric~2024", 3);
        StoreRevisions(backend, "m/metric~-arch", 3);

        List<(string Key, ReadOnlyKeyValueEntry Current, ReadOnlyKeyValueEntry? Snapshot)> page =
            backend.GetKeyValueByPrefixAtOrBefore("m", Ts(250));

        Assert.Equal(3, page.Count);
        page.Sort(static (a, b) => string.CompareOrdinal(a.Key, b.Key));
        Assert.Equal(["m/metric", "m/metric~-arch", "m/metric~2024"], page.Select(p => p.Key).ToArray());

        foreach ((string key, ReadOnlyKeyValueEntry current, ReadOnlyKeyValueEntry? snapshot) in page)
        {
            // At ts=250 every key's as-of is its own revision 1 (physical 200), never a value
            // leaked from a sibling's rows.
            Assert.NotNull(snapshot);
            Assert.Equal(1, snapshot!.Revision);
            Assert.Equal("val1", Encoding.UTF8.GetString(snapshot.Value!));
            Assert.True(current.Revision > snapshot.Revision, $"head of {key} must be newer than its as-of");
        }
    }

    [Fact]
    public void TestMemorySiblingKeysAttributedIndependently()
    {
        using MemoryPersistenceBackend backend = new();
        AssertSiblingKeysAttributedIndependently(backend);
    }

    [Fact]
    public void TestSqliteSiblingKeysAttributedIndependently()
    {
        using SqlitePersistenceBackend backend = new(TempPath(), "v1");
        AssertSiblingKeysAttributedIndependently(backend);
    }

    [Fact]
    public void TestRocksDbSiblingKeysAttributedIndependently()
    {
        using RocksDbPersistenceBackend backend = new(TempPath(), "v1");
        AssertSiblingKeysAttributedIndependently(backend);
    }

    // ─── Cross-backend parity: the RocksDB single-pass override matches the composition ───────────

    [Fact]
    public void TestRocksDbMatchesCompositionSemantics()
    {
        using MemoryPersistenceBackend reference = new();
        using RocksDbPersistenceBackend rocks = new(TempPath(), "v1");

        foreach (IPersistenceBackend backend in new IPersistenceBackend[] { reference, rocks })
        {
            StoreRevisions(backend, "p/a", 4);
            StoreRevisions(backend, "p/b", 1);
            StoreRevisions(backend, "p/c", 3, 1000);
            backend.StoreKeyValues([Rev("p/d", 0, 100)]);
            backend.StoreKeyValues([Rev("p/d", 1, 200, KeyValueState.Deleted)]);
            StoreRevisions(backend, "p/e~7", 3);
        }

        IPersistenceBackend referenceBackend = reference;

        foreach (long ts in new long[] { 50, 150, 250, 450, 5000 })
        {
            var expected = referenceBackend.GetKeyValueByPrefixAtOrBefore("p/", Ts(ts))
                .Select(p => (p.Key, p.Current.Revision, p.Snapshot?.Revision, p.Snapshot?.State))
                .ToList();
            var actual = rocks.GetKeyValueByPrefixAtOrBefore("p/", Ts(ts))
                .Select(p => (p.Key, p.Current.Revision, p.Snapshot?.Revision, p.Snapshot?.State))
                .ToList();

            Assert.Equal(expected, actual);
        }
    }

    // ─── Cancellation: an aborted scan stops early instead of running to completion ───────────────

    [Fact]
    public void TestRocksDbAbortStopsScan()
    {
        using RocksDbPersistenceBackend backend = new(TempPath(), "v1");
        StoreRevisions(backend, "big/a", 10);
        StoreRevisions(backend, "big/b", 10);

        List<(string Key, ReadOnlyKeyValueEntry Current, ReadOnlyKeyValueEntry? Snapshot)> page =
            backend.GetKeyValueByPrefixAtOrBefore("big/", Ts(5000), static () => true);

        Assert.Empty(page);
    }
}
