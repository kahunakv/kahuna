using System.Text;

using Kahuna.Server.KeyValues;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Kommander.Time;

namespace Kahuna.Server.Tests;

/// <summary>
/// Tests for <c>GetKeyValueRevisionAtOrBefore</c> across all three persistence backends — the
/// single-seek snapshot lookup that replaced the per-revision point-read walk.
///
/// The sibling-key cases guard the RocksDB prefix-iterator path specifically: because key names may
/// contain the '~' delimiter, a key K and another key literally named "K~…" share the "K~" row
/// prefix. The scan must not (a) mis-parse a sibling's "K~2024~5" row as a revision of K, nor
/// (b) break early on a sibling's "~CURRENT" row that sorts before K's own revision rows. Memory and
/// SQLite resolve K by exact match, so they are immune by construction and assert cross-backend parity.
/// </summary>
public sealed class TestRevisionAtOrBefore
{
    private static PersistenceRequestItem Rev(string key, long revision, long physical) =>
        new(key,
            Encoding.UTF8.GetBytes("val" + revision),
            revision: revision,
            expiresNode: 0, expiresPhysical: 0, expiresCounter: 0,
            lastUsedNode: 0, lastUsedPhysical: 0, lastUsedCounter: 0,
            lastModifiedNode: 0, lastModifiedPhysical: physical, lastModifiedCounter: 0,
            state: (int)KeyValueState.Set);

    // Stores revisions 0..count-1 for key, revision r carrying lastModifiedPhysical (r + 1) * 100.
    private static void StoreRevisions(IPersistenceBackend backend, string key, int count)
    {
        for (int rev = 0; rev < count; rev++)
            backend.StoreKeyValues([Rev(key, rev, (rev + 1) * 100)]);
    }

    private static HLCTimestamp Ts(long physical) => new(0, physical, 0u);

    private static string RocksDbTempPath()
    {
        string dir = Path.Combine(Path.GetTempPath(), "kahuna_rev_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        return dir;
    }

    private static string ValueOf(KeyValueEntry? entry) => Encoding.UTF8.GetString(entry!.Value!);

    // ─── Basic snapshot semantics (highest revision with revision ≤ max and LastModified ≤ ts) ────

    private static void AssertBasicSnapshot(IPersistenceBackend backend)
    {
        StoreRevisions(backend, "metric", 3); // rev 0/1/2 at physical 100/200/300

        // A snapshot between rev1 (200) and rev2 (300) sees rev1.
        KeyValueEntry? mid = backend.GetKeyValueRevisionAtOrBefore("metric", 2, Ts(250));
        Assert.NotNull(mid);
        Assert.Equal(1, mid!.Revision);
        Assert.Equal("val1", ValueOf(mid));

        // A snapshot before the first revision sees nothing.
        Assert.Null(backend.GetKeyValueRevisionAtOrBefore("metric", 2, Ts(99)));

        // maxRevision caps the result even when a newer revision is visible by timestamp.
        KeyValueEntry? capped = backend.GetKeyValueRevisionAtOrBefore("metric", 1, Ts(10_000));
        Assert.NotNull(capped);
        Assert.Equal(1, capped!.Revision);
    }

    [Fact]
    public void TestMemoryBasicSnapshot()
    {
        using MemoryPersistenceBackend backend = new();
        AssertBasicSnapshot(backend);
    }

    [Fact]
    public void TestSqliteBasicSnapshot()
    {
        using SqlitePersistenceBackend backend = new(RocksDbTempPath(), "v1");
        AssertBasicSnapshot(backend);
    }

    [Fact]
    public void TestRocksDbBasicSnapshot()
    {
        using RocksDbPersistenceBackend backend = new(RocksDbTempPath(), "v1");
        AssertBasicSnapshot(backend);
    }

    // ─── Sibling key "K~2024" must not be mis-attributed as a revision of K ───────────────────────

    private static void AssertSiblingDigitKeyNotMisattributed(IPersistenceBackend backend)
    {
        StoreRevisions(backend, "metric", 3);        // metric~0/1/2 + metric~CURRENT
        StoreRevisions(backend, "metric~2024", 3);   // metric~2024~0/1/2 + metric~2024~CURRENT

        // The newest visible revision of "metric" is its own rev 2 — never the sibling's rows, whose
        // "2024~r" suffix would parse to a bogus revision 2024 without the full-consumption guard.
        KeyValueEntry? result = backend.GetKeyValueRevisionAtOrBefore("metric", long.MaxValue, Ts(10_000));
        Assert.NotNull(result);
        Assert.Equal(2, result!.Revision);
        Assert.Equal("val2", ValueOf(result));
    }

    [Fact]
    public void TestMemorySiblingDigitKeyNotMisattributed()
    {
        using MemoryPersistenceBackend backend = new();
        AssertSiblingDigitKeyNotMisattributed(backend);
    }

    [Fact]
    public void TestSqliteSiblingDigitKeyNotMisattributed()
    {
        using SqlitePersistenceBackend backend = new(RocksDbTempPath(), "v1");
        AssertSiblingDigitKeyNotMisattributed(backend);
    }

    [Fact]
    public void TestRocksDbSiblingDigitKeyNotMisattributed()
    {
        using RocksDbPersistenceBackend backend = new(RocksDbTempPath(), "v1");
        AssertSiblingDigitKeyNotMisattributed(backend);
    }

    // ─── Sibling key whose suffix sorts before K's digit rows must not truncate K's scan ──────────

    private static void AssertSiblingDelimiterKeyDoesNotTruncateScan(IPersistenceBackend backend)
    {
        // "metric~-arch" sorts before "metric~0" ('-' = 0x2D < '0' = 0x30); its "~CURRENT" row would
        // break a naive scan before any of "metric"'s own revision rows were read.
        StoreRevisions(backend, "metric~-arch", 3);
        StoreRevisions(backend, "metric", 3);

        KeyValueEntry? result = backend.GetKeyValueRevisionAtOrBefore("metric", long.MaxValue, Ts(10_000));
        Assert.NotNull(result);
        Assert.Equal(2, result!.Revision);
        Assert.Equal("val2", ValueOf(result));
    }

    [Fact]
    public void TestMemorySiblingDelimiterKeyDoesNotTruncateScan()
    {
        using MemoryPersistenceBackend backend = new();
        AssertSiblingDelimiterKeyDoesNotTruncateScan(backend);
    }

    [Fact]
    public void TestSqliteSiblingDelimiterKeyDoesNotTruncateScan()
    {
        using SqlitePersistenceBackend backend = new(RocksDbTempPath(), "v1");
        AssertSiblingDelimiterKeyDoesNotTruncateScan(backend);
    }

    [Fact]
    public void TestRocksDbSiblingDelimiterKeyDoesNotTruncateScan()
    {
        using RocksDbPersistenceBackend backend = new(RocksDbTempPath(), "v1");
        AssertSiblingDelimiterKeyDoesNotTruncateScan(backend);
    }
}
