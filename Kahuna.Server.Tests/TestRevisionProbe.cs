
using Kahuna.Server.KeyValues;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace Kahuna.Server.Tests;

/// <summary>
/// The RocksDB as-of read (<c>GetKeyValueRevisionAtOrBefore</c>) probes exact revision keys downward
/// from the ceiling before falling back to the forward walk over every revision row. These tests pin
/// that the probe returns exactly what the walk returned for every shape of chain and snapshot —
/// inside the probe window, past it, before the first revision, with holes in the chain, and from a
/// ceiling with no head to anchor on — and that the hydration read returns the head with its newest
/// revisions, newest first. CamusDB feature 80af367a.
/// </summary>
public class TestRevisionProbe
{
    private static string RocksDbTempPath()
    {
        string dir = Path.Combine(Path.GetTempPath(), "kahuna_probe_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        return dir;
    }

    /// <summary>Revision <paramref name="revision"/> committed at physical time <c>revision * 10</c>.</summary>
    private static PersistenceRequestItem Item(string key, long revision) =>
        new(key,
            System.Text.Encoding.UTF8.GetBytes("val" + revision),
            revision: revision,
            expiresNode: 0, expiresPhysical: 0, expiresCounter: 0,
            lastUsedNode: 0, lastUsedPhysical: 0, lastUsedCounter: 0,
            lastModifiedNode: 0, lastModifiedPhysical: revision * 10, lastModifiedCounter: 0,
            state: (int)KeyValueState.Set);

    private static HLCTimestamp At(long physical) => new(0, physical, 0);

    /// <summary>The contract, computed by brute force over the revisions that were written.</summary>
    private static long? Expected(IEnumerable<long> written, long maxRevision, long tsPhysical)
    {
        long? best = null;
        foreach (long r in written)
            if (r <= maxRevision && r * 10 <= tsPhysical && (best is null || r > best))
                best = r;
        return best;
    }

    private static void AssertMatchesContract(IPersistenceBackend backend, string key, long[] written, long maxRevision, long tsPhysical)
    {
        long? expected = Expected(written, maxRevision, tsPhysical);
        KeyValueEntry? actual = backend.GetKeyValueRevisionAtOrBefore(key, maxRevision, At(tsPhysical));

        if (expected is null)
        {
            Assert.Null(actual);
            return;
        }

        Assert.NotNull(actual);
        Assert.Equal(expected.Value, actual.Revision);
        Assert.Equal("val" + expected.Value, System.Text.Encoding.UTF8.GetString(actual.Value!));
    }

    [Theory]
    [InlineData("rocksdb")]
    [InlineData("memory")]
    public void AsOfRead_MatchesContract_AcrossProbeWindowWalkAndEdges(string kind)
    {
        using IDisposable backendLifetime = Create(kind, out IPersistenceBackend backend);
        const string key = "probe/contiguous";
        long[] written = Enumerable.Range(1, 200).Select(i => (long)i).ToArray();
        foreach (long r in written)
            backend.StoreKeyValues([Item(key, r)]);

        // Snapshots inside the 64-revision probe window below the head, past it (walk), before every
        // revision, exactly on a revision, and after the newest.
        foreach (long ts in new long[] { 1995, 1990, 1985, 1500, 1360, 1350, 700, 105, 100, 15, 10, 9, 1, 0, 5000 })
        {
            AssertMatchesContract(backend, key, written, maxRevision: 199, ts);   // head - 1, the handler's shape
            AssertMatchesContract(backend, key, written, maxRevision: 50, ts);    // ceiling inside the chain
            AssertMatchesContract(backend, key, written, maxRevision: 250, ts);   // ceiling above the chain
            AssertMatchesContract(backend, key, written, maxRevision: long.MaxValue, ts);   // no anchor: walk only
        }

        Assert.Null(backend.GetKeyValueRevisionAtOrBefore(key, -1, At(5000)));
        Assert.Null(backend.GetKeyValueRevisionAtOrBefore("probe/absent", 199, At(5000)));
    }

    [Theory]
    [InlineData("rocksdb")]
    [InlineData("memory")]
    public void AsOfRead_ChainWithHole_FallsThroughToOlderRevisions(string kind)
    {
        using IDisposable backendLifetime = Create(kind, out IPersistenceBackend backend);
        const string key = "probe/hole";
        // 1..10 exist, 11..99 were never written (a head jump), 100..110 exist. A probe from 109 sees
        // 100..109, then an entirely absent batch — it must hand off to the walk, not stop.
        long[] written = [.. Enumerable.Range(1, 10).Select(i => (long)i), .. Enumerable.Range(100, 11).Select(i => (long)i)];
        foreach (long r in written)
            backend.StoreKeyValues([Item(key, r)]);

        foreach (long ts in new long[] { 1095, 1000, 999, 500, 105, 100, 95, 15, 10, 5 })
        {
            AssertMatchesContract(backend, key, written, maxRevision: 109, ts);
            AssertMatchesContract(backend, key, written, maxRevision: 99, ts);
        }
    }

    [Theory]
    [InlineData("rocksdb")]
    [InlineData("memory")]
    public void AsOfRead_PrunedPrefix_ReturnsNullBelowTheFloor(string kind)
    {
        using IDisposable backendLifetime = Create(kind, out IPersistenceBackend backend);
        const string key = "probe/pruned";
        // Only the newest 5 of 80 revisions survive: a snapshot older than revision 76 has no answer.
        long[] written = Enumerable.Range(76, 5).Select(i => (long)i).ToArray();
        foreach (long r in Enumerable.Range(1, 80))
            if (r >= 76) backend.StoreKeyValues([Item(key, r)]);
            else backend.StoreKeyValues([Item(key, r)]);

        if (backend is RocksDbPersistenceBackend rocks)
        {
            Assert.True(rocks.PruneKeyValueRevisions([key], retentionCount: 5, TimeSpan.Zero, batchSize: 1000, HLCTimestamp.Zero, out _));
            foreach (long ts in new long[] { 795, 790, 770, 760, 755, 100, 5 })
                AssertMatchesContract(backend, key, written, maxRevision: 79, ts);
        }
    }

    [Theory]
    [InlineData("rocksdb")]
    [InlineData("memory")]
    public void Hydration_ReturnsHeadWithNewestRevisionsNewestFirst(string kind)
    {
        using IDisposable backendLifetime = Create(kind, out IPersistenceBackend backend);
        const string key = "probe/hydrate";
        foreach (long r in Enumerable.Range(1, 200))
            backend.StoreKeyValues([Item(key, r)]);

        KeyValueHydration h = backend.GetKeyValueWithRecentRevisions(key, 16);
        Assert.NotNull(h.Head);
        Assert.Equal(200, h.Head.Revision);
        Assert.Equal(16, h.RecentRevisions.Count);
        Assert.Equal(Enumerable.Range(0, 16).Select(i => 199L - i), h.RecentRevisions.Select(r => r.Revision));
        Assert.All(h.RecentRevisions, r => Assert.Equal("val" + r.Revision, System.Text.Encoding.UTF8.GetString(r.Value!)));
        Assert.All(h.RecentRevisions, r => Assert.Equal(r.Revision * 10, r.LastModified.L));

        // A short chain returns what exists; a missing key returns no head and nothing archived.
        const string shortKey = "probe/short";
        foreach (long r in Enumerable.Range(1, 3))
            backend.StoreKeyValues([Item(shortKey, r)]);
        KeyValueHydration s = backend.GetKeyValueWithRecentRevisions(shortKey, 16);
        Assert.Equal(3, s.Head!.Revision);
        Assert.Equal([2L, 1L], s.RecentRevisions.Select(r => r.Revision));

        KeyValueHydration none = backend.GetKeyValueWithRecentRevisions("probe/absent", 16);
        Assert.Null(none.Head);
        Assert.Empty(none.RecentRevisions);

        KeyValueHydration zero = backend.GetKeyValueWithRecentRevisions(key, 0);
        Assert.Equal(200, zero.Head!.Revision);
        Assert.Empty(zero.RecentRevisions);
    }

    private static IDisposable Create(string kind, out IPersistenceBackend backend)
    {
        switch (kind)
        {
            case "rocksdb":
                RocksDbPersistenceBackend rocks = new(RocksDbTempPath(), "v1");
                backend = rocks;
                return rocks;
            case "memory":
                MemoryPersistenceBackend memory = new();
                backend = memory;
                return memory;
            default:
                throw new ArgumentOutOfRangeException(nameof(kind));
        }
    }
}
