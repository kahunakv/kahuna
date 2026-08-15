
using Kommander;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Server.Locks.Data;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;

namespace Kahuna.Server.Tests;

/// <summary>
/// The per-partition data-ownership primitive replica seeding and un-host purging build on: given
/// a partition id, enumerate exactly the keys and locks in the node-global backend that the
/// routing data assigns to it — key-range spaces by their descriptors, everything else by the
/// key-space hash — with bounded, resumable pages that neither drop nor duplicate a key.
/// </summary>
public sealed class TestPartitionDataEnumerator
{
    private const int HashPoolSize = 3;

    private static readonly string[] HashSpaces = ["hash-a", "hash-b"];

    /// <summary>
    /// Two hash spaces and two key-range spaces over a 3-partition hash pool: "ranged1" is a
    /// single unsplit range on partition 2; "ranged2" is split at "ranged2/m" between partition 1
    /// and the split-created partition 4 (outside the hash pool).
    /// </summary>
    private static RangeMap BuildMap() => new([
        new RangeDescriptor { KeySpace = "ranged1", PartitionId = 2, Generation = 1 },
        new RangeDescriptor { KeySpace = "ranged2", EndKey = "ranged2/m", PartitionId = 1, Generation = 2 },
        new RangeDescriptor { KeySpace = "ranged2", StartKey = "ranged2/m", PartitionId = 4, Generation = 2 }
    ]);

    private static PersistenceRequestItem KvItem(string key) => new(
        key, [1], revision: 1,
        expiresNode: 0, expiresPhysical: 0, expiresCounter: 0,
        lastUsedNode: 0, lastUsedPhysical: 0, lastUsedCounter: 0,
        lastModifiedNode: 0, lastModifiedPhysical: 1, lastModifiedCounter: 0,
        state: (int)KeyValueState.Set);

    private static PersistenceRequestItem LockItem(string resource) => new(
        resource, [2], revision: 1,
        expiresNode: 0, expiresPhysical: 1000, expiresCounter: 0,
        lastUsedNode: 0, lastUsedPhysical: 0, lastUsedCounter: 0,
        lastModifiedNode: 0, lastModifiedPhysical: 1, lastModifiedCounter: 0,
        state: (int)LockState.Locked);

    private static (PartitionDataEnumerator Enumerator, HashSet<string> AllKeys, HashSet<string> AllLocks) BuildFixture()
    {
        MemoryPersistenceBackend backend = new();
        RangeMap map = BuildMap();

        HashSet<string> allKeys = [];
        List<PersistenceRequestItem> kvItems = [];

        foreach (string space in HashSpaces)
        {
            for (int i = 0; i < 6; i++)
            {
                string key = $"{space}/{(char)('a' + i)}";
                kvItems.Add(KvItem(key));
                allKeys.Add(key);
            }
        }

        foreach (string key in (string[])[
            "ranged1/a", "ranged1/k", "ranged1/z",
            "ranged2/a", "ranged2/l", "ranged2/m", "ranged2/x"])
        {
            kvItems.Add(KvItem(key));
            allKeys.Add(key);
        }

        Assert.True(backend.StoreKeyValues(kvItems));

        HashSet<string> allLocks = [];
        List<PersistenceRequestItem> lockItems = [];
        foreach (string space in HashSpaces)
        {
            for (int i = 0; i < 4; i++)
            {
                string resource = $"{space}/lock{i}";
                lockItems.Add(LockItem(resource));
                allLocks.Add(resource);
            }
        }

        Assert.True(backend.StoreLocks(lockItems));

        return (new PartitionDataEnumerator(backend, () => map, HashPoolSize), allKeys, allLocks);
    }

    private static async Task<List<string>> CollectKeys(PartitionDataEnumerator enumerator, int partitionId, int pageSize)
    {
        List<string> keys = [];
        await foreach (IReadOnlyList<(string Key, ReadOnlyKeyValueEntry Entry)> page in
            enumerator.EnumerateKeyValuesAsync(partitionId, pageSize, TestContext.Current.CancellationToken))
        {
            Assert.NotEmpty(page);
            foreach ((string key, ReadOnlyKeyValueEntry entry) in page)
            {
                Assert.NotNull(entry);
                keys.Add(key);
            }
        }

        return keys;
    }

    private static async Task<List<string>> CollectLocks(PartitionDataEnumerator enumerator, int partitionId, int pageSize)
    {
        List<string> resources = [];
        await foreach (IReadOnlyList<(string Resource, LockEntry Entry)> page in
            enumerator.EnumerateLocksAsync(partitionId, pageSize, TestContext.Current.CancellationToken))
        {
            Assert.NotEmpty(page);
            foreach ((string resource, LockEntry entry) in page)
            {
                Assert.NotNull(entry);
                resources.Add(resource);
            }
        }

        return resources;
    }

    [Fact]
    public void HashClassifier_MatchesTheRequestPathRouterFormula()
    {
        // The request path routes a key by hashing its key-space prefix (the part before the last
        // '/') onto partitions [1, pool]. The pure classifier must agree with that formula for
        // keys with a space prefix, nested separators, and no separator at all.
        foreach (string key in (string[])["hash-a/x", "a/b/c", "standalone", "hash-b/deep/leaf"])
        {
            int viaRouter = 1 + (int)HashUtils.InversePrefixedHash(key, '/', HashPoolSize);
            int viaClassifier = PartitionDataEnumerator.HashPartitionOfKeySpace(
                KeySpaceRegistry.ExtractKeySpace(key), HashPoolSize);

            Assert.Equal(viaRouter, viaClassifier);
        }
    }

    [Fact]
    public async Task Enumerations_ArePartitionDisjointAndCoverEveryKey()
    {
        (PartitionDataEnumerator enumerator, HashSet<string> allKeys, _) = BuildFixture();

        List<string> union = [];
        foreach (int partitionId in (int[])[1, 2, 3, 4])
            union.AddRange(await CollectKeys(enumerator, partitionId, pageSize: 64));

        Assert.Equal(allKeys.Count, union.Count);          // disjoint: no key claimed twice
        Assert.Equal(allKeys, union.ToHashSet());          // complete: no key unclaimed
    }

    [Fact]
    public async Task RangedSpaces_FollowTheirDescriptors_NotTheHash()
    {
        (PartitionDataEnumerator enumerator, _, _) = BuildFixture();

        List<string> p2 = await CollectKeys(enumerator, 2, pageSize: 64);
        Assert.Superset(new HashSet<string> { "ranged1/a", "ranged1/k", "ranged1/z" }, p2.ToHashSet());

        List<string> p1 = await CollectKeys(enumerator, 1, pageSize: 64);
        Assert.Contains("ranged2/a", p1);
        Assert.Contains("ranged2/l", p1);
        Assert.DoesNotContain("ranged2/m", p1);            // split boundary is exclusive on the left

        // The split-created partition sits outside the hash pool, so it owns exactly its
        // descriptor's keys — the boundary key included — and can never claim hash data.
        List<string> p4 = await CollectKeys(enumerator, 4, pageSize: 64);
        Assert.Equal(["ranged2/m", "ranged2/x"], p4.Order());
    }

    [Fact]
    public async Task UnknownPartition_EnumeratesNothing()
    {
        (PartitionDataEnumerator enumerator, _, _) = BuildFixture();

        Assert.Empty(await CollectKeys(enumerator, 9, pageSize: 64));
        Assert.Empty(await CollectLocks(enumerator, 9, pageSize: 64));
    }

    [Fact]
    public async Task PagingBoundary_NeverDropsNorDuplicatesAKey()
    {
        (PartitionDataEnumerator enumerator, _, _) = BuildFixture();

        foreach (int partitionId in (int[])[1, 2, 3, 4])
        {
            List<string> wide = await CollectKeys(enumerator, partitionId, pageSize: 1000);

            foreach (int pageSize in (int[])[1, 2, 3])
            {
                List<string> paged = await CollectKeys(enumerator, partitionId, pageSize);
                Assert.Equal(wide.Order(), paged.Order());
            }
        }
    }

    [Fact]
    public async Task Locks_ArePartitionDisjointAndCoverEveryResource_AndNeverLandOutsideTheHashPool()
    {
        (PartitionDataEnumerator enumerator, _, HashSet<string> allLocks) = BuildFixture();

        List<string> union = [];
        foreach (int partitionId in (int[])[1, 2, 3])
            union.AddRange(await CollectLocks(enumerator, partitionId, pageSize: 3));

        Assert.Equal(allLocks.Count, union.Count);
        Assert.Equal(allLocks, union.ToHashSet());

        // Lock resources route purely by hash: a split-created partition owns none.
        Assert.Empty(await CollectLocks(enumerator, 4, pageSize: 64));
    }

    [Fact]
    public void KeyRangeDescriptorsOf_ListsExactlyThePartitionsDescriptors()
    {
        (PartitionDataEnumerator enumerator, _, _) = BuildFixture();

        IReadOnlyList<RangeDescriptor> p4 = enumerator.KeyRangeDescriptorsOf(4);
        RangeDescriptor descriptor = Assert.Single(p4);
        Assert.Equal("ranged2", descriptor.KeySpace);
        Assert.Equal("ranged2/m", descriptor.StartKey);
        Assert.Null(descriptor.EndKey);

        Assert.Empty(enumerator.KeyRangeDescriptorsOf(3));
    }
}
