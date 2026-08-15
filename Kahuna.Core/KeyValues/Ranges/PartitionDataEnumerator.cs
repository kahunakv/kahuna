
using System.Runtime.CompilerServices;
using Kommander;

using Kahuna.Server.Locks.Data;
using Kahuna.Server.Persistence.Backend;

namespace Kahuna.Server.KeyValues.Ranges;

/// <summary>
/// Enumerates everything in this node's persistence backend that belongs to one Raft partition —
/// the primitive whole-partition snapshot export and un-host purging are built on. The backend is
/// node-global and keyed by key string with no per-partition index, so ownership is derived from
/// the same routing data the request path uses: a key in a key-range space belongs to the
/// partition of the range-map descriptor covering it; any other key belongs to the hash partition
/// of its key space (the prefix before the last <c>'/'</c>), locks included — lock resources are
/// always hash-routed.
///
/// <para>
/// <b>Cost:</b> because there is no catalog of hash key spaces, every enumeration is a paged scan
/// of the whole family — O(this node's data), not O(the partition's data). It is bounded by what
/// the node itself stores (under a replication factor a node holds only its own partitions' data)
/// and runs once per replica move, but a per-partition storage layout (per-partition column family
/// or a key-space→partition index) is the structural fix if this ever dominates.
/// </para>
///
/// <para>
/// <b>Consistency:</b> each enumeration captures one <see cref="RangeMap"/> snapshot up front and
/// classifies every key against it, so a split or merge committing mid-enumeration cannot make the
/// same key owned by two partitions within one pass — the answer is simply as of the captured map.
/// The scan itself reads the physical family only; committed writes still queued in the background
/// writer are invisible, so callers needing completeness against the commit frontier must drain
/// the writer first (the scan contract on <see cref="IPersistenceBackend.ScanKeyValues"/>).
/// Completion receipts, transaction records and prepared intents are not enumerated here — those
/// stores are already keyed by partition and enumerate themselves directly.
/// </para>
/// </summary>
internal sealed class PartitionDataEnumerator
{
    private readonly IPersistenceBackend persistenceBackend;

    private readonly Func<RangeMap> currentMap;

    private readonly int hashPoolSize;

    /// <param name="persistenceBackend">The node's backend (scans read the physical family).</param>
    /// <param name="currentMap">Supplier of the committed range map (captured once per enumeration).</param>
    /// <param name="hashPoolSize">The hash pool size — <c>RaftConfiguration.InitialPartitions</c>,
    /// the fixed set of partitions hash key spaces map onto.</param>
    public PartitionDataEnumerator(IPersistenceBackend persistenceBackend, Func<RangeMap> currentMap, int hashPoolSize)
    {
        this.persistenceBackend = persistenceBackend;
        this.currentMap = currentMap;
        this.hashPoolSize = hashPoolSize;
    }

    /// <summary>
    /// The hash partition a key space maps onto: partitions <c>[1, poolSize]</c>, by the same
    /// jump-consistent hash <see cref="DataPartitionRouter.Locate"/> applies to a key's key-space
    /// prefix. Pure — depends only on its arguments — so the "which spaces belong to P" question
    /// is answerable (and testable) without a backend or a live node.
    /// </summary>
    public static int HashPartitionOfKeySpace(string keySpace, int hashPoolSize) =>
        1 + HashUtils.ConsistentHash(keySpace, hashPoolSize);

    /// <summary>
    /// The partition that owns <paramref name="key"/> under <paramref name="map"/>: the covering
    /// descriptor's partition when the key's space is a key-range space in the map, otherwise the
    /// key space's hash partition. Mirrors <c>RangeRouting.Locate</c> against an explicit map
    /// snapshot (a validated map has no gaps, so a ranged key always has a covering descriptor).
    /// </summary>
    public static int OwnerOfKey(RangeMap map, string key, int hashPoolSize)
    {
        string keySpace = KeySpaceRegistry.ExtractKeySpace(key);

        RangeDescriptor? descriptor = map.Find(keySpace, key);
        if (descriptor is not null)
            return descriptor.PartitionId;

        return HashPartitionOfKeySpace(keySpace, hashPoolSize);
    }

    /// <summary>
    /// The key-range descriptors currently assigned to <paramref name="partitionId"/> — exact,
    /// contiguous bounds, for consumers that can read a range more efficiently than filtering the
    /// whole-family scan (e.g. an MVCC range export).
    /// </summary>
    public IReadOnlyList<RangeDescriptor> KeyRangeDescriptorsOf(int partitionId)
    {
        List<RangeDescriptor> result = [];

        foreach (RangeDescriptor descriptor in currentMap().Descriptors)
        {
            if (descriptor.PartitionId == partitionId)
                result.Add(descriptor);
        }

        return result;
    }

    /// <summary>
    /// Enumerates every current key-value row owned by <paramref name="partitionId"/>, in bounded
    /// pages (each at most <paramref name="pageSize"/> entries after filtering; empty pages are
    /// never yielded). Each backend page read runs off the caller's thread.
    /// </summary>
    public async IAsyncEnumerable<IReadOnlyList<(string Key, ReadOnlyKeyValueEntry Entry)>> EnumerateKeyValuesAsync(
        int partitionId,
        int pageSize,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        RangeMap map = currentMap();

        string? cursor = null;

        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();

            string? capturedCursor = cursor;
            KeyValueScanPage page = await Task.Run(
                () => persistenceBackend.ScanKeyValues(capturedCursor, pageSize), cancellationToken).ConfigureAwait(false);

            List<(string, ReadOnlyKeyValueEntry)>? owned = null;

            foreach ((string key, ReadOnlyKeyValueEntry entry) in page.Items)
            {
                if (OwnerOfKey(map, key, hashPoolSize) == partitionId)
                    (owned ??= []).Add((key, entry));
            }

            if (owned is not null)
                yield return owned;

            if (page.NextCursor is null)
                yield break;

            cursor = page.NextCursor;
        }
    }

    /// <summary>
    /// Enumerates every persistent lock owned by <paramref name="partitionId"/>. Lock resources
    /// route purely by key-space hash (there are no key-range lock spaces), so only partitions in
    /// the hash pool can own locks — a split-created partition enumerates none.
    /// </summary>
    public async IAsyncEnumerable<IReadOnlyList<(string Resource, LockEntry Entry)>> EnumerateLocksAsync(
        int partitionId,
        int pageSize,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        string? cursor = null;

        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();

            string? capturedCursor = cursor;
            LockScanPage page = await Task.Run(
                () => persistenceBackend.ScanLocks(capturedCursor, pageSize), cancellationToken).ConfigureAwait(false);

            List<(string, LockEntry)>? owned = null;

            foreach ((string resource, LockEntry entry) in page.Items)
            {
                if (HashPartitionOfKeySpace(KeySpaceRegistry.ExtractKeySpace(resource), hashPoolSize) == partitionId)
                    (owned ??= []).Add((resource, entry));
            }

            if (owned is not null)
                yield return owned;

            if (page.NextCursor is null)
                yield break;

            cursor = page.NextCursor;
        }
    }
}
