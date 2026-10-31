
using System.Diagnostics.CodeAnalysis;
using Kahuna.Utils;

namespace Kahuna.Server.KeyValues;

/// <summary>
/// The resident entries of one key-value actor. Two structures hold the same key set and the same entry
/// references: an ordered B-tree that serves range, bucket and whole-store walks, and an ordinal hash index
/// beside it that serves point lookups. A cold point lookup in the tree costs 700-850 ns at a million resident
/// keys (a binary search per node that chases string references); the hash index answers in about 100 ns.
///
/// <para><see cref="Insert"/> and <see cref="Remove"/> are the only mutations and update both structures, so
/// they can never disagree; <see cref="KeyValueContext.InsertStoreEntry"/> and
/// <see cref="KeyValueContext.RemoveStoreEntry"/> are their only callers. The index shares the key string
/// instance with the tree, so a resident key costs one hash slot (<see cref="KeyValueStoreAccounting.PointIndexSlotBytes"/>)
/// and no second string. Owned by a single actor; no synchronisation.</para>
/// </summary>
internal sealed class KeyValueStore
{
    /// <summary>Node order of the ordered tree: 32 keys per node keeps a million-key tree four levels deep.</summary>
    public const int DefaultOrder = 32;

    /// <summary>Smallest index the store starts with, so a small or empty actor pays one tiny allocation.</summary>
    private const int MinInitialCapacity = 1024;

    private readonly BTree<string, KeyValueEntry> ordered;
    private readonly Dictionary<string, KeyValueEntry> index;

    /// <param name="order">Node order of the ordered tree.</param>
    /// <param name="expectedEntries">
    /// How many entries the actor is allowed to keep resident (<c>MaxEntriesPerActor</c>); sizes the index so it
    /// reaches that count in a few doublings instead of a dozen. Zero or less means "unknown" and starts small.
    /// </param>
    public KeyValueStore(int order = DefaultOrder, int expectedEntries = 0)
    {
        ordered = new(order);
        index = new(InitialIndexCapacity(expectedEntries), StringComparer.Ordinal);
    }

    /// <summary>
    /// The index capacity to start from for an actor allowed <paramref name="maxEntries"/> resident entries: an
    /// eighth of the cap, at least <see cref="MinInitialCapacity"/>. The index then reaches the cap in three
    /// doublings, each a single array copy, while an actor that stays small or empty pays about 150 KB at the
    /// default cap instead of 1.4 MB for a full-size table.
    /// </summary>
    internal static int InitialIndexCapacity(int maxEntries) =>
        maxEntries <= 0 ? MinInitialCapacity : Math.Max(MinInitialCapacity, maxEntries / 8);

    /// <summary>Number of resident entries.</summary>
    public int Count => index.Count;

    /// <summary>Point lookup through the hash index.</summary>
    public bool TryGetValue(string key, [MaybeNullWhen(false)] out KeyValueEntry value) =>
        index.TryGetValue(key, out value);

    /// <summary>Point lookup through the hash index; null when the key is not resident.</summary>
    public KeyValueEntry? Get(string key) => index.GetValueOrDefault(key);

    /// <summary>Whether the key is resident, through the hash index.</summary>
    public bool ContainsKey(string key) => index.ContainsKey(key);

    /// <summary>
    /// Inserts or replaces the entry filed under <paramref name="key"/> in both structures. Only
    /// <see cref="KeyValueContext.InsertStoreEntry"/> may call this.
    /// </summary>
    public void Insert(string key, KeyValueEntry value)
    {
        index[key] = value;
        ordered.Insert(key, value);
    }

    /// <summary>
    /// Removes <paramref name="key"/> from both structures. Only <see cref="KeyValueContext.RemoveStoreEntry"/>
    /// may call this. Returns false when the key was not resident.
    /// </summary>
    public bool Remove(string key)
    {
        if (!index.Remove(key))
            return false;

        ordered.Remove(key);
        return true;
    }

    /// <summary>Every resident entry in ascending ordinal key order.</summary>
    public IEnumerable<KeyValuePair<string, KeyValueEntry>> GetItems() => ordered.GetItems();

    /// <summary>
    /// Resident entries inside an ordinal key range, in ascending key order, at most <paramref name="limit"/>
    /// of them. See <see cref="BTree{TKey,TValue}.GetByRange"/>.
    /// </summary>
    public IEnumerable<KeyValuePair<string, KeyValueEntry>> GetByRange(
        string start, bool startInclusive, string? end, bool endInclusive, int limit) =>
        ordered.GetByRange(start, startInclusive, end, endInclusive, limit);

    /// <summary>Resident entries whose key starts with <paramref name="prefix"/>, in ascending key order.</summary>
    public IEnumerable<KeyValuePair<string, KeyValueEntry>> GetByBucket(string prefix) => ordered.GetByBucket(prefix);

    /// <summary>
    /// Compares the index and the tree entry by entry. Returns null when they hold the same keys mapped to the
    /// same entry instances, otherwise a description of the first difference. A full walk of both structures:
    /// for tests and diagnostics, never for the request path.
    /// </summary>
    internal string? FindIncoherence()
    {
        if (ordered.Count != index.Count)
            return $"tree holds {ordered.Count} entries, index holds {index.Count}";

        int walked = 0;
        foreach (KeyValuePair<string, KeyValueEntry> kv in ordered.GetItems())
        {
            walked++;
            if (!index.TryGetValue(kv.Key, out KeyValueEntry? indexed))
                return $"key '{kv.Key}' is in the tree but not in the index";
            if (!ReferenceEquals(indexed, kv.Value))
                return $"key '{kv.Key}' maps to different entry instances in the tree and the index";
        }

        if (walked != index.Count)
            return $"tree walk yielded {walked} entries, index holds {index.Count}";

        return null;
    }
}
