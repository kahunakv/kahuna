
using System.Diagnostics;

namespace Kahuna.Server.KeyValues;

/// <summary>
/// Compact ordered container for a key's archived revision history. Stores
/// <c>(long revisionNumber, KeyValueRevisionEntry)</c> pairs in ascending revision order
/// using a plain array rather than a <c>Dictionary</c>, eliminating per-bucket and
/// per-entry hash-table overhead while preserving all required lookup semantics.
///
/// <para>Revision numbers are monotonically increasing at the point of archival, so new
/// entries are always appended at the tail. Trim removes from the front (oldest first),
/// making both operations O(1) or O(n) shift at worst for the bounded <c>n ≤ 17</c>
/// (RevisionRetention + optional floor-boundary entry). Exact-revision lookup uses binary
/// search; at-or-before scan is a forward scan through the ordered array.</para>
/// </summary>
internal sealed class KeyValueRevisionHistory
{
    // Items are kept in ascending Key order (revision numbers are monotonic at archive time).
    // Initial capacity 1 keeps the first-allocation footprint minimal and makes per-entry
    // accounting exact: each slot costs exactly one array resize (header 16 + 1 × 72 = 88 bytes
    // for the initial array; 72 bytes per additional slot on the doubling reallocations).
    private (long Key, KeyValueRevisionEntry Value)[] _items;
    private int _count;

    public KeyValueRevisionHistory()
    {
        _items = new (long, KeyValueRevisionEntry)[1];
    }

    public int Count => _count;

    /// <summary>
    /// Archives <paramref name="value"/> at revision <paramref name="key"/>. Since callers
    /// always archive in ascending revision order the common path is a tail-append. An
    /// overwrite of the same key (idempotent archive after a delete→re-set cycle) replaces
    /// the existing entry in place.
    /// </summary>
    public KeyValueRevisionEntry this[long key]
    {
        set
        {
            // Callers archive in ascending revision order, or overwrite an existing revision on an
            // idempotent re-commit. A brand-new key smaller than the current maximum would be
            // tail-appended below, breaking the ascending-order invariant that the TryGetValue /
            // ContainsKey binary searches depend on. Assert the contract so any future out-of-order
            // archival fails loudly in tests instead of silently corrupting lookups.
            Debug.Assert(_count == 0 || key >= _items[_count - 1].Key || ContainsKey(key),
                "revision archived out of ascending order; sorted-array invariant would break");

            // Scan from the tail — the key is most likely the newest entry or an overwrite
            // of the last-appended entry, so checking backward first is O(1) in the common case.
            for (int i = _count - 1; i >= 0; i--)
            {
                if (_items[i].Key == key) { _items[i] = (key, value); return; }
                if (_items[i].Key < key) break; // not found further back; fall through to append
            }

            // Capacity only grows (doubling); it is never shrunk on Remove. This is bounded because the
            // history itself is bounded (RevisionRetention + optional floor boundary, n ≤ 17), so the
            // backing array never exceeds ~32 slots regardless of how many trim/append cycles occur.
            if (_count == _items.Length)
            {
                int newCap = _items.Length * 2;
                Array.Resize(ref _items, newCap);
            }

            _items[_count++] = (key, value);
        }
    }

    /// <summary>
    /// Binary search for <paramref name="key"/> among the ascending-ordered entries.
    /// </summary>
    public bool TryGetValue(long key, out KeyValueRevisionEntry value)
    {
        int lo = 0, hi = _count - 1;
        while (lo <= hi)
        {
            int mid = (lo + hi) >> 1;
            long k = _items[mid].Key;
            if (k == key) { value = _items[mid].Value; return true; }
            if (k < key) lo = mid + 1;
            else hi = mid - 1;
        }
        value = default;
        return false;
    }

    /// <summary>
    /// Returns true when <paramref name="key"/> exists in the history.
    /// </summary>
    public bool ContainsKey(long key)
    {
        int lo = 0, hi = _count - 1;
        while (lo <= hi)
        {
            int mid = (lo + hi) >> 1;
            long k = _items[mid].Key;
            if (k == key) return true;
            if (k < key) lo = mid + 1;
            else hi = mid - 1;
        }
        return false;
    }

    /// <summary>
    /// Removes the entry with <paramref name="key"/> and shifts subsequent entries left.
    /// O(n) shift is acceptable for n ≤ 17. Returns true when the key was found and removed.
    /// </summary>
    public bool Remove(long key)
    {
        for (int i = 0; i < _count; i++)
        {
            if (_items[i].Key != key) continue;

            _count--;
            if (i < _count)
                Array.Copy(_items, i + 1, _items, i, _count - i);
            _items[_count] = default; // clear slot so the GC can collect the referenced value
            return true;
        }
        return false;
    }

    /// <summary>
    /// Returns an enumerator over <see cref="KeyValuePair{TKey, TValue}"/> in ascending key order.
    /// </summary>
    public Enumerator GetEnumerator() => new(this);

    /// <summary>
    /// Value-typed enumerator — avoids boxing. Yields <see cref="KeyValuePair{Int64, KeyValueRevisionEntry}"/>
    /// so that both <c>foreach (var kv in ...)</c> and <c>foreach ((long k, var v) in ...)</c> patterns work
    /// (the latter uses <see cref="KeyValuePair{TKey,TValue}.Deconstruct"/>).
    /// </summary>
    public struct Enumerator
    {
        private readonly KeyValueRevisionHistory _h;
        private int _index;

        internal Enumerator(KeyValueRevisionHistory h) { _h = h; _index = -1; }

        public bool MoveNext() => ++_index < _h._count;

        public KeyValuePair<long, KeyValueRevisionEntry> Current =>
            new(_h._items[_index].Key, _h._items[_index].Value);
    }
}
