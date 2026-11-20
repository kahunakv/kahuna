
using System.Collections;
using System.Diagnostics.CodeAnalysis;
using Kommander.Time;

namespace Kahuna.Server.KeyValues;

/// <summary>
/// The MVCC entries of one key, by transaction: the read pins and the staged writes of the transactions that
/// touched it. Owned by the key's actor.
///
/// <para>Every add, replace and remove tells the owning <see cref="KeyValueEntry"/>, which retracts its published
/// head record and queues a new one. The record carries a copy of this table's shape (see
/// <see cref="PublishedHead.Pins"/>), and a reader on another thread decides from that copy whether a transaction
/// reads its own pin or its own staged write. A change that did not reach the record would let a transaction
/// read the committed head over its own write, so the table has no mutation path that skips the notification.
/// A change of an entry's revision, stamp or staged state reaches the same notification through
/// <see cref="KeyValueMvccEntry.Table"/>.</para>
/// </summary>
internal sealed class KeyValueMvccTable : IEnumerable<KeyValuePair<HLCTimestamp, KeyValueMvccEntry>>
{
    private readonly Dictionary<HLCTimestamp, KeyValueMvccEntry> entries = new();

    /// <summary>The entry this table belongs to, or null while it belongs to none. Set by the
    /// <see cref="KeyValueEntry.MvccEntries"/> setter.</summary>
    internal KeyValueEntry? Owner;

    public int Count => entries.Count;

    public Dictionary<HLCTimestamp, KeyValueMvccEntry>.ValueCollection Values => entries.Values;

    public bool TryGetValue(HLCTimestamp transactionId, [MaybeNullWhen(false)] out KeyValueMvccEntry entry) =>
        entries.TryGetValue(transactionId, out entry);

    public bool ContainsKey(HLCTimestamp transactionId) => entries.ContainsKey(transactionId);

    public KeyValueMvccEntry this[HLCTimestamp transactionId]
    {
        get => entries[transactionId];
        set
        {
            if (entries.TryGetValue(transactionId, out KeyValueMvccEntry? previous) && !ReferenceEquals(previous, value))
                previous.Table = null;

            entries[transactionId] = value;
            value.Table = this;
            Changed();
        }
    }

    public void Add(HLCTimestamp transactionId, KeyValueMvccEntry entry)
    {
        entries.Add(transactionId, entry);
        entry.Table = this;
        Changed();
    }

    public bool Remove(HLCTimestamp transactionId, [MaybeNullWhen(false)] out KeyValueMvccEntry removed)
    {
        if (!entries.Remove(transactionId, out removed))
            return false;

        removed.Table = null;
        Changed();
        return true;
    }

    /// <summary>The struct enumerator of the backing dictionary, so a <c>foreach</c> allocates nothing.</summary>
    public Dictionary<HLCTimestamp, KeyValueMvccEntry>.Enumerator GetEnumerator() => entries.GetEnumerator();

    IEnumerator<KeyValuePair<HLCTimestamp, KeyValueMvccEntry>> IEnumerable<KeyValuePair<HLCTimestamp, KeyValueMvccEntry>>.GetEnumerator() =>
        entries.GetEnumerator();

    IEnumerator IEnumerable.GetEnumerator() => entries.GetEnumerator();

    /// <summary>Tells the owning entry that the shape of this table changed.</summary>
    internal void Changed() => Owner?.MvccChanged();
}
