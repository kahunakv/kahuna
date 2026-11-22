using System.Collections;

namespace Kahuna.Utils;

/// <summary>
/// An append-only list stored in chunks of <see cref="ChunkSize"/> items. A capture of a large collection
/// uses it in place of a <see cref="List{T}"/>: a list sized for a million items is one array of tens of
/// megabytes, and a node near its heap limit cannot always allocate it. A chunk of the item types used here
/// stays below the large object threshold.
/// </summary>
internal sealed class ChunkedList<T> : IEnumerable<T>
{
    private const int ChunkShift = 10;

    /// <summary>Items in one chunk.</summary>
    internal const int ChunkSize = 1 << ChunkShift;

    private readonly List<T[]> chunks = [];

    private int count;

    public int Count => count;

    public T this[int index]
    {
        get
        {
            ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual((uint)index, (uint)count, nameof(index));
            return chunks[index >> ChunkShift][index & (ChunkSize - 1)];
        }
    }

    public void Add(T item)
    {
        int slot = count & (ChunkSize - 1);

        if (slot == 0 && (count >> ChunkShift) == chunks.Count)
            chunks.Add(new T[ChunkSize]);

        chunks[count >> ChunkShift][slot] = item;
        count++;
    }

    /// <summary>Empties the list and keeps its chunks for the next fill. The chunks keep their old items until
    /// they are written again.</summary>
    public void Clear() => count = 0;

    public Enumerator GetEnumerator() => new(this);

    IEnumerator<T> IEnumerable<T>.GetEnumerator() => GetEnumerator();

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    public struct Enumerator(ChunkedList<T> list) : IEnumerator<T>
    {
        private int index = -1;

        public readonly T Current => list.chunks[index >> ChunkShift][index & (ChunkSize - 1)];

        readonly object? IEnumerator.Current => Current;

        public bool MoveNext() => ++index < list.count;

        public void Reset() => index = -1;

        public readonly void Dispose()
        {
        }
    }
}
