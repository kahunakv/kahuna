using Kahuna.Utils;

namespace Kahuna.Server.Tests;

/// <summary>
/// The list a capture fills in place of a <see cref="List{T}"/>: it stores its items in chunks of a fixed
/// size, so a capture of a million items makes no allocation larger than one chunk.
/// </summary>
public sealed class TestChunkedList
{
    [Fact]
    public void ItemsAreRead_InTheOrderTheyWereAdded_AcrossChunks()
    {
        const int count = ChunkedList<int>.ChunkSize * 3 + 17;

        ChunkedList<int> list = new();
        Assert.Empty(list);

        for (int i = 0; i < count; i++)
            list.Add(i * 7);

        Assert.Equal(count, list.Count);

        for (int i = 0; i < count; i++)
            Assert.Equal(i * 7, list[i]);

        int next = 0;
        foreach (int item in list)
            Assert.Equal(next++ * 7, item);

        Assert.Equal(count, next);
        Assert.Throws<ArgumentOutOfRangeException>(() => list[count]);
        Assert.Throws<ArgumentOutOfRangeException>(() => list[-1]);
    }

    [Fact]
    public void AFullChunk_EndsTheEnumeration_AtItsLastItem()
    {
        ChunkedList<string> list = new();
        for (int i = 0; i < ChunkedList<string>.ChunkSize; i++)
            list.Add($"item{i}");

        int enumerated = 0;
        foreach (string _ in list)
            enumerated++;

        Assert.Equal(ChunkedList<string>.ChunkSize, enumerated);
        Assert.Equal($"item{ChunkedList<string>.ChunkSize - 1}", list[ChunkedList<string>.ChunkSize - 1]);
    }

    [Fact]
    public void Clear_EmptiesTheList_AndTheNextFillStartsAtTheFirstItem()
    {
        // A capture that a prune tore clears its heads and scans them again into the same list.
        ChunkedList<int> list = new();
        for (int i = 0; i < ChunkedList<int>.ChunkSize + 100; i++)
            list.Add(i);

        list.Clear();
        Assert.Empty(list);
        Assert.Throws<ArgumentOutOfRangeException>(() => list[0]);

        for (int i = 0; i < ChunkedList<int>.ChunkSize * 2 + 5; i++)
            list.Add(-i);

        Assert.Equal(ChunkedList<int>.ChunkSize * 2 + 5, list.Count);

        int next = 0;
        foreach (int item in list)
            Assert.Equal(-(next++), item);
    }
}
