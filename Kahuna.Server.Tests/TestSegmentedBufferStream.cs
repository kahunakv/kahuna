
using System.Buffers;
using Google.Protobuf;
using Kahuna.Server.Replication.Protos;
using Kahuna.Utils;

namespace Kahuna.Server.Tests;

/// <summary>
/// The pooled, segmented stream the whole-partition and meta-partition exports build their snapshots in:
/// any payload size round-trips exactly through segment boundaries, reads consume independently of the
/// write cursor, every segment stays below the large-object-heap threshold, and dispose hands every
/// rented segment back to the pool.
/// </summary>
public sealed class TestSegmentedBufferStream
{
    private const int LargeObjectHeapThreshold = 85_000;

    private sealed class CountingPool : ArrayPool<byte>
    {
        public int Rented;
        public int Returned;

        public override byte[] Rent(int minimumLength)
        {
            Rented++;
            return new byte[minimumLength];
        }

        public override void Return(byte[] array, bool clearArray = false) => Returned++;
    }

    private static byte[] Pattern(int length, int seed)
    {
        byte[] data = new byte[length];
        new Random(seed).NextBytes(data);
        return data;
    }

    // Writes the data in chunks of varying sizes so writes straddle segment boundaries in every way.
    private static void WriteInChunks(SegmentedBufferStream stream, byte[] data, int seed)
    {
        Random random = new(seed);
        int offset = 0;

        while (offset < data.Length)
        {
            int n = Math.Min(data.Length - offset, random.Next(1, 3 * SegmentedBufferStream.SegmentSize));
            stream.Write(data, offset, n);
            offset += n;
        }
    }

    private static byte[] ReadInChunks(Stream stream, int seed)
    {
        Random random = new(seed);
        using MemoryStream collected = new();
        byte[] buffer = new byte[3 * SegmentedBufferStream.SegmentSize];

        while (true)
        {
            int n = stream.Read(buffer, 0, random.Next(1, buffer.Length));
            if (n == 0)
                break;

            collected.Write(buffer, 0, n);
        }

        return collected.ToArray();
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(SegmentedBufferStream.SegmentSize - 1)]
    [InlineData(SegmentedBufferStream.SegmentSize)]
    [InlineData(SegmentedBufferStream.SegmentSize + 1)]
    [InlineData(5 * SegmentedBufferStream.SegmentSize + 4_321)]
    public void WritesRoundTripAcrossSegmentBoundaries(int length)
    {
        byte[] data = Pattern(length, seed: length);

        using SegmentedBufferStream stream = new();
        WriteInChunks(stream, data, seed: 7);

        Assert.Equal(length, stream.Length);
        Assert.Equal(0, stream.Position);
        Assert.Equal((length + SegmentedBufferStream.SegmentSize - 1) / SegmentedBufferStream.SegmentSize, stream.SegmentCount);

        Assert.Equal(data, ReadInChunks(stream, seed: 11));
        Assert.Equal(length, stream.Position);

        // The read cursor moves independently of the write cursor: seeking back re-reads the same bytes.
        stream.Position = 0;
        Assert.Equal(data, ReadInChunks(stream, seed: 13));

        for (int i = 0; i < stream.SegmentCount; i++)
        {
            ArraySegment<byte> segment = stream.GetSegment(i);
            Assert.True(segment.Count <= SegmentedBufferStream.SegmentSize);
            Assert.True(segment.Array!.Length < LargeObjectHeapThreshold);
        }
    }

    [Fact]
    public async Task AsyncReadsAndCopiesProduceTheSameBytes()
    {
        byte[] data = Pattern(3 * SegmentedBufferStream.SegmentSize + 99, seed: 5);
        CancellationToken ct = TestContext.Current.CancellationToken;

        using SegmentedBufferStream stream = new();
        await stream.WriteAsync(data, ct);

        using MemoryStream viaReadAsync = new();
        byte[] buffer = new byte[10_000];
        while (true)
        {
            int n = await stream.ReadAsync(buffer, ct);
            if (n == 0)
                break;
            viaReadAsync.Write(buffer, 0, n);
        }
        Assert.Equal(data, viaReadAsync.ToArray());

        stream.Seek(0, SeekOrigin.Begin);
        using MemoryStream viaCopyToAsync = new();
        await stream.CopyToAsync(viaCopyToAsync, ct);
        Assert.Equal(data, viaCopyToAsync.ToArray());

        // A copy resumes from the read cursor, not from the start.
        stream.Seek(-1_000, SeekOrigin.End);
        using MemoryStream tail = new();
        stream.CopyTo(tail);
        Assert.Equal(data[^1_000..], tail.ToArray());

        Assert.Equal(-1, stream.ReadByte());
    }

    [Fact]
    public void DisposeReturnsEverySegmentToThePool_AndRejectsFurtherUse()
    {
        CountingPool pool = new();
        SegmentedBufferStream stream = new(pool);

        stream.Write(Pattern(4 * SegmentedBufferStream.SegmentSize + 1, seed: 3));
        Assert.Equal(5, pool.Rented);
        Assert.Equal(0, pool.Returned);

        stream.Dispose();
        Assert.Equal(5, pool.Returned);

        // A second dispose returns nothing twice.
        stream.Dispose();
        Assert.Equal(5, pool.Returned);

        Assert.Throws<ObjectDisposedException>(() => stream.Write([1]));
        Assert.Throws<ObjectDisposedException>(() => stream.Read(new byte[1], 0, 1));
        Assert.Throws<ObjectDisposedException>(() => stream.Length);
    }

    [Fact]
    public void ProtobufWritesAndParsesThroughTheStream_AcrossSegments()
    {
        // The exports write length-delimited messages into the stream and the meta import parses a message
        // straight out of one; a value larger than a segment proves the parser reads across the boundary.
        RangeSnapshotPage page = new() { HasMore = true, Checksum = 42 };
        page.Entries.Add(new RangeSnapshotEntry
        {
            Key = "k",
            Revision = 3,
            Value = ByteString.CopyFrom(Pattern(3 * SegmentedBufferStream.SegmentSize + 7, seed: 9))
        });
        PartitionStateHeader header = new() { PartitionId = 4, UpToIndex = 99 };

        using SegmentedBufferStream stream = new();
        header.WriteDelimitedTo(stream);
        page.WriteDelimitedTo(stream);
        Assert.True(stream.SegmentCount > 3);

        PartitionStateHeader parsedHeader = PartitionStateHeader.Parser.ParseDelimitedFrom(stream);
        RangeSnapshotPage parsedPage = RangeSnapshotPage.Parser.ParseDelimitedFrom(stream);
        Assert.Equal(header, parsedHeader);
        Assert.Equal(page, parsedPage);
        Assert.Equal(stream.Length, stream.Position);

        // A whole-stream parse (the meta import's shape) from the start.
        using SegmentedBufferStream whole = new();
        page.WriteTo(whole);
        Assert.Equal(page, RangeSnapshotPage.Parser.ParseFrom(whole));
    }
}
