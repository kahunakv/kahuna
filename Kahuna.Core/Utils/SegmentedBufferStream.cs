
using System.Buffers;

namespace Kahuna.Utils;

/// <summary>
/// An in-memory stream that keeps its content in fixed-size pooled segments instead of one contiguous
/// buffer. Appending never copies what was already written and never allocates anything larger than one
/// segment, so a payload of any size is built without the doubling-growth ladder of
/// <see cref="MemoryStream"/> — where every doubling above 85 KB is a large-object-heap allocation and the
/// abandoned smaller buffers fragment that heap. Segments are rented from an <see cref="ArrayPool{T}"/>
/// (the shared pool by default) and returned on dispose, so a build-then-drain cycle that repeats on a
/// cadence (a whole-partition snapshot export on every rescue attempt) reuses memory instead of
/// allocating it anew.
///
/// <para>Writes always append at the end. Reads consume from <see cref="Position"/>, which starts at zero
/// and is independent of the write cursor, so a producer builds the content and hands the stream to a
/// consumer that reads it from the start; <see cref="Position"/> can be moved back to re-read. Not
/// thread-safe: one thread at a time.</para>
/// </summary>
internal sealed class SegmentedBufferStream : Stream
{
    /// <summary>
    /// Segment size: the largest power-of-two pool bucket that stays below the large-object-heap threshold
    /// (85,000 bytes), so a segment is a small-object allocation even when the pool has none to lend.
    /// </summary>
    public const int SegmentSize = 64 * 1024;

    private readonly ArrayPool<byte> pool;

    private readonly List<byte[]> segments = [];

    // Bytes written so far (the write cursor is always here).
    private long length;

    // Read cursor.
    private long position;

    private bool disposed;

    public SegmentedBufferStream() : this(ArrayPool<byte>.Shared) { }

    internal SegmentedBufferStream(ArrayPool<byte> pool)
    {
        this.pool = pool;
    }

    /// <summary>Number of segments currently backing the content.</summary>
    internal int SegmentCount => segments.Count;

    /// <summary>The written bytes held by segment <paramref name="index"/> (the last one may be partial).</summary>
    internal ArraySegment<byte> GetSegment(int index)
    {
        ThrowIfDisposed();
        long start = (long)index * SegmentSize;
        int count = (int)Math.Min(SegmentSize, length - start);
        return new ArraySegment<byte>(segments[index], 0, count);
    }

    public override bool CanRead => !disposed;

    public override bool CanSeek => !disposed;

    public override bool CanWrite => !disposed;

    public override long Length
    {
        get
        {
            ThrowIfDisposed();
            return length;
        }
    }

    public override long Position
    {
        get
        {
            ThrowIfDisposed();
            return position;
        }
        set
        {
            ThrowIfDisposed();
            ArgumentOutOfRangeException.ThrowIfNegative(value);
            position = value;
        }
    }

    // ── write ────────────────────────────────────────────────────────────────────

    public override void Write(ReadOnlySpan<byte> buffer)
    {
        ThrowIfDisposed();

        while (!buffer.IsEmpty)
        {
            int segmentIndex = (int)(length / SegmentSize);
            int offset = (int)(length % SegmentSize);

            if (segmentIndex == segments.Count)
                segments.Add(pool.Rent(SegmentSize));

            int n = Math.Min(SegmentSize - offset, buffer.Length);
            buffer[..n].CopyTo(segments[segmentIndex].AsSpan(offset, n));

            buffer = buffer[n..];
            length += n;
        }
    }

    public override void Write(byte[] buffer, int offset, int count)
    {
        ValidateBufferArguments(buffer, offset, count);
        Write(buffer.AsSpan(offset, count));
    }

    public override void WriteByte(byte value)
    {
        ReadOnlySpan<byte> one = [value];
        Write(one);
    }

    public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
    {
        if (cancellationToken.IsCancellationRequested)
            return ValueTask.FromCanceled(cancellationToken);

        Write(buffer.Span);
        return ValueTask.CompletedTask;
    }

    public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
    {
        ValidateBufferArguments(buffer, offset, count);
        return WriteAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();
    }

    public override void Flush() { }

    public override Task FlushAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    // ── read ─────────────────────────────────────────────────────────────────────

    public override int Read(Span<byte> buffer)
    {
        ThrowIfDisposed();

        int total = 0;

        while (!buffer.IsEmpty && position < length)
        {
            int segmentIndex = (int)(position / SegmentSize);
            int offset = (int)(position % SegmentSize);

            int available = (int)Math.Min(SegmentSize - offset, length - position);
            int n = Math.Min(available, buffer.Length);

            segments[segmentIndex].AsSpan(offset, n).CopyTo(buffer);

            buffer = buffer[n..];
            position += n;
            total += n;
        }

        return total;
    }

    public override int Read(byte[] buffer, int offset, int count)
    {
        ValidateBufferArguments(buffer, offset, count);
        return Read(buffer.AsSpan(offset, count));
    }

    public override int ReadByte()
    {
        Span<byte> one = stackalloc byte[1];
        return Read(one) == 0 ? -1 : one[0];
    }

    public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
    {
        if (cancellationToken.IsCancellationRequested)
            return ValueTask.FromCanceled<int>(cancellationToken);

        return new ValueTask<int>(Read(buffer.Span));
    }

    public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
    {
        ValidateBufferArguments(buffer, offset, count);
        return ReadAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();
    }

    /// <summary>Copies the unread content segment by segment — no intermediate buffer.</summary>
    public override void CopyTo(Stream destination, int bufferSize)
    {
        ThrowIfDisposed();
        ArgumentNullException.ThrowIfNull(destination);

        while (position < length)
        {
            ArraySegment<byte> chunk = UnreadChunk();
            destination.Write(chunk.Array!, chunk.Offset, chunk.Count);
            position += chunk.Count;
        }
    }

    public override async Task CopyToAsync(Stream destination, int bufferSize, CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        ArgumentNullException.ThrowIfNull(destination);

        while (position < length)
        {
            ArraySegment<byte> chunk = UnreadChunk();
            await destination.WriteAsync(chunk.AsMemory(), cancellationToken).ConfigureAwait(false);
            position += chunk.Count;
        }
    }

    // The unread bytes of the segment the read cursor is in.
    private ArraySegment<byte> UnreadChunk()
    {
        int segmentIndex = (int)(position / SegmentSize);
        int offset = (int)(position % SegmentSize);
        int count = (int)Math.Min(SegmentSize - offset, length - position);
        return new ArraySegment<byte>(segments[segmentIndex], offset, count);
    }

    public override long Seek(long offset, SeekOrigin origin)
    {
        ThrowIfDisposed();

        long target = origin switch
        {
            SeekOrigin.Begin => offset,
            SeekOrigin.Current => position + offset,
            SeekOrigin.End => length + offset,
            _ => throw new ArgumentOutOfRangeException(nameof(origin))
        };

        if (target < 0)
            throw new IOException("Cannot seek before the beginning of the stream.");

        position = target;
        return position;
    }

    public override void SetLength(long value) => throw new NotSupportedException();

    // ── lifetime ─────────────────────────────────────────────────────────────────

    protected override void Dispose(bool disposing)
    {
        if (!disposed)
        {
            disposed = true;

            foreach (byte[] segment in segments)
                pool.Return(segment);

            segments.Clear();
            length = 0;
            position = 0;
        }

        base.Dispose(disposing);
    }

    private void ThrowIfDisposed() => ObjectDisposedException.ThrowIf(disposed, this);
}
