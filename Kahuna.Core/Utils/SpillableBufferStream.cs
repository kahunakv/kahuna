
namespace Kahuna.Utils;

/// <summary>
/// A stream that keeps its content in pooled memory up to a budget and moves it to a private file past it, so a payload
/// whose size follows the data, such as a whole-partition snapshot export, does not have to fit on the heap. A file
/// is only used when a directory is given; without one the stream stays in memory whatever it holds.
///
/// <para>Like <see cref="SegmentedBufferStream"/>, writes always append at the end and reads consume from
/// <see cref="Position"/>, which starts at zero and is independent of the write cursor. Not thread-safe: one thread at
/// a time. The spill file is deleted when the stream is disposed, and a file left by a process that died is removed by
/// whoever owns the directory, at startup.</para>
/// </summary>
internal sealed class SpillableBufferStream : Stream
{
    /// <summary>Extension of every spill file, which is what a startup sweep looks for.</summary>
    internal const string SpillFileExtension = ".snapshot-export";

    private const int FileBufferSize = 64 * 1024;

    private readonly string? spillDirectory;

    private readonly long memoryBudget;

    // Exactly one of these holds the content: memory until the spill, the file after it.
    private SegmentedBufferStream? memory = new();

    private FileStream? file;

    private long length;

    private long position;

    private bool disposed;

    public SpillableBufferStream(string? spillDirectory, long memoryBudget)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(memoryBudget);

        this.spillDirectory = spillDirectory;
        this.memoryBudget = memoryBudget;
    }

    /// <summary>The pooled segments holding the content, or null once it has moved to a file.</summary>
    internal SegmentedBufferStream? Memory => memory;

    /// <summary>Whether the content has moved to a file.</summary>
    internal bool IsSpilled => file is not null;

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

        if (memory is not null && spillDirectory is not null && length + buffer.Length > memoryBudget)
            Spill(spillDirectory);

        if (memory is not null)
        {
            memory.Write(buffer);
        }
        else
        {
            if (file!.Position != length)
                file.Position = length;

            file.Write(buffer);
        }

        length += buffer.Length;
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

    // A failed spill leaves the content in memory and the partial file removed, so the caller sees the error and
    // disposes a stream that is still whole.
    private void Spill(string directory)
    {
        string path = Path.Combine(directory, Guid.NewGuid().ToString("N") + SpillFileExtension);
        FileStream created = new(path, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.None, FileBufferSize, FileOptions.DeleteOnClose);

        try
        {
            memory!.Position = 0;
            memory.CopyTo(created);
        }
        catch
        {
            created.Dispose();
            throw;
        }

        memory.Dispose();
        memory = null;
        file = created;
    }

    // ── read ─────────────────────────────────────────────────────────────────────

    public override int Read(Span<byte> buffer)
    {
        ThrowIfDisposed();

        if (position >= length)
            return 0;

        int read;

        if (memory is not null)
        {
            memory.Position = position;
            read = memory.Read(buffer);
        }
        else
        {
            if (file!.Position != position)
                file.Position = position;

            read = file.Read(buffer);
        }

        position += read;
        return read;
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

    public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        ThrowIfDisposed();

        if (memory is not null)
            return Read(buffer.Span);

        if (position >= length)
            return 0;

        if (file!.Position != position)
            file.Position = position;

        int read = await file.ReadAsync(buffer, cancellationToken).ConfigureAwait(false);
        position += read;
        return read;
    }

    public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
    {
        ValidateBufferArguments(buffer, offset, count);
        return ReadAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();
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

            memory?.Dispose();
            memory = null;

            file?.Dispose();
            file = null;

            length = 0;
            position = 0;
        }

        base.Dispose(disposing);
    }

    private void ThrowIfDisposed() => ObjectDisposedException.ThrowIf(disposed, this);
}
