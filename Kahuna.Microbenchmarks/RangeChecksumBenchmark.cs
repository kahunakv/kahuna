using BenchmarkDotNet.Attributes;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;

namespace Kahuna.Microbenchmarks;

/// <summary>
/// Before/after comparison for <c>KvStateMachineTransfer.ChecksumOf</c>.
///
/// <para><b>Old</b>: serializes all page entries into a <c>MemoryStream</c>, growing a
/// page-sized intermediate buffer, then iterates the buffer byte-by-byte for FNV-1a hashing.
/// For a page of 100 × 1 KB entries the buffer reaches 100 KB before a single hash byte is
/// processed.</para>
///
/// <para><b>New</b>: a <c>FnvHashStream</c> adapter folds each byte directly into the running
/// FNV-1a state as protobuf writes it. No bytes are retained; peak extra memory is
/// <c>sizeof(ulong)</c>.</para>
///
/// <para>Approximates page entries with <see cref="BytesValue"/> — a single-<c>bytes</c>-field
/// proto message. Timing and allocation numbers transfer directly to the real
/// <c>RangeSnapshotEntry</c> messages.</para>
///
/// <para>The fold loop hoists the accumulator into a local (register-resident across the loop instead
/// of a field load+store per byte) and iterates the span bounds-check-free; the <c>byte[]</c> overload
/// delegates to the span one. FNV-1a is a serial dependency chain, so that register-resident tight loop
/// is the whole game — without it the stream path runs ~1.4–1.8× slower than the old flat-buffer loop.</para>
///
/// <para><b>Result</b> (net10.0, Release) — faster AND lower allocation at every size:
/// <list type="bullet">
///   <item>1 entry  / 128 B:  Old 347 ns / 4.42 KB → New 322 ns / 4.12 KB (0.93× time, 0.93× alloc).</item>
///   <item>10 entries / 128 B: Old 3.77 µs / 44.8 KB → New 3.57 µs / 40.9 KB (0.95× time, 0.91× alloc).</item>
///   <item>1 entry  / 8192 B: Old 9.98 µs / 32.2 KB → New 8.38 µs / 4.12 KB (0.84× time, 0.13× alloc).</item>
///   <item>10 entries / 8192 B: Old 104 µs / 293 KB (Gen2) → New 84.9 µs / 40.9 KB (0.81× time, 0.14× alloc).</item>
/// </list>
/// The page-sized intermediate buffer is gone, so allocation is flat (~4 KB — the per-entry
/// <c>CodedOutputStream</c> buffer that <c>IMessage.WriteTo(Stream)</c> creates, common to both paths)
/// regardless of page size. Critically, the old path's page-sized buffer lands on the LOH/Gen2 for large
/// pages (293 KB / Gen2 collections at 10 × 8 KB); the new path removes that GC pressure entirely while
/// also running faster.</para>
/// </summary>
[MemoryDiagnoser]
public class RangeChecksumBenchmark
{
    private const ulong FnvOffsetBasis = 14695981039346656037UL;
    private const ulong FnvPrime       = 1099511628211UL;

    [Params(1, 10)]
    public int EntryCount;

    [Params(128, 8192)]
    public int ValueSize;

    private IMessage[] _entries = null!;

    [GlobalSetup]
    public void Setup()
    {
        byte[] payload = new byte[ValueSize];
        new Random(42).NextBytes(payload);
        _entries = new IMessage[EntryCount];
        for (int i = 0; i < EntryCount; i++)
            _entries[i] = new BytesValue { Value = ByteString.CopyFrom(payload) };
    }

    /// <summary>Old: buffer all serialized bytes into MemoryStream, then hash.</summary>
    [Benchmark(Baseline = true)]
    public ulong Old()
    {
        using MemoryStream buffer = new();
        foreach (IMessage entry in _entries)
            entry.WriteTo(buffer);

        ulong hash = FnvOffsetBasis;
        foreach (byte b in buffer.GetBuffer().AsSpan(0, (int)buffer.Length))
        {
            hash ^= b;
            hash *= FnvPrime;
        }
        return hash;
    }

    /// <summary>New: hash bytes directly as protobuf writes them — no intermediate buffer.</summary>
    [Benchmark]
    public ulong New()
    {
        FnvHashingStream hasher = new();
        foreach (IMessage entry in _entries)
            entry.WriteTo(hasher);
        return hasher.Hash;
    }

    /// <summary>
    /// Local reproduction of <c>KvStateMachineTransfer.FnvHashStream</c> — reproduced verbatim
    /// because the production type is <c>internal</c> and <c>Kahuna.Microbenchmarks</c> has no
    /// project reference to <c>Kahuna.Core</c>.
    /// </summary>
    private sealed class FnvHashingStream : Stream
    {
        private ulong _hash = FnvOffsetBasis;

        public ulong Hash => _hash;

        public override void Write(byte[] buffer, int offset, int count) =>
            Write(buffer.AsSpan(offset, count));

        public override void Write(ReadOnlySpan<byte> buffer)
        {
            ulong hash = _hash;
            foreach (byte b in buffer)
                hash = (hash ^ b) * FnvPrime;
            _hash = hash;
        }

        public override bool CanRead  => false;
        public override bool CanSeek  => false;
        public override bool CanWrite => true;
        public override long Length   => throw new NotSupportedException();
        public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }
        public override void Flush() { }
        public override int  Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
        public override long Seek(long offset, SeekOrigin origin)       => throw new NotSupportedException();
        public override void SetLength(long value)                       => throw new NotSupportedException();
    }
}
