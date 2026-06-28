using System.Buffers;
using BenchmarkDotNet.Attributes;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Microsoft.IO;

namespace Kahuna.Microbenchmarks;

/// <summary>
/// Before/after for the write-path serialization in <c>RocksDbPersistenceBackend</c>
/// (<c>PutLocksItems</c>/<c>PutStoreItems</c>).
///
/// <para><b>Old</b> = <c>manager.GetStream()</c> (RecyclableMemoryStream) + <c>WriteTo(stream)</c> +
/// <c>stream.ToArray()</c> — the <c>ToArray()</c> is an unconditional heap copy of the serialized
/// bytes. <b>New</b> = <c>CalculateSize()</c> + <c>ArrayPool.Rent</c> + <c>CodedOutputStream</c> +
/// <c>AsSpan(0, size)</c> — zero heap, but two passes over the message (size + write).</para>
///
/// The real message (<c>RocksDbKeyValueMessage</c>) is a value-carrying protobuf, approximated here
/// by a <see cref="BytesValue"/> whose payload size is parametrised. <b>New</b> folds the span into a
/// checksum (as the real code hands it to <c>batch.Put</c> in-method); <b>Old</b> returns the
/// <c>byte[]</c> so its allocation is counted.
/// </summary>
[MemoryDiagnoser]
public class SerializeBenchmark
{
    private static readonly RecyclableMemoryStreamManager manager = new();

    // 0 = scalar-only message (no value bytes); 128/1024 = typical / large value payloads.
    [Params(0, 128, 1024)]
    public int PayloadSize;

    private IMessage _message = null!;

    [GlobalSetup]
    public void Setup()
    {
        _message = PayloadSize == 0
            ? new Int64Value { Value = 9_223_372_036_854_775L }
            : new BytesValue { Value = ByteString.CopyFrom(new byte[PayloadSize]) };
    }

    [Benchmark(Baseline = true)]
    public byte[] Old()
    {
        using RecyclableMemoryStream stream = manager.GetStream();
        _message.WriteTo((Stream)stream);
        return stream.ToArray();
    }

    [Benchmark]
    public int New()
    {
        int size = _message.CalculateSize();
        byte[] rented = ArrayPool<byte>.Shared.Rent(size);
        try
        {
            using (CodedOutputStream cos = new(rented))
                _message.WriteTo(cos);

            // O(1) consumption: the real path hands the span to batch.Put (a memcpy), not a managed
            // byte-by-byte loop, so touch the last written byte to defeat elision without an O(n) cost.
            ReadOnlySpan<byte> serialized = rented.AsSpan(0, size);
            return size + serialized[size - 1];
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(rented);
        }
    }
}
