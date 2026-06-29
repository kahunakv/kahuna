using BenchmarkDotNet.Attributes;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Microsoft.IO;

namespace Kahuna.Microbenchmarks;

/// <summary>
/// Before/after comparison for the protobuf deserialization helpers in
/// <c>RocksDbPersistenceBackend</c>.
///
/// <para><b>Old</b>: <c>UnserializeLockMessage</c> / <c>UnserializeKeyValueMessage</c> wrapped the
/// incoming <c>ReadOnlySpan&lt;byte&gt;</c> in a <c>RecyclableMemoryStream</c> (via
/// <c>manager.GetStream</c>) before calling <c>ParseFrom(stream)</c>. A dead
/// <c>MaxMessageSize</c> branch existed but both arms did the same thing, so it was pure overhead.
/// </para>
///
/// <para><b>New</b>: call <c>Parser.ParseFrom(ReadOnlySpan&lt;byte&gt;)</c> directly, confirmed
/// available in Google.Protobuf 3.29.6. The <c>MaxMessageSize</c> dead branch and the
/// <c>RecyclableMemoryStreamManager</c> field are removed entirely.</para>
///
/// <para>Approximates <c>RocksDbLockMessage</c> / <c>RocksDbKeyValueMessage</c> with
/// <c>Int64Value</c> / <c>BytesValue</c> from <c>Google.Protobuf.WellKnownTypes</c> — both are
/// single-field proto messages. Timing and allocation numbers transfer directly.</para>
///
/// <para><b>Result</b> (net10.0, Release; measured for this benchmark):
/// <list type="bullet">
///   <item>0 B:    Old 581 ns / 4600 B → New 11.9 ns / 32 B (~49× faster, ~144× less alloc). This
///     case parses a scalar <c>Int64Value</c> (no payload), so the new path allocates only the
///     message object — hence the outsized ratios versus the byte-carrying sizes below.</item>
///   <item>128 B:  Old 592 ns / 4784 B → New 21.3 ns / 216 B (~28× faster, ~22× less alloc).</item>
///   <item>1024 B: Old 623 ns / 5680 B → New 63.5 ns / 1112 B (~10× faster, ~5× less alloc).</item>
///   <item>8192 B: Old 1792 ns / 21208 B → New 621 ns / 8280 B (~2.9× faster, ~2.6× less alloc).</item>
/// </list>
/// The stream-setup cost dominates at small sizes; at 8 KB the payload copy narrows the gap but
/// new still wins. The <c>RecyclableMemoryStream</c> overhead is high even when the stream is
/// pooled — consistent with the <see cref="ReplicationSerializerBenchmark"/> findings (whose
/// deserialize column uses <c>BytesValue</c> at every size, so its 0-byte row is not directly
/// comparable to this benchmark's scalar 0-byte case).</para>
/// </summary>
[MemoryDiagnoser]
public class RocksDbProtobufParseBenchmark
{
    private static readonly RecyclableMemoryStreamManager Manager = new();

    [Params(0, 128, 1024, 8192)]
    public int PayloadSize;

    private byte[] _serialized = null!;

    [GlobalSetup]
    public void Setup()
    {
        IMessage msg = PayloadSize == 0
            ? new Int64Value { Value = 1_725_000_000_000L }
            : new BytesValue { Value = ByteString.CopyFrom(new byte[PayloadSize]) };

        int size = msg.CalculateSize();
        _serialized = new byte[size];
        using CodedOutputStream cos = new(_serialized);
        msg.WriteTo(cos);
    }

    /// <summary>Old path: span → RecyclableMemoryStream → ParseFrom(stream).</summary>
    [Benchmark(Baseline = true)]
    public IMessage OldParse()
    {
        using MemoryStream ms = Manager.GetStream(_serialized);
        return PayloadSize == 0
            ? Int64Value.Parser.ParseFrom(ms)
            : BytesValue.Parser.ParseFrom(ms);
    }

    /// <summary>New path: ParseFrom(ReadOnlySpan&lt;byte&gt;) directly — no stream wrapper.</summary>
    [Benchmark]
    public IMessage NewParse() =>
        PayloadSize == 0
            ? Int64Value.Parser.ParseFrom(_serialized.AsSpan())
            : BytesValue.Parser.ParseFrom(_serialized.AsSpan());
}
