using System.Buffers;
using BenchmarkDotNet.Attributes;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Microsoft.IO;

namespace Kahuna.Microbenchmarks;

/// <summary>
/// Before/after for the replication serializer in <c>ReplicationSerializer</c>
/// (<c>Serialize</c>/<c>Unserialize*</c>).
///
/// <para><b>Old</b> = <c>manager.GetStream()</c> (RecyclableMemoryStream or MemoryStream) +
/// <c>WriteTo(stream)</c> + <c>stream.ToArray()</c>. The <c>ToArray()</c> is an unconditional heap
/// copy; for deserialization a second stream wraps the input span before passing it to the parser.
/// The <c>MaxMessageSize</c> branch selects between the two stream variants but both still
/// copy.</para>
///
/// <para><b>New</b> = serialize: <c>CalculateSize()</c> + <c>new byte[size]</c> +
/// <c>CodedOutputStream</c> written straight into the final array — one allocation, no stream,
/// no extra copy. Deserialize: <c>MessageParser.ParseFrom(ReadOnlySpan&lt;byte&gt;)</c> —
/// no stream wrapper at all.</para>
///
/// <para>Approximates the replication message types (<c>LockMessage</c>, <c>KeyValueMessage</c>,
/// <c>RangeMapMessage</c>) with <see cref="BytesValue"/> / <see cref="Int64Value"/> from
/// <c>Google.Protobuf.WellKnownTypes</c>, parametrised by payload size. The proto wire format
/// is identical for a single <c>bytes</c> field, so timing and allocation numbers transfer
/// directly to the real messages.</para>
///
/// <para><b>Result</b> (net10.0, Release; payload → serialize old vs new / deserialize old vs new):
/// <list type="bullet">
///   <item>0: ser 541 ns/4496 B → 19 ns/96 B; deser 633 ns/5000 B → 47 ns/432 B.</item>
///   <item>128: ser 542 ns/4624 B → 21 ns/224 B; deser 563 ns/4784 B → 20 ns/216 B.</item>
///   <item>1024: ser 548 ns/5520 B → 62 ns/1120 B; deser 585 ns/5680 B → 57 ns/1112 B.</item>
///   <item>8192: ser 1055 ns/12688 B → 667 ns/8288 B; deser 1701 ns/21208 B → 601 ns/8280 B.</item>
/// </list>
/// New wins on both directions at every size — ~9–28× faster serialize and ~7–13× faster deserialize
/// for small/medium messages, narrowing to ~1.6–1.8× at 8 KB where the payload copy dominates both.
/// The old path's fixed stream overhead is large even for tiny messages (≈4.5 KB / 540 ns to serialize
/// an empty message), disproving the "stream pooling may already be competitive for tiny messages"
/// caveat. Deserialize allocation is especially lopsided at 8 KB (21 KB old vs 8 KB new) because the
/// old path wraps the input in a stream before parsing.</para>
/// </summary>
[MemoryDiagnoser]
public class ReplicationSerializerBenchmark
{
    private static readonly RecyclableMemoryStreamManager Manager = new();

    // 0 = scalar-only (mirrors LockMessage with no owner); 128/1024/8192 = value-carrying messages.
    [Params(0, 128, 1024, 8192)]
    public int PayloadSize;

    private IMessage _message = null!;
    private byte[] _serialized = null!;

    [GlobalSetup]
    public void Setup()
    {
        _message = PayloadSize == 0
            ? new Int64Value { Value = 1_725_000_000_000L }
            : new BytesValue { Value = ByteString.CopyFrom(new byte[PayloadSize]) };

        _serialized = NewSerialize(_message);
    }

    // ── serialize ────────────────────────────────────────────────────────────────

    [Benchmark(Baseline = true)]
    public byte[] OldSerialize()
    {
        using MemoryStream ms = Manager.GetStream();
        _message.WriteTo(ms);
        return ms.ToArray();
    }

    [Benchmark]
    public byte[] NewSerialize() => NewSerialize(_message);

    // ── deserialize ──────────────────────────────────────────────────────────────

    [Benchmark]
    public IMessage OldDeserialize()
    {
        using MemoryStream ms = Manager.GetStream(_serialized);
        // Mirrors the production path: parser chosen at the call site; BytesValue here.
        return BytesValue.Parser.ParseFrom(ms);
    }

    [Benchmark]
    public IMessage NewDeserialize() => BytesValue.Parser.ParseFrom(_serialized.AsSpan());

    // ── helpers ──────────────────────────────────────────────────────────────────

    private static byte[] NewSerialize(IMessage message)
    {
        int size = message.CalculateSize();
        byte[] buf = new byte[size];
        using CodedOutputStream cos = new(buf);
        message.WriteTo(cos);
        return buf;
    }
}
