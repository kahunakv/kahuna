using System.Buffers.Binary;
using System.Text;
using System.Text.Json;
using BenchmarkDotNet.Attributes;

namespace Kahuna.Microbenchmarks;

/// <summary>
/// Before/after for the sequence-state storage codec in <c>SequencerManager</c>
/// (<c>Serialize</c>/<c>Deserialize</c>), invoked on every <c>NextSequenceValue</c>/
/// <c>ReserveSequenceRange</c>.
///
/// <para><b>Old</b> = <see cref="JsonSerializer"/> (Web defaults) to/from UTF-8 bytes. <b>New</b> =
/// exact-size custom binary: a version byte + length-prefixed fields + the idempotency map, written
/// with <see cref="BinaryPrimitives"/> into a single right-sized <c>byte[]</c>. The codec below is a
/// faithful copy of the production one (same field order/layout) so the measured sizes and allocations
/// match the real path.</para>
///
/// <para>Parametrised by idempotency-map size (0/1/32/1024): the per-call cost — and JSON's
/// disadvantage — scale with the number of idempotency records rewritten on every allocation.
/// <c>JsonSerialize</c> is the baseline; compare <c>BinarySerialize</c> against it (same operation) and
/// <c>BinaryDeserialize</c> against <c>JsonDeserialize</c>.</para>
///
/// <para><b>Result</b> (net10.0, Release; <c>GetByteCount</c>-sized, encode-into-span codec; idempotency
/// entries → JSON serialize vs binary serialize):
/// <list type="bullet">
///   <item>0: JSON 265 ns/656 B → binary 17 ns/136 B (0.07× time, 0.21× alloc).</item>
///   <item>1: JSON 373 ns/832 B → binary 45 ns/232 B (0.12×, 0.28×).</item>
///   <item>32: JSON 3.56 µs/6256 B → binary 878 ns/3080 B (0.25×, 0.49×).</item>
///   <item>1024: JSON 116.9 µs/184 KB → binary 32.2 µs/94 KB (0.28×, 0.51×).</item>
/// </list>
/// Binary now wins on both CPU (3.5–15×) and allocation (≈½ of JSON) at every size; deserialize is
/// likewise ~5–6× faster. The earlier large-map allocation regression — caused by pre-encoding each
/// string into a temp <c>byte[]</c> just to size the buffer — is gone now that the codec sizes with
/// <c>Encoding.UTF8.GetByteCount</c> and encodes straight into the destination span.</para>
/// </summary>
[MemoryDiagnoser]
public class SequenceStateCodecBenchmark
{
    private const byte BinaryFormatVersion = 1;

    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);

    [Params(0, 1, 32, 1024)]
    public int IdempotencyEntries;

    private SeqState _state = null!;
    private byte[] _json = null!;
    private byte[] _binary = null!;

    [GlobalSetup]
    public void Setup()
    {
        _state = MakeState(IdempotencyEntries);
        _json = JsonSerializer.SerializeToUtf8Bytes(_state, JsonOptions);
        _binary = SerializeBinary(_state);
    }

    [Benchmark(Baseline = true)]
    public byte[] JsonSerialize() => JsonSerializer.SerializeToUtf8Bytes(_state, JsonOptions);

    [Benchmark]
    public SeqState? JsonDeserialize() => JsonSerializer.Deserialize<SeqState>(_json, JsonOptions);

    [Benchmark]
    public byte[] BinarySerialize() => SerializeBinary(_state);

    [Benchmark]
    public SeqState? BinaryDeserialize() => DeserializeBinary(_binary);

    private static SeqState MakeState(int idempotencyEntries)
    {
        SeqState state = new()
        {
            Name = "orders/0000000000000000000000000000000a",
            CurrentValue = 9_223_372_036_854L,
            InitialValue = 0,
            Increment = 1,
            MaxValue = 1_000_000_000,
            CreatedAt = new Hlc(3, 1_725_000_000_000, 7),
            UpdatedAt = new Hlc(3, 1_725_000_500_000, 11)
        };

        for (int i = 0; i < idempotencyEntries; i++)
        {
            long start = i * 4L + 1;
            state.Idempotency[$"reserve:idem-{i:D8}"] =
                new Alloc(state.Name, start, start + 3, 4, i + 1);
        }

        return state;
    }

    // --- Faithful copy of SequencerManager's binary codec ---

    private static byte[] SerializeBinary(SeqState state)
    {
        int nameLen = Encoding.UTF8.GetByteCount(state.Name);

        int size = 1
            + 2 + nameLen
            + 8 + 8 + 8
            + 1 + (state.MaxValue.HasValue ? 8 : 0)
            + 16 + 16
            + 4;

        foreach (KeyValuePair<string, Alloc> kvp in state.Idempotency)
            size += 2 + Encoding.UTF8.GetByteCount(kvp.Key)
                  + 2 + Encoding.UTF8.GetByteCount(kvp.Value.Name)
                  + 8 + 8 + 4 + 8;

        byte[] buf = new byte[size];
        int pos = 0;

        buf[pos++] = BinaryFormatVersion;

        pos = WriteString(buf, pos, state.Name, nameLen);

        BinaryPrimitives.WriteInt64LittleEndian(buf.AsSpan(pos), state.CurrentValue); pos += 8;
        BinaryPrimitives.WriteInt64LittleEndian(buf.AsSpan(pos), state.InitialValue); pos += 8;
        BinaryPrimitives.WriteInt64LittleEndian(buf.AsSpan(pos), state.Increment); pos += 8;

        if (state.MaxValue.HasValue)
        {
            buf[pos++] = 1;
            BinaryPrimitives.WriteInt64LittleEndian(buf.AsSpan(pos), state.MaxValue.Value); pos += 8;
        }
        else
        {
            buf[pos++] = 0;
        }

        WriteHlc(buf, ref pos, state.CreatedAt);
        WriteHlc(buf, ref pos, state.UpdatedAt);

        BinaryPrimitives.WriteInt32LittleEndian(buf.AsSpan(pos), state.Idempotency.Count); pos += 4;

        foreach (KeyValuePair<string, Alloc> kvp in state.Idempotency)
        {
            pos = WriteString(buf, pos, kvp.Key, Encoding.UTF8.GetByteCount(kvp.Key));
            pos = WriteString(buf, pos, kvp.Value.Name, Encoding.UTF8.GetByteCount(kvp.Value.Name));

            BinaryPrimitives.WriteInt64LittleEndian(buf.AsSpan(pos), kvp.Value.Start); pos += 8;
            BinaryPrimitives.WriteInt64LittleEndian(buf.AsSpan(pos), kvp.Value.End); pos += 8;
            BinaryPrimitives.WriteInt32LittleEndian(buf.AsSpan(pos), kvp.Value.Count); pos += 4;
            BinaryPrimitives.WriteInt64LittleEndian(buf.AsSpan(pos), kvp.Value.Revision); pos += 8;
        }

        return buf;
    }

    private static int WriteString(byte[] buf, int pos, string value, int byteLen)
    {
        BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(pos), (ushort)byteLen); pos += 2;
        Encoding.UTF8.GetBytes(value.AsSpan(), buf.AsSpan(pos, byteLen)); pos += byteLen;
        return pos;
    }

    private static void WriteHlc(byte[] buf, ref int pos, Hlc ts)
    {
        BinaryPrimitives.WriteInt32LittleEndian(buf.AsSpan(pos), ts.N); pos += 4;
        BinaryPrimitives.WriteInt64LittleEndian(buf.AsSpan(pos), ts.L); pos += 8;
        BinaryPrimitives.WriteUInt32LittleEndian(buf.AsSpan(pos), ts.C); pos += 4;
    }

    private static SeqState? DeserializeBinary(byte[] value)
    {
        ReadOnlySpan<byte> span = value;
        int pos = 0;

        if (span[pos++] != BinaryFormatVersion)
            return null;

        ushort nameLen = BinaryPrimitives.ReadUInt16LittleEndian(span[pos..]); pos += 2;
        string name = Encoding.UTF8.GetString(span.Slice(pos, nameLen)); pos += nameLen;

        long currentValue = BinaryPrimitives.ReadInt64LittleEndian(span[pos..]); pos += 8;
        long initialValue = BinaryPrimitives.ReadInt64LittleEndian(span[pos..]); pos += 8;
        long increment = BinaryPrimitives.ReadInt64LittleEndian(span[pos..]); pos += 8;

        long? maxValue = null;
        if (span[pos++] != 0)
        {
            maxValue = BinaryPrimitives.ReadInt64LittleEndian(span[pos..]); pos += 8;
        }

        Hlc createdAt = ReadHlc(span, ref pos);
        Hlc updatedAt = ReadHlc(span, ref pos);

        int idempotencyCount = BinaryPrimitives.ReadInt32LittleEndian(span[pos..]); pos += 4;
        Dictionary<string, Alloc> idempotency = new(idempotencyCount);

        for (int i = 0; i < idempotencyCount; i++)
        {
            ushort keyLen = BinaryPrimitives.ReadUInt16LittleEndian(span[pos..]); pos += 2;
            string key = Encoding.UTF8.GetString(span.Slice(pos, keyLen)); pos += keyLen;

            ushort aNameLen = BinaryPrimitives.ReadUInt16LittleEndian(span[pos..]); pos += 2;
            string aName = Encoding.UTF8.GetString(span.Slice(pos, aNameLen)); pos += aNameLen;

            long start = BinaryPrimitives.ReadInt64LittleEndian(span[pos..]); pos += 8;
            long end = BinaryPrimitives.ReadInt64LittleEndian(span[pos..]); pos += 8;
            int count = BinaryPrimitives.ReadInt32LittleEndian(span[pos..]); pos += 4;
            long revision = BinaryPrimitives.ReadInt64LittleEndian(span[pos..]); pos += 8;

            idempotency[key] = new Alloc(aName, start, end, count, revision);
        }

        return new SeqState
        {
            Name = name,
            CurrentValue = currentValue,
            InitialValue = initialValue,
            Increment = increment,
            MaxValue = maxValue,
            CreatedAt = createdAt,
            UpdatedAt = updatedAt,
            Idempotency = idempotency
        };
    }

    private static Hlc ReadHlc(ReadOnlySpan<byte> span, ref int pos)
    {
        int n = BinaryPrimitives.ReadInt32LittleEndian(span[pos..]); pos += 4;
        long l = BinaryPrimitives.ReadInt64LittleEndian(span[pos..]); pos += 8;
        uint c = BinaryPrimitives.ReadUInt32LittleEndian(span[pos..]); pos += 4;
        return new Hlc(n, l, c);
    }

    // Stand-ins mirroring HLCTimestamp (N/L/C) and SequenceAllocation field layouts.
    public readonly record struct Hlc(int N, long L, uint C);

    public readonly record struct Alloc(string Name, long Start, long End, int Count, long Revision);

    public sealed class SeqState
    {
        public string Name { get; set; } = "";
        public long CurrentValue { get; set; }
        public long InitialValue { get; set; }
        public long Increment { get; set; }
        public long? MaxValue { get; set; }
        public Hlc CreatedAt { get; set; }
        public Hlc UpdatedAt { get; set; }
        public Dictionary<string, Alloc> Idempotency { get; set; } = [];
    }
}
