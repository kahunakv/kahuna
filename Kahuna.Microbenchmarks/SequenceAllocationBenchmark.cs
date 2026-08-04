using System.Buffers.Binary;
using System.Text;
using BenchmarkDotNet.Attributes;
using Polly.Contrib.WaitAndRetry;

namespace Kahuna.Microbenchmarks;

/// <summary>
/// Before/after for the per-allocation managed work in the sequencer's <c>next</c>/<c>reserve</c> path.
///
/// <para><b>Old</b> = every allocated value was a full read-modify-write of the durable record: build the
/// storage key, build the retry-backoff schedule, deserialize the whole record (scalars plus the entire
/// idempotency map), advance the counter, then serialize the whole record again for a compare-and-swap.
/// <b>New</b> = the value comes out of a block the actor already reserved, so the entire sequence above
/// happens once per <c>blockSize</c> values instead of once per value; a value inside the block is a
/// widened comparison against the block ceiling and the returned allocation struct, and nothing else.</para>
///
/// <para>The two paths below are faithful copies of the production code (same field order and layout, same
/// per-call incidentals) so the measured allocation figures match the real path without this project
/// taking a dependency on the server assembly — the convention the other benchmarks here follow.</para>
///
/// <para>Parametrised by idempotency-map size, because the record the old path re-serialized on <em>every
/// value</em> grew with the number of retained entries — and it was retained without bound. The block path
/// is flat in that parameter: it does not touch the record at all.</para>
///
/// <para><b>Result</b> (net10.0, Release, Apple silicon; retained idempotency entries → per-value cost):
/// <list type="bullet">
///   <item>0: 67.6 ns / 592 B → 0 ns / 0 B.</item>
///   <item>1: 144.0 ns / 1,184 B → 0 ns / 0 B.</item>
///   <item>32: 2,070.1 ns / 13,880 B → 0 ns / 0 B.</item>
/// </list>
/// A value taken from a reserved block allocates nothing at all, so the reduction is total rather than
/// the order of magnitude the design aimed at — the whole record round trip is simply absent. Note how
/// steeply the old path degraded with retained idempotency entries: the record was re-encoded on every
/// single value, and nothing ever pruned it.</para>
///
/// <para>The remaining cost the benchmark cannot show is the one that dominated in production: each of
/// those old writes was a Raft proposal with an fsync behind it. Block reservation turns N of those into
/// ceil(N / blockSize), which <c>TestSequences.TestAllocationsAreAmortizedOverOneWritePerBlock</c> pins
/// by counting the proposals a real node actually issues.</para>
/// </summary>
[MemoryDiagnoser]
public class SequenceAllocationBenchmark
{
    private const byte BinaryFormatVersion = 2;

    private const int MaxRetries = 16;

    private const string ReservedPrefix = "__kahuna:sequences:";

    /// <summary>The block path materialises its retry schedule once, not per call.</summary>
    private static readonly TimeSpan[] RetryDelays =
        Backoff.DecorrelatedJitterBackoffV2(medianFirstRetryDelay: TimeSpan.FromMilliseconds(1), retryCount: MaxRetries).ToArray();

    [Params(0, 1, 32)]
    public int IdempotencyEntries;

    private const string SequenceName = "orders/2f6c1b18f0a24d4f9c6f1b2a3c4d5e6f";

    private SeqState _state = null!;

    private byte[] _record = null!;

    private long _blockCurrent;

    private long _blockCeiling;

    [GlobalSetup]
    public void Setup()
    {
        _state = MakeState(IdempotencyEntries);
        _record = Serialize(_state);
        _blockCurrent = 0;
        _blockCeiling = long.MaxValue;
    }

    /// <summary>
    /// One value the old way: key string, backoff schedule, decode the record, advance, re-encode it.
    /// </summary>
    [Benchmark(Baseline = true)]
    public long ReadModifyWritePerValue()
    {
        string storageKey = ReservedPrefix + SequenceName;

        long consumed = storageKey.Length;

        foreach (TimeSpan delay in Backoff.DecorrelatedJitterBackoffV2(medianFirstRetryDelay: TimeSpan.FromMilliseconds(1), retryCount: MaxRetries))
        {
            SeqState state = Deserialize(_record);

            long start = state.CurrentValue + state.Increment;
            state.CurrentValue = start;

            byte[] encoded = Serialize(state);

            consumed += encoded.Length + start + (long)delay.TotalMilliseconds;
            break;
        }

        return consumed;
    }

    /// <summary>
    /// One value from a reserved block: no key string, no schedule, no record, no encoding.
    /// </summary>
    [Benchmark]
    public long AllocateFromReservedBlock()
    {
        Int128 increment = _state.Increment;
        Int128 last = (Int128)_blockCurrent + increment;

        if (last > _blockCeiling)
            return -1;

        long start = (long)((Int128)_blockCurrent + increment);
        _blockCurrent = (long)last;

        SeqAllocation allocation = new(SequenceName, start, (long)last, 1, 0);

        return allocation.End + RetryDelays.Length;
    }

    // ── codec copy (production layout) ──────────────────────────────────────────────────────────

    private static SeqState MakeState(int idempotencyEntries)
    {
        SeqState state = new()
        {
            Name = SequenceName,
            CurrentValue = 4_096,
            InitialValue = 0,
            Increment = 1,
            MaxValue = null
        };

        for (int i = 0; i < idempotencyEntries; i++)
            state.Idempotency["reserve:" + Guid.NewGuid().ToString("N")] =
                new SeqIdempotencyEntry(new SeqAllocation(SequenceName, i, i, 1, i), i);

        return state;
    }

    private static byte[] Serialize(SeqState state)
    {
        int nameLen = Encoding.UTF8.GetByteCount(state.Name);

        int size = 1
            + 2 + nameLen
            + 8 + 8 + 8
            + 1 + (state.MaxValue.HasValue ? 8 : 0)
            + 16 + 16
            + 4;

        foreach (KeyValuePair<string, SeqIdempotencyEntry> kvp in state.Idempotency)
            size += 2 + Encoding.UTF8.GetByteCount(kvp.Key)
                  + 2 + Encoding.UTF8.GetByteCount(kvp.Value.Allocation.Name)
                  + 8 + 8 + 4 + 8
                  + 16;

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

        WriteTimestamp(buf, ref pos);
        WriteTimestamp(buf, ref pos);

        BinaryPrimitives.WriteInt32LittleEndian(buf.AsSpan(pos), state.Idempotency.Count); pos += 4;

        foreach (KeyValuePair<string, SeqIdempotencyEntry> kvp in state.Idempotency)
        {
            SeqAllocation allocation = kvp.Value.Allocation;

            pos = WriteString(buf, pos, kvp.Key, Encoding.UTF8.GetByteCount(kvp.Key));
            pos = WriteString(buf, pos, allocation.Name, Encoding.UTF8.GetByteCount(allocation.Name));

            BinaryPrimitives.WriteInt64LittleEndian(buf.AsSpan(pos), allocation.Start); pos += 8;
            BinaryPrimitives.WriteInt64LittleEndian(buf.AsSpan(pos), allocation.End); pos += 8;
            BinaryPrimitives.WriteInt32LittleEndian(buf.AsSpan(pos), allocation.Count); pos += 4;
            BinaryPrimitives.WriteInt64LittleEndian(buf.AsSpan(pos), allocation.Revision); pos += 8;

            BinaryPrimitives.WriteInt32LittleEndian(buf.AsSpan(pos), 0); pos += 4;
            BinaryPrimitives.WriteInt64LittleEndian(buf.AsSpan(pos), kvp.Value.CreatedAt); pos += 8;
            BinaryPrimitives.WriteUInt32LittleEndian(buf.AsSpan(pos), 0); pos += 4;
        }

        return buf;
    }

    private static SeqState Deserialize(byte[] value)
    {
        ReadOnlySpan<byte> span = value;
        int pos = 1;

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

        pos += 32; // CreatedAt + UpdatedAt

        int idempotencyCount = BinaryPrimitives.ReadInt32LittleEndian(span[pos..]); pos += 4;
        Dictionary<string, SeqIdempotencyEntry> idempotency = new(idempotencyCount);

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

            pos += 4;
            long createdAt = BinaryPrimitives.ReadInt64LittleEndian(span[pos..]); pos += 8;
            pos += 4;

            idempotency[key] = new SeqIdempotencyEntry(new SeqAllocation(aName, start, end, count, revision), createdAt);
        }

        return new SeqState
        {
            Name = name,
            CurrentValue = currentValue,
            InitialValue = initialValue,
            Increment = increment,
            MaxValue = maxValue,
            Idempotency = idempotency
        };
    }

    private static int WriteString(byte[] buf, int pos, string value, int byteLen)
    {
        BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(pos), (ushort)byteLen); pos += 2;
        Encoding.UTF8.GetBytes(value.AsSpan(), buf.AsSpan(pos, byteLen)); pos += byteLen;
        return pos;
    }

    private static void WriteTimestamp(byte[] buf, ref int pos)
    {
        BinaryPrimitives.WriteInt32LittleEndian(buf.AsSpan(pos), 0); pos += 4;
        BinaryPrimitives.WriteInt64LittleEndian(buf.AsSpan(pos), 0); pos += 8;
        BinaryPrimitives.WriteUInt32LittleEndian(buf.AsSpan(pos), 0); pos += 4;
    }

    private sealed class SeqState
    {
        public string Name { get; set; } = "";
        public long CurrentValue { get; set; }
        public long InitialValue { get; set; }
        public long Increment { get; set; }
        public long? MaxValue { get; set; }
        public Dictionary<string, SeqIdempotencyEntry> Idempotency { get; set; } = [];
    }

    private readonly record struct SeqAllocation(string Name, long Start, long End, int Count, long Revision);

    private readonly record struct SeqIdempotencyEntry(SeqAllocation Allocation, long CreatedAt);
}
