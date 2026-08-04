using System.Buffers.Binary;
using System.Diagnostics;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Kahuna.Shared.Sequences;
using Kommander.Time;

namespace Kahuna.Server.Sequencer.Data;

/// <summary>
/// Storage codec for <see cref="SequenceState"/>.
///
/// <para>Records are written as a compact binary frame whose first byte is a format version. Three
/// formats are readable:</para>
/// <list type="bullet">
///   <item><b>JSON</b> — the original format, recognised by its leading <c>'{'</c> (0x7B), which is
///   never a valid version byte.</item>
///   <item><b>Version 1</b> — binary, idempotency entries without a timestamp.</item>
///   <item><b>Version 2</b> — binary, each idempotency entry carries the timestamp its retention
///   window is measured from.</item>
/// </list>
/// <para>Only version 2 is ever written; reading an older record and writing it back migrates it.</para>
/// </summary>
internal static class SequenceStateCodec
{
    private const byte BinaryFormatVersionWithoutEntryTimestamps = 1;

    private const byte BinaryFormatVersion = 2;

    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);

    /// <summary>Longest idempotency key accepted, in UTF-8 bytes. Keeps the frame's 2-byte length prefixes valid.</summary>
    public const int MaxIdempotencyKeyBytes = 1024;

    public static byte[] Serialize(SequenceState state)
    {
        // Size with GetByteCount (allocation-free) and encode straight into the buffer span below;
        // no per-string temporary byte[] are materialised.
        int nameLen = Encoding.UTF8.GetByteCount(state.Name);

        int size = 1                                           // version
            + 2 + nameLen                                     // name
            + 8 + 8 + 8                                       // CurrentValue, InitialValue, Increment
            + 1 + (state.MaxValue.HasValue ? 8 : 0)           // MaxValue flag + optional value
            + 16 + 16                                          // CreatedAt, UpdatedAt
            + 4;                                               // idempotency count

        foreach (KeyValuePair<string, SequenceIdempotencyEntry> kvp in state.Idempotency)
            size += 2 + Encoding.UTF8.GetByteCount(kvp.Key)
                  + 2 + Encoding.UTF8.GetByteCount(kvp.Value.Allocation.Name)
                  + 8 + 8 + 4 + 8
                  + 16;                                        // entry timestamp

        byte[] buf = new byte[size];
        int pos = 0;

        buf[pos++] = BinaryFormatVersion;

        pos = WriteString(buf, pos, state.Name, nameLen, "Sequence name");

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

        WriteHlcTimestamp(buf, ref pos, state.CreatedAt);
        WriteHlcTimestamp(buf, ref pos, state.UpdatedAt);

        BinaryPrimitives.WriteInt32LittleEndian(buf.AsSpan(pos), state.Idempotency.Count); pos += 4;

        foreach (KeyValuePair<string, SequenceIdempotencyEntry> kvp in state.Idempotency)
        {
            SequenceAllocation allocation = kvp.Value.Allocation;

            pos = WriteString(buf, pos, kvp.Key, Encoding.UTF8.GetByteCount(kvp.Key), "Idempotency key");
            pos = WriteString(buf, pos, allocation.Name, Encoding.UTF8.GetByteCount(allocation.Name), "Allocation name");

            BinaryPrimitives.WriteInt64LittleEndian(buf.AsSpan(pos), allocation.Start); pos += 8;
            BinaryPrimitives.WriteInt64LittleEndian(buf.AsSpan(pos), allocation.End); pos += 8;
            BinaryPrimitives.WriteInt32LittleEndian(buf.AsSpan(pos), allocation.Count); pos += 4;
            BinaryPrimitives.WriteInt64LittleEndian(buf.AsSpan(pos), allocation.Revision); pos += 8;

            WriteHlcTimestamp(buf, ref pos, kvp.Value.CreatedAt);
        }

        return buf;
    }

    public static SequenceState? Deserialize(byte[] value)
    {
        if (value.Length == 0)
            return null;

        return value[0] == (byte)'{'
            ? DeserializeJson(value)
            : DeserializeBinary(value);
    }

    /// <summary>
    /// Drops idempotency entries that have fallen out of the retention window, so the persisted record
    /// stays small no matter how many distinct keys a client uses over the sequence's lifetime. Age is
    /// measured from the entry's own timestamp; entries recovered from a format that carried none are
    /// treated as maximally old and evicted first.
    /// </summary>
    /// <param name="state">Record to prune in place.</param>
    /// <param name="now">Current hybrid-logical time.</param>
    /// <param name="maxEntries">Hard cap on retained entries; the oldest beyond it are dropped. Zero or less disables the cap.</param>
    /// <param name="ttl">Age beyond which an entry is dropped. Zero or less disables age pruning.</param>
    /// <param name="protectedKey">Entry that must survive regardless — the one being written right now.</param>
    public static void Prune(SequenceState state, HLCTimestamp now, int maxEntries, TimeSpan ttl, string? protectedKey)
    {
        if (state.Idempotency.Count == 0)
            return;

        if (ttl > TimeSpan.Zero && now.L > 0)
        {
            long cutoff = now.L - (long)ttl.TotalMilliseconds;

            List<string>? expired = null;

            foreach (KeyValuePair<string, SequenceIdempotencyEntry> kvp in state.Idempotency)
            {
                if (kvp.Key == protectedKey || kvp.Value.CreatedAt.L > cutoff)
                    continue;

                (expired ??= []).Add(kvp.Key);
            }

            if (expired is not null)
                foreach (string key in expired)
                    state.Idempotency.Remove(key);
        }

        if (maxEntries <= 0 || state.Idempotency.Count <= maxEntries)
            return;

        // Order oldest-first and drop from the front until the cap is met. The protected entry is
        // excluded from the candidate list so a freshly written allocation is never the one evicted.
        List<KeyValuePair<string, SequenceIdempotencyEntry>> candidates = new(state.Idempotency.Count);

        foreach (KeyValuePair<string, SequenceIdempotencyEntry> kvp in state.Idempotency)
            if (kvp.Key != protectedKey)
                candidates.Add(kvp);

        candidates.Sort(static (a, b) => a.Value.CreatedAt.CompareTo(b.Value.CreatedAt));

        int toRemove = state.Idempotency.Count - maxEntries;

        for (int i = 0; i < toRemove && i < candidates.Count; i++)
            state.Idempotency.Remove(candidates[i].Key);
    }

    /// <summary>Writes a 2-byte length prefix then the UTF-8 bytes of <paramref name="value"/>
    /// (whose byte length must equal <paramref name="byteLen"/>) directly into <paramref name="buf"/>.</summary>
    private static int WriteString(byte[] buf, int pos, string value, int byteLen, string label)
    {
        Debug.Assert(byteLen <= ushort.MaxValue, $"{label} UTF-8 exceeds ushort range");
        BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(pos), (ushort)byteLen); pos += 2;
        Encoding.UTF8.GetBytes(value.AsSpan(), buf.AsSpan(pos, byteLen)); pos += byteLen;
        return pos;
    }

    private static void WriteHlcTimestamp(byte[] buf, ref int pos, HLCTimestamp ts)
    {
        BinaryPrimitives.WriteInt32LittleEndian(buf.AsSpan(pos), ts.N); pos += 4;
        BinaryPrimitives.WriteInt64LittleEndian(buf.AsSpan(pos), ts.L); pos += 8;
        BinaryPrimitives.WriteUInt32LittleEndian(buf.AsSpan(pos), ts.C); pos += 4;
    }

    private static SequenceState? DeserializeBinary(byte[] value)
    {
        try
        {
            ReadOnlySpan<byte> span = value;
            int pos = 0;

            byte version = span[pos++];
            if (version is not (BinaryFormatVersion or BinaryFormatVersionWithoutEntryTimestamps))
                return null;

            bool hasEntryTimestamps = version == BinaryFormatVersion;

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

            HLCTimestamp createdAt = ReadHlcTimestamp(span, ref pos);
            HLCTimestamp updatedAt = ReadHlcTimestamp(span, ref pos);

            int idempotencyCount = BinaryPrimitives.ReadInt32LittleEndian(span[pos..]); pos += 4;

            // The count steers the dictionary's initial capacity, so bound it by what the remaining
            // bytes could structurally hold before trusting it — a corrupt record must fail the
            // decode, not force a huge allocation first.
            int minEntryBytes = hasEntryTimestamps ? 48 : 32;
            if (idempotencyCount < 0 || idempotencyCount > (span.Length - pos) / minEntryBytes)
                return null;

            Dictionary<string, SequenceIdempotencyEntry> idempotency = new(idempotencyCount);

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

                HLCTimestamp entryCreatedAt = hasEntryTimestamps ? ReadHlcTimestamp(span, ref pos) : HLCTimestamp.Zero;

                idempotency[key] = new(new SequenceAllocation(aName, start, end, count, revision), entryCreatedAt);
            }

            return new SequenceState
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
        catch (Exception)
        {
            return null;
        }
    }

    private static HLCTimestamp ReadHlcTimestamp(ReadOnlySpan<byte> span, ref int pos)
    {
        int n = BinaryPrimitives.ReadInt32LittleEndian(span[pos..]); pos += 4;
        long l = BinaryPrimitives.ReadInt64LittleEndian(span[pos..]); pos += 8;
        uint c = BinaryPrimitives.ReadUInt32LittleEndian(span[pos..]); pos += 4;
        return new HLCTimestamp(n, l, c);
    }

    private static SequenceState? DeserializeJson(byte[] value)
    {
        JsonSequenceState? parsed = JsonSerializer.Deserialize<JsonSequenceState>(value, JsonOptions);
        if (parsed is null)
            return null;

        Dictionary<string, SequenceIdempotencyEntry> idempotency = new(parsed.Idempotency.Count);

        // The JSON format carried no per-entry timestamp, so recovered entries sort as the oldest and
        // are the first evicted once the retention bounds engage.
        foreach (KeyValuePair<string, SequenceAllocation> kvp in parsed.Idempotency)
            idempotency[kvp.Key] = new(kvp.Value, HLCTimestamp.Zero);

        return new SequenceState
        {
            Name = parsed.Name,
            CurrentValue = parsed.CurrentValue,
            InitialValue = parsed.InitialValue,
            Increment = parsed.Increment,
            MaxValue = parsed.MaxValue,
            CreatedAt = parsed.CreatedAt,
            UpdatedAt = parsed.UpdatedAt,
            Idempotency = idempotency
        };
    }

    /// <summary>Shape of the original JSON record; read-only, never written.</summary>
    private sealed class JsonSequenceState
    {
        public string Name { get; set; } = "";

        public long CurrentValue { get; set; }

        public long InitialValue { get; set; }

        public long Increment { get; set; }

        public long? MaxValue { get; set; }

        public HLCTimestamp CreatedAt { get; set; }

        public HLCTimestamp UpdatedAt { get; set; }

        [JsonPropertyName("idempotency")]
        public Dictionary<string, SequenceAllocation> Idempotency { get; set; } = [];
    }
}
