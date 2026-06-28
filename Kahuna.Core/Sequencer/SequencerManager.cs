using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text;
using System.Text.Json;
using Kahuna.Server.KeyValues;
using Kahuna.Shared.KeyValue;
using Kahuna.Shared.Sequences;
using Kommander.Time;
using Polly.Contrib.WaitAndRetry;

namespace Kahuna.Server.Sequencer;

internal sealed class SequencerManager
{
    private const int MaxRetries = 64;

    private const string ReservedPrefix = "__kahuna:sequences:";

    // Format marker stored as byte 0 in every newly written record.
    // Existing JSON records start with '{' (0x7B), which is never a valid version byte here.
    private const byte BinaryFormatVersion = 1;

    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);

    private readonly KeyValuesManager keyValues;

    private readonly ILogger<IKahuna> logger;

    private readonly ConcurrentDictionary<string, SemaphoreSlim> sequenceLocks = new();

    public SequencerManager(KeyValuesManager keyValues, ILogger<IKahuna> logger)
    {
        this.keyValues = keyValues;
        this.logger = logger;
    }

    public async Task<(SequenceResponseType, ReadOnlySequenceEntry?)> LocateAndGetSequence(
        string name,
        SequenceDurability durability,
        CancellationToken cancellationToken
    )
    {
        if (!TryValidate(name, durability, out string normalizedName, out SequenceResponseType error))
            return (error, null);

        (KeyValueResponseType response, ReadOnlyKeyValueEntry? entry) = await keyValues.LocateAndTryGetValue(
            HLCTimestamp.Zero,
            GetStorageKey(normalizedName),
            -1,
            HLCTimestamp.Zero,
            ToKeyValueDurability(durability),
            cancellationToken
        ).ConfigureAwait(false);

        if (response == KeyValueResponseType.DoesNotExist)
            return (SequenceResponseType.NotFound, null);

        if (response != KeyValueResponseType.Get || entry?.Value is null)
            return (Map(response), null);

        SequenceState? state = Deserialize(entry.Value);
        if (state is null)
            return (SequenceResponseType.Error, null);

        return (SequenceResponseType.Success, ToReadOnlyEntry(state, entry.Revision, durability));
    }

    public async Task<(SequenceResponseType, long)> LocateAndCreateSequence(
        string name,
        long initialValue,
        long increment,
        long? maxValue,
        SequenceDurability durability,
        CancellationToken cancellationToken
    )
    {
        if (!TryValidate(name, durability, out string normalizedName, out SequenceResponseType error))
            return (error, -1);

        if (increment <= 0 || maxValue.HasValue && maxValue.Value < initialValue)
            return (SequenceResponseType.InvalidInput, -1);

        HLCTimestamp now = HLCTimestamp.Zero;
        SequenceState state = new()
        {
            Name = normalizedName,
            CurrentValue = initialValue,
            InitialValue = initialValue,
            Increment = increment,
            MaxValue = maxValue,
            CreatedAt = now,
            UpdatedAt = now
        };

        (KeyValueResponseType response, long revision, _) = await keyValues.LocateAndTrySetKeyValue(
            HLCTimestamp.Zero,
            GetStorageKey(normalizedName),
            Serialize(state),
            null,
            -1,
            KeyValueFlags.SetIfNotExists,
            0,
            ToKeyValueDurability(durability),
            cancellationToken
        ).ConfigureAwait(false);

        return response switch
        {
            KeyValueResponseType.Set => (SequenceResponseType.Success, revision),
            KeyValueResponseType.NotSet => (SequenceResponseType.AlreadyExists, -1),
            _ => (Map(response), revision)
        };
    }

    public Task<(SequenceResponseType, SequenceAllocation)> LocateAndNextSequenceValue(
        string name,
        string? idempotencyKey,
        SequenceDurability durability,
        CancellationToken cancellationToken
    )
    {
        return LocateAndReserveSequenceRange(name, 1, idempotencyKey, durability, cancellationToken);
    }

    public async Task<(SequenceResponseType, SequenceAllocation)> LocateAndReserveSequenceRange(
        string name,
        int count,
        string? idempotencyKey,
        SequenceDurability durability,
        CancellationToken cancellationToken
    )
    {
        if (!TryValidate(name, durability, out string normalizedName, out SequenceResponseType error))
            return (error, default);

        if (count <= 0)
            return (SequenceResponseType.InvalidInput, default);

        // Reject idempotency keys whose UTF-8 encoding would overflow the 2-byte length prefix.
        // 1024 bytes is generous for a key and keeps the binary frame well within ushort.MaxValue.
        if (idempotencyKey is not null && Encoding.UTF8.GetByteCount(idempotencyKey.Trim()) > 1024)
            return (SequenceResponseType.InvalidInput, default);

        SemaphoreSlim sequenceLock = sequenceLocks.GetOrAdd(normalizedName, _ => new(1, 1));
        await sequenceLock.WaitAsync(cancellationToken).ConfigureAwait(false);

        try
        {
            string? idempotencyKeyName = string.IsNullOrWhiteSpace(idempotencyKey) ? null : $"reserve:{idempotencyKey.Trim()}";

            foreach (TimeSpan delay in Backoff.DecorrelatedJitterBackoffV2(medianFirstRetryDelay: TimeSpan.FromMilliseconds(1), retryCount: MaxRetries))
            {
                (KeyValueResponseType getResponse, ReadOnlyKeyValueEntry? entry) = await keyValues.LocateAndTryGetValue(
                    HLCTimestamp.Zero,
                    GetStorageKey(normalizedName),
                    -1,
                    HLCTimestamp.Zero,
                    ToKeyValueDurability(durability),
                    cancellationToken
                ).ConfigureAwait(false);

                if (getResponse == KeyValueResponseType.DoesNotExist)
                    return (SequenceResponseType.NotFound, default);

                if (getResponse != KeyValueResponseType.Get || entry?.Value is null)
                    return (Map(getResponse), default);

                SequenceState? state = Deserialize(entry.Value);
                if (state is null)
                    return (SequenceResponseType.Error, default);

                if (idempotencyKeyName is not null && state.Idempotency.TryGetValue(idempotencyKeyName, out SequenceAllocation allocation))
                    return (SequenceResponseType.Success, allocation);

                if (!TryAllocate(state, count, entry.Revision, out SequenceAllocation nextAllocation, out SequenceResponseType allocateError))
                    return (allocateError, default);

                if (idempotencyKeyName is not null)
                    state.Idempotency[idempotencyKeyName] = nextAllocation;

                (KeyValueResponseType setResponse, _, _) = await keyValues.LocateAndTrySetKeyValue(
                    HLCTimestamp.Zero,
                    GetStorageKey(normalizedName),
                    Serialize(state),
                    null,
                    entry.Revision,
                    KeyValueFlags.SetIfEqualToRevision,
                    0,
                    ToKeyValueDurability(durability),
                    cancellationToken
                ).ConfigureAwait(false);

                if (setResponse == KeyValueResponseType.Set)
                    return (SequenceResponseType.Success, nextAllocation);

                if (setResponse is KeyValueResponseType.NotSet or KeyValueResponseType.MustRetry)
                {
                    await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
                    continue;
                }

                return (Map(setResponse), default);
            }

            logger.LogWarning("Sequence allocation exhausted retries for {Name}", normalizedName);
            return (SequenceResponseType.MustRetry, default);
        }
        finally
        {
            sequenceLock.Release();
        }
    }

    public async Task<SequenceResponseType> LocateAndDeleteSequence(
        string name,
        SequenceDurability durability,
        CancellationToken cancellationToken
    )
    {
        if (!TryValidate(name, durability, out string normalizedName, out SequenceResponseType error))
            return error;

        (KeyValueResponseType response, _, _) = await keyValues.LocateAndTryDeleteKeyValue(
            HLCTimestamp.Zero,
            GetStorageKey(normalizedName),
            ToKeyValueDurability(durability),
            cancellationToken
        ).ConfigureAwait(false);

        return response switch
        {
            KeyValueResponseType.Deleted => SequenceResponseType.Success,
            KeyValueResponseType.DoesNotExist => SequenceResponseType.NotFound,
            _ => Map(response)
        };
    }

    private static bool TryAllocate(
        SequenceState state,
        int count,
        long sourceRevision,
        out SequenceAllocation allocation,
        out SequenceResponseType error
    )
    {
        allocation = default;
        error = SequenceResponseType.Success;

        try
        {
            long start = checked(state.CurrentValue + state.Increment);
            long end = checked(state.CurrentValue + state.Increment * count);

            if (state.MaxValue.HasValue && end > state.MaxValue.Value)
            {
                error = SequenceResponseType.MaxValueExceeded;
                return false;
            }

            state.CurrentValue = end;
            state.UpdatedAt = HLCTimestamp.Zero;

            allocation = new(state.Name, start, end, count, sourceRevision);
            return true;
        }
        catch (OverflowException)
        {
            error = SequenceResponseType.MaxValueExceeded;
            return false;
        }
    }

    private static bool TryValidate(string name, SequenceDurability durability, out string normalizedName, out SequenceResponseType error)
    {
        normalizedName = name?.Trim() ?? "";

        if (durability != SequenceDurability.Persistent ||
            string.IsNullOrEmpty(normalizedName) ||
            normalizedName.StartsWith(ReservedPrefix, StringComparison.Ordinal) ||
            normalizedName.Length > 1024)
        {
            error = SequenceResponseType.InvalidInput;
            return false;
        }

        error = SequenceResponseType.Success;
        return true;
    }

    private static string GetStorageKey(string name)
    {
        return ReservedPrefix + name;
    }

    private static KeyValueDurability ToKeyValueDurability(SequenceDurability durability)
    {
        return durability switch
        {
            SequenceDurability.Persistent => KeyValueDurability.Persistent,
            _ => KeyValueDurability.Persistent
        };
    }

    private static ReadOnlySequenceEntry ToReadOnlyEntry(SequenceState state, long revision, SequenceDurability durability)
    {
        return new(
            state.Name,
            state.CurrentValue,
            state.InitialValue,
            state.Increment,
            state.MaxValue,
            revision,
            durability,
            state.CreatedAt,
            state.UpdatedAt
        );
    }

    private static SequenceResponseType Map(KeyValueResponseType response)
    {
        return response switch
        {
            KeyValueResponseType.DoesNotExist => SequenceResponseType.NotFound,
            KeyValueResponseType.NotSet => SequenceResponseType.AlreadyExists,
            KeyValueResponseType.InvalidInput => SequenceResponseType.InvalidInput,
            KeyValueResponseType.MustRetry => SequenceResponseType.MustRetry,
            KeyValueResponseType.Aborted => SequenceResponseType.Aborted,
            _ => SequenceResponseType.Error
        };
    }

    private static byte[] Serialize(SequenceState state)
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

        foreach (KeyValuePair<string, SequenceAllocation> kvp in state.Idempotency)
            size += 2 + Encoding.UTF8.GetByteCount(kvp.Key)
                  + 2 + Encoding.UTF8.GetByteCount(kvp.Value.Name)
                  + 8 + 8 + 4 + 8;

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

        foreach (KeyValuePair<string, SequenceAllocation> kvp in state.Idempotency)
        {
            pos = WriteString(buf, pos, kvp.Key, Encoding.UTF8.GetByteCount(kvp.Key), "Idempotency key");
            pos = WriteString(buf, pos, kvp.Value.Name, Encoding.UTF8.GetByteCount(kvp.Value.Name), "Allocation name");

            BinaryPrimitives.WriteInt64LittleEndian(buf.AsSpan(pos), kvp.Value.Start); pos += 8;
            BinaryPrimitives.WriteInt64LittleEndian(buf.AsSpan(pos), kvp.Value.End); pos += 8;
            BinaryPrimitives.WriteInt32LittleEndian(buf.AsSpan(pos), kvp.Value.Count); pos += 4;
            BinaryPrimitives.WriteInt64LittleEndian(buf.AsSpan(pos), kvp.Value.Revision); pos += 8;
        }

        return buf;
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

    // Backward-compat: records written before this version start with '{' (0x7B).
    private static SequenceState? Deserialize(byte[] value)
    {
        if (value.Length == 0)
            return null;

        return value[0] == (byte)'{'
            ? JsonSerializer.Deserialize<SequenceState>(value, JsonOptions)
            : DeserializeBinary(value);
    }

    private static SequenceState? DeserializeBinary(byte[] value)
    {
        try
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

            HLCTimestamp createdAt = ReadHlcTimestamp(span, ref pos);
            HLCTimestamp updatedAt = ReadHlcTimestamp(span, ref pos);

            int idempotencyCount = BinaryPrimitives.ReadInt32LittleEndian(span[pos..]); pos += 4;
            Dictionary<string, SequenceAllocation> idempotency = new(idempotencyCount);

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

                idempotency[key] = new SequenceAllocation(aName, start, end, count, revision);
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

    private sealed class SequenceState
    {
        public string Name { get; set; } = "";

        public long CurrentValue { get; set; }

        public long InitialValue { get; set; }

        public long Increment { get; set; }

        public long? MaxValue { get; set; }

        public HLCTimestamp CreatedAt { get; set; }

        public HLCTimestamp UpdatedAt { get; set; }

        public Dictionary<string, SequenceAllocation> Idempotency { get; set; } = [];
    }
}
