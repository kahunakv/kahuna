using System.Text.Json;
using System.Collections.Concurrent;
using Kahuna.Server.KeyValues;
using Kahuna.Shared.KeyValue;
using Kahuna.Shared.Sequences;
using Kommander.Time;

namespace Kahuna.Server.Sequencer;

internal sealed class SequencerManager
{
    private const int MaxRetries = 64;

    private const string ReservedPrefix = "__kahuna:sequences:";

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

        SemaphoreSlim sequenceLock = sequenceLocks.GetOrAdd(normalizedName, _ => new(1, 1));
        await sequenceLock.WaitAsync(cancellationToken).ConfigureAwait(false);

        try
        {
            string? idempotencyKeyName = string.IsNullOrWhiteSpace(idempotencyKey) ? null : $"reserve:{idempotencyKey.Trim()}";

            for (int i = 0; i < MaxRetries; i++)
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
                    await Task.Delay(1, cancellationToken).ConfigureAwait(false);
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
        return JsonSerializer.SerializeToUtf8Bytes(state, JsonOptions);
    }

    private static SequenceState? Deserialize(byte[] value)
    {
        return JsonSerializer.Deserialize<SequenceState>(value, JsonOptions);
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
