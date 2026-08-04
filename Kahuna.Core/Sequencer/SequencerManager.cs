using System.Text;
using Kahuna.Server.Communication.Internode;
using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues;
using Kahuna.Server.Sequencer.Data;
using Kahuna.Shared.KeyValue;
using Kahuna.Shared.Sequences;
using Kommander;
using Kommander.Time;
using Nixie;
using Nixie.Routers;

namespace Kahuna.Server.Sequencer;

/// <summary>
/// Entry point for sequence operations.
///
/// <para>Routing mirrors locks: <see cref="SequenceLocator"/> resolves the leader for the sequence's
/// partition and forwards there, and the owning node dispatches to the <see cref="SequenceActor"/> that
/// holds the name — so a sequence has one owning actor in the cluster, and its ceiling bump is a local
/// write against a record on the same partition.</para>
///
/// <para>Allocation state lives in the actors, not here: routing every increment through a full
/// read-modify-write of a general-purpose key-value record made each value cost a Raft commit. The
/// actors reserve blocks instead; see <see cref="SequenceActor"/>.</para>
/// </summary>
internal sealed class SequencerManager
{
    private const string ReservedPrefix = SequenceActor.ReservedPrefix;

    /// <summary>
    /// Longest accepted idempotency key, in UTF-8 bytes. Generous for a key while keeping the encoded
    /// record's 2-byte length prefixes valid.
    /// </summary>
    private const int MaxIdempotencyKeyBytes = SequenceStateCodec.MaxIdempotencyKeyBytes;

    private readonly KeyValuesManager keyValues;

    private readonly List<IActorRef<SequenceActor, SequenceRequest, SequenceResponse>> instances;

    private readonly IActorRef<ConsistentHashActor<SequenceActor, SequenceRequest, SequenceResponse>, SequenceRequest, SequenceResponse> router;

    private readonly SequenceLocator locator;

    public SequencerManager(
        ActorSystem actorSystem,
        IRaft raft,
        IInterNodeCommunication interNodeCommunication,
        KeyValuesManager keyValues,
        KahunaConfiguration configuration,
        ILogger<IKahuna> logger
    )
    {
        this.keyValues = keyValues;

        // At least one actor: a consistent-hash router over an empty pool has nothing to hash against.
        int workers = Math.Max(1, configuration.SequencerWorkers);

        instances = new(workers);

        for (int i = 0; i < workers; i++)
            instances.Add(actorSystem.Spawn<SequenceActor, SequenceRequest, SequenceResponse>(
                "sequence-" + i,
                keyValues,
                raft,
                configuration,
                logger
            ));

        router = actorSystem.CreateConsistentHashRouter(instances);

        locator = new(this, raft, interNodeCommunication, logger);
    }

    // ── locating entry points ───────────────────────────────────────────────────────────────────

    public Task<(SequenceResponseType, ReadOnlySequenceEntry?)> LocateAndGetSequence(
        string name,
        SequenceDurability durability,
        CancellationToken cancellationToken
    )
    {
        if (!TryValidate(name, durability, out string normalizedName, out SequenceResponseType error))
            return Task.FromResult<(SequenceResponseType, ReadOnlySequenceEntry?)>((error, null));

        return locator.LocateAndGetSequence(normalizedName, durability, cancellationToken);
    }

    public Task<(SequenceResponseType, long)> LocateAndCreateSequence(
        string name,
        long initialValue,
        long increment,
        long? maxValue,
        SequenceDurability durability,
        CancellationToken cancellationToken
    )
    {
        if (!TryValidate(name, durability, out string normalizedName, out SequenceResponseType error))
            return Task.FromResult((error, -1L));

        if (increment <= 0 || maxValue.HasValue && maxValue.Value < initialValue)
            return Task.FromResult((SequenceResponseType.InvalidInput, -1L));

        return locator.LocateAndCreateSequence(normalizedName, initialValue, increment, maxValue, durability, cancellationToken);
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

    public Task<(SequenceResponseType, SequenceAllocation)> LocateAndReserveSequenceRange(
        string name,
        int count,
        string? idempotencyKey,
        SequenceDurability durability,
        CancellationToken cancellationToken
    )
    {
        if (!TryValidate(name, durability, out string normalizedName, out SequenceResponseType error))
            return Task.FromResult((error, default(SequenceAllocation)));

        if (!TryValidateReserve(count, idempotencyKey, out string? normalizedIdempotencyKey))
            return Task.FromResult((SequenceResponseType.InvalidInput, default(SequenceAllocation)));

        return locator.LocateAndReserveSequenceRange(normalizedName, count, normalizedIdempotencyKey, durability, cancellationToken);
    }

    public Task<SequenceResponseType> LocateAndDeleteSequence(
        string name,
        SequenceDurability durability,
        CancellationToken cancellationToken
    )
    {
        if (!TryValidate(name, durability, out string normalizedName, out SequenceResponseType error))
            return Task.FromResult(error);

        return locator.LocateAndDeleteSequence(normalizedName, durability, cancellationToken);
    }

    // ── owning-node entry points ────────────────────────────────────────────────────────────────
    // Reached once this node is established as the leader for the sequence's partition, either by the
    // locator above or by another node forwarding here.

    /// <summary>
    /// Reads the durable record. Deliberately off the actor path: it reports the reserved high-water
    /// mark, which is what an observer of the sequence should see, and keeping reads out of the actor's
    /// mailbox means a stream of them cannot delay allocations.
    /// </summary>
    public async Task<(SequenceResponseType, ReadOnlySequenceEntry?)> GetSequence(
        string name,
        SequenceDurability durability,
        CancellationToken cancellationToken
    )
    {
        if (!TryValidate(name, durability, out string normalizedName, out SequenceResponseType error))
            return (error, null);

        (KeyValueResponseType response, ReadOnlyKeyValueEntry? entry) = await keyValues.LocateAndTryGetValue(
            HLCTimestamp.Zero,
            SequenceActor.GetStorageKey(normalizedName),
            -1,
            HLCTimestamp.Zero,
            ToKeyValueDurability(durability),
            cancellationToken
        ).ConfigureAwait(false);

        if (response == KeyValueResponseType.DoesNotExist)
            return (SequenceResponseType.NotFound, null);

        if (response != KeyValueResponseType.Get || entry?.Value is null)
            return (SequenceActor.Map(response), null);

        SequenceState? state = SequenceStateCodec.Deserialize(entry.Value);
        if (state is null)
            return (SequenceResponseType.Error, null);

        return (SequenceResponseType.Success, ToReadOnlyEntry(state, entry.Revision, durability));
    }

    public async Task<(SequenceResponseType, long)> CreateSequence(
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

        SequenceResponse? response = await router.Ask(new SequenceRequest(
            SequenceRequestType.Create,
            normalizedName,
            initialValue: initialValue,
            increment: increment,
            maxValue: maxValue,
            cancellationToken: cancellationToken
        )).ConfigureAwait(false);

        return response is null ? (SequenceResponseType.Error, -1) : (response.Type, response.Revision);
    }

    public Task<(SequenceResponseType, SequenceAllocation)> NextSequenceValue(
        string name,
        string? idempotencyKey,
        SequenceDurability durability,
        CancellationToken cancellationToken
    )
    {
        return ReserveSequenceRange(name, 1, idempotencyKey, durability, cancellationToken);
    }

    public async Task<(SequenceResponseType, SequenceAllocation)> ReserveSequenceRange(
        string name,
        int count,
        string? idempotencyKey,
        SequenceDurability durability,
        CancellationToken cancellationToken
    )
    {
        if (!TryValidate(name, durability, out string normalizedName, out SequenceResponseType error))
            return (error, default);

        if (!TryValidateReserve(count, idempotencyKey, out string? normalizedIdempotencyKey))
            return (SequenceResponseType.InvalidInput, default);

        SequenceResponse? response = await router.Ask(new SequenceRequest(
            SequenceRequestType.Reserve,
            normalizedName,
            count,
            normalizedIdempotencyKey,
            cancellationToken: cancellationToken
        )).ConfigureAwait(false);

        return response is null ? (SequenceResponseType.Error, default) : (response.Type, response.Allocation);
    }

    public async Task<SequenceResponseType> DeleteSequence(
        string name,
        SequenceDurability durability,
        CancellationToken cancellationToken
    )
    {
        if (!TryValidate(name, durability, out string normalizedName, out SequenceResponseType error))
            return error;

        SequenceResponse? response = await router.Ask(new SequenceRequest(
            SequenceRequestType.Delete,
            normalizedName,
            cancellationToken: cancellationToken
        )).ConfigureAwait(false);

        return response?.Type ?? SequenceResponseType.Error;
    }

    /// <summary>
    /// Discards every reserved block on this node when partition leadership moves. A block is per-node
    /// state a new leader cannot reconstruct, so it is surrendered rather than drained once the revision
    /// chain it was won against has changed hands. The abandoned values become gaps.
    /// </summary>
    public void OnLeaderChanged()
    {
        SequenceRequest invalidate = new(SequenceRequestType.Invalidate, "");

        foreach (IActorRef<SequenceActor, SequenceRequest, SequenceResponse> instance in instances)
            instance.Send(invalidate);
    }

    // ── validation ──────────────────────────────────────────────────────────────────────────────

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

    private static bool TryValidateReserve(int count, string? idempotencyKey, out string? normalizedIdempotencyKey)
    {
        normalizedIdempotencyKey = string.IsNullOrWhiteSpace(idempotencyKey) ? null : idempotencyKey.Trim();

        if (count <= 0)
            return false;

        // Reject idempotency keys whose UTF-8 encoding would overflow the record's 2-byte length prefix.
        return normalizedIdempotencyKey is null || Encoding.UTF8.GetByteCount(normalizedIdempotencyKey) <= MaxIdempotencyKeyBytes;
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
}
