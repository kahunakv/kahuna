using System.Diagnostics;
using System.Text;
using Kahuna.Server.Communication.Internode;
using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Server.Sequencer.Data;
using Kahuna.Shared.KeyValue;
using Kahuna.Shared.Sequences;
using Kommander;
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

    private readonly KahunaConfiguration configuration;

    private readonly ILogger<IKahuna> logger;

    private readonly IRaft raft;

    private readonly DataPartitionRouter dataPartitionRouter;

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
        this.configuration = configuration;
        this.logger = logger;
        this.raft = raft;
        this.dataPartitionRouter = new(raft);

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

        locator = new(this, raft, interNodeCommunication, configuration, logger);
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
        int? blockSize,
        SequenceDurability durability,
        CancellationToken cancellationToken
    )
    {
        if (!TryValidate(name, durability, out string normalizedName, out SequenceResponseType error))
            return Task.FromResult((error, -1L));

        if (!TryValidateCreate(initialValue, increment, maxValue, blockSize))
            return Task.FromResult((SequenceResponseType.InvalidInput, -1L));

        return locator.LocateAndCreateSequence(normalizedName, initialValue, increment, maxValue, blockSize, durability, cancellationToken);
    }

    public Task<(SequenceResponseType, long)> LocateAndUpdateSequence(
        string name,
        SequenceUpdate update,
        SequenceDurability durability,
        CancellationToken cancellationToken
    )
    {
        if (!TryValidate(name, durability, out string normalizedName, out SequenceResponseType error))
            return Task.FromResult((error, -1L));

        if (!TryValidateUpdate(update))
            return Task.FromResult((SequenceResponseType.InvalidInput, -1L));

        return locator.LocateAndUpdateSequence(normalizedName, update, durability, cancellationToken);
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
    // locator above or by another node forwarding here. Each re-checks leadership itself: a forward
    // races the very leader change that made it stale, and serving on a non-leader would put two
    // actors in the cluster behind one sequence. A stale forward gets MustRetry — never re-forwarded,
    // so disagreeing leadership views cannot bounce a request between nodes.

    /// <summary>
    /// Confirms this node leads the partition owning <paramref name="normalizedName"/>'s record.
    /// </summary>
    private async ValueTask<bool> IsLocalOwner(string normalizedName, CancellationToken cancellationToken)
    {
        if (!raft.Joined)
            return false;

        int partitionId = dataPartitionRouter.Locate(SequenceActor.GetStorageKey(normalizedName));

        return await raft.AmILeaderIfHosted(partitionId, cancellationToken).ConfigureAwait(false);
    }

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

        if (!await IsLocalOwner(normalizedName, cancellationToken).ConfigureAwait(false))
            return (SequenceResponseType.MustRetry, null);

        (KeyValueResponseType response, ReadOnlyKeyValueEntry? entry) = await keyValues.SystemGetKeyValue(
            SequenceActor.GetStorageKey(normalizedName),
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
        int? blockSize,
        SequenceDurability durability,
        CancellationToken cancellationToken
    )
    {
        if (!TryValidate(name, durability, out string normalizedName, out SequenceResponseType error))
            return (error, -1);

        if (!TryValidateCreate(initialValue, increment, maxValue, blockSize))
            return (SequenceResponseType.InvalidInput, -1);

        if (!await IsLocalOwner(normalizedName, cancellationToken).ConfigureAwait(false))
            return (SequenceResponseType.MustRetry, -1);

        SequenceResponse? response = await router.Ask(new SequenceRequest(
            SequenceRequestType.Create,
            normalizedName,
            initialValue: initialValue,
            increment: increment,
            maxValue: maxValue,
            blockSize: blockSize,
            cancellationToken: cancellationToken
        )).ConfigureAwait(false);

        return response is null ? (SequenceResponseType.Error, -1) : (response.Type, response.Revision);
    }

    /// <summary>
    /// Rewrites a sequence's parameters, breaking its identity as a value stream so that a block
    /// reserved from the previous record is voided rather than drained by whoever holds it.
    ///
    /// <para><b>This call takes about one <c>SequencerBlockLease</c> to answer, on purpose.</b> A
    /// reserved block is served with no storage traffic at all, so a node that has lost the sequence's
    /// partition without noticing keeps issuing from its window until the lease forces it to revalidate.
    /// Reporting success before then would report a guarantee that does not hold yet. Every caller of an
    /// update is a DDL-shaped statement, so a bounded delay is the right price; an instant answer that is
    /// wrong for a lease period is the failure being paid to avoid.</para>
    ///
    /// <para>The wait happens here rather than in the actor: an actor serves every sequence hashed to it
    /// and processes one request at a time, so sleeping inside it would stall allocations on sequences
    /// the update never touched.</para>
    /// </summary>
    public async Task<(SequenceResponseType, long)> UpdateSequence(
        string name,
        SequenceUpdate update,
        SequenceDurability durability,
        CancellationToken cancellationToken
    )
    {
        if (!TryValidate(name, durability, out string normalizedName, out SequenceResponseType error))
            return (error, -1);

        if (!TryValidateUpdate(update))
            return (SequenceResponseType.InvalidInput, -1);

        // Revalidation is what eventually voids a stale window, and this setting turns it off. With it
        // off no wait is long enough, so the operation is refused rather than answered with a guarantee
        // the node cannot keep. Refusing beats silently substituting a default: an operator who disabled
        // revalidation did so deliberately and needs to be told the two settings conflict.
        if (configuration.SequencerBlockLease <= TimeSpan.Zero)
        {
            logger.LogWarning(
                "Refusing to update sequence '{Name}': SequencerBlockLease is disabled, so a block reserved from the " +
                "replaced incarnation would never be revalidated and could keep issuing values indefinitely",
                normalizedName);

            return (SequenceResponseType.InvalidInput, -1);
        }

        if (!await IsLocalOwner(normalizedName, cancellationToken).ConfigureAwait(false))
            return (SequenceResponseType.MustRetry, -1);

        SequenceResponse? response = await router.Ask(new SequenceRequest(
            SequenceRequestType.Update,
            normalizedName,
            update: update,
            cancellationToken: cancellationToken
        )).ConfigureAwait(false);

        if (response is null)
            return (SequenceResponseType.Error, -1);

        if (response.Type != SequenceResponseType.Success)
            return (response.Type, response.Revision);

        await WaitForStaleWindow(response.StaleWindowClosesAt, cancellationToken).ConfigureAwait(false);

        return (SequenceResponseType.Success, response.Revision);
    }

    /// <summary>
    /// Sleeps until <paramref name="deadline"/>, a monotonic instant the actor stamped from its confirmed
    /// write. Measured with <see cref="Stopwatch"/> rather than a wall clock so a clock adjustment during
    /// the wait can neither shorten nor extend it.
    /// </summary>
    private static async Task WaitForStaleWindow(long deadline, CancellationToken cancellationToken)
    {
        // Looped rather than delayed once, because a timer may fire early and answering early is exactly
        // what this wait exists to prevent.
        while (true)
        {
            long now = Stopwatch.GetTimestamp();

            if (now >= deadline)
                return;

            await Task.Delay(Stopwatch.GetElapsedTime(now, deadline), cancellationToken).ConfigureAwait(false);
        }
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

        if (!await IsLocalOwner(normalizedName, cancellationToken).ConfigureAwait(false))
            return (SequenceResponseType.MustRetry, default);

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

        if (!await IsLocalOwner(normalizedName, cancellationToken).ConfigureAwait(false))
            return SequenceResponseType.MustRetry;

        SequenceResponse? response = await router.Ask(new SequenceRequest(
            SequenceRequestType.Delete,
            normalizedName,
            cancellationToken: cancellationToken
        )).ConfigureAwait(false);

        return response?.Type ?? SequenceResponseType.Error;
    }

    /// <summary>
    /// Discards the reserved blocks tied to a partition whose leadership moved. A block is per-node
    /// state a new leader cannot reconstruct, so it is surrendered rather than drained once the revision
    /// chain it was won against has changed hands. The abandoned values become gaps. Scoped to the one
    /// partition: blocks on partitions this node still leads are untouched, so an unrelated election
    /// does not burn their tails or stampede the store with reloads.
    /// </summary>
    public void OnLeaderChanged(int partitionId)
    {
        SequenceRequest invalidate = new(SequenceRequestType.Invalidate, "", partitionId: partitionId);

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

    /// <summary>
    /// Parameter checks a create must pass before anything is written. Shared with the routed half so the
    /// two cannot drift apart.
    /// </summary>
    private static bool TryValidateCreate(long initialValue, long increment, long? maxValue, int? blockSize)
    {
        return increment > 0
            && (!maxValue.HasValue || maxValue.Value >= initialValue)
            && blockSize is not < 1;
    }

    /// <summary>
    /// What a change set can be rejected for without reading the record. Anything that depends on the
    /// record as it will be — a maximum below a current value the caller left alone — is checked inside
    /// the actor, against the folded record, where it is actually decidable.
    /// </summary>
    private static bool TryValidateUpdate(SequenceUpdate update)
    {
        if (update.IsEmpty)
            return false;

        if (update.Increment is <= 0)
            return false;

        if (update.BlockSize is < 1)
            return false;

        // A caller that both clears a setting and supplies a value for it has contradicted itself; the
        // safest reading of a contradiction is to write neither.
        if (update.RemoveMaxValue && update.MaxValue.HasValue)
            return false;

        return !(update.RemoveBlockSize && update.BlockSize.HasValue);
    }

    private static bool TryValidateReserve(int count, string? idempotencyKey, out string? normalizedIdempotencyKey)
    {
        normalizedIdempotencyKey = string.IsNullOrWhiteSpace(idempotencyKey) ? null : idempotencyKey.Trim();

        if (count <= 0)
            return false;

        // Reject idempotency keys whose UTF-8 encoding would overflow the record's 2-byte length prefix.
        return normalizedIdempotencyKey is null || Encoding.UTF8.GetByteCount(normalizedIdempotencyKey) <= MaxIdempotencyKeyBytes;
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
            state.UpdatedAt,
            state.BlockSize,
            state.Incarnation
        );
    }
}
