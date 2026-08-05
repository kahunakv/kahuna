using Kahuna.Server.Communication.Internode;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Shared.Sequences;
using Kommander;

namespace Kahuna.Server.Sequencer;

/// <summary>
/// Resolves which node owns a sequence and sends the request there.
///
/// <para>A sequence is routed by the partition of its storage key (<c>__kahuna:sequences:{name}</c>),
/// which is the same partition the durable record lives on. So the node that ends up serving the
/// request is also the leader for the record it compare-and-swaps: the ceiling bump is a local write,
/// and a sequence has exactly one owning actor in the cluster at a time rather than one per node.</para>
/// </summary>
internal sealed class SequenceLocator
{
    private readonly SequencerManager manager;

    private readonly IRaft raft;

    private readonly DataPartitionRouter dataPartitionRouter;

    private readonly IInterNodeCommunication interNodeCommunication;

    private readonly ILogger<IKahuna> logger;

    public SequenceLocator(SequencerManager manager, IRaft raft, IInterNodeCommunication interNodeCommunication, ILogger<IKahuna> logger)
    {
        this.manager = manager;
        this.raft = raft;
        this.dataPartitionRouter = new DataPartitionRouter(raft);
        this.interNodeCommunication = interNodeCommunication;
        this.logger = logger;
    }

    public async Task<(SequenceResponseType, ReadOnlySequenceEntry?)> LocateAndGetSequence(
        string name,
        SequenceDurability durability,
        CancellationToken cancellationToken
    )
    {
        (bool servedLocally, string leader) = await ResolveOwner(name, confirmLeadership: true, cancellationToken).ConfigureAwait(false);

        if (leader.Length == 0)
            return (SequenceResponseType.MustRetry, null);

        if (servedLocally)
            return await manager.GetSequence(name, durability, cancellationToken).ConfigureAwait(false);

        return await interNodeCommunication.GetSequence(leader, name, durability, cancellationToken).ConfigureAwait(false);
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
        (bool servedLocally, string leader) = await ResolveOwner(name, confirmLeadership: false, cancellationToken).ConfigureAwait(false);

        if (leader.Length == 0)
            return (SequenceResponseType.MustRetry, -1);

        if (servedLocally)
            return await manager.CreateSequence(name, initialValue, increment, maxValue, durability, cancellationToken).ConfigureAwait(false);

        return await interNodeCommunication.CreateSequence(leader, name, initialValue, increment, maxValue, durability, cancellationToken).ConfigureAwait(false);
    }

    public async Task<(SequenceResponseType, SequenceAllocation)> LocateAndReserveSequenceRange(
        string name,
        int count,
        string? idempotencyKey,
        SequenceDurability durability,
        CancellationToken cancellationToken
    )
    {
        (bool servedLocally, string leader) = await ResolveOwner(name, confirmLeadership: false, cancellationToken).ConfigureAwait(false);

        if (leader.Length == 0)
            return (SequenceResponseType.MustRetry, default);

        if (servedLocally)
            return await manager.ReserveSequenceRange(name, count, idempotencyKey, durability, cancellationToken).ConfigureAwait(false);

        return await interNodeCommunication.ReserveSequenceRange(leader, name, count, idempotencyKey, durability, cancellationToken).ConfigureAwait(false);
    }

    public async Task<SequenceResponseType> LocateAndDeleteSequence(
        string name,
        SequenceDurability durability,
        CancellationToken cancellationToken
    )
    {
        (bool servedLocally, string leader) = await ResolveOwner(name, confirmLeadership: false, cancellationToken).ConfigureAwait(false);

        if (leader.Length == 0)
            return SequenceResponseType.MustRetry;

        if (servedLocally)
            return await manager.DeleteSequence(name, durability, cancellationToken).ConfigureAwait(false);

        return await interNodeCommunication.DeleteSequence(leader, name, durability, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Resolves the node that owns <paramref name="name"/>.
    /// </summary>
    /// <param name="name">The sequence name.</param>
    /// <param name="confirmLeadership">
    /// When <see langword="true"/>, local ownership requires a quorum-confirmed leadership check
    /// (Raft read-index) instead of local belief. Read paths must pass <see langword="true"/>: a
    /// minority-partitioned leader keeps believing it leads and would answer a read with stale
    /// state. Mutation paths pass <see langword="false"/> — their CAS write replicates, so a
    /// deposed leader fails them anyway.
    /// </param>
    /// <param name="cancellationToken"></param>
    /// <returns>
    /// <c>(true, local endpoint)</c> when this node owns the sequence, <c>(false, leader)</c> when another
    /// does, and an empty leader when ownership cannot be resolved right now — the caller turns that into
    /// a retryable answer rather than an error, because it means an election is in progress.
    /// </returns>
    private async Task<(bool ServedLocally, string Leader)> ResolveOwner(string name, bool confirmLeadership, CancellationToken cancellationToken)
    {
        if (!raft.Joined)
            return (false, "");

        int partitionId = dataPartitionRouter.Locate(SequenceActor.GetStorageKey(name));

        bool servesLocally = confirmLeadership
            ? await raft.ConfirmLeadershipAsync(partitionId, cancellationToken).ConfigureAwait(false)
            : await raft.AmILeader(partitionId, cancellationToken).ConfigureAwait(false);

        if (servesLocally)
            return (true, raft.GetLocalEndpoint());

        string leader;

        try
        {
            leader = await raft.WaitForLeader(partitionId, cancellationToken).ConfigureAwait(false);
        }
        catch (RaftException ex)
        {
            // No leader decided yet for this partition (cluster startup, or a failover in progress). Tell
            // the caller to retry rather than surface the election as a server error, but log the reason so
            // a resolution failure that is not a routine election delay stays visible.
            logger.LogWarning("Sequence leader not resolved for partition {PartitionId} ('{Name}'): {Reason}", partitionId, name, ex.Message);

            return (false, "");
        }

        if (leader == raft.GetLocalEndpoint())
            return confirmLeadership ? (false, "") : (true, leader);

        if (logger.IsEnabled(LogLevel.Debug))
            logger.LogDebug("Sequence '{Name}' on partition {PartitionId} redirected to {Leader}", name, partitionId, leader);

        return (false, leader);
    }
}
