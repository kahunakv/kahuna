using Kommander;
using Kommander.Time;

using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues;

/// <summary>
/// The refcounted, leased MVCC snapshot-hold surface: acquiring, renewing, releasing and reaping holds,
/// and answering the effective snapshot floor those holds imply. The floor is what keeps MVCC pruning
/// from reclaiming revisions a live point-in-time reader still needs, so the read is leadership-gated
/// rather than served from whatever this node happens to hold.
/// </summary>
internal sealed class SnapshotHoldService
{
    private readonly KeyValuesRuntime runtime;

    internal SnapshotHoldService(KeyValuesRuntime runtime) => this.runtime = runtime;

    // Aliases matching the field names the moved bodies use, so those bodies stay byte-for-byte as they were.
    private IRaft raft => runtime.Raft;

    private SnapshotFloorStore snapshotFloorStore => runtime.SnapshotFloorStore;

    private KeyValueLocator locator => runtime.Locator;

    private KeySpaceRegistry keySpaceRegistry => runtime.KeySpaceRegistry;

    private Kahuna.Server.Communication.Internode.IInterNodeCommunication interNodeCommunication => runtime.InterNodeCommunication;

    /// <summary>
    /// Acquires or renews a refcounted hold protecting all revisions at/after
    /// <paramref name="timestamp"/>. Idempotent by (holderId, timestamp).
    /// </summary>
    public async Task<(KeyValueResponseType Type, string HoldId, HLCTimestamp LeaseExpiry)> AcquireSnapshotHold(
        string holderId, HLCTimestamp timestamp, int leaseMs, CancellationToken ct)
    {
        if (!raft.Joined)
            return (KeyValueResponseType.MustRetry, string.Empty, HLCTimestamp.Zero);

        if (await raft.AmILeaderIfHosted(RangeMapStore.MetaPartitionId, ct).ConfigureAwait(false))
            return await snapshotFloorStore.AcquireAsync(holderId, timestamp, leaseMs, ct).ConfigureAwait(false);

        string leader = await raft.WaitForLeader(RangeMapStore.MetaPartitionId, ct).ConfigureAwait(false);
        if (leader == raft.GetLocalEndpoint())
            return await snapshotFloorStore.AcquireAsync(holderId, timestamp, leaseMs, ct).ConfigureAwait(false);

        return await interNodeCommunication.AcquireSnapshotHold(leader, holderId, timestamp, leaseMs, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Renews the lease on an existing hold. Fails when the hold has already expired or was never
    /// registered.
    /// </summary>
    public async Task<(KeyValueResponseType Type, HLCTimestamp LeaseExpiry)> RenewSnapshotHold(
        string holdId, int leaseMs, CancellationToken ct)
    {
        if (!raft.Joined)
            return (KeyValueResponseType.MustRetry, HLCTimestamp.Zero);

        if (await raft.AmILeaderIfHosted(RangeMapStore.MetaPartitionId, ct).ConfigureAwait(false))
            return await snapshotFloorStore.RenewAsync(holdId, leaseMs, ct).ConfigureAwait(false);

        string leader = await raft.WaitForLeader(RangeMapStore.MetaPartitionId, ct).ConfigureAwait(false);
        if (leader == raft.GetLocalEndpoint())
            return await snapshotFloorStore.RenewAsync(holdId, leaseMs, ct).ConfigureAwait(false);

        return await interNodeCommunication.RenewSnapshotHold(leader, holdId, leaseMs, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Releases a hold. The effective floor rises when the lowest hold is released.
    /// </summary>
    public async Task<KeyValueResponseType> ReleaseSnapshotHold(string holdId, CancellationToken ct)
    {
        if (!raft.Joined)
            return KeyValueResponseType.MustRetry;

        if (await raft.AmILeaderIfHosted(RangeMapStore.MetaPartitionId, ct).ConfigureAwait(false))
            return await snapshotFloorStore.ReleaseAsync(holdId, ct).ConfigureAwait(false);

        string leader = await raft.WaitForLeader(RangeMapStore.MetaPartitionId, ct).ConfigureAwait(false);
        if (leader == raft.GetLocalEndpoint())
            return await snapshotFloorStore.ReleaseAsync(holdId, ct).ConfigureAwait(false);

        return await interNodeCommunication.ReleaseSnapshotHold(leader, holdId, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Removes all holds whose lease has expired. Normally called by the background reaper;
    /// exposed here so tests can trigger a purge cycle without waiting for the timer.
    /// </summary>
    internal Task<int> PurgeExpiredSnapshotHoldsAsync(CancellationToken ct = default) =>
        snapshotFloorStore.PurgeExpiredHoldsAsync(ct);

    /// <summary>
    /// Returns the current effective floor (minimum live held timestamp, or
    /// <see cref="HLCTimestamp.Zero"/> when no hold is live) and the count of live holds.
    ///
    /// <para>Answers locally only under read-index leadership confirmation — never from local
    /// belief alone. A node that merely thinks itself meta-partition leader (a stale view during
    /// an election, or a fresh leader that has not yet applied inherited hold mutations) would
    /// report an empty registry: zero live holds and a <see cref="HLCTimestamp.Zero"/> floor, the
    /// value that means "reclaim anything". When leadership cannot be confirmed and no other
    /// node is named leader, the answer is <see cref="KeyValueResponseType.MustRetry"/> — failing
    /// closed instead of failing open.</para>
    /// </summary>
    public async Task<(KeyValueResponseType Type, HLCTimestamp EffectiveFloor, int LiveHolds)> GetSnapshotFloor(CancellationToken ct)
    {
        if (!raft.Joined)
            return (KeyValueResponseType.MustRetry, HLCTimestamp.Zero, 0);

        if (await raft.ConfirmLeadershipIfHosted(RangeMapStore.MetaPartitionId, ct).ConfigureAwait(false))
            return ReadLocalSnapshotFloor();

        string leader = await raft.WaitForLeader(RangeMapStore.MetaPartitionId, ct).ConfigureAwait(false);
        if (leader == raft.GetLocalEndpoint())
        {
            // The election settled on this node after the confirmation above failed; confirm
            // again before answering. If it still cannot be confirmed, the local hold registry
            // may not include committed mutations from the prior term — refuse to answer.
            if (await raft.ConfirmLeadershipIfHosted(RangeMapStore.MetaPartitionId, ct).ConfigureAwait(false))
                return ReadLocalSnapshotFloor();

            return (KeyValueResponseType.MustRetry, HLCTimestamp.Zero, 0);
        }

        return await interNodeCommunication.GetSnapshotFloor(leader, ct).ConfigureAwait(false);
    }

    private (KeyValueResponseType Type, HLCTimestamp EffectiveFloor, int LiveHolds) ReadLocalSnapshotFloor()
    {
        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());
        (HLCTimestamp floor, int liveHolds) = snapshotFloorStore.GetEffectiveFloorAndCount(now);
        return (KeyValueResponseType.Get, floor, liveHolds);
    }
}
