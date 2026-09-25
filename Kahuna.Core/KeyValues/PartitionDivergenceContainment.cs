using System.Collections.Concurrent;
using Kommander;
using Kommander.Data;

using Kahuna.Server.KeyValues.Logging;

namespace Kahuna.Server.KeyValues;

/// <summary>
/// Contains a partition whose apply projection on this node is known to be incomplete — the
/// fingerprint comparison found a peer holding more at the same applied kv log id, or recovery proved
/// that a prepared intent this node still holds was settled on a quorum of its peers. Detection alone
/// changed nothing in the fault soaks that found these shapes: the short replica led, served reads that
/// missed acknowledged writes, held settled intents as read-only keys, and the partition went read-only
/// within minutes. Containment is what turns the detection into an outcome:
/// <list type="bullet">
/// <item><b>Relinquish leadership.</b> A short leader hands the partition to the fullest peer the
/// evidence names (Kommander's leadership transfer), or steps down when the transfer is refused, so the
/// partition is served by a complete projection.</item>
/// <item><b>Refuse to serve.</b> The partition stays gated on this node — every locally served read or
/// write answers <c>MustRetry</c> and routes to the leader — until a whole-partition snapshot install
/// replaces the projection. The gate outlives a later re-election: a gated node promoted again
/// relinquishes at once, without re-probing, because the flag is the evidence.</item>
/// <item><b>Re-seed.</b> While gated the replica withholds its candidacy (<see cref="IRaft.SetCandidacyWithheld"/>)
/// so it never campaigns from the incomplete projection, and asks the leader for a whole-partition
/// snapshot (<see cref="IRaft.RequestReseedAsync"/>): Kommander holds this replica's applies, takes a fresh
/// checkpoint, ships the snapshot marked as requested, and installs it over the log this replica already
/// holds. The request is renewed on a cadence until the install lands.</item>
/// </list>
/// The gate clears only through <see cref="Ranges.PartitionStateTransfer.ImportPartitionState"/>, the
/// one path that rebuilds the projection from a complete replica; clearing releases the candidacy. The
/// gate is in-memory by design: a restart replays the log, which is itself a repair for the apply-skip
/// shape.
/// </summary>
internal sealed class PartitionDivergenceContainment
{
    /// <summary>Bound on relinquishing leadership: the transfer waits for the target to catch up for one election timeout.</summary>
    private const int RelinquishTimeoutMs = 15_000;

    /// <summary>
    /// How often a gated replica renews its re-seed request while the gate stands. A request the leader
    /// could not act on (deposed before it took the checkpoint, the transfer refused by a stalled disk)
    /// expires on Kommander's own bound; the renewal asks again, from whoever leads by then.
    /// </summary>
    private const int ReseedRenewalMs = 60_000;

    private readonly IRaft raft;

    private readonly ILogger<IKahuna> logger;

    // Partitions whose projection this node must not serve, with the evidence that gated them.
    private readonly ConcurrentDictionary<int, GatedPartition> gated = new();

    // Partitions with a relinquish in flight, so a burst of detections runs one transfer, not a pile.
    private readonly ConcurrentDictionary<int, byte> relinquishInFlight = new();

    private sealed record GatedPartition(string FullerPeer, string Evidence, DateTime SinceUtc, CancellationTokenSource Lifetime);

    public PartitionDivergenceContainment(IRaft raft, ILogger<IKahuna> logger)
    {
        this.raft = raft;
        this.logger = logger;
    }

    /// <summary>Whether this node must refuse to serve <paramref name="partitionId"/> from its own projection.</summary>
    internal bool IsGated(int partitionId) => !gated.IsEmpty && gated.ContainsKey(partitionId);

    /// <summary>Whether any partition is gated here: the one read a hot path pays before routing a key to ask <see cref="IsGated"/>.</summary>
    internal bool AnyGated => !gated.IsEmpty;

    /// <summary>The gated partitions, for diagnostics and tests.</summary>
    internal IReadOnlyList<int> GatedPartitions => [.. gated.Keys];

    /// <summary>
    /// Gates the partition on this node and, when this node leads it, relinquishes leadership to
    /// <paramref name="fullerPeer"/>. Idempotent: a partition already gated only re-runs the relinquish,
    /// which is what a re-elected gated node needs.
    /// </summary>
    /// <param name="partitionId">The partition whose local projection is incomplete.</param>
    /// <param name="fullerPeer">The replica the evidence names as complete; the preferred successor.</param>
    /// <param name="evidence">What proved the divergence, for the log line.</param>
    /// <param name="moment">Where it was detected (promotion, recovery), for the log line and the metric tag.</param>
    internal async Task ContainAsync(int partitionId, string fullerPeer, string evidence, string moment)
    {
        GatedPartition state = new(fullerPeer, evidence, DateTime.UtcNow, new CancellationTokenSource());

        if (gated.TryAdd(partitionId, state))
        {
            KeyValueApplyMetrics.DivergenceContained.Add(1, new KeyValuePair<string, object?>("action", "gated"));
            logger.LogApplyDivergenceGated(partitionId, raft.GetLocalEndpoint(), moment, evidence, fullerPeer);

            // Never campaign from the incomplete projection: a term this replica won would be served from
            // it, and every relinquish costs the partition an election. Released when the gate clears.
            raft.SetCandidacyWithheld(partitionId, true);

            _ = RequestReseedUntilRepairedAsync(partitionId, state.Lifetime.Token);
        }
        else
        {
            state.Lifetime.Dispose();
        }

        await RelinquishIfLeadingAsync(partitionId, fullerPeer).ConfigureAwait(false);
    }

    /// <summary>
    /// Asks the partition's leader to replace this replica's projection with a whole-partition snapshot,
    /// and keeps asking on a cadence while the gate stands. The request is refused while this node still
    /// leads (the relinquish runs first and the next renewal finds it a follower), and the install that
    /// answers it clears the gate through <see cref="ClearAsync"/>, which ends the loop.
    /// </summary>
    private async Task RequestReseedUntilRepairedAsync(int partitionId, CancellationToken lifetime)
    {
        int attempt = 0;

        try
        {
            while (!lifetime.IsCancellationRequested && gated.ContainsKey(partitionId))
            {
                // Give the relinquish a moment to land before the first request; a leader cannot be re-seeded.
                await Task.Delay(attempt == 0 ? 500 : ReseedRenewalMs, lifetime).ConfigureAwait(false);
                attempt++;

                if (!gated.ContainsKey(partitionId))
                    return;

                RaftOperationStatus status;

                try
                {
                    using CancellationTokenSource timeout = CancellationTokenSource.CreateLinkedTokenSource(lifetime);
                    timeout.CancelAfter(RelinquishTimeoutMs);
                    status = await raft.RequestReseedAsync(partitionId, timeout.Token).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (lifetime.IsCancellationRequested)
                {
                    return;
                }
                catch (Exception ex)
                {
                    logger.LogWarning(ex, "KeyValues: re-seed request for gated partition {PartitionId} did not complete", partitionId);
                    continue;
                }

                if (status == RaftOperationStatus.Success)
                {
                    KeyValueApplyMetrics.DivergenceContained.Add(1, new KeyValuePair<string, object?>("action", "reseed_requested"));
                    logger.LogApplyDivergenceReseedRequested(partitionId, raft.GetLocalEndpoint(), attempt);
                }
                else
                {
                    logger.LogApplyDivergenceReseedRefused(partitionId, raft.GetLocalEndpoint(), status.ToString(), attempt);

                    // A node that still leads is asked again sooner than the renewal cadence: the relinquish
                    // it is waiting on completes in seconds, not minutes.
                    if (status == RaftOperationStatus.NodeIsNotLeader)
                    {
                        await RelinquishIfLeadingAsync(partitionId, gated.TryGetValue(partitionId, out GatedPartition? state) ? state.FullerPeer : null).ConfigureAwait(false);
                        attempt = 0;
                    }
                }
            }
        }
        catch (OperationCanceledException)
        {
            // The gate cleared.
        }
    }

    /// <summary>
    /// Hands leadership of a gated partition away when this node leads it: a transfer to
    /// <paramref name="preferredPeer"/> first, a plain step-down when the transfer is refused (the peer is
    /// not a voter, not caught up inside the bound, or the transfer surface is unavailable). Either way the
    /// election that follows is decided by log freshness, which the gated node shares with its peers; it
    /// may be elected again, and then relinquishes again from <see cref="OnPromotedAsync"/>.
    /// </summary>
    private async Task RelinquishIfLeadingAsync(int partitionId, string? preferredPeer)
    {
        if (!relinquishInFlight.TryAdd(partitionId, 0))
            return;

        try
        {
            using CancellationTokenSource timeout = new(RelinquishTimeoutMs);

            if (!await raft.AmILeaderIfHosted(partitionId, timeout.Token).ConfigureAwait(false))
                return;

            string local = raft.GetLocalEndpoint();
            RaftOperationStatus status = RaftOperationStatus.Errored;

            if (!string.IsNullOrEmpty(preferredPeer) && preferredPeer != local)
            {
                status = await raft.TransferLeadershipAsync(partitionId, preferredPeer, timeout.Token).ConfigureAwait(false);

                if (status is RaftOperationStatus.Success or RaftOperationStatus.Pending)
                {
                    KeyValueApplyMetrics.DivergenceContained.Add(1, new KeyValuePair<string, object?>("action", "transferred"));
                    logger.LogApplyDivergenceLeadershipTransferred(partitionId, local, preferredPeer, status.ToString());
                    return;
                }

                logger.LogApplyDivergenceLeadershipTransferRefused(partitionId, local, preferredPeer, status.ToString());
            }

            status = await raft.StepDownAsync(partitionId, timeout.Token).ConfigureAwait(false);

            if (status == RaftOperationStatus.Success)
            {
                KeyValueApplyMetrics.DivergenceContained.Add(1, new KeyValuePair<string, object?>("action", "stepped_down"));
                logger.LogApplyDivergenceSteppedDown(partitionId, local);
                return;
            }

            KeyValueApplyMetrics.DivergenceContained.Add(1, new KeyValuePair<string, object?>("action", "relinquish_failed"));
            logger.LogApplyDivergenceRelinquishFailed(partitionId, local, status.ToString());
        }
        catch (Exception ex)
        {
            KeyValueApplyMetrics.DivergenceContained.Add(1, new KeyValuePair<string, object?>("action", "relinquish_failed"));
            logger.LogWarning(ex, "KeyValues: relinquishing leadership of gated partition {PartitionId} did not complete", partitionId);
        }
        finally
        {
            relinquishInFlight.TryRemove(partitionId, out _);
        }
    }

    /// <summary>
    /// A gated node was promoted again: relinquish at once, on the evidence that gated it. Returns
    /// whether the partition was gated (and so whether the caller can skip the fingerprint probe).
    /// </summary>
    internal async Task<bool> OnPromotedAsync(int partitionId)
    {
        if (!gated.TryGetValue(partitionId, out GatedPartition? state))
            return false;

        // The notification reaches every replica; only the gated node that now leads has anything to do.
        using CancellationTokenSource timeout = new(RelinquishTimeoutMs);
        if (!await raft.AmILeaderIfHosted(partitionId, timeout.Token).ConfigureAwait(false))
            return true;

        logger.LogApplyDivergenceGatedNodePromoted(partitionId, raft.GetLocalEndpoint(), state.FullerPeer);
        await RelinquishIfLeadingAsync(partitionId, state.FullerPeer).ConfigureAwait(false);
        return true;
    }

    /// <summary>Ends every re-seed renewal loop; the node is shutting down.</summary>
    internal void Shutdown()
    {
        foreach (GatedPartition state in gated.Values)
            state.Lifetime.Cancel();
    }

    /// <summary>
    /// A whole-partition install replaced this node's projection of <paramref name="partitionId"/> with a
    /// complete replica's: the evidence that gated it no longer describes the node. Wired as a
    /// resident-state invalidation hook of the transfer so every install clears the gate.
    /// </summary>
    internal Task ClearAsync(int partitionId)
    {
        if (gated.TryRemove(partitionId, out GatedPartition? state))
        {
            state.Lifetime.Cancel();
            state.Lifetime.Dispose();

            raft.SetCandidacyWithheld(partitionId, false);

            KeyValueApplyMetrics.DivergenceRepaired.Add(1);
            logger.LogApplyDivergenceRepaired(partitionId, raft.GetLocalEndpoint(), (long)(DateTime.UtcNow - state.SinceUtc).TotalMilliseconds);
        }

        return Task.CompletedTask;
    }
}
