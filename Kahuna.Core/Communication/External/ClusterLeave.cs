
using Microsoft.AspNetCore.Http;
using Kommander;
using Kommander.Data;
using Kahuna.Shared.Communication.Rest;

namespace Kahuna.Communication.External;

/// <summary>
/// Runs a graceful decommission of the local node and translates the consensus-level outcome into
/// the answer the REST and gRPC surfaces hand back. Both transports share this so an orchestrator
/// sees the same verdict whichever one it speaks.
/// </summary>
public static class ClusterLeave
{
    /// <summary>
    /// Margin added to the consensus-side bound to produce this request's deadline. Consensus
    /// bounds its own attempt — a drain by <c>DecommissionDrainTimeout</c>, everything else well
    /// below that — so this guard exists only so a wedged attempt cannot hold a request open
    /// indefinitely, and must sit above the consensus bound rather than cut it short. Cancelling a
    /// drain mid-flight would report a timeout for a departure that then commits anyway.
    /// </summary>
    private static readonly TimeSpan DeadlineMargin = TimeSpan.FromSeconds(30);

    private static TimeSpan DeadlineFor(IRaft raft)
    {
        try
        {
            TimeSpan drainTimeout = raft.Configuration.DecommissionDrainTimeout;
            return drainTimeout > TimeSpan.Zero ? drainTimeout + DeadlineMargin : DeadlineMargin;
        }
        catch (Exception)
        {
            return DeadlineMargin;
        }
    }

    /// <summary>
    /// Asks consensus to remove the local node from the roster and reports the outcome, never
    /// throwing: an orchestrator driving a scale-down needs a verdict it can branch on, and an
    /// unclassifiable transport failure would leave it unable to tell "the node is out" from
    /// "nothing happened". A cancelled or wedged attempt maps to
    /// <see cref="LeaveClusterOutcome.Timeout"/>, which the caller resolves by re-reading the
    /// roster rather than by assuming either result.
    /// </summary>
    public static async Task<LeaveClusterResult> ExecuteAsync(IRaft raft, CancellationToken cancellationToken)
    {
        using CancellationTokenSource deadline = new(DeadlineFor(raft));
        using CancellationTokenSource linked =
            CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, deadline.Token);

        try
        {
            return await raft.RequestLeaveAsync(linked.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            return new(LeaveClusterOutcome.Timeout, SafeMembershipVersion(raft));
        }
        catch (Exception)
        {
            // A node too early in boot to have membership state, or one whose consensus layer is
            // already tearing down, cannot commit a removal — report a state the caller can act on.
            return new(LeaveClusterOutcome.NotInitialized, SafeMembershipVersion(raft));
        }
    }

    private static long SafeMembershipVersion(IRaft raft)
    {
        try
        {
            return raft.GetMembership().MembershipVersion;
        }
        catch (Exception)
        {
            return 0;
        }
    }

    /// <summary>
    /// Whether repeating the request could produce a different answer. A committed removal and a
    /// permanent refusal are both final; everything else failed for a reason that can pass.
    /// </summary>
    public static bool IsRetryable(LeaveClusterOutcome outcome) => outcome switch
    {
        LeaveClusterOutcome.Committed => false,
        LeaveClusterOutcome.NotAMember => false,
        LeaveClusterOutcome.RefusedInsufficientVoters => false,
        _ => true
    };

    /// <summary>
    /// HTTP status for the outcome. 200 means the node is out of the roster; 409 marks the
    /// permanent refusal so a caller that only reads status codes never retries it; 503 covers a
    /// node that could not attempt the removal — including one refused because another node is
    /// mid-drain, which passes on its own; and 504 an attempt that did not resolve in time.
    /// </summary>
    public static int ToStatusCode(LeaveClusterOutcome outcome) => outcome switch
    {
        LeaveClusterOutcome.Committed => StatusCodes.Status200OK,
        LeaveClusterOutcome.NotAMember => StatusCodes.Status200OK,
        LeaveClusterOutcome.RefusedInsufficientVoters => StatusCodes.Status409Conflict,
        LeaveClusterOutcome.NotInitialized => StatusCodes.Status503ServiceUnavailable,
        LeaveClusterOutcome.NoLeader => StatusCodes.Status503ServiceUnavailable,
        // Deliberately not 409: that code is the contract for "never retry", and a drain already
        // running elsewhere clears on its own.
        LeaveClusterOutcome.RefusedDrainInProgress => StatusCodes.Status503ServiceUnavailable,
        LeaveClusterOutcome.DrainTimedOut => StatusCodes.Status504GatewayTimeout,
        _ => StatusCodes.Status504GatewayTimeout
    };

    public static string ToReason(LeaveClusterOutcome outcome) => outcome switch
    {
        LeaveClusterOutcome.Committed =>
            "Removal committed. The node is out of the roster and can now be stopped.",
        LeaveClusterOutcome.NotAMember =>
            "The node is not in the committed roster; it already left or was evicted. Nothing to do.",
        LeaveClusterOutcome.RefusedInsufficientVoters =>
            "Refused: removing this node would leave the cluster without a voter. Do not retry — add a voter first, or stop the cluster instead of shrinking it.",
        LeaveClusterOutcome.NotInitialized =>
            "The node has not finished cluster initialization, so it has no roster entry to remove.",
        LeaveClusterOutcome.NoLeader =>
            "No system-partition leader could be reached, so the removal was not attempted.",
        LeaveClusterOutcome.RefusedDrainInProgress =>
            "Refused: another node is already draining, and only one decommission may run at a time. Nothing changed — retry once that drain finishes.",
        LeaveClusterOutcome.DrainTimedOut =>
            "The node's replicas were not fully evacuated before the drain deadline, so it stays in the roster and keeps serving. Replicas already moved stay moved — retry to resume the drain, or raise the drain timeout.",
        _ =>
            "The removal was not confirmed before the deadline. It may still commit — re-read the membership roster before deciding."
    };

    public static KahunaClusterLeaveResponse ToResponse(LeaveClusterResult result) => new()
    {
        Left = result.Left,
        Drained = result.Drained,
        Outcome = result.Outcome.ToString(),
        MembershipVersion = result.MembershipVersion,
        Retryable = IsRetryable(result.Outcome),
        Reason = ToReason(result.Outcome)
    };
}
