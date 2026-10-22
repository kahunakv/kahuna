
using Kommander;
using Microsoft.Extensions.Logging;

namespace Kahuna;

/// <summary>
/// The wait for a partition leader while an embedded node starts.
/// <para>
/// Kommander's <c>WaitForLeader</c> bounds one election by a fixed budget (10 s once the partition's WAL
/// restore is done) and reports the budget running out as a <see cref="RaftException"/>. At start that
/// budget is not a verdict on the node: a cluster that is briefly without a leader (the leader died while
/// this node was restoring, an election split) elects one a little later, and a node that gives up then
/// also withdraws the vote its peers may need for exactly that election. One restart that exits can so
/// become a whole-cluster outage. The node therefore keeps waiting, one Kommander round after another,
/// logging each round, until a leader appears, the caller cancels, or the optional
/// <see cref="EmbeddedKahunaOptions.StartupLeaderWait"/> budget runs out.
/// </para>
/// <para>
/// Only the undecided outcomes are retried: the election budget running out (a bare
/// <see cref="RaftException"/>, no cause attached) and <see cref="RaftNodeNotReadyException"/> (the node
/// has not finished assembling). A restore failure is reported by Kommander as a
/// <see cref="RaftException"/> wrapping its cause and stays fatal, as does
/// <see cref="PartitionNotHostedException"/>, which the caller handles by skipping the partition.
/// </para>
/// </summary>
internal static class StartupLeaderWait
{
    /// <summary>Pause between two rounds, so a node that is not assembled yet does not spin on the refusal.</summary>
    internal static readonly TimeSpan RetryPause = TimeSpan.FromMilliseconds(250);

    /// <summary>
    /// Waits for the leader of <paramref name="partitionId"/> through <paramref name="waitForLeader"/>,
    /// retrying every undecided round until a leader is known.
    /// </summary>
    /// <param name="waitForLeader">One bounded Kommander wait for the partition's leader.</param>
    /// <param name="partitionId">The partition, for the log lines.</param>
    /// <param name="budget">
    /// Overall ceiling on the wait, or <see cref="Timeout.InfiniteTimeSpan"/> to wait until cancelled. When
    /// it runs out the last undecided outcome is rethrown wrapped in a <see cref="RaftException"/> that
    /// names the partition and the time spent.
    /// </param>
    /// <param name="logger">Receives a warning per undecided round and a line when a late leader appears.</param>
    /// <param name="cancellationToken">Ends the wait (the caller's shutdown or the node's disposal).</param>
    internal static async ValueTask<string> WaitAsync(
        Func<CancellationToken, ValueTask<string>> waitForLeader,
        int partitionId,
        TimeSpan budget,
        ILogger logger,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(waitForLeader);
        ArgumentNullException.ThrowIfNull(logger);

        long startedTicks = Environment.TickCount64;
        int undecidedRounds = 0;

        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();

            try
            {
                string leader = await waitForLeader(cancellationToken).ConfigureAwait(false);

                if (undecidedRounds > 0 && logger.IsEnabled(LogLevel.Information))
                    logger.LogInformation(
                        "Startup: partition {PartitionId} has leader {Leader} after {ElapsedMs} ms and {Rounds} undecided round(s)",
                        partitionId, leader, Environment.TickCount64 - startedTicks, undecidedRounds);

                return leader;
            }
            catch (RaftException ex) when (IsUndecided(ex))
            {
                undecidedRounds++;
                long elapsedMs = Environment.TickCount64 - startedTicks;

                if (budget != Timeout.InfiniteTimeSpan && elapsedMs >= budget.TotalMilliseconds)
                    throw new RaftException(
                        $"No leader for partition {partitionId} after {elapsedMs} ms and {undecidedRounds} undecided round(s); the startup leader wait budget ({budget}) ran out",
                        ex);

                logger.LogWarning(
                    "Startup: still waiting for a leader of partition {PartitionId} after {ElapsedMs} ms ({Rounds} undecided round(s), last: {Reason}); the node keeps waiting until a leader is elected or it is shut down",
                    partitionId, elapsedMs, undecidedRounds, ex.Message);
            }

            await Task.Delay(RetryPause, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// True for the outcomes that mean "no leader yet": the election budget ran out, or the node is still
    /// assembling. A wrapped cause (a faulted restore) or a partition this node does not host is not one.
    /// </summary>
    internal static bool IsUndecided(RaftException ex) =>
        ex is RaftNodeNotReadyException || (ex is not PartitionNotHostedException && ex.InnerException is null);
}
