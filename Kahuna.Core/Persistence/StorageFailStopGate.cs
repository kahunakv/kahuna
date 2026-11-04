
using System.Runtime.CompilerServices;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Server.Persistence.Logging;
using Kommander;
using Kommander.Data;
using Kommander.System;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Persistence;

/// <summary>
/// The node-level consequence of a fail-stopped store. The backend latches storage damage itself and
/// refuses every later operation; this gate turns that latch into a consensus decision, once: the node
/// withholds its candidacy for every partition it hosts and steps down where it leads, so another
/// replica serves the partition while this one waits for an operator. The gate is tripped by the first
/// read that classifies a <see cref="StorageCorruptionException"/> (on the actor thread, right before
/// the caller is answered <c>StorageCorrupt</c>) and by the background writer when it observes the
/// backend's latch on its tick, whichever comes first; later trips are no-ops.
/// <para>
/// One gate per backend instance, obtained through <see cref="For"/>: the actors receive their backend
/// through a fixed positional constructor list, so the gate is looked up by the backend they hold rather
/// than threaded through every constructor (the same shape as <see cref="KeyValuePointReadExecutor"/>).
/// The composition root attaches the Raft manager and the logger; a gate that was never attached (a test
/// over a bare backend) only records the reason.
/// </para>
/// </summary>
internal sealed class StorageFailStopGate
{
    private static readonly ConditionalWeakTable<IPersistenceBackend, StorageFailStopGate> gates = new();

    private IRaft? raft;

    private ILogger? logger;

    private string endpoint = "(unattached)";

    private string? reason;

    private int containing;

    /// <summary>
    /// The notice recorded at start when the store was found empty while the Raft log holds history,
    /// or null. Set once by the composition root, read by the readiness probe.
    /// </summary>
    internal string? ReplacedStoreNotice { get; set; }

    /// <summary>The failure the gate tripped on, or null while it is open.</summary>
    internal string? FailStopReason => Volatile.Read(ref reason);

    internal bool IsTripped => Volatile.Read(ref reason) is not null;

    /// <summary>The gate of <paramref name="backend"/>, created on first use.</summary>
    internal static StorageFailStopGate For(IPersistenceBackend backend) => gates.GetValue(backend, static _ => new StorageFailStopGate());

    /// <summary>Gives the gate what the containment needs. Called once by the composition root.</summary>
    internal void Attach(IRaft raft, ILogger logger)
    {
        this.raft = raft;
        this.logger = logger;

        try
        {
            endpoint = raft.GetLocalEndpoint();
        }
        catch (Exception)
        {
            endpoint = "(unknown)";
        }
    }

    /// <summary>
    /// Trips the gate on <paramref name="failure"/>. The first call wins and schedules the containment on
    /// the thread pool (the callers run on an actor thread); every later call returns at once. Safe from
    /// any thread.
    /// </summary>
    internal void Trip(string failure)
    {
        if (Interlocked.CompareExchange(ref reason, failure, null) is not null)
            return;

        PersistenceMetrics.StoreFailStops.Add(1);

        logger?.LogNodeStoreFailStop(endpoint, failure);

        _ = ContainAsync();
    }

    /// <summary>
    /// Re-applies the containment while tripped: a partition this node started hosting after the trip, or
    /// a leadership it was handed meanwhile, is gated too. Called from the background writer's tick; a no-op
    /// while the gate is open or a containment is already running.
    /// </summary>
    internal Task ReassertAsync() => IsTripped ? ContainAsync() : Task.CompletedTask;

    private async Task ContainAsync()
    {
        if (raft is null || Interlocked.Exchange(ref containing, 1) == 1)
            return;

        try
        {
            IReadOnlyList<RaftPartitionRange> map;
            try
            {
                map = raft.GetPartitionMap();
            }
            catch (Exception)
            {
                // Too early in boot for a partition map; the writer's tick re-asserts once there is one.
                return;
            }

            foreach (RaftPartitionRange range in map)
            {
                if (range.State == RaftPartitionState.Removed || !raft.HostsPartition(range.PartitionId))
                    continue;

                int partitionId = range.PartitionId;

                // Withhold before stepping down: a replica that still campaigns would win the election it
                // just forced and serve the next term from the damaged store.
                raft.SetCandidacyWithheld(partitionId, true);

                using CancellationTokenSource timeout = new(TimeSpan.FromSeconds(10));

                if (!await raft.AmILeaderIfHosted(partitionId, timeout.Token).ConfigureAwait(false))
                    continue;

                RaftOperationStatus status = await raft.StepDownAsync(partitionId, timeout.Token).ConfigureAwait(false);

                if (status == RaftOperationStatus.Success)
                    logger?.LogSteppedDownOnStoreFailStop(endpoint, partitionId);
                else
                    logger?.LogStepDownOnStoreFailStopRefused(endpoint, partitionId, status.ToString());
            }
        }
        catch (Exception ex)
        {
            logger?.LogStoreFailStopContainmentFailed(ex, endpoint);
        }
        finally
        {
            Volatile.Write(ref containing, 0);
        }
    }
}
