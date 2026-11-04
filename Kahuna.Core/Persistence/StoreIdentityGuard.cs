
using Kahuna.Server.Persistence.Backend;
using Kahuna.Server.Persistence.Logging;
using Kommander;
using Kommander.System;
using Kommander.WAL;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Persistence;

/// <summary>
/// Thrown at node construction when the store at the storage path belongs to another node: its
/// durability floors certify a different Raft log, so opening it as this node's store would let
/// Kommander skip replay over entries this copy never applied and serve another node's rows as
/// this node's. The node does not start; the operator points it at its own store or removes the
/// foreign one so the node is rebuilt from its peers.
/// </summary>
public sealed class StoreIdentityMismatchException : Exception
{
    public StoreIdentityMismatchException(string message) : base(message)
    {
    }
}

/// <summary>
/// Checks, once at node construction and before Kommander consults any durability floor, that the
/// persistent store at the storage path is this node's own and was not emptied behind the node's back.
/// <list type="bullet">
/// <item>An identity for another node refuses the start (<see cref="StoreIdentityMismatchException"/>).</item>
/// <item>No identity but durable state (rows or floors): a store written by a build that predates the
/// identity; it is stamped and used.</item>
/// <item>No identity, no durable state, and a Raft log without history: a fresh node; stamped and used.</item>
/// <item>No identity, no durable state, but a Raft log with history: the store directory was lost or
/// replaced. The node starts, so a replicated partition can be rebuilt from its peers, but the condition
/// is logged as Critical and reported by the readiness probe, and the empty store answers a durability
/// floor of 0 for every data partition, so Kommander rebuilds each partition by snapshot where its WAL
/// no longer holds the prefix (and quarantines a single-replica partition instead of serving it short).</item>
/// </list>
/// A backend with no identity support (the in-memory backend, test fakes) is left alone.
/// </summary>
internal static class StoreIdentityGuard
{
    /// <summary>
    /// Runs the check. Returns the replaced-store notice when the store was found empty while the log holds
    /// history; null otherwise. Throws <see cref="StoreIdentityMismatchException"/> for a foreign store and
    /// <see cref="StorageCorruptionException"/> for an identity row that cannot be decoded.
    /// </summary>
    internal static string? Verify(IPersistenceBackend backend, IRaft raft, string storagePath, ILogger logger)
    {
        if (!backend.SupportsStoreIdentity)
            return null;

        string endpoint = raft.GetLocalEndpoint();

        PersistentStoreIdentity? identity = backend.GetStoreIdentity();

        if (identity is not null)
        {
            if (!string.Equals(identity.NodeEndpoint, endpoint, StringComparison.Ordinal))
            {
                throw new StoreIdentityMismatchException(
                    $"The store at '{storagePath}' belongs to node {identity.NodeEndpoint} (store {identity}), not to this node ({endpoint}). "
                    + "Its durability floors certify another node's Raft log, so it cannot be opened as this node's store: point the node at its own store, "
                    + "or move this directory aside so the node is rebuilt from its peers.");
            }

            return null;
        }

        bool holdsState = backend.HoldsDurableState();
        bool logHoldsHistory = WalHoldsHistory(raft.WalAdapter);

        PersistentStoreIdentity fresh = PersistentStoreIdentity.Create(endpoint);

        if (!backend.WriteStoreIdentity(fresh))
            throw new IOException($"The store at '{storagePath}' could not record its identity ({fresh}); the node cannot start over a store it cannot write.");

        if (holdsState)
        {
            logger.LogStoreIdentityStampedOnExistingStore(storagePath, fresh);
            return null;
        }

        if (!logHoldsHistory)
        {
            logger.LogStoreCreated(storagePath, fresh);
            return null;
        }

        string notice =
            $"The store at '{storagePath}' was empty when node {endpoint} started while its Raft log holds history: the store directory was lost or replaced. "
            + "Every data partition answers a durability floor of 0, so a partition whose log was compacted is rebuilt by snapshot from its peers, "
            + "and a single-replica partition stays quarantined instead of serving a store missing its prefix; restore it from a backup.";

        PersistenceMetrics.StoreReplacedAtStart.Add(1);
        logger.LogStoreReplacedAtStart(notice, fresh);

        return notice;
    }

    /// <summary>
    /// Whether the Raft log has ever recorded anything on this node. The system partition's log is the
    /// witness: a node that joined or formed a cluster has entries there before any data partition does,
    /// and a compacted system log keeps a compaction floor above its first id.
    /// </summary>
    private static bool WalHoldsHistory(IWAL? wal)
    {
        if (wal is null)
            return false;

        int system = RaftSystemConfig.SystemPartition;

        return wal.GetMaxLog(system) > 0 || wal.GetLastCheckpoint(system) > 0 || wal.GetCompactionFloor(system) > 1;
    }
}
