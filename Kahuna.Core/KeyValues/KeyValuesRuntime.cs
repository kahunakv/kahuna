using Nixie;

using Kommander;
using Kommander.WAL.IO;

using Kahuna.Server.Configuration;
using Kahuna.Server.Communication.Internode;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;

namespace Kahuna.Server.KeyValues;

/// <summary>
/// The infrastructure and shared state every key-value collaborator needs, gathered in one place so a
/// collaborator declares a single dependency instead of a dozen constructor parameters.
///
/// Three pieces of mutable state live here rather than inside one collaborator because more than one
/// reads them: <see cref="KeySpaceRegistry"/> (routing, router construction, replication apply and the
/// snapshot floor), <see cref="WriteFrequencyRegistry"/> (replication apply and the split/merge triggers),
/// and <see cref="DurableApplyResults"/> (written by the durable replication gateway, read by the
/// replication dispatcher for the same log entry). Everything else stays owned by its single collaborator.
///
/// <see cref="Locator"/> and the collaborator slots are assigned after construction: the locator is built
/// late (it takes the manager), and collaborators reference each other, so they are wired in a second pass
/// once all of them exist. They are never null once the manager's constructor has returned.
/// </summary>
internal sealed class KeyValuesRuntime
{
    internal required ActorSystem ActorSystem { get; init; }

    internal required IRaft Raft { get; init; }

    internal required IRaftReadScheduler BackendReadScheduler { get; init; }

    internal required IInterNodeCommunication InterNodeCommunication { get; init; }

    internal required IPersistenceBackend PersistenceBackend { get; init; }

    internal required IActorRef<BackgroundWriterActor, BackgroundWriteRequest> BackgroundWriter { get; init; }

    internal required KahunaConfiguration Configuration { get; init; }

    internal required ILogger<IKahuna> Logger { get; init; }

    /// <summary>Per-partition application-durability floor tracker, shared node-wide. Null when the node runs
    /// without one (direct-construction tests).</summary>
    internal PartitionDurabilityTracker? DurabilityTracker { get; init; }

    /// <summary>Per-node record of which key spaces are range-routed rather than hash-routed.</summary>
    internal required KeySpaceRegistry KeySpaceRegistry { get; init; }

    /// <summary>Per-key write counters feeding the load-based split trigger.</summary>
    internal required KeyWriteFrequencyRegistry WriteFrequencyRegistry { get; init; }

    /// <summary>Carries each durable entry's apply result from the consumer apply to the write scheduler's
    /// completion for the same log entry, so the completion reuses it instead of deserializing and
    /// re-applying an identical delta.</summary>
    internal required DurableApplyResultLedger DurableApplyResults { get; init; }

    internal required RangeMapStore RangeMapStore { get; init; }

    internal required PartitionDataEnumerator PartitionDataEnumerator { get; init; }

    /// <summary>Whole-partition state transfer, used to drop resident entries of an installed partition.</summary>
    internal required PartitionStateTransfer PartitionStateTransfer { get; init; }

    internal required SnapshotFloorStore SnapshotFloorStore { get; init; }

    internal required CompletionReceiptStore CompletionReceiptStore { get; init; }

    internal required TransactionRecordStore TransactionRecordStore { get; init; }

    internal required PreparedIntentStore PreparedIntentStore { get; init; }

    internal required Writes.PartitionWriteAggregator WriteAggregator { get; init; }

    /// <summary>Routes a request to the Raft leader for the key's partition. Assigned in the second wiring
    /// pass because the locator itself takes the manager.</summary>
    internal KeyValueLocator Locator { get; set; } = null!;

    /// <summary>The ephemeral/persistent actor rings. Assigned immediately after construction.</summary>
    internal KeyValueActorRouters Routers { get; set; } = null!;

    /// <summary>The durable-2PC replication path. Assigned in the second wiring pass.</summary>
    internal Writes.DurableReplicationGateway DurableReplication { get; set; } = null!;
}
