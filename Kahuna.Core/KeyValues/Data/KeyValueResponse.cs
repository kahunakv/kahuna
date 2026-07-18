
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace Kahuna.Server.KeyValues;

/// <summary>
/// Represents a response object for the KeyValueActor operations
/// </summary>
/// <remarks>
/// The <see cref="KeyValueResponse"/> class encapsulates the result of operations performed within the
/// key-value actor. It is designed to handle various response scenarios, including success, errors,
/// locks, and other specific outcomes. The response may include additional metadata such as the revision
/// number, timestamp, or context items depending on the operation performed.
/// </remarks>
public sealed class KeyValueResponse
{
    /// <summary>
    /// Gets the response type of the key-value operation.
    /// </summary>
    /// <remarks>
    /// The <see cref="Type"/> property indicates the result or response type of the operation performed
    /// against the key-value system. It provides information on whether the operation was successful,
    /// encountered errors, involved locks, or resulted in other specific states. The value is derived from
    /// the <see cref="KeyValueResponseType"/> enumeration, which defines all possible response types.
    /// </remarks>
    public KeyValueResponseType Type { get; }

    /// <summary>
    /// Gets the revision number associated with the key-value operation.
    /// </summary>
    /// <remarks>
    /// The <see cref="Revision"/> property represents a unique, incremental value that indicates
    /// the specific version or state of the key-value store after a particular operation.
    /// It is used for concurrency control and tracking changes within the system, ensuring that
    /// modifications can be identified with precision based on their order of execution.
    /// </remarks>
    public long Revision { get; }

    /// <summary>
    /// Gets the hybrid logical clock (HLC) timestamp associated with preparation of the mutation
    /// when the key-value operation was last modified or processed as part of a transaction.
    /// </summary>
    public HLCTimestamp Ticket { get; }

    /// <summary>
    /// Gets the read-only key-value context associated with the response. Used in the 'get' operation.
    /// </summary>
    /// <remarks>
    /// The <see cref="Entry"/> property provides additional context about the key-value operation if available.
    /// It encapsulates metadata or supplementary information relevant to the request, such as conditions
    /// or detailed operation state. This property can return null if no context is associated with the response.
    /// </remarks>
    public ReadOnlyKeyValueEntry? Entry { get; }
    
    /// <summary>
    /// Used in the 'get by bucket' operation to return all the found values.
    /// </summary>
    public List<(string, ReadOnlyKeyValueEntry)>? Items { get; }

    /// <summary>
    /// Used in the 'get by range' operation to return a paged result with cursor.
    /// </summary>
    public KeyValueGetByRangeResult? RangeResult { get; }

    /// <summary>
    /// Used by GetRangeLocks to return the live range-lock entries for a key space.
    /// </summary>
    public List<KeyValueRangeLock>? RangeLockList { get; private set; }

    /// <summary>
    /// Transaction id of the conflicting lock holder at denial time.
    /// Advisory only — may be stale by the time the caller acts.
    /// <see cref="HLCTimestamp.Zero"/> means no holder info (non-denial outcomes, or denials
    /// where no single holder applies such as <c>WaitingForReplication</c> / infrastructural
    /// <c>Errored</c>). Never gate correctness on this field; the actual lock is the sole
    /// safety mechanism.
    /// </summary>
    public HLCTimestamp HolderTransactionId { get; private set; }

    /// <summary>
    /// Factory for a denial response that carries the conflicting holder's transaction id.
    /// Use instead of <see cref="KeyValueStaticResponses.AlreadyLockedResponse"/> at sites
    /// where the holder is known.
    /// </summary>
    public static KeyValueResponse Denied(KeyValueResponseType type, HLCTimestamp holder) =>
        new(type) { HolderTransactionId = holder };

    /// <summary>
    /// Construtor
    /// </summary>
    /// <param name="type"></param>
    public KeyValueResponse(KeyValueResponseType type)
    {
        Type = type;
    }
    
    public KeyValueResponse(KeyValueResponseType type, long revision)
    {
        Type = type;
        Revision = revision;
    }
    
    public KeyValueResponse(KeyValueResponseType type, long revision, HLCTimestamp lastModified)
    {
        Type = type;
        Revision = revision;
        Ticket = lastModified;
    }
    
    public KeyValueResponse(KeyValueResponseType type, HLCTimestamp ticket)
    {
        Type = type;
        Ticket = ticket;
    }
    
    public KeyValueResponse(KeyValueResponseType type, ReadOnlyKeyValueEntry? entry)
    {
        Type = type;
        Entry = entry;
    }
    
    public KeyValueResponse(KeyValueResponseType type, List<(string, ReadOnlyKeyValueEntry)> items)
    {
        Type = type;
        Items = items;
    }

    public KeyValueResponse(KeyValueResponseType type, KeyValueGetByRangeResult rangeResult)
    {
        Type = type;
        RangeResult = rangeResult;
    }

    public static KeyValueResponse ForRangeLocks(List<KeyValueRangeLock> locks) =>
        new(KeyValueResponseType.RangeLocks) { RangeLockList = locks };

    /// <summary>
    /// The serialized committed proposal produced by a batched prepare stage
    /// (<see cref="Kahuna.Shared.KeyValue.KeyValueRequestType.StagePrepareMutations"/>). Non-null only on a
    /// Prepared stage response that has a persistent mutation to replicate; null when the key prepared with
    /// nothing to propose (ephemeral, an Undefined read, or a not-joined single node). The manager batches
    /// every non-null proposal for a partition into one <c>ReplicateLogs</c>.
    /// </summary>
    public byte[]? StagedProposal { get; private set; }

    /// <summary>
    /// Partition the staged proposal resolves to as the participant actor itself sees it. The manager
    /// groups by partition before staging; this lets it batch — and assert grouping did not drift — by the
    /// authoritative value rather than its own pre-stage guess.
    /// </summary>
    public int StagedPartitionId { get; private set; }

    /// <summary>
    /// Proposal id of a staged non-transactional set, correlating the staged write intent with the later
    /// <see cref="Kahuna.Shared.KeyValue.KeyValueRequestType.CompleteProposal"/> /
    /// <see cref="Kahuna.Shared.KeyValue.KeyValueRequestType.ReleaseProposal"/> that applies or releases it
    /// once the manager has proposed the partition batch. Zero on a 2PC prepare stage, which uses a Raft
    /// ticket rather than a proposal id.
    /// </summary>
    public int StagedProposalId { get; private set; }

    /// <summary>
    /// A staged prepare outcome for the batched path: Prepared, carrying an optional serialized proposal and
    /// the partition it routes to. A null proposal means the key prepared with nothing to replicate.
    /// </summary>
    public static KeyValueResponse Staged(byte[]? stagedProposal, int stagedPartitionId) =>
        new(KeyValueResponseType.Prepared) { StagedProposal = stagedProposal, StagedPartitionId = stagedPartitionId };

    /// <summary>
    /// A staged non-transactional set for the batched write path: Prepared, carrying the serialized proposal,
    /// the partition it routes to, and the proposal id that its <c>CompleteProposal</c>/<c>ReleaseProposal</c>
    /// must reference. The write intent is already installed on the entry; the manager proposes the partition
    /// batch, then completes (on success) or releases (on failure) each staged key by this id.
    /// </summary>
    public static KeyValueResponse StagedSet(byte[] stagedProposal, int stagedPartitionId, int stagedProposalId) =>
        new(KeyValueResponseType.Prepared)
        {
            StagedProposal = stagedProposal,
            StagedPartitionId = stagedPartitionId,
            StagedProposalId = stagedProposalId
        };
}