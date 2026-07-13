
using Kommander.Time;

using Kahuna.Server.KeyValues.Transactions.Data;

namespace Kahuna.Server.KeyValues;

internal sealed class KeyValueWriteIntent
{
    public HLCTimestamp TransactionId { get; set; }

    public HLCTimestamp Expires { get; set; }

    /// <summary>
    /// The timestamp the committed revision will carry (= mvccEntry.LastModified stamped at write time).
    /// Zero means this is a plain per-key lock or a not-yet-prepared intent — commit ts is undetermined.
    /// Non-Zero means the intent has been prepared via 2PC and the pending commit ts is known.
    /// </summary>
    public HLCTimestamp CommitTimestamp { get; set; }

    /// <summary>
    /// The transaction's canonical record anchor (its first confirmed persistent modified key), supplied by
    /// the coordinator at prepare. Null for a plain per-key lock or a transaction with no persistent write.
    /// Participant-side metadata that a durable completion receipt is later keyed by.
    /// </summary>
    public string? RecordAnchorKey { get; set; }

    /// <summary>
    /// The initial durable coordinator decision to install when this intent's mutation commits. Set only
    /// on the anchor key's write intent for a Durable transaction (the coordinator prepares the anchor last,
    /// once every non-anchor ticket is known). The leader reads it here to install the record inline as the
    /// anchor commit applies; followers and restore read the equivalent serialized copy from the committed
    /// key-value envelope. Null on every non-anchor intent and on all best-effort transactions.
    /// </summary>
    public CoordinatorDecisionRecord? EmbeddedDecision { get; set; }
}