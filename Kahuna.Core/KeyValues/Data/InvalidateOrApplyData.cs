
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace Kahuna.Server.KeyValues;

/// <summary>
/// Structured payload for an <c>InvalidateOrApply</c> actor message. Keeping these fields in a
/// dedicated record removes the need to pack them into general-purpose <see cref="KeyValueRequest"/>
/// fields (CompareRevision, TransactionId, etc.) and makes the mapping explicit at the call site.
/// </summary>
internal sealed record InvalidateOrApplyData(
    long         Revision,
    byte[]?      Value,
    HLCTimestamp Expires,
    HLCTimestamp LastUsed,
    HLCTimestamp LastModified,
    KeyValueState State,
    // When true this is a durable-intent resolution commit-apply on the leader, not the ordinary follower-style
    // cache coherence. The leader applies direct writes via CompleteProposal, and a durable transaction leaves the
    // key resident with a live write intent + MVCC snapshot that the ordinary path deliberately does not touch — so
    // without this the committed value never lands on the leader. A commit-apply clears the committing
    // transaction's write intent and MVCC snapshot, applies the value to the base entry, and persists it,
    // inserting the entry when the key is not resident.
    bool ForceResident = false,
    // The committing transaction, to identify and clear its write intent/MVCC on a commit-apply.
    HLCTimestamp TransactionId = default,
    int PartitionId = 0,
    bool NoRevision = false,
    // When true (with ForceResident), this is a durable ABORT cleanup rather than a commit-apply: clear the
    // transaction's staged write intent and MVCC snapshot without applying any value (the durable analog of
    // ApplyConfirmedRollback), so an aborted transaction does not leave the key blocked until the intent expires.
    bool IsRollback = false
);
