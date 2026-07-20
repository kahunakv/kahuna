
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
    // When true, insert the value into the cache even if the key is not resident, instead of the ordinary no-op.
    // Used by durable-intent resolution on the leader: the committed value is not otherwise made resident there
    // (the leader applies direct writes via CompleteProposal, not this path), so a read after the intent settles
    // would miss it. Followers keep the no-op (they load from disk on the next read).
    bool ForceResident = false
);
