
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
    KeyValueState State
);
