
using Kommander.Time;
using Kahuna.Shared.Locks;

namespace Kahuna.Server.Locks.Data;

/// <summary>
/// Structured payload for an <c>InvalidateOrApply</c> actor message: the full committed lock state
/// from a replicated log entry, so the owning actor can bring a resident cache entry up to date.
/// The owner travels on the request's <c>Owner</c> field (null for an unlock, matching the state
/// <c>CompleteProposal</c> installs on the proposing leader).
/// </summary>
internal sealed record LockInvalidateOrApplyData(
    long FencingToken,
    HLCTimestamp Expires,
    HLCTimestamp LastUsed,
    HLCTimestamp LastModified,
    LockState State
);
