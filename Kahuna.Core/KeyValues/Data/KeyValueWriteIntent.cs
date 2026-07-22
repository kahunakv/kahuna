
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

}

/// <summary>
/// Defines the lease contract shared by point and predicate write intents. A zero-duration request
/// creates a session-owned intent with no clock deadline; it remains live until explicit transaction
/// cleanup releases it. Positive durations start at the actor's acquisition-time HLC, preventing a
/// long-running transaction from receiving a lock whose deadline predates the successful acquire.
/// Keeping this policy centralized prevents readers and writers from disagreeing about whether an
/// accepted lock is still active.
/// </summary>
internal static class KeyValueWriteIntentLease
{
    /// <summary>
    /// Converts a validated lock duration into its stored deadline. Callers must reject negative
    /// durations before invoking this method.
    /// </summary>
    internal static HLCTimestamp FromRequest(HLCTimestamp currentTime, int expiresMs) =>
        expiresMs == 0 ? HLCTimestamp.Zero : currentTime + expiresMs;

    /// <summary>
    /// Returns whether an intent still owns its lock. A zero deadline is session-owned rather than
    /// expired; only a positive deadline at or before the current HLC is stale.
    /// </summary>
    internal static bool IsLive(KeyValueWriteIntent intent, HLCTimestamp currentTime) =>
        intent.Expires == HLCTimestamp.Zero || intent.Expires - currentTime > TimeSpan.Zero;
}
