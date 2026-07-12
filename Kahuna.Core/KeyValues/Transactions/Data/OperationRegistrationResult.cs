
namespace Kahuna.Server.KeyValues.Transactions.Data;

/// <summary>
/// Distinguishes the outcomes a <see cref="TransactionContext.BeginOperation"/> call can return.
/// </summary>
public enum OperationRegistrationOutcome
{
    /// <summary>The operation was freshly registered; the caller should proceed to execute it.</summary>
    New,

    /// <summary>An earlier in-flight copy of the same operation is still running; the caller should wait.</summary>
    AlreadyPending,

    /// <summary>A previous execution of the same operation already succeeded; use the cached response.</summary>
    AlreadyCompleted,

    /// <summary>The session is no longer accepting new operations.</summary>
    RejectedSessionClosed,

    /// <summary>An operation with the same ID but a conflicting declaration (kind/payload) was received.</summary>
    RejectedDuplicate,

    /// <summary>The session already holds the maximum number of pending operations.</summary>
    RejectedCapacity
}

/// <summary>
/// The result returned by <see cref="TransactionContext.BeginOperation"/>.
/// </summary>
internal readonly struct OperationRegistrationResult(
    OperationRegistrationOutcome outcome,
    object? cachedResponse = null,
    string? recordAnchorKey = null)
{
    public OperationRegistrationOutcome Outcome { get; } = outcome;

    /// <summary>Non-null only when <see cref="Outcome"/> is <see cref="OperationRegistrationOutcome.AlreadyCompleted"/>.</summary>
    public object? CachedResponse { get; } = cachedResponse;

    /// <summary>
    /// The transaction's record anchor as known at registration time, or null if no persistent write has
    /// been confirmed yet. Carried on every outcome so a retry that hits an already-completed operation
    /// recovers the same canonical anchor without reapplying the participant operation.
    /// </summary>
    public string? RecordAnchorKey { get; } = recordAnchorKey;
}
