using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues.Transactions.Data;

/// <summary>What a release (a completion with a transient outcome) did to the record of its operation.</summary>
internal enum OperationCancelOutcome
{
    /// <summary>This node holds no session for the transaction.</summary>
    NoSession,

    /// <summary>The record is absent or already completed, so there was nothing to cancel.</summary>
    NotPending,

    /// <summary>The pending record was removed, and its id is free for a new registration.</summary>
    Cancelled,

    /// <summary>The record is pending, and the release waits for the session to stop taking new operations
    /// (<see cref="OperationCompletionPayload.ReleaseOnceClosed"/>). Nothing changed.</summary>
    Deferred
}

/// <summary>One pending operation of a session, for the log of a finalize that waits on it.</summary>
internal readonly record struct PendingOperationInfo(TransactionOperationId OperationId, OperationKind Kind, long AgeMs);
