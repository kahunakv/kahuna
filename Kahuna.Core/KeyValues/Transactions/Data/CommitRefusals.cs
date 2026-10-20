using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues.Transactions.Data;

/// <summary>Maps the canonical record's abort class to the refusal class a commit answer carries.</summary>
internal static class CommitRefusals
{
    /// <summary>
    /// The refusal class for an abort read back from the canonical record. The record keeps only the abort
    /// class, so a class that several arms share maps to the arm that most often records it: a retryable-failure
    /// abort is the prepare loop giving up, and a lost exclusion is a lost lock (the coordinator's own result
    /// names a lost staging more precisely when this attempt detected it).
    /// </summary>
    internal static CommitRefusalClass FromAbortClass(TransactionAbortClass abortClass) => abortClass switch
    {
        TransactionAbortClass.Conflict => CommitRefusalClass.Conflict,
        TransactionAbortClass.RetryableFailure => CommitRefusalClass.PrepareExhausted,
        TransactionAbortClass.ExplicitRollback => CommitRefusalClass.ExplicitRollback,
        TransactionAbortClass.PresumedAbort => CommitRefusalClass.PresumedAbort,
        TransactionAbortClass.LostExclusion => CommitRefusalClass.LostLock,
        _ => CommitRefusalClass.None
    };
}
