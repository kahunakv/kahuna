namespace Kahuna.Server.KeyValues.Transactions.Data;

/// <summary>
/// The verdict the prepared-intent store gives a one-phase bundled commit at its apply position, consumed by
/// the transaction-record store's bundled commit gate. Every input behind it — the live intent set, the
/// committed-head ledger and its watermark — is a function of the partition's applied log prefix, so every
/// replica reaches the same verdict for the same log entry.
/// </summary>
internal enum BundledCommitVerdict
{
    /// <summary>The bundle's own prepare holds every bundled key and every checked base and read still holds.</summary>
    Admit,

    /// <summary>A bundled key is not held by this transaction's live intent: its prepare, earlier in the same
    /// atomic batch, was rejected (another transaction owns the key).</summary>
    PrepareMissing,

    /// <summary>A co-bundled intent's validated base was moved past by a settled commit, or the transaction is
    /// older than the ledger's retention horizon so its base cannot be verified.</summary>
    StaleBase,

    /// <summary>A read-only dependency is held by a foreign undecided or committed intent, or the ledger's head
    /// for it moved past the observed state.</summary>
    StaleRead
}

/// <summary>A bundled commit verdict with the reason a rejection was given (null on admit).</summary>
internal readonly record struct BundledCommitJudgement(BundledCommitVerdict Verdict, string? Reason)
{
    public static readonly BundledCommitJudgement Admitted = new(BundledCommitVerdict.Admit, null);

    public bool IsAdmit => Verdict == BundledCommitVerdict.Admit;
}
