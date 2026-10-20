
namespace Kahuna.Shared.KeyValue;

/// <summary>
/// The answer to a transaction commit: the outcome type, the coordinator's canonical record anchor (null when
/// the transaction wrote nothing persistent), and — when the commit was refused — the refusal class and the
/// coordinator's reason. Deconstructs to the <c>(type, recordAnchorKey)</c> pair the commit answered before the
/// refusal class existed, so a caller that only reads the outcome keeps compiling unchanged.
///
/// <para><see cref="CommitTimestamp"/> is the transaction's commit timestamp on a commit: the timestamp every one of
/// its writes carries, and the one its watch events report. A change-feed consumer aligns a snapshot read with a
/// watch cursor by it. Zero when the commit wrote no persistent key through the durable path, when the answer was
/// read back from the transaction's record after the session was lost, or when the answering node predates the
/// field.</para>
/// </summary>
public readonly record struct TransactionCommitAnswer(
    KeyValueResponseType Type,
    string? RecordAnchorKey,
    CommitRefusalClass Refusal = CommitRefusalClass.None,
    string? Reason = null,
    Kommander.Time.HLCTimestamp CommitTimestamp = default)
{
    /// <summary>Deconstructs to the outcome type and the record anchor only.</summary>
    public void Deconstruct(out KeyValueResponseType type, out string? recordAnchorKey)
    {
        type = Type;
        recordAnchorKey = RecordAnchorKey;
    }

    /// <summary>An answer that carries no refusal: a commit, or a transport-level retry with the caller's own anchor.</summary>
    public static TransactionCommitAnswer Of(KeyValueResponseType type, string? recordAnchorKey) => new(type, recordAnchorKey);
}
