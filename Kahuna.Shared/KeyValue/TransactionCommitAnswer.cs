
namespace Kahuna.Shared.KeyValue;

/// <summary>
/// The answer to a transaction commit: the outcome type, the coordinator's canonical record anchor (null when
/// the transaction wrote nothing persistent), and — when the commit was refused — the refusal class and the
/// coordinator's reason. Deconstructs to the <c>(type, recordAnchorKey)</c> pair the commit answered before the
/// refusal class existed, so a caller that only reads the outcome keeps compiling unchanged.
/// </summary>
public readonly record struct TransactionCommitAnswer(
    KeyValueResponseType Type,
    string? RecordAnchorKey,
    CommitRefusalClass Refusal = CommitRefusalClass.None,
    string? Reason = null)
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
