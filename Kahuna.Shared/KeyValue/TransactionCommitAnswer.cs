
namespace Kahuna.Shared.KeyValue;

/// <summary>
/// The answer to a transaction commit: the outcome type, the coordinator's canonical record anchor (null when
/// the transaction wrote nothing persistent), and — when the commit was refused — the refusal class and the
/// coordinator's reason. Deconstructs to the <c>(type, recordAnchorKey)</c> pair the commit answered before the
/// refusal class existed, so a caller that only reads the outcome keeps compiling unchanged.
///
/// <para><see cref="CommitTimestamp"/> is the transaction's commit timestamp on a commit: the timestamp every one of
/// its writes carries, and the one its watch events report. A change-feed consumer aligns a snapshot read with a
/// watch cursor by it. A commit answered from the transaction's record after the session was lost carries the
/// timestamp frozen in that record. Zero when the commit wrote no persistent key through the durable path, or when
/// the answering node predates the field.</para>
///
/// <para>Order between two transactions:</para>
/// <list type="bullet">
///   <item>Write-write on one key: each mutation of a key has a later commit timestamp than the one before it.</item>
///   <item>Write-read on any partitions: a transaction that read a committed value through a read registered with
///   its coordinator, or through a script statement, commits above that value's commit timestamp. A latest read
///   that finds a deleted key and a read without a coordinator key give no such bound.</item>
///   <item>Read-write anti-dependency: no order. A transaction can commit above a later transaction that
///   overwrote a key it read.</item>
/// </list>
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
