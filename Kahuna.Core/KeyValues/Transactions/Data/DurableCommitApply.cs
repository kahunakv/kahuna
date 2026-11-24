
namespace Kahuna.Server.KeyValues.Transactions.Data;

/// <summary>
/// What the apply of a committed prepared intent on the actor that owns its key did.
/// </summary>
internal enum DurableCommitApply : byte
{
    /// <summary>The actor did not confirm the committed head: the apply was refused, failed, or must be retried.</summary>
    NotApplied,

    /// <summary>The committed head is in place on the actor. The apply found no write intent of the transaction
    /// on the key (the head was already in place, or the intent was gone), or it ran on another node.</summary>
    Applied,

    /// <summary>The committed head is in place, and the same actor turn on this node removed the transaction's own
    /// write intent and MVCC entry. The transaction holds nothing on the key on this node.</summary>
    AppliedAndReleased
}
