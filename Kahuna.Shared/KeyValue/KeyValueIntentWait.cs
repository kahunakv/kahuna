namespace Kahuna.Shared.KeyValue;

/// <summary>
/// Lets the first transactional latest read of a key wait for a live write intent of another transaction instead of
/// pinning the committed head below it.
///
/// <para>Without the wait, such a read pins the revision the other transaction is about to replace, so a later write
/// of the key by the reading transaction can commit only if the other transaction rolls back. With the wait, the
/// read parks on the server and runs again when the intent goes away (the holder commits, rolls back, or its lease
/// ends), and then pins the head it finds. No client poll is involved.</para>
///
/// <para>The wait applies only to a read with a transaction and no read timestamp, of a key the transaction has not
/// read or written before. Every case below answers exactly as a read without the wait does:</para>
/// <list type="bullet">
///   <item>the wait reaches <see cref="LimitMs"/> (the server also caps it);</item>
///   <item>too many reads already wait on the key or on the server's share of keys;</item>
///   <item><see cref="HoldsLocks"/> is true and the reading transaction is younger than the holder (wait-die: a
///   reader that holds a write intent or a lock could be waited on in turn, so the younger one does not wait).</item>
/// </list>
/// </summary>
/// <param name="LimitMs">Most milliseconds the read waits. Zero or less turns the wait off.</param>
/// <param name="HoldsLocks">Whether the reading transaction already holds a write intent or a lock anywhere. The
/// server cannot see the transaction's other keys, so the caller states it. A wrong value costs only performance:
/// true when false gives up a wait the reader could take; false when true can make two transactions wait on each
/// other until the limit.</param>
public readonly record struct KeyValueIntentWait(int LimitMs, bool HoldsLocks)
{
    /// <summary>No wait: the read behaves as it always did.</summary>
    public static KeyValueIntentWait None => default;

    /// <summary>True when the read may wait.</summary>
    public bool IsEnabled => LimitMs > 0;
}
