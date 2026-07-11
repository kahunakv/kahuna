
using Kommander.Time;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues.Transactions.Data;

/// <summary>
/// Generic transaction context holding identity, policy, lifecycle state, and confirmed working-set
/// entries. Contains no parser, AST, variable, or script-execution references.
/// </summary>
internal class TransactionContext
{
    /// <summary>
    /// HLC timestamp that uniquely identifies this transaction.
    /// </summary>
    public HLCTimestamp TransactionId { get; init; }

    /// <summary>
    /// Maximum duration in milliseconds before the transaction times out.
    /// </summary>
    public int Timeout { get; init; }

    /// <summary>
    /// Pessimistic or optimistic locking strategy for this transaction.
    /// </summary>
    public KeyValueTransactionLocking Locking { get; init; }

    /// <summary>
    /// Transaction-wide snapshot timestamp for reads. Zero means "latest".
    /// </summary>
    public HLCTimestamp ReadTimestamp { get; init; }

    /// <summary>
    /// Last result of the current key-value execution.
    /// </summary>
    public KeyValueTransactionResult? Result { get; set; }

    /// <summary>
    /// Last result of a key-value write operation.
    /// </summary>
    public KeyValueTransactionResult? ModifiedResult { get; set; }

    /// <summary>
    /// Whether the transaction should commit or abort.
    /// </summary>
    public KeyValueTransactionAction Action { get; set; }

    /// <summary>
    /// Whether transaction resources should be released asynchronously upon completion.
    /// </summary>
    public bool AsyncRelease { get; set; }

    /// <summary>
    /// Point locks acquired during execution.
    /// </summary>
    public HashSet<(string, KeyValueDurability)>? LocksAcquired { get; set; }

    /// <summary>
    /// Prefix locks acquired during execution.
    /// </summary>
    public HashSet<(string, KeyValueDurability)>? PrefixLocksAcquired { get; set; }

    /// <summary>
    /// Keys modified during the transaction along with their durability.
    /// </summary>
    public HashSet<(string, KeyValueDurability)>? ModifiedKeys { get; set; }

    /// <summary>
    /// Keys read during the transaction and their observed revisions.
    /// </summary>
    public Dictionary<(string, KeyValueDurability), KeyValueTransactionReadKey>? ReadKeys { get; set; }

    /// <summary>
    /// Internal 2PC state field; advanced atomically via <see cref="SetState"/>.
    /// </summary>
    private KeyValueTransactionState state = KeyValueTransactionState.Pending;

    /// <summary>
    /// Current 2PC state of the transaction.
    /// </summary>
    public KeyValueTransactionState State => state;

    /// <summary>
    /// Atomically advances the transaction state from <paramref name="expectedState"/> to
    /// <paramref name="newState"/>. Returns true when the CAS succeeds.
    /// </summary>
    public bool SetState(KeyValueTransactionState newState, KeyValueTransactionState expectedState)
    {
        return expectedState == Interlocked.CompareExchange(ref state, newState, expectedState);
    }
}
