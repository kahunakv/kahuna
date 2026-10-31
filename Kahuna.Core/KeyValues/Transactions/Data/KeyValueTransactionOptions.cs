
using Kommander.Time;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues.Transactions.Data;

/// <summary>
/// Represents configuration options for an interactive key-value transaction.
/// </summary>
public sealed class KeyValueTransactionOptions
{
    /// <summary>
    /// A stable key that pins this transaction to the partition whose leader owns the coordinator session.
    /// The client generates it once (typically a GUID) and reuses it for the lifetime of the transaction
    /// so that start, commit, and rollback are all routed to the same node.
    /// </summary>
    public string CoordinatorKey { get; set; } = string.Empty;

    /// <summary>
    /// The data partition whose leader hosts the coordinator session, for a caller whose data is not placed by
    /// the hash rule (a key-range table lives on its range descriptor's partition). Null keeps the hash rule on
    /// <see cref="CoordinatorKey"/>. When set, the start refuses an id that names no data partition, and the
    /// returned handle carries a pinned coordinator key (see <see cref="CoordinatorKeys"/>) that routes commit,
    /// rollback and every operation registration to this partition. The partition is fixed for the life of the
    /// session: a later split or move of the caller's range does not move it. Set it only once every node in
    /// the cluster reads pinned keys: a node that predates them refuses the session's operation registrations.
    /// </summary>
    public int? CoordinatorPartitionId { get; set; }

    /// <summary>
    /// Timeout in milliseconds for the transaction.
    /// </summary>
    public int Timeout { get; set; }

    /// <summary>
    /// Milliseconds this caller is willing to queue for an admission slot when the node is at its session
    /// ceiling. Deliberately separate from <see cref="Timeout"/>: that is the session's lifetime — the window
    /// the reaper enforces — whereas this is only how long the caller waits at the door, and a transaction
    /// that intends to live a long time is not thereby willing to wait a long time to start.
    /// A value &lt;= 0 means "use the server default"; the server also clamps this to its own maximum, so a
    /// caller cannot occupy a queue slot for longer than the operator permits.
    /// </summary>
    public int AdmissionWaitMs { get; set; }

    /// <summary>
    /// Specifies the locking strategy to be used for key-value transactions.
    /// </summary>
    public KeyValueTransactionLocking Locking { get; set; } = KeyValueTransactionLocking.Pessimistic;

    /// <summary>
    /// Whether the locks should be released asynchronously.
    /// </summary>
    public bool AsyncRelease { get; set; }

    /// <summary>
    /// Whether the transaction should be automatically committed after all operations are completed.
    /// </summary>
    public bool AutoCommit { get; set; }

    /// <summary>
    /// Controls whether reads are tracked and validated for write-skew at commit time.
    /// </summary>
    public ReadValidation ReadValidation { get; set; } = ReadValidation.None;

    /// <summary>
    /// Controls how durable the coordinator decision record must be before the client receives the outcome.
    /// </summary>
    public DecisionDurability DecisionDurability { get; set; } = DecisionDurability.BestEffort;

    /// <summary>
    /// Transaction-wide snapshot timestamp for reads. Zero means "latest".
    /// </summary>
    public HLCTimestamp ReadTimestamp { get; set; }

    /// <summary>
    /// Relative importance of this transaction for admission ordering when the node is at its concurrency
    /// ceiling. Defaults to <see cref="TransactionPriority.Normal"/>, so a caller that never sets it competes
    /// exactly as it did before priorities existed.
    /// </summary>
    public TransactionPriority Priority { get; set; } = TransactionPriority.Normal;

    /// <summary>
    /// Whether this transaction yields its point-key write intents to foreground writers. Defaults to
    /// <see cref="TransactionConflictPolicy.Normal"/>, so a caller that never sets it behaves exactly as before
    /// the option existed. See <see cref="TransactionConflictPolicy.Yield"/> for the contract and its limits.
    /// </summary>
    public TransactionConflictPolicy ConflictPolicy { get; set; } = TransactionConflictPolicy.Normal;

    /// <summary>
    /// A short label naming what produced this transaction's writes (for example a schema rewrite or a cleanup
    /// sweep), carried on every prepared intent and on every watch event the transaction produces, so a change-feed
    /// consumer can tell maintenance writes from user writes. Null for an ordinary transaction. At most 64
    /// characters. Kept only on a cluster that enables the setting <c>TransactionOriginTags</c>.
    /// </summary>
    public string? Origin { get; set; }

    /// <summary>
    /// The most keys the session may write and the most locks it may take (see
    /// <see cref="TransactionContext.MutationLimit"/>). Set by a client entry point from its configuration, never from
    /// the client; zero (embedded callers, Kahuna's own transactions) means no limit.
    /// </summary>
    public int MutationLimit { get; set; }
}
