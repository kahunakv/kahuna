namespace Kahuna.Shared.KeyValue;

/// <summary>
/// Relative importance of a transaction when the node has to choose which of several waiting
/// transactions to start next. Priority governs <b>admission order only</b>: it never preempts a
/// transaction that is already running, and it never changes 2PC, MVCC, locking, or commit semantics.
///
/// <para>The scale is a small fixed ordinal set rather than an arbitrary integer so the orderer needs a
/// bounded number of queues and aging stays cheap to reason about. <see cref="Normal"/> is the default at
/// every layer, so a caller that never sets a priority behaves exactly as before this existed.</para>
///
/// <para><see cref="Critical"/> is reserved for work that must not be deferred behind anything else — a
/// node's own maintenance transactions, or an operation whose delay would compromise availability.
/// Callers should not tag ordinary application traffic <see cref="Critical"/>: if everything is critical
/// the ordering carries no information.</para>
/// </summary>
public enum TransactionPriority
{
    /// <summary>Bulk, deferrable work (compaction sweeps, analytics). Yields to everything else.</summary>
    Background = 0,

    /// <summary>Lower than ordinary traffic but still latency-relevant.</summary>
    Low = 1,

    /// <summary>Default for every transaction that does not specify a priority.</summary>
    Normal = 2,

    /// <summary>Latency-critical application work that should start ahead of ordinary traffic.</summary>
    High = 3,

    /// <summary>Must not be starved. Never demoted, and never aged (it is already at the top).</summary>
    Critical = 4
}
