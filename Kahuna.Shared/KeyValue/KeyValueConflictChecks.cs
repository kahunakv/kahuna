
namespace Kahuna.Shared.KeyValue;

/// <summary>
/// Which classes of concurrent-transaction conflict the commit-time probe must answer for one key.
/// Combinable, and selected per key rather than per call: a transaction's read set and its write set need
/// different questions asked of the same batched probe.
/// </summary>
[Flags]
public enum KeyValueConflictChecks
{
    /// <summary>Ask nothing — the probe answers "no conflict" for the key.</summary>
    None = 0,

    /// <summary>
    /// A live write intent from another transaction: the in-memory intent placed while a peer stages its
    /// writes, or an undecided durable prepared intent (which survives leader change). This is the
    /// write-skew guard applied to a transaction's read set.
    /// </summary>
    WriteIntent = 1 << 0,

    /// <summary>
    /// A foreign range lock covering the key, exclusive or shared — a write needs exclusive on
    /// <c>[K,K]</c>, which is incompatible with both modes. This is the decide-time fence applied to a
    /// transaction's write set, catching a range lock acquired after the write was staged and therefore
    /// invisible to the write-time fence.
    /// </summary>
    ForeignRangeLock = 1 << 1,

    /// <summary>
    /// The key's committed head no longer matches the base the probe carries
    /// (<c>KeyValueConflictProbe.BaseRevision</c>). This is the post-prepare staged-base fence for a
    /// read-modify-write key: the pre-propose staged-base compare-and-set leaves a window between its
    /// probe and the prepare landing in which a competitor can commit the same base — under a paused
    /// coordinator whose in-memory write-intent lease lapsed, that admitted a silent lost update. Asked
    /// after the transaction's own durable prepared intents are live on every written key, the answer is
    /// stable: single-live-intent excludes any later competing commit until this transaction settles.
    /// Answered with <see cref="KeyValueResponseType.NotSet"/> (compare failed) rather than Aborted, so
    /// the caller can attribute the conflict precisely.
    /// </summary>
    StagedBase = 1 << 2
}
