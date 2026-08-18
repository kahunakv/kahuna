
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
    ForeignRangeLock = 1 << 1
}
