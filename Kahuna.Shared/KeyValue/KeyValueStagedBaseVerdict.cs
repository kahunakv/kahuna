
namespace Kahuna.Shared.KeyValue;

/// <summary>
/// One replica's staged-base fence verdict for one validated-base prepare, evaluated against the replica's own
/// applied intent and committed-head memory. Heads record only settled commits, in the same log order the
/// prepare applied in, so at the position where the replica holds the transaction's still-pending intent the
/// verdict is deterministic: a replica can be behind (it answers <see cref="NotApplied"/>) but never wrongly
/// ahead, and a <see cref="StaleBase"/> answer proves a competitor committed the base after validation.
/// </summary>
public enum KeyValueStagedBaseVerdict
{
    /// <summary>The replica does not hold the transaction's prepared intent for the key. It has not applied
    /// the prepare yet, or the intent already settled and was removed. The replica cannot judge the base, and
    /// absence of a verdict is never an objection.</summary>
    NotApplied = 0,

    /// <summary>The replica holds the intent and its fence memory does not prove the validated base moved.
    /// This includes a base at the remembered head, a head behind the base, no remembered head, and an
    /// already-resolved intent (the canonical record owns the outcome from there).</summary>
    Clear = 1,

    /// <summary>The replica holds the still-pending intent and its fence memory proves the validated base
    /// moved: a competitor's commit settled on the key after this transaction validated its base. Committing
    /// would silently discard that competitor's write.</summary>
    StaleBase = 2
}

/// <summary>A replica's verdict for one key, with the committed-head revision the verdict was judged against
/// (-1 when the fence memory held nothing for the key).</summary>
public readonly record struct KeyValueStagedBaseVerdictEntry(KeyValueStagedBaseVerdict Verdict, long HeadRevision);
