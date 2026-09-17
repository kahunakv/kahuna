using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues;

/// <summary>
/// Ambient carrier for the conflict policy that a planting request (an exclusive lock, or a transactional
/// set, delete or extend) must record on the write intent it plants.
///
/// <para>The policy is authoritative from the coordinator: the routed operation that registered the request
/// with the session's coordinator enters this scope with the session's own policy before it dispatches the
/// planting call, so the intent records whether its owner yields from the coordinator's record and never from
/// a value a caller could forge. It flows in-process down the whole dispatch chain — locator, local facade,
/// the actor request builder — without a parameter on every signature. Across an inter-node hop it does not
/// travel by itself; the transport copies the current value onto the wire request, and the receiving node
/// re-enters the scope from that field before it serves the forwarded call.</para>
///
/// <para>Unset means <see cref="TransactionConflictPolicy.Normal"/>, so a plain write, a script transaction,
/// or any path that does not enter the scope plants a non-yielding intent exactly as before.</para>
/// </summary>
internal static class YieldingIntentPolicyScope
{
    private static readonly AsyncLocal<TransactionConflictPolicy> current = new();

    /// <summary>The policy the current dispatch is planting under; <see cref="TransactionConflictPolicy.Normal"/> when none is set.</summary>
    public static TransactionConflictPolicy Current => current.Value;

    /// <summary>
    /// Enters a scope whose planting requests record <paramref name="policy"/> on their intents. Returns a
    /// scope that restores the previous value when disposed by the same async flow.
    /// </summary>
    public static Scope Enter(TransactionConflictPolicy policy)
    {
        TransactionConflictPolicy previous = current.Value;
        current.Value = policy;
        return new Scope(previous);
    }

    public readonly struct Scope(TransactionConflictPolicy previous) : IDisposable
    {
        public void Dispose() => current.Value = previous;
    }
}
