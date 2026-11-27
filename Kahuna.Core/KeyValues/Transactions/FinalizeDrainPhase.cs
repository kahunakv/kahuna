namespace Kahuna.Server.KeyValues.Transactions;

/// <summary>The finalize call whose drain waits for the pending operations of a session.</summary>
internal enum FinalizeDrainPhase
{
    Commit,
    Rollback,
    Close
}

/// <summary>Names of <see cref="FinalizeDrainPhase"/> for logs and metric tags, without a per-call string.</summary>
internal static class FinalizeDrainPhases
{
    internal static string Name(FinalizeDrainPhase phase) => phase switch
    {
        FinalizeDrainPhase.Commit => "commit",
        FinalizeDrainPhase.Rollback => "rollback",
        _ => "close"
    };
}
