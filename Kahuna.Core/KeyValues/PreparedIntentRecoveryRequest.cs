namespace Kahuna.Server.KeyValues;

/// <summary>Message of <see cref="PreparedIntentRecoveryActor"/>: a periodic tick, or a pass over the intents a
/// partition's new leader inherited. Carries no partition — each pass reads the current state.</summary>
public sealed class PreparedIntentRecoveryRequest
{
    /// <summary>Runs only the settlement of inherited intents, ahead of the next tick.</summary>
    internal bool InheritedOnly { get; init; }

    /// <summary>The pass repeats one that left intents behind.</summary>
    internal bool IsRetry { get; init; }
}
