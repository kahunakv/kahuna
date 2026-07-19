namespace Kahuna.Server.KeyValues;

/// <summary>Tick message for <see cref="PreparedIntentRecoveryActor"/>. Carries no payload — the sweep reads the
/// current due set each time.</summary>
public sealed class PreparedIntentRecoveryRequest
{
}
