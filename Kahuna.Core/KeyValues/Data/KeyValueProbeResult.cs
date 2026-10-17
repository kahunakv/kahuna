using Kommander.Time;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues;

/// <summary>
/// The answer for one key of a batched probe, at the key's position in the request.
/// <para><see cref="Type"/> and <see cref="Entry"/> mean exactly what a single <c>TryGet</c> of the key answers.
/// When the probe also takes a Shared lock per key, a key whose lock was refused answers the lock's refusal
/// instead (<see cref="KeyValueResponseType.AlreadyLocked"/> or <see cref="KeyValueResponseType.WaitingForReplication"/>)
/// with the holder in <see cref="HolderTransactionId"/>, and holds no lock.</para>
/// <para><see cref="Locked"/> is true when the key holds this transaction's Shared <c>[k,k]</c> lock after the probe,
/// whatever its read answered. It is always false for a probe without locks.</para>
/// </summary>
public readonly record struct KeyValueProbeResult(
    KeyValueResponseType Type,
    ReadOnlyKeyValueEntry? Entry,
    HLCTimestamp HolderTransactionId,
    bool Locked)
{
    /// <summary>A key the probe did not answer: retry it.</summary>
    public static KeyValueProbeResult MustRetry => new(KeyValueResponseType.MustRetry, null, HLCTimestamp.Zero, false);
}
