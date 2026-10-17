using Kommander.Time;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues;

/// <summary>
/// The answer for one bounded seek of a batched range probe, at the seek's position in the request.
/// <para><see cref="Type"/>, <see cref="Items"/> and <see cref="HasMore"/> mean exactly what a single range page over
/// the same bounds and limit answers. When <see cref="HasMore"/> is true, the seek's remaining entries start after
/// <see cref="ResumeKey"/>: the caller reads them with a range scan from that key, exclusive. A null
/// <see cref="ResumeKey"/> with <see cref="HasMore"/> means the rest starts at the seek's own start bound.</para>
/// <para>When the probe also takes a Shared range lock per seek, a seek whose lock was refused answers the lock's
/// refusal instead (<see cref="KeyValueResponseType.AlreadyLocked"/> or
/// <see cref="KeyValueResponseType.WaitingForReplication"/>) with the holder in <see cref="HolderTransactionId"/>, and
/// holds no lock. <see cref="Locked"/> is true when the seek's bounds hold this transaction's Shared range lock after
/// the probe, whatever its read answered.</para>
/// </summary>
public readonly record struct KeyValueProbeRangeResult(
    KeyValueResponseType Type,
    List<(string, ReadOnlyKeyValueEntry)> Items,
    bool HasMore,
    string? ResumeKey,
    HLCTimestamp HolderTransactionId,
    bool Locked)
{
    /// <summary>A seek the probe did not answer: retry it.</summary>
    public static KeyValueProbeRangeResult MustRetry => new(KeyValueResponseType.MustRetry, [], false, null, HLCTimestamp.Zero, false);
}
