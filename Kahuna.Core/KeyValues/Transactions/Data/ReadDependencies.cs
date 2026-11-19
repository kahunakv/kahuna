using Kommander.Time;

namespace Kahuna.Server.KeyValues.Transactions.Data;

/// <summary>
/// Builds the read dependency of a transactional read (<see cref="OperationCompletionPayload.ReadDependency"/>): the
/// highest of its read timestamp and the <c>LastModified</c> of every entry it served. A transaction minted above
/// it commits after every commit whose value it read, on any partition. A read that finds no value adds only its
/// read timestamp, so a latest read of a deleted key adds nothing.
/// </summary>
internal static class ReadDependencies
{
    /// <summary>The later of <paramref name="current"/> and <paramref name="candidate"/>.</summary>
    public static HLCTimestamp Max(HLCTimestamp current, HLCTimestamp candidate) =>
        candidate.CompareTo(current) > 0 ? candidate : current;

    /// <summary>The later of <paramref name="current"/> and the <c>LastModified</c> of <paramref name="served"/>.</summary>
    public static HLCTimestamp Max(HLCTimestamp current, ReadOnlyKeyValueEntry? served) =>
        served is null ? current : Max(current, served.LastModified);

    /// <summary>The read dependency of one point read at <paramref name="readTimestamp"/> that served
    /// <paramref name="served"/> (null when it served no entry).</summary>
    public static HLCTimestamp Of(HLCTimestamp readTimestamp, ReadOnlyKeyValueEntry? served) =>
        Max(readTimestamp, served);
}
