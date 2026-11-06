using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues.Handlers;

/// <summary>Which backend read stage 1 of a point read left to its caller.</summary>
internal enum PointReadDetach
{
    /// <summary>Stage 1 answered; nothing to read.</summary>
    None,

    /// <summary>A non-transactional latest read of a key that is not resident: load the head
    /// (<see cref="PointReadContinuation"/>).</summary>
    LatestMiss,

    /// <summary>A snapshot read of a key that is not resident: load the head with its recent history
    /// (<see cref="SnapshotReadContinuation"/>, hydrating).</summary>
    SnapshotHydrate,

    /// <summary>A snapshot read whose resident archive no longer holds the as-of revision: read the persisted
    /// history at or below <see cref="PointReadStep.AsOfCeiling"/> (<see cref="SnapshotReadContinuation"/>).</summary>
    SnapshotHistory,
}

/// <summary>
/// The outcome of stage 1 of a point read: the answer, or the backend read that must run before the key can be
/// answered (see <see cref="TryGetHandler.ResolvePointRead"/>).
/// <para>
/// An answer that serves a value is carried as the row alone (<see cref="Served"/>), with no response object:
/// a single read wraps it in its reply (<see cref="ToResponse"/>), and a batched probe copies it into the key's
/// slot of its result array, so a probe of many keys does not allocate a response per key. Every other answer
/// is a <see cref="Response"/>, almost always one of the shared static ones.
/// </para>
/// </summary>
internal readonly record struct PointReadStep(KeyValueResponse? Response, PointReadDetach Detach, long AsOfCeiling, ReadOnlyKeyValueEntry? Served = null)
{
    public static implicit operator PointReadStep(KeyValueResponse response) => new(response, PointReadDetach.None, -1);

    public static PointReadStep Detached(PointReadDetach detach, long asOfCeiling) => new(null, detach, asOfCeiling);

    /// <summary>Stage 1 answered with the live value <paramref name="entry"/>.</summary>
    public static PointReadStep Serve(ReadOnlyKeyValueEntry entry) => new(null, PointReadDetach.None, -1, entry);

    /// <summary>The answer as the response a single read replies with. Null when stage 1 did not answer.</summary>
    public KeyValueResponse? ToResponse() =>
        Served is not null ? new KeyValueResponse(KeyValueResponseType.Get, Served) : Response;
}
