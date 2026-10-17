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
/// </summary>
internal readonly record struct PointReadStep(KeyValueResponse? Response, PointReadDetach Detach, long AsOfCeiling)
{
    public static implicit operator PointReadStep(KeyValueResponse response) => new(response, PointReadDetach.None, -1);

    public static PointReadStep Detached(PointReadDetach detach, long asOfCeiling) => new(null, detach, asOfCeiling);
}
