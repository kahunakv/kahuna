
namespace Kahuna.Server.Persistence.Backend;

/// <summary>
/// Thrown by a backend's <c>CreateCheckpointAsOf</c> when it cannot produce an exact as-of image —
/// specifically when a historyless key (written with <c>SetNoRevision</c>) has a value newer than
/// the requested cut, so its state as-of the cut cannot be reconstructed (no retained revision to
/// roll back to, and an overwrite is indistinguishable from a brand-new key). The backup fails
/// closed rather than emitting an image that silently drops or over-includes such a key.
/// </summary>
internal sealed class ExactCheckpointUnavailableException : Exception
{
    public ExactCheckpointUnavailableException(string message) : base(message) { }
}
