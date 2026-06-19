
using Kommander.Time;

namespace Kahuna.Server.Persistence.Pitr;

/// <summary>
/// Returned by <see cref="Backend.IPersistenceBackend.CreateCheckpoint"/> on success.
/// Callers use <see cref="Manifest"/> to record which WAL index the image corresponds to
/// so the restore path knows where to start replaying.
/// </summary>
internal sealed record CheckpointResult(
    string CheckpointPath,
    CheckpointManifest Manifest)
{
    public long AppliedIndex => Manifest.AppliedIndex;
    public HLCTimestamp AppliedTime => Manifest.AppliedTime;
}
