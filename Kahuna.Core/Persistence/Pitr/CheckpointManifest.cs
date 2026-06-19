
using System.Text.Json;
using Kommander.Time;

namespace Kahuna.Server.Persistence.Pitr;

/// <summary>
/// Persisted metadata that accompanies every backend checkpoint: the WAL index and HLC
/// timestamp at which the snapshot was taken. Written as a small JSON sidecar file so
/// the restore path can locate the correct WAL replay start point without opening the
/// storage engine.
/// </summary>
internal sealed record CheckpointManifest(
    long AppliedIndex,
    int AppliedTimeNode,
    long AppliedTimePhysical,
    uint AppliedTimeCounter)
{
    public const string FileName = "checkpoint.manifest";

    public HLCTimestamp AppliedTime =>
        new(AppliedTimeNode, AppliedTimePhysical, AppliedTimeCounter);

    public static CheckpointManifest From(long appliedIndex, HLCTimestamp appliedTime) =>
        new(appliedIndex, appliedTime.N, appliedTime.L, appliedTime.C);

    public void WriteTo(string checkpointPath)
    {
        string file = Path.Combine(checkpointPath, FileName);
        File.WriteAllText(file, JsonSerializer.Serialize(this));
    }

    public static CheckpointManifest ReadFrom(string checkpointPath)
    {
        string file = Path.Combine(checkpointPath, FileName);
        string json = File.ReadAllText(file);
        return JsonSerializer.Deserialize<CheckpointManifest>(json)
               ?? throw new InvalidDataException($"Empty checkpoint manifest at {file}");
    }
}
