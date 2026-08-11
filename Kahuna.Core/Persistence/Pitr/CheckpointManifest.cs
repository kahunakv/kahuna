
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

    /// <summary>
    /// Reads the sidecar out of an artifact store rather than a local directory, for verifying a backup
    /// whose bytes may not be on this filesystem at all.
    /// </summary>
    public static async Task<CheckpointManifest> ReadFromStoreAsync(
        IBackupArtifactStore store, Guid backupId, CancellationToken ct = default)
    {
        string key = LocalDirectoryArtifactStore.CheckpointDirectoryName + "/" + FileName;
        await using Stream stream = await store.OpenReadAsync(backupId, key, ct: ct).ConfigureAwait(false);
        return await JsonSerializer.DeserializeAsync<CheckpointManifest>(stream, cancellationToken: ct)
                   .ConfigureAwait(false)
               ?? throw new InvalidDataException($"Empty checkpoint manifest for backup {backupId:N}");
    }
}
