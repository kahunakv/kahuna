
using System.Text.Json;

namespace Kahuna.Server.Persistence.Pitr;

/// <summary>
/// Stores backup manifests as JSON files in a local directory.
/// Each manifest is written to <c>{directory}/{backupId}.manifest</c>.
/// </summary>
internal sealed class LocalDirectoryStorageTarget : IBackupStorageTarget
{
    private const string Extension = ".manifest";

    private readonly string _directory;

    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        WriteIndented = true
    };

    public LocalDirectoryStorageTarget(string directory)
    {
        _directory = directory;
        Directory.CreateDirectory(directory);
    }

    public void Put(BackupManifest manifest)
    {
        string path = ManifestPath(manifest.BackupId);
        string tmp = path + ".tmp_" + Guid.NewGuid().ToString("N")[..8];
        File.WriteAllText(tmp, JsonSerializer.Serialize(manifest, JsonOptions));
        File.Move(tmp, path, overwrite: true);
    }

    public BackupManifest? Get(Guid backupId)
    {
        string path = ManifestPath(backupId);
        if (!File.Exists(path))
            return null;

        return JsonSerializer.Deserialize<BackupManifest>(File.ReadAllText(path), JsonOptions);
    }

    public IReadOnlyList<BackupManifest> List(CancellationToken ct = default)
    {
        List<BackupManifest> results = [];

        foreach (string file in Directory.GetFiles(_directory, "*" + Extension))
        {
            ct.ThrowIfCancellationRequested();
            try
            {
                BackupManifest? m = JsonSerializer.Deserialize<BackupManifest>(File.ReadAllText(file), JsonOptions);
                if (m is not null)
                    results.Add(m);
            }
            catch (JsonException)
            {
                // Corrupt/partially-written manifests are not returned here so one bad file does not
                // blind the entire listing, but they are NOT silently dropped: ListCorrupt() reports
                // them so callers can present them as invalid entries.
            }
        }

        return results;
    }

    public IReadOnlyList<(Guid backupId, string reason)> ListCorrupt(CancellationToken ct = default)
    {
        List<(Guid, string)> corrupt = [];

        foreach (string file in Directory.GetFiles(_directory, "*" + Extension))
        {
            ct.ThrowIfCancellationRequested();
            try
            {
                BackupManifest? m = JsonSerializer.Deserialize<BackupManifest>(File.ReadAllText(file), JsonOptions);
                if (m is null)
                    corrupt.Add((ParseId(file), "Manifest deserialized to null."));
            }
            catch (JsonException ex)
            {
                corrupt.Add((ParseId(file), $"Manifest is not valid JSON: {ex.Message}"));
            }
            catch (IOException ex)
            {
                corrupt.Add((ParseId(file), $"Manifest could not be read: {ex.Message}"));
            }
        }

        return corrupt;
    }

    /// <summary>
    /// Recovers the backup id from a manifest filename ({id:N}.manifest). Returns
    /// <see cref="Guid.Empty"/> when the filename is not a recognizable id.
    /// </summary>
    private static Guid ParseId(string filePath)
    {
        string name = Path.GetFileNameWithoutExtension(filePath);
        return Guid.TryParseExact(name, "N", out Guid id) ? id : Guid.Empty;
    }

    private string ManifestPath(Guid id) =>
        Path.Combine(_directory, id.ToString("N") + Extension);
}
