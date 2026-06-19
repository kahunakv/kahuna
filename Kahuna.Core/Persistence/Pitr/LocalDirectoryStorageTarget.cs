
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

    public IReadOnlyList<BackupManifest> List()
    {
        List<BackupManifest> results = [];

        foreach (string file in Directory.GetFiles(_directory, "*" + Extension))
        {
            try
            {
                BackupManifest? m = JsonSerializer.Deserialize<BackupManifest>(File.ReadAllText(file), JsonOptions);
                if (m is not null)
                    results.Add(m);
            }
            catch (JsonException)
            {
                // Skip corrupt or partially-written manifests so one bad file does not
                // blind the entire listing. Callers that need the specific file (Get,
                // ResolveChain) will surface the error when they attempt to load it directly.
            }
        }

        return results;
    }

    private string ManifestPath(Guid id) =>
        Path.Combine(_directory, id.ToString("N") + Extension);
}
