
using System.Text.Json;

namespace Kahuna.Server.Persistence.Pitr;

/// <summary>
/// Stores backup manifests as JSON files in a local directory.
/// Each manifest is written to <c>{directory}/{backupId}.manifest</c>.
/// <para>
/// Public because it is the reference implementation of <see cref="IBackupStorageTarget"/> and the
/// default an embedding host can select explicitly.
/// </para>
/// </summary>
public sealed class LocalDirectoryStorageTarget : IBackupStorageTarget
{
    private const string Extension = ".manifest";

    private readonly string _directory;

    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        WriteIndented = true
    };

    public LocalDirectoryStorageTarget(string directory)
    {
        // Refuse an unsafe root before creating or touching anything — see the same check in
        // LocalDirectoryArtifactStore: creating the directory would tighten its mode and mask the problem.
        BackupFilePermissions.EnsureRootSecure(directory);
        _directory = directory;
        // Owner-only (0700 on POSIX): a manifest names every artifact and its digest, so it must not be
        // readable or writable by other users on the host.
        BackupFilePermissions.CreateDirectory(directory);
    }

    public async Task PutAsync(BackupManifest manifest, CancellationToken ct = default)
    {
        string path = ManifestPath(manifest.BackupId);
        string tmp = path + ".tmp_" + Guid.NewGuid().ToString("N")[..8];
        await File.WriteAllTextAsync(tmp, JsonSerializer.Serialize(manifest, JsonOptions), ct).ConfigureAwait(false);
        // Restrict before the atomic rename so the published manifest is owner-only (0600) from the
        // instant it appears at its final path — never briefly world-readable.
        BackupFilePermissions.RestrictFile(tmp);
        File.Move(tmp, path, overwrite: true);
    }

    public Task DeleteAsync(Guid backupId, CancellationToken ct = default)
    {
        string path = ManifestPath(backupId);
        // File.Delete is a no-op when the file is already absent, so this is idempotent.
        File.Delete(path);
        return Task.CompletedTask;
    }

    public async Task<BackupManifest?> GetAsync(Guid backupId, CancellationToken ct = default)
    {
        string path = ManifestPath(backupId);
        if (!File.Exists(path))
            return null;

        return JsonSerializer.Deserialize<BackupManifest>(
            await File.ReadAllTextAsync(path, ct).ConfigureAwait(false), JsonOptions);
    }

    public async Task<IReadOnlyList<BackupManifest>> ListAsync(CancellationToken ct = default)
    {
        List<BackupManifest> results = [];

        foreach (string file in Directory.GetFiles(_directory, "*" + Extension))
        {
            ct.ThrowIfCancellationRequested();
            try
            {
                BackupManifest? m = JsonSerializer.Deserialize<BackupManifest>(
                    await File.ReadAllTextAsync(file, ct).ConfigureAwait(false), JsonOptions);
                if (m is not null)
                    results.Add(m);
            }
            catch (JsonException)
            {
                // Corrupt/partially-written manifests are not returned here so one bad file does not
                // blind the entire listing, but they are NOT silently dropped: ListCorruptAsync()
                // reports them so callers can present them as invalid entries.
            }
        }

        return results;
    }

    public async Task<IReadOnlyList<(Guid backupId, string reason)>> ListCorruptAsync(CancellationToken ct = default)
    {
        List<(Guid, string)> corrupt = [];

        foreach (string file in Directory.GetFiles(_directory, "*" + Extension))
        {
            ct.ThrowIfCancellationRequested();
            try
            {
                BackupManifest? m = JsonSerializer.Deserialize<BackupManifest>(
                    await File.ReadAllTextAsync(file, ct).ConfigureAwait(false), JsonOptions);
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

    public Task<IReadOnlyList<Guid>> ListManifestIdsAsync(CancellationToken ct = default)
    {
        List<Guid> ids = [];

        // Filename-only scan: the id is recovered from {id:N}.manifest without opening the file, so a
        // corrupt, partially-written, or transiently unreadable manifest still yields its owning id and
        // its artifact directory stays protected from the orphan sweep. Non-id filenames (e.g. stray
        // *.manifest with a non-GUID name) are skipped — they own no {id:N} artifact directory.
        foreach (string file in Directory.GetFiles(_directory, "*" + Extension))
        {
            ct.ThrowIfCancellationRequested();
            Guid id = ParseId(file);
            if (id != Guid.Empty)
                ids.Add(id);
        }

        return Task.FromResult<IReadOnlyList<Guid>>(ids);
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
