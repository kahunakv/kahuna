using System.Collections.Concurrent;
using System.Globalization;

namespace Kahuna.Server.KeyValues.Watch;

/// <summary>
/// The highest log index a whole-partition snapshot installed on this node, per partition. The entries at or below it
/// never applied here one by one: the install replaced the partition's state, so this node has no per-entry history
/// there even when WAL rows below it remain. A watch resume whose replay would start at or below it answers
/// <c>HistoryUnavailable</c>. Persisted on every install (a rare event), so a restart keeps it.
/// </summary>
internal sealed class PartitionInstallBoundaries
{
    private readonly ConcurrentDictionary<int, long> boundaries = new();

    private readonly string? path;

    private readonly object fileLock = new();

    private readonly ILogger<IKahuna> logger;

    public PartitionInstallBoundaries(string? storagePath, string? storageRevision, ILogger<IKahuna> logger)
    {
        this.logger = logger;
        path = string.IsNullOrEmpty(storagePath) ? null : Path.Combine(storagePath, $"installboundaries_{storageRevision}.txt");
        Load();
    }

    /// <summary>The install boundary of <paramref name="partitionId"/>, or zero when no snapshot was installed.</summary>
    public long Of(int partitionId) => boundaries.TryGetValue(partitionId, out long boundary) ? boundary : 0;

    /// <summary>Records that a snapshot installed <paramref name="partitionId"/> through <paramref name="upToIndex"/>.</summary>
    public void Record(int partitionId, long upToIndex)
    {
        boundaries.AddOrUpdate(partitionId, upToIndex, (_, current) => Math.Max(current, upToIndex));
        Persist();
    }

    private void Persist()
    {
        if (path is null)
            return;

        try
        {
            string text = string.Join('\n', boundaries.Select(b => string.Create(CultureInfo.InvariantCulture, $"{b.Key} {b.Value}")));

            lock (fileLock)
            {
                string tmp = path + ".tmp";
                File.WriteAllText(tmp, text);
                File.Move(tmp, path, overwrite: true);
            }
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to persist the partition install boundaries to {Path}", path);
        }
    }

    private void Load()
    {
        if (path is null || !File.Exists(path))
            return;

        try
        {
            string[] lines;
            lock (fileLock)
                lines = File.ReadAllLines(path);

            foreach (string line in lines)
            {
                string[] parts = line.Split(' ', StringSplitOptions.RemoveEmptyEntries);
                if (parts.Length == 2
                    && int.TryParse(parts[0], NumberStyles.Integer, CultureInfo.InvariantCulture, out int partitionId)
                    && long.TryParse(parts[1], NumberStyles.Integer, CultureInfo.InvariantCulture, out long boundary))
                    boundaries[partitionId] = boundary;
            }
        }
        catch (Exception ex)
        {
            // Without the boundary, only the replay's WAL gap check stands between a resume and a replay across an
            // install made before this restart.
            logger.LogError(ex, "Failed to load the partition install boundaries from {Path}", path);
        }
    }
}
