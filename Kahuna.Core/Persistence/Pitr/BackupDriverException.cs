
namespace Kahuna.Server.Persistence.Pitr;

/// <summary>
/// Thrown by <see cref="BackupDriver"/> when a backup operation cannot proceed —
/// for example, when the requested incremental range starts below the WAL compaction floor.
/// </summary>
internal sealed class BackupDriverException : Exception
{
    /// <summary>
    /// True when the incremental range starts below the WAL compaction floor and the caller
    /// should retry by taking a new full backup instead.
    /// </summary>
    public bool NeedsFullBackup { get; init; }

    public BackupDriverException(string message) : base(message) { }
}
