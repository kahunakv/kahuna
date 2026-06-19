
namespace Kahuna.Server.Persistence.Pitr;

/// <summary>
/// Thrown by <see cref="BackupDriver"/> when a backup operation cannot proceed —
/// for example, when the requested incremental range starts below the WAL compaction floor.
/// </summary>
internal sealed class BackupDriverException : Exception
{
    public BackupDriverException(string message) : base(message) { }
}
