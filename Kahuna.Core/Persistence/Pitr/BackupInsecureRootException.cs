
namespace Kahuna.Server.Persistence.Pitr;

/// <summary>
/// Thrown when a backup or restore root is unsafe to write into — a symlink/reparse point, or (on POSIX)
/// group- or world-writable — so backups are refused before any artifact is written rather than being
/// placed where another user could read or tamper with them.
/// </summary>
internal sealed class BackupInsecureRootException : Exception
{
    public BackupInsecureRootException(string message) : base(message) { }
}
