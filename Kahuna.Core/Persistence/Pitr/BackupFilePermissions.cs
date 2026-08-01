
namespace Kahuna.Server.Persistence.Pitr;

/// <summary>
/// Restrictive filesystem permissions for backup artifacts and manifests. A node-wide backup contains
/// every tenant's data (including anything an application stored as a secret), so its files must not be
/// readable by other users on the host. On POSIX this creates directories as <c>0700</c> and files as
/// <c>0600</c> and refuses a backup root that is world/group-writable or a symlink. On Windows these are
/// no-ops: NTFS inheritance from an access-controlled parent is relied upon instead (documented), and the
/// symlink/reparse-point guards elsewhere still apply.
/// </summary>
internal static class BackupFilePermissions
{
    private const UnixFileMode DirMode = UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.UserExecute; // 0700
    private const UnixFileMode FileMode = UnixFileMode.UserRead | UnixFileMode.UserWrite; // 0600

    private const UnixFileMode GroupOrOtherWrite =
        UnixFileMode.GroupWrite | UnixFileMode.OtherWrite;

    private const UnixFileMode GroupOrOtherAny =
        UnixFileMode.GroupRead | UnixFileMode.GroupWrite | UnixFileMode.GroupExecute |
        UnixFileMode.OtherRead | UnixFileMode.OtherWrite | UnixFileMode.OtherExecute;

    /// <summary>Creates <paramref name="path"/> (and missing parents) and restricts it to owner-only on POSIX.</summary>
    public static void CreateDirectory(string path)
    {
        Directory.CreateDirectory(path);
        RestrictDirectory(path);
    }

    /// <summary>Restricts an existing directory to owner-only (0700) on POSIX; no-op on Windows.</summary>
    public static void RestrictDirectory(string path)
    {
        if (!OperatingSystem.IsWindows())
            File.SetUnixFileMode(path, DirMode);
    }

    /// <summary>Restricts an existing file to owner-only (0600) on POSIX; no-op on Windows.</summary>
    public static void RestrictFile(string path)
    {
        if (!OperatingSystem.IsWindows())
            File.SetUnixFileMode(path, FileMode);
    }

    /// <summary>
    /// Recursively restricts every directory (0700) and file (0600) under <paramref name="root"/> to
    /// owner-only on POSIX. Used to lock down a checkpoint or WAL-segment tree the backend created at the
    /// process umask before the backup is published. Does not follow reparse points (the caller has
    /// already rejected a symlinked artifact root).
    /// </summary>
    public static void RestrictTree(string root)
    {
        if (OperatingSystem.IsWindows() || !Directory.Exists(root))
            return;

        RestrictDirectory(root);
        foreach (string dir in Directory.EnumerateDirectories(root, "*", SearchOption.AllDirectories))
        {
            if (!IsReparsePoint(dir))
                File.SetUnixFileMode(dir, DirMode);
        }
        foreach (string file in Directory.EnumerateFiles(root, "*", SearchOption.AllDirectories))
        {
            if (!IsReparsePoint(file))
                File.SetUnixFileMode(file, FileMode);
        }
    }

    /// <summary>
    /// Verifies a backup/restore root is safe to write into: it must not be a symlink/reparse point and,
    /// on POSIX, must not be group- or world-writable (which would let another user tamper with the
    /// artifacts or their recorded digests). Throws <see cref="BackupInsecureRootException"/> otherwise.
    /// Windows relies on NTFS ACLs; only the reparse-point check applies there.
    /// </summary>
    public static void EnsureRootSecure(string path)
    {
        if (Directory.Exists(path) && IsReparsePoint(path))
            throw new BackupInsecureRootException("The backup directory is a symlink/reparse point.");

        if (OperatingSystem.IsWindows() || !Directory.Exists(path))
            return;

        UnixFileMode mode = File.GetUnixFileMode(path);
        if ((mode & GroupOrOtherWrite) != 0)
            throw new BackupInsecureRootException(
                "The backup directory is group- or world-writable; another user could tamper with backups. " +
                "Restrict it to the server's user (0700) before enabling backups.");
    }

    /// <summary>True when the recorded mode grants any group/other access — a heuristic for logging only.</summary>
    public static bool IsGroupOrWorldAccessible(string path) =>
        !OperatingSystem.IsWindows() && Directory.Exists(path) &&
        (File.GetUnixFileMode(path) & GroupOrOtherAny) != 0;

    private static bool IsReparsePoint(string path)
    {
        try { return File.GetAttributes(path).HasFlag(FileAttributes.ReparsePoint); }
        catch { return false; }
    }
}
