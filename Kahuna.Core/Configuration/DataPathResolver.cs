
namespace Kahuna.Server.Configuration;

/// <summary>
/// Resolves where a node keeps its key-value storage and its Raft write-ahead log when the operator
/// did not say.
///
/// An unset path cannot be passed through untouched: every backend composes its directory as
/// <c>$"{path}/{revision}"</c>, so an empty path silently becomes an absolute path at the root of
/// the filesystem (<c>/v1</c>). That either fails outright on a read-only root volume or, running as
/// a privileged user, quietly writes a database somewhere nobody will look for it.
///
/// Resolution therefore always produces a concrete, writable, user-owned directory. Callers that
/// launch with explicit paths — the container entrypoint and the cluster run scripts — are
/// unaffected: a configured path is returned verbatim, never rewritten.
/// </summary>
public static class DataPathResolver
{
    /// <summary>
    /// Overrides the root beneath which both the storage and WAL directories are placed, so a user
    /// can relocate everything a node owns with a single variable.
    /// </summary>
    public const string HomeVariable = "KAHUNA_HOME";

    private const string StorageDirectoryName = "data";

    private const string WalDirectoryName = "wal";

    /// <summary>
    /// The directory beneath which unset paths are placed: <c>$KAHUNA_HOME</c> when set, otherwise the
    /// platform's per-user data location (<c>$XDG_DATA_HOME/kahuna</c> or <c>~/.local/share/kahuna</c>
    /// on Linux/macOS, <c>%LOCALAPPDATA%\kahuna</c> on Windows).
    /// </summary>
    public static string ResolveRoot()
    {
        string? home = Environment.GetEnvironmentVariable(HomeVariable);
        if (!string.IsNullOrWhiteSpace(home))
            return Path.GetFullPath(home);

        if (OperatingSystem.IsWindows())
        {
            string localAppData = Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData);

            // GetFolderPath returns "" when the location cannot be determined; falling through to a
            // relative "kahuna" would reintroduce the cwd-relative behavior this type exists to remove.
            if (!string.IsNullOrWhiteSpace(localAppData))
                return Path.Combine(localAppData, "kahuna");
        }
        else
        {
            string? xdgDataHome = Environment.GetEnvironmentVariable("XDG_DATA_HOME");
            if (!string.IsNullOrWhiteSpace(xdgDataHome))
                return Path.Combine(Path.GetFullPath(xdgDataHome), "kahuna");

            string userProfile = Environment.GetFolderPath(Environment.SpecialFolder.UserProfile);
            if (!string.IsNullOrWhiteSpace(userProfile))
                return Path.Combine(userProfile, ".local", "share", "kahuna");
        }

        // Last resort: a temp-rooted directory is still absolute and writable, which beats the
        // filesystem root. Reached only when the platform cannot name a home directory at all.
        return Path.Combine(Path.GetTempPath(), "kahuna");
    }

    /// <summary>
    /// The key-value storage directory. A configured path wins verbatim; otherwise
    /// <c>&lt;root&gt;/data</c>.
    /// </summary>
    public static string ResolveStoragePath(string? configured) => Resolve(configured, StorageDirectoryName);

    /// <summary>
    /// The Raft write-ahead log directory. A configured path wins verbatim; otherwise
    /// <c>&lt;root&gt;/wal</c>.
    /// </summary>
    public static string ResolveWalPath(string? configured) => Resolve(configured, WalDirectoryName);

    private static string Resolve(string? configured, string defaultDirectoryName)
    {
        if (!string.IsNullOrWhiteSpace(configured))
            return configured;

        return Path.Combine(ResolveRoot(), defaultDirectoryName);
    }

    /// <summary>
    /// Whether a storage/WAL backend name identifies a purely in-memory backend, which owns no
    /// directory and must not have one created for it.
    /// </summary>
    public static bool IsInMemory(string? storage) => string.Equals(storage, "memory", StringComparison.OrdinalIgnoreCase);

    /// <summary>
    /// The storage revision, which names the directory the key-value backend opens beneath the
    /// storage path.
    ///
    /// An unset revision must not reach the embedded node: it mints a fresh GUID per boot so that
    /// in-process test nodes get an isolated scratch keyspace. A long-lived server inheriting that
    /// would open a brand-new empty database on every start, leak the previous one on disk forever,
    /// and appear to keep its data only for as long as the Raft log still replays it — silent loss
    /// once the log compacts. Pinning it matches the WAL revision, which already defaults to "v1".
    /// </summary>
    public static string ResolveStorageRevision(string? configured) =>
        string.IsNullOrWhiteSpace(configured) ? DefaultStorageRevision : configured;

    /// <summary>
    /// The revision a server uses when none is configured. Matches the <c>--wal-revision</c> default.
    /// </summary>
    public const string DefaultStorageRevision = "v1";
}
