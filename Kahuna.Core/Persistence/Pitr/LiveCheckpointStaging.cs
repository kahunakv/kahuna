using System.Collections.Concurrent;
using System.Runtime.InteropServices;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Persistence.Pitr;

/// <summary>
/// Chooses where a local backup target stages the checkpoint that the node's own RocksDB store takes.
/// <para>
/// RocksDB builds a checkpoint from hard links to the live SST files, which costs no I/O. A hard link cannot
/// cross a filesystem, so when the backup directory is on another filesystem than the store, RocksDB copies
/// every live SST file into it before the as-of trim drops the rows newer than the cut. That copy reads the
/// whole live store, dead revisions included, on the disk where every commit waits for its fsync.
/// </para>
/// <para>
/// This class detects that case with a hard-link probe and then picks a staging root on the store's
/// filesystem. The backend links, trims and compacts there, and the commit copies only the trimmed image to the
/// backup directory under the capture throttle. When the backup directory accepts the link, or when no staging
/// root can accept it with room for the image, the checkpoint stays in place in the backup directory.
/// </para>
/// <para>
/// Every staging directory and probe file in use is registered before it is created, so the leftover listing
/// reports only what a stopped process left behind, never the tree of a capture that is still running.
/// </para>
/// </summary>
internal sealed class LiveCheckpointStaging
{
    /// <summary>
    /// Free space the staging filesystem must keep above the image estimate. The trim copy also holds the
    /// copied WAL, and the live store keeps writing to the same filesystem while the image is staged.
    /// </summary>
    internal const long FreeSpaceMargin = 256L * 1024 * 1024;

    private readonly string _probeSourceDirectory;
    private readonly string _liveStorePath;
    private readonly IReadOnlyList<string> _stagingRoots;
    private readonly Func<string, string, bool> _tryHardLink;
    private readonly Func<string, long> _availableFreeSpace;
    private readonly ILogger? _logger;
    private readonly ConcurrentDictionary<string, byte> _inUse = new(StringComparer.Ordinal);

    /// <param name="probeSourceDirectory">A directory on the store's filesystem that the probe may write a file
    /// into (the storage path).</param>
    /// <param name="liveStorePath">The directory of the live store, whose size bounds the size of the image.</param>
    /// <param name="stagingRoots">Candidate staging roots, in order of preference.</param>
    /// <param name="copyThrottleBytesPerSec">Pace of the copy into the backup directory; 0 = unlimited.</param>
    /// <param name="tryHardLink">Creates a hard link (existing file, new link path); false when it cannot.
    /// Tests inject it; the default calls the operating system.</param>
    /// <param name="availableFreeSpace">Free bytes on the filesystem that holds a path. Tests inject it.</param>
    internal LiveCheckpointStaging(
        string probeSourceDirectory,
        string liveStorePath,
        IReadOnlyList<string> stagingRoots,
        long copyThrottleBytesPerSec,
        ILogger? logger = null,
        Func<string, string, bool>? tryHardLink = null,
        Func<string, long>? availableFreeSpace = null)
    {
        _probeSourceDirectory = probeSourceDirectory;
        _liveStorePath = liveStorePath;
        _stagingRoots = stagingRoots;
        CopyThrottleBytesPerSec = copyThrottleBytesPerSec;
        _logger = logger;
        _tryHardLink = tryHardLink ?? TryCreateHardLink;
        _availableFreeSpace = availableFreeSpace ?? (static path => new DriveInfo(path).AvailableFreeSpace);
    }

    internal long CopyThrottleBytesPerSec { get; }

    internal IReadOnlyList<string> StagingRoots => _stagingRoots;

    /// <summary>
    /// The directory to stage <paramref name="backupId"/>'s checkpoint in, or null when the checkpoint should be
    /// written in place under <paramref name="backupRoot"/>. A returned directory exists, is owner-only and is
    /// registered as in use until <see cref="Release"/>.
    /// </summary>
    internal string? Reserve(Guid backupId, string backupRoot)
    {
        if (CanLinkFromStore(backupRoot))
            return null;

        long imageBound = -1;
        foreach (string root in _stagingRoots)
        {
            // A staging root is ours: refuse a symlinked or shared one instead of writing backup bytes into it.
            BackupFilePermissions.EnsureRootSecure(root);
            BackupFilePermissions.CreateDirectory(root);

            if (!CanLinkFromStore(root))
                continue;

            if (imageBound < 0)
                imageBound = DirectorySize(_liveStorePath);

            long free;
            try
            {
                free = _availableFreeSpace(root);
            }
            catch (Exception ex) when (ex is IOException or UnauthorizedAccessException or ArgumentException)
            {
                // Staging fills the filesystem the live store writes to; without a free-space figure, do not risk it.
                _logger?.LogWarning(ex, "Backup staging: cannot read the free space under {Root}; this root is not used.", root);
                continue;
            }

            if (free < imageBound + FreeSpaceMargin)
            {
                _logger?.LogWarning(
                    "Backup staging: {Root} has {Free} free bytes, less than the live store's {Bound} bytes plus a margin of {Margin}; this root is not used.",
                    root, free, imageBound, FreeSpaceMargin);
                continue;
            }

            string directory = Path.Combine(root, backupId.ToString("N"));
            _inUse[directory] = 0;
            try
            {
                BackupFilePermissions.CreateDirectory(directory);
            }
            catch
            {
                _inUse.TryRemove(directory, out _);
                throw;
            }

            if (_logger?.IsEnabled(LogLevel.Information) == true)
            {
                _logger.LogInformation(
                    "Backup {BackupId}: the backup directory is not on the store's filesystem, so the checkpoint is staged under {Root} and only the trimmed image is copied.",
                    backupId, root);
            }
            return directory;
        }

        _logger?.LogWarning(
            "Backup {BackupId}: the backup directory is not on the store's filesystem and no staging root qualifies, so RocksDB copies every live SST file into the backup directory.",
            backupId);
        return null;
    }

    /// <summary>Deletes a reserved staging directory and ends its registration. Best-effort: the leftover sweep
    /// reclaims a tree that cannot be deleted now.</summary>
    internal void Release(string directory)
    {
        try
        {
            if (Directory.Exists(directory))
                Directory.Delete(directory, recursive: true);
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            _logger?.LogWarning(ex, "Backup staging: could not delete {Directory}; the backup GC reclaims it later.", directory);
        }
        finally
        {
            _inUse.TryRemove(directory, out _);
        }
    }

    /// <summary>Every entry under a staging root that no capture of this process uses.</summary>
    internal IEnumerable<BackupArtifactLeftover> ListLeftovers(CancellationToken ct)
    {
        foreach (string root in _stagingRoots)
        {
            if (!Directory.Exists(root))
                continue;

            foreach (string entry in Directory.EnumerateFileSystemEntries(root))
            {
                ct.ThrowIfCancellationRequested();

                // Registration happens before creation, so an entry seen here that is in use is still in the set.
                if (_inUse.ContainsKey(entry))
                    continue;

                bool isDirectory = Directory.Exists(entry);
                yield return new BackupArtifactLeftover(
                    entry,
                    Path.GetFileName(entry),
                    isDirectory ? "interrupted checkpoint staging directory" : "interrupted staging probe file",
                    isDirectory);
            }
        }
    }

    /// <summary>True when <paramref name="path"/> is directly under one of the staging roots.</summary>
    internal bool OwnsLeftover(string path)
    {
        string? parent = Path.GetDirectoryName(Path.GetFullPath(path));
        if (parent is null)
            return false;

        foreach (string root in _stagingRoots)
            if (string.Equals(Path.TrimEndingDirectorySeparator(Path.GetFullPath(root)), parent, StringComparison.Ordinal))
                return true;
        return false;
    }

    /// <summary>
    /// True when a file on the store's filesystem can be hard-linked into <paramref name="directory"/>, which is
    /// what RocksDB needs to take its checkpoint there without copying.
    /// </summary>
    private bool CanLinkFromStore(string directory)
    {
        string suffix = Guid.NewGuid().ToString("N");
        string source = Path.Combine(_probeSourceDirectory, ".backup-link-probe-" + suffix);
        // The leftover marker lets the backup root's sweep reclaim a probe a crash left behind.
        string target = Path.Combine(directory, ".tmp_link-probe-" + suffix);

        _inUse[target] = 0;
        try
        {
            Directory.CreateDirectory(_probeSourceDirectory);
            File.WriteAllBytes(source, []);
            return _tryHardLink(source, target);
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            _logger?.LogWarning(ex, "Backup staging: the hard-link probe into {Directory} failed.", directory);
            return false;
        }
        finally
        {
            TryDelete(target);
            TryDelete(source);
            _inUse.TryRemove(target, out _);
        }
    }

    private static void TryDelete(string file)
    {
        try
        {
            File.Delete(file);
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            // A probe file left behind is a few bytes; the sweep of its root reclaims it.
        }
    }

    private static long DirectorySize(string directory)
    {
        if (!Directory.Exists(directory))
            return 0;

        long total = 0;
        foreach (string file in Directory.EnumerateFiles(directory, "*", SearchOption.AllDirectories))
        {
            try
            {
                total += new FileInfo(file).Length;
            }
            catch (FileNotFoundException)
            {
                // A compaction removed it between the listing and the stat.
            }
        }
        return total;
    }

    private static bool TryCreateHardLink(string existingFile, string newLink)
    {
        try
        {
            if (OperatingSystem.IsWindows())
                return CreateHardLinkW(newLink, existingFile, IntPtr.Zero);

            return UnixLink(existingFile, newLink) == 0;
        }
        catch (Exception ex) when (ex is DllNotFoundException or EntryPointNotFoundException)
        {
            return false;
        }
    }

    [DllImport("libc", EntryPoint = "link", SetLastError = true)]
    private static extern int UnixLink(string existingFile, string newLink);

    [DllImport("kernel32.dll", EntryPoint = "CreateHardLinkW", CharSet = CharSet.Unicode, SetLastError = true)]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static extern bool CreateHardLinkW(string newLink, string existingFile, IntPtr securityAttributes);
}
