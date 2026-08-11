using System.Buffers;
using System.Diagnostics;

namespace Kahuna.Server.Persistence.Pitr;

/// <summary>
/// Stores backup artifacts as files under <c>{root}/{backupId:N}/</c> on the local filesystem — the
/// reference implementation of <see cref="IBackupArtifactStore"/>, and the behavior every other target
/// is measured against.
/// <para>
/// This is where the filesystem-specific hardening lives, because it is meaningful only here: artifacts
/// and their directories are restricted to the owner (0600/0700), and a symlink or reparse point
/// anywhere in a tree is refused rather than followed, so a swapped-in link can never redirect a read,
/// copy, or delete outside the configured backup root. Object-storage targets have no equivalent and
/// declare <see cref="BackupArtifactStoreCapabilities.SupportsPosixHardening"/> false instead of
/// pretending to enforce it.
/// </para>
/// </summary>
public sealed class LocalDirectoryArtifactStore : IBackupArtifactStore
{
    /// <summary>
    /// Name fragments marking debris from an interrupted publish, delete, or restore. These exist
    /// because publishing here is temp-file-plus-rename: a crash between the two steps leaves a
    /// recognisable partial file. A store whose writes are atomic has no equivalent debris class.
    /// </summary>
    private static readonly string[] LeftoverMarkers = [".tmp_", ".staging_", ".quarantine_", ".merge_"];

    private const int CopyBufferSize = 1 << 20; // 1 MiB streamed chunks — bounded memory per file

    private readonly string _root;

    public LocalDirectoryArtifactStore(string root)
    {
        // Refuse an unsafe root before creating or touching anything. This check belongs here, not in a
        // caller: creating the directory tightens its mode to 0700, so a component that created the root
        // first would silently repair a world-writable directory instead of refusing to use it — and an
        // operator would never learn their backups had been exposed. Whoever constructs a local store
        // first therefore gets the refusal, whatever the construction order happens to be.
        BackupFilePermissions.EnsureRootSecure(root);
        _root = root;
        BackupFilePermissions.CreateDirectory(root);
    }

    public BackupArtifactStoreCapabilities Capabilities { get; } = new(
        SupportsPosixHardening: true,
        SupportsCheapRangeReads: true,
        RequiresLocalScratch: false);

    public Task<bool> BackupExistsAsync(Guid backupId, CancellationToken ct = default) =>
        Task.FromResult(Directory.Exists(BackupPath(backupId)));

    public Task<bool> ExistsAsync(Guid backupId, string relativePath, CancellationToken ct = default) =>
        Task.FromResult(File.Exists(ResolveSafe(backupId, relativePath)));

    public Task<IReadOnlyList<BackupArtifactEntry>> ListAsync(Guid backupId, CancellationToken ct = default)
    {
        string backupPath = BackupPath(backupId);
        List<BackupArtifactEntry> results = [];

        if (!Directory.Exists(backupPath))
            return Task.FromResult<IReadOnlyList<BackupArtifactEntry>>(results);

        // The enumeration root itself must not be a link: enumerating through a symlinked {backupId}
        // directory would walk a tree outside the backup root while every child check still passed.
        EnsureNotReparsePoint(backupPath, $"Backup {backupId:N}: artifact root is a symlink/reparse point.");

        string rootFull = Path.GetFullPath(backupPath);
        Stack<string> stack = new();
        stack.Push(rootFull);

        while (stack.Count > 0)
        {
            ct.ThrowIfCancellationRequested();
            string dir = stack.Pop();

            foreach (string entry in Directory.EnumerateFileSystemEntries(dir))
            {
                FileAttributes attr = File.GetAttributes(entry);
                if ((attr & FileAttributes.ReparsePoint) != 0)
                    throw new BackupArtifactException($"Backup artifact contains a symlink/reparse point: '{entry}'.");

                if ((attr & FileAttributes.Directory) != 0)
                    stack.Push(entry);
                else
                    results.Add(new BackupArtifactEntry(
                        Path.GetRelativePath(rootFull, entry).Replace(Path.DirectorySeparatorChar, '/'),
                        new FileInfo(entry).Length));
            }
        }

        results.Sort(static (a, b) => string.CompareOrdinal(a.RelativePath, b.RelativePath));
        return Task.FromResult<IReadOnlyList<BackupArtifactEntry>>(results);
    }

    public Task<Stream> OpenReadAsync(
        Guid backupId, string relativePath, long offset = 0, long? length = null, CancellationToken ct = default)
    {
        string path = ResolveSafe(backupId, relativePath);
        EnsureNoReparsePointOnPath(Path.GetFullPath(BackupPath(backupId)), path, backupId);

        FileStream fs = new(path, FileMode.Open, FileAccess.Read, FileShare.Read, CopyBufferSize, useAsync: true);
        if (offset > 0)
            fs.Seek(offset, SeekOrigin.Begin);

        Stream result = length is null ? fs : new BoundedReadStream(fs, length.Value);
        return Task.FromResult(result);
    }

    public Task<IBackupArtifactWriter> OpenWriteAsync(Guid backupId, string relativePath, CancellationToken ct = default)
    {
        string path = ResolveSafe(backupId, relativePath);
        string? parent = Path.GetDirectoryName(path);
        if (parent is not null)
            BackupFilePermissions.CreateDirectory(parent);

        return Task.FromResult<IBackupArtifactWriter>(new AtomicPublishWriter(path));
    }

    public Task<IReadOnlyList<Guid>> ListBackupIdsAsync(CancellationToken ct = default)
    {
        List<Guid> ids = [];
        // Directory.GetDirectories throws when the root is unreadable rather than returning empty, which
        // is the fail-closed behavior the sweep depends on — an unreadable listing must never be read as
        // "no artifacts, safe to reclaim".
        if (!Directory.Exists(_root))
            return Task.FromResult<IReadOnlyList<Guid>>(ids);

        foreach (string dir in Directory.GetDirectories(_root))
        {
            ct.ThrowIfCancellationRequested();
            string name = Path.GetFileName(dir);
            if (ContainsLeftoverMarker(name))
                continue;
            if (Guid.TryParseExact(name, "N", out Guid id))
                ids.Add(id);
        }

        return Task.FromResult<IReadOnlyList<Guid>>(ids);
    }

    public Task<IReadOnlyList<BackupArtifactLeftover>> ListLeftoversAsync(CancellationToken ct = default)
    {
        List<BackupArtifactLeftover> leftovers = [];
        if (!Directory.Exists(_root))
            return Task.FromResult<IReadOnlyList<BackupArtifactLeftover>>(leftovers);

        foreach (string dir in Directory.GetDirectories(_root))
        {
            ct.ThrowIfCancellationRequested();
            if (ContainsLeftoverMarker(Path.GetFileName(dir)))
                leftovers.Add(new BackupArtifactLeftover(
                    dir, Path.GetFileName(dir), "interrupted staging/temporary directory", IsDirectory: true));
        }

        // Manifest-write temporaries ({id}.manifest.tmp_xxxx) are plain files that the directory scan
        // above misses. They never parse as a manifest, so they are debris and reclaimed here — the
        // manifest and artifact roots are the same directory today.
        foreach (string file in Directory.GetFiles(_root))
        {
            ct.ThrowIfCancellationRequested();
            if (ContainsLeftoverMarker(Path.GetFileName(file)))
                leftovers.Add(new BackupArtifactLeftover(
                    file, Path.GetFileName(file), "interrupted temporary file", IsDirectory: false));
        }

        return Task.FromResult<IReadOnlyList<BackupArtifactLeftover>>(leftovers);
    }

    public Task DeleteLeftoverAsync(BackupArtifactLeftover leftover, CancellationToken ct = default)
    {
        string path = leftover.Handle;
        // Confine the delete to this store's root: a handle is opaque to callers, but it arrived from a
        // listing and must not be trusted to address anything outside the tree we own.
        EnsureUnderRoot(path);

        if (Directory.Exists(path))
            RemoveDirectory(path);
        else
            File.Delete(path);

        return Task.CompletedTask;
    }

    public Task DeleteAllAsync(Guid backupId, CancellationToken ct = default)
    {
        RemoveDirectory(BackupPath(backupId));
        return Task.CompletedTask;
    }

    public Task<IBackupCheckpointStagingArea> BeginCheckpointAsync(Guid backupId, CancellationToken ct = default)
    {
        // The local store's artifact directory IS the destination, so the backend checkpoints straight
        // into its final home and commit is a no-op. Transiting scratch here would double the I/O and
        // the peak disk of every full backup for no benefit.
        //
        // Only the parent is created. The backends publish a checkpoint by writing a sibling temp tree
        // and renaming it onto this path, which fails if the path already exists — so the leaf must not
        // be pre-created, and any store handing out a staging path must leave it absent too.
        BackupFilePermissions.CreateDirectory(BackupPath(backupId));
        string checkpointPath = Path.Combine(BackupPath(backupId), CheckpointDirectoryName);
        return Task.FromResult<IBackupCheckpointStagingArea>(new InPlaceStagingArea(checkpointPath));
    }

    public async Task MaterializeAsync(
        Guid backupId,
        string relativePrefix,
        string destinationDirectory,
        long throttleBytesPerSec = 0,
        CancellationToken ct = default)
    {
        string source = string.IsNullOrEmpty(relativePrefix)
            ? BackupPath(backupId)
            : ResolveSafe(backupId, relativePrefix.TrimEnd('/'));

        if (!Directory.Exists(source))
            throw new BackupArtifactException(
                $"Backup {backupId:N}: artifacts under '{relativePrefix}' are missing.");

        await CopyDirectoryAsync(source, destinationDirectory, throttleBytesPerSec, ct).ConfigureAwait(false);
    }

    internal const string CheckpointDirectoryName = "checkpoint";

    // ── helpers ───────────────────────────────────────────────────────────────────────────────

    private string BackupPath(Guid backupId) => Path.Combine(_root, backupId.ToString("N"));

    /// <summary>
    /// Resolves a relative artifact path under a backup's root, refusing anything that would escape it.
    /// Absolute paths, rooted paths, and <c>..</c> traversal are rejected before the path is used.
    /// </summary>
    private string ResolveSafe(Guid backupId, string relativePath)
    {
        if (string.IsNullOrWhiteSpace(relativePath))
            throw new BackupArtifactException($"Backup {backupId:N}: empty artifact path.");

        if (Path.IsPathRooted(relativePath) || relativePath.Contains(':'))
            throw new BackupArtifactException($"Backup {backupId:N}: unsafe artifact path '{relativePath}'.");

        string backupPath = BackupPath(backupId);
        string rootFull = Path.TrimEndingDirectorySeparator(Path.GetFullPath(backupPath));
        string candidate = Path.GetFullPath(
            Path.Combine(backupPath, relativePath.Replace('/', Path.DirectorySeparatorChar)));

        if (!(candidate.Equals(rootFull, StringComparison.OrdinalIgnoreCase)
              || candidate.StartsWith(rootFull + Path.DirectorySeparatorChar, StringComparison.OrdinalIgnoreCase)))
            throw new BackupArtifactException(
                $"Backup {backupId:N}: artifact '{relativePath}' escapes the artifact root.");

        return candidate;
    }

    private void EnsureUnderRoot(string path)
    {
        string rootFull = Path.TrimEndingDirectorySeparator(Path.GetFullPath(_root));
        string full = Path.GetFullPath(path);
        if (!full.StartsWith(rootFull + Path.DirectorySeparatorChar, StringComparison.OrdinalIgnoreCase))
            throw new BackupArtifactException($"Refusing to operate on '{path}': it is outside the backup root.");
    }

    private static bool ContainsLeftoverMarker(string name)
    {
        foreach (string marker in LeftoverMarkers)
            if (name.Contains(marker, StringComparison.Ordinal))
                return true;
        return false;
    }

    private static void EnsureNotReparsePoint(string dir, string message)
    {
        if (Directory.Exists(dir) && (File.GetAttributes(dir) & FileAttributes.ReparsePoint) != 0)
            throw new BackupArtifactException(message);
    }

    private static void EnsureNoReparsePointOnPath(string rootFull, string filePath, Guid backupId)
    {
        string current = Path.GetFullPath(filePath);
        while (!string.IsNullOrEmpty(current) &&
               !current.Equals(rootFull, StringComparison.OrdinalIgnoreCase))
        {
            if (File.Exists(current) || Directory.Exists(current))
            {
                if ((File.GetAttributes(current) & FileAttributes.ReparsePoint) != 0)
                    throw new BackupArtifactException(
                        $"Backup {backupId:N}: artifact path passes through a symlink/reparse point.");
            }

            string? parent = Path.GetDirectoryName(current);
            if (parent is null || parent == current)
                break;
            current = parent;
        }
    }

    /// <summary>
    /// Removes a directory without following a top-level symlink: a reparse point AT the directory is
    /// unlinked, not recursed into, so a swapped-in link cannot redirect the delete to the link
    /// target's contents. Recursive delete of a real directory does not descend into reparse-point
    /// subdirectories on this runtime, so inner links are removed as links, never followed. Idempotent.
    /// </summary>
    private static void RemoveDirectory(string path)
    {
        if (!Directory.Exists(path))
            return;

        if (File.GetAttributes(path).HasFlag(FileAttributes.ReparsePoint))
            Directory.Delete(path, recursive: false);
        else
            Directory.Delete(path, recursive: true);
    }

    private static async Task CopyDirectoryAsync(
        string source, string destination, long throttleBytesPerSec, CancellationToken ct)
    {
        Directory.CreateDirectory(destination);

        foreach (string file in Directory.GetFiles(source))
        {
            ct.ThrowIfCancellationRequested();
            // Never follow a symlink out of the source tree — a reparse point could redirect the read
            // to arbitrary bytes outside the verified artifact.
            if ((File.GetAttributes(file) & FileAttributes.ReparsePoint) != 0)
                throw new BackupArtifactException($"Backup artifact contains a symlink/reparse point: '{file}'.");

            await CopyFileAsync(file, Path.Combine(destination, Path.GetFileName(file)),
                throttleBytesPerSec, ct).ConfigureAwait(false);
        }

        foreach (string subDir in Directory.GetDirectories(source))
        {
            if ((File.GetAttributes(subDir) & FileAttributes.ReparsePoint) != 0)
                throw new BackupArtifactException($"Backup artifact contains a symlinked directory: '{subDir}'.");

            await CopyDirectoryAsync(subDir, Path.Combine(destination, Path.GetFileName(subDir)),
                throttleBytesPerSec, ct).ConfigureAwait(false);
        }
    }

    private static async Task CopyFileAsync(
        string source, string destination, long throttleBytesPerSec, CancellationToken ct)
    {
        await using FileStream src = new(
            source, FileMode.Open, FileAccess.Read, FileShare.Read, CopyBufferSize, useAsync: true);
        await using FileStream dst = new(
            destination, FileMode.Create, FileAccess.Write, FileShare.None, CopyBufferSize, useAsync: true);

        if (throttleBytesPerSec <= 0)
        {
            await src.CopyToAsync(dst, CopyBufferSize, ct).ConfigureAwait(false);
            return;
        }

        byte[] buffer = ArrayPool<byte>.Shared.Rent(CopyBufferSize);
        try
        {
            long copied = 0;
            long start = Stopwatch.GetTimestamp();
            int n;
            while ((n = await src.ReadAsync(buffer.AsMemory(0, CopyBufferSize), ct).ConfigureAwait(false)) > 0)
            {
                await dst.WriteAsync(buffer.AsMemory(0, n), ct).ConfigureAwait(false);
                copied += n;
                double elapsed = Stopwatch.GetElapsedTime(start).TotalSeconds;
                double target = (double)copied / throttleBytesPerSec;
                if (target - elapsed > 0.001)
                    await Task.Delay(TimeSpan.FromSeconds(target - elapsed), ct).ConfigureAwait(false);
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    /// <summary>The local store checkpoints in place, so committing publishes nothing.</summary>
    private sealed class InPlaceStagingArea(string localPath) : IBackupCheckpointStagingArea
    {
        public string LocalPath { get; } = localPath;

        public Task CommitAsync(CancellationToken ct = default) => Task.CompletedTask;

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    /// <summary>
    /// Writes to a temp sibling and publishes with an atomic rename on <see cref="CompleteAsync"/>, so a
    /// reader never observes a partially written artifact at the final path. Disposal without completing
    /// removes the temp file; if that cleanup itself fails, the leftover sweep reclaims it.
    /// </summary>
    private sealed class AtomicPublishWriter : IBackupArtifactWriter
    {
        private readonly string _finalPath;
        private readonly string _tempPath;
        private readonly FileStream _inner;
        private bool _completed;
        private bool _disposed;

        internal AtomicPublishWriter(string finalPath)
        {
            _finalPath = finalPath;
            _tempPath = finalPath + ".tmp_" + Guid.NewGuid().ToString("N")[..8];
            _inner = new FileStream(
                _tempPath, FileMode.Create, FileAccess.Write, FileShare.None, 65536, useAsync: true);
        }

        public Stream Stream => _inner;

        public async Task CompleteAsync(CancellationToken ct = default)
        {
            if (_completed)
                throw new InvalidOperationException("This artifact write has already been completed.");

            await _inner.FlushAsync(ct).ConfigureAwait(false);
            await _inner.DisposeAsync().ConfigureAwait(false);

            // Restrict before the rename so the published artifact is owner-only from the instant it
            // appears at its final path — never briefly readable by other users on the host.
            BackupFilePermissions.RestrictFile(_tempPath);
            File.Move(_tempPath, _finalPath, overwrite: true);
            _completed = true;
        }

        public async ValueTask DisposeAsync()
        {
            if (_disposed)
                return;
            _disposed = true;

            if (_completed)
                return;

            // Abandoned write: drop the handle and the partial bytes rather than publishing them.
            try { await _inner.DisposeAsync().ConfigureAwait(false); }
            catch { /* the file is being discarded; a close failure changes nothing */ }

            try
            {
                if (File.Exists(_tempPath))
                    File.Delete(_tempPath);
            }
            catch
            {
                // Best-effort: an abandoned temp file is reclaimed by the leftover sweep.
            }
        }
    }

    /// <summary>Limits reads to the first <c>length</c> bytes of the underlying stream.</summary>
    private sealed class BoundedReadStream(Stream inner, long length) : Stream
    {
        private readonly long _length = length;
        private long _remaining = length;

        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => _length;
        public override long Position { get => _length - _remaining; set => throw new NotSupportedException(); }

        public override void Flush() { }
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();

        public override int Read(byte[] buffer, int offset, int count)
        {
            int allowed = (int)Math.Min(count, _remaining);
            if (allowed <= 0)
                return 0;
            int read = inner.Read(buffer, offset, allowed);
            _remaining -= read;
            return read;
        }

        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken ct = default)
        {
            int allowed = (int)Math.Min(buffer.Length, _remaining);
            if (allowed <= 0)
                return 0;
            int read = await inner.ReadAsync(buffer[..allowed], ct).ConfigureAwait(false);
            _remaining -= read;
            return read;
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
                inner.Dispose();
        }

        public override ValueTask DisposeAsync() => inner.DisposeAsync();
    }
}
