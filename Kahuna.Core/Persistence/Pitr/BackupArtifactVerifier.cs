
using System.Security.Cryptography;

namespace Kahuna.Server.Persistence.Pitr;

/// <summary>
/// Enumerates and verifies the on-disk artifacts of a backup against the file set, sizes, and
/// SHA-256 digests recorded in its <see cref="BackupManifest"/>.  Used both before a backup is
/// published (so a corrupt artifact never enters the catalog) and before a restore copies/replays
/// it (so corruption is caught before it can reach a target).  Verification fails closed: any
/// missing, truncated, padded, wrong-digest, extra, duplicate, unsafe-path, or symlinked file throws
/// <see cref="BackupArtifactException"/>; a legacy/unsupported manifest throws
/// <see cref="BackupUnsupportedFormatException"/>.
/// </summary>
internal static class BackupArtifactVerifier
{
    /// <summary>
    /// Hashes every regular file under <paramref name="directory"/> recursively, returning digests
    /// and byte lengths keyed by artifact-relative path (forward-slash separated, prefixed with
    /// <paramref name="keyPrefix"/>). Symlinks/reparse points are rejected — a checkpoint must be
    /// plain files. Paths are ordinal-sorted for determinism.
    /// </summary>
    internal static (Dictionary<string, string> checksums, Dictionary<string, long> sizes)
        HashDirectory(string directory, string keyPrefix, CancellationToken ct = default)
    {
        Dictionary<string, string> checksums = [];
        Dictionary<string, long> sizes = [];

        foreach (string file in EnumerateRegularFiles(directory))
        {
            ct.ThrowIfCancellationRequested();
            string full = Path.Combine(directory, file.Replace('/', Path.DirectorySeparatorChar));
            string key = keyPrefix + file;
            checksums[key] = ComputeSha256(full);
            sizes[key] = new FileInfo(full).Length;
        }

        return (checksums, sizes);
    }

    /// <summary>
    /// Verifies that the artifact directory for <paramref name="manifest"/> under
    /// <paramref name="artifactsDir"/> contains exactly the files recorded in the manifest, each with
    /// the recorded length and digest — no missing, extra, altered, unsafe-path, or symlinked file.
    /// A manifest below <see cref="BackupManifest.CurrentFormatVersion"/> is rejected as unsupported
    /// (legacy) rather than corrupt. Always safe to call, including for a genuinely empty incremental.
    /// </summary>
    /// <exception cref="BackupUnsupportedFormatException">Legacy/unsupported manifest format.</exception>
    /// <exception cref="BackupArtifactException">On any integrity discrepancy.</exception>
    internal static void Verify(BackupManifest manifest, string artifactsDir, CancellationToken ct = default)
    {
        if (manifest.FormatVersion < BackupManifest.CurrentFormatVersion)
            throw new BackupUnsupportedFormatException(
                $"Backup {manifest.BackupId:N} is in legacy format version {manifest.FormatVersion} " +
                $"(current is {BackupManifest.CurrentFormatVersion}); it cannot be verified or restored " +
                "by this version without an explicit upgrade.");

        string artifactPath = Path.Combine(artifactsDir, manifest.BackupId.ToString("N"));

        // A Full always has a checkpoint; an Incremental has a segment per range. A manifest that
        // should have artifacts but records none is corrupt. A truly empty incremental (no ranges)
        // legitimately has none.
        bool expectsArtifacts = manifest.Type == BackupType.Full || manifest.PartitionRanges.Count > 0;

        if (manifest.Checksums.Count == 0)
        {
            if (expectsArtifacts)
                throw new BackupArtifactException(
                    $"Backup {manifest.BackupId:N} records no artifact checksums but should have artifacts.");

            // Empty incremental: nothing to verify, but the directory must not contain stray files.
            if (Directory.Exists(artifactPath))
            {
                foreach (string _ in EnumerateRegularFiles(artifactPath))
                    throw new BackupArtifactException(
                        $"Backup {manifest.BackupId:N}: unexpected files present for an empty incremental.");
            }
            return;
        }

        if (!Directory.Exists(artifactPath))
            throw new BackupArtifactException(
                $"Artifact directory for backup {manifest.BackupId:N} is missing.");

        // Validate + normalize the declared keys before touching the filesystem.
        HashSet<string> expected = new(StringComparer.Ordinal);
        HashSet<string> normalizedSeen = new(StringComparer.OrdinalIgnoreCase);
        string rootFull = Path.TrimEndingDirectorySeparator(Path.GetFullPath(artifactPath));

        foreach ((string key, string expectedDigest) in manifest.Checksums)
        {
            ct.ThrowIfCancellationRequested();

            if (!IsSafeRelativeKey(key))
                throw new BackupArtifactException(
                    $"Backup {manifest.BackupId:N}: unsafe artifact path '{key}'.");

            string filePath = Path.Combine(artifactPath, key.Replace('/', Path.DirectorySeparatorChar));
            string fullResolved = Path.GetFullPath(filePath);

            // Containment: the resolved path must stay under the artifact root.
            if (!(fullResolved.Equals(rootFull, StringComparison.OrdinalIgnoreCase)
                  || fullResolved.StartsWith(rootFull + Path.DirectorySeparatorChar, StringComparison.OrdinalIgnoreCase)))
                throw new BackupArtifactException(
                    $"Backup {manifest.BackupId:N}: artifact '{key}' escapes the artifact root.");

            // Duplicate-normalized keys (e.g. case variants on a case-insensitive filesystem).
            if (!normalizedSeen.Add(fullResolved))
                throw new BackupArtifactException(
                    $"Backup {manifest.BackupId:N}: duplicate artifact path '{key}'.");

            expected.Add(key);

            if (!File.Exists(filePath))
                throw new BackupArtifactException(
                    $"Backup {manifest.BackupId:N}: declared artifact '{key}' is missing.");

            // No symlinked artifact — a reparse point could redirect reads/copies outside the root.
            EnsureNoReparsePointOnPath(rootFull, filePath, manifest.BackupId);

            if (manifest.Sizes.TryGetValue(key, out long expectedSize))
            {
                long actualSize = new FileInfo(filePath).Length;
                if (actualSize != expectedSize)
                    throw new BackupArtifactException(
                        $"Backup {manifest.BackupId:N}: artifact '{key}' has size {actualSize}, expected {expectedSize}.");
            }

            string actualDigest = ComputeSha256(filePath);
            if (!string.Equals(actualDigest, expectedDigest, StringComparison.OrdinalIgnoreCase))
                throw new BackupArtifactException(
                    $"Backup {manifest.BackupId:N}: artifact '{key}' failed digest verification.");
        }

        // No unexpected extra file may sit in the artifact directory (also rejects symlinks).
        foreach (string actual in EnumerateRegularFiles(artifactPath))
        {
            if (!expected.Contains(actual))
                throw new BackupArtifactException(
                    $"Backup {manifest.BackupId:N}: unexpected extra artifact '{actual}' not present in the manifest.");
        }
    }

    /// <summary>
    /// Verifies the checkpoint files that were copied into <paramref name="checkpointDir"/> (the
    /// staging location that will actually be opened) against the Full manifest's <c>checkpoint/*</c>
    /// checksums. This closes the verify-then-use gap for the base image: the bytes about to be opened
    /// are the bytes that were hashed, even if the source artifact changed after the up-front check.
    /// </summary>
    internal static void VerifyCheckpointCopy(BackupManifest full, string checkpointDir, CancellationToken ct = default)
    {
        const string prefix = "checkpoint/";
        string rootFull = Path.TrimEndingDirectorySeparator(Path.GetFullPath(checkpointDir));
        HashSet<string> expected = new(StringComparer.Ordinal);

        foreach ((string key, string expectedDigest) in full.Checksums)
        {
            if (!key.StartsWith(prefix, StringComparison.Ordinal))
                continue;

            ct.ThrowIfCancellationRequested();

            string rel = key[prefix.Length..];
            if (!IsSafeRelativeKey(rel))
                throw new BackupArtifactException($"Backup {full.BackupId:N}: unsafe checkpoint path '{key}'.");

            string filePath = Path.Combine(checkpointDir, rel.Replace('/', Path.DirectorySeparatorChar));
            if (!File.Exists(filePath))
                throw new BackupArtifactException($"Backup {full.BackupId:N}: staged checkpoint file '{rel}' is missing.");

            EnsureNoReparsePointOnPath(rootFull, filePath, full.BackupId);
            expected.Add(rel);

            if (full.Sizes.TryGetValue(key, out long expectedSize) && new FileInfo(filePath).Length != expectedSize)
                throw new BackupArtifactException($"Backup {full.BackupId:N}: staged checkpoint file '{rel}' has wrong size.");

            if (!string.Equals(ComputeSha256(filePath), expectedDigest, StringComparison.OrdinalIgnoreCase))
                throw new BackupArtifactException($"Backup {full.BackupId:N}: staged checkpoint file '{rel}' failed digest verification.");
        }

        foreach (string actual in EnumerateRegularFiles(checkpointDir))
        {
            if (!expected.Contains(actual))
                throw new BackupArtifactException(
                    $"Backup {full.BackupId:N}: unexpected file '{actual}' in the staged checkpoint.");
        }
    }

    /// <summary>
    /// A safe artifact-relative key: non-empty, not rooted, no <c>.</c>/<c>..</c>/empty segments,
    /// under either path separator.
    /// </summary>
    private static bool IsSafeRelativeKey(string key)
    {
        if (string.IsNullOrEmpty(key) || Path.IsPathRooted(key))
            return false;

        foreach (string part in key.Split('/', '\\'))
        {
            if (part.Length == 0 || part == "." || part == "..")
                return false;
        }

        return true;
    }

    /// <summary>
    /// Enumerates regular files under <paramref name="directory"/> recursively as forward-slash
    /// relative paths (ordinal-sorted), refusing to follow symlinks: a reparse-point directory or
    /// file anywhere in the tree throws <see cref="BackupArtifactException"/>.
    /// </summary>
    private static List<string> EnumerateRegularFiles(string directory)
    {
        List<string> results = [];
        string rootFull = Path.GetFullPath(directory);
        Stack<string> stack = new();
        stack.Push(rootFull);

        while (stack.Count > 0)
        {
            string dir = stack.Pop();
            foreach (string entry in Directory.EnumerateFileSystemEntries(dir))
            {
                FileAttributes attr = File.GetAttributes(entry);
                if ((attr & FileAttributes.ReparsePoint) != 0)
                    throw new BackupArtifactException($"Backup artifact contains a symlink/reparse point: '{entry}'.");

                if ((attr & FileAttributes.Directory) != 0)
                    stack.Push(entry);
                else
                    results.Add(Path.GetRelativePath(rootFull, entry).Replace(Path.DirectorySeparatorChar, '/'));
            }
        }

        results.Sort(StringComparer.Ordinal);
        return results;
    }

    /// <summary>
    /// Rejects a reparse point at the file itself or on any directory between the artifact root and
    /// the file, so a symlinked ancestor cannot redirect the read outside the root.
    /// </summary>
    private static void EnsureNoReparsePointOnPath(string rootFull, string filePath, Guid backupId)
    {
        string current = Path.GetFullPath(filePath);
        while (!string.IsNullOrEmpty(current) &&
               !current.Equals(rootFull, StringComparison.OrdinalIgnoreCase))
        {
            if ((File.GetAttributes(current) & FileAttributes.ReparsePoint) != 0)
                throw new BackupArtifactException(
                    $"Backup {backupId:N}: artifact path passes through a symlink/reparse point.");

            string? parent = Path.GetDirectoryName(current);
            if (parent is null || parent == current)
                break;
            current = parent;
        }
    }

    internal static string ComputeSha256(string filePath)
    {
        using FileStream stream = File.OpenRead(filePath);
        byte[] hash = SHA256.HashData(stream);
        return Convert.ToHexString(hash).ToLowerInvariant();
    }
}
