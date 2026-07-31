
using System.Security.Cryptography;
using System.Text.Json;
using Kommander.Time;

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
        EnsureSupportedVersion(manifest);

        // Manifest-level schema: type/parent/base-cut consistency, no duplicate partition ranges,
        // valid index/HLC bounds, and the artifact-name set required by the type. Checked before any
        // filesystem work so a structurally invalid manifest fails fast.
        ValidateManifestSchema(manifest);

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

            // Empty incremental: nothing to verify, but the directory must not contain stray files
            // and must not itself be a symlinked root.
            if (Directory.Exists(artifactPath))
            {
                EnsureDirectoryNotReparsePoint(artifactPath,
                    $"Backup {manifest.BackupId:N}: artifact root is a symlink/reparse point.");
                foreach (string _ in EnumerateRegularFiles(artifactPath))
                    throw new BackupArtifactException(
                        $"Backup {manifest.BackupId:N}: unexpected files present for an empty incremental.");
            }
            return;
        }

        if (!Directory.Exists(artifactPath))
            throw new BackupArtifactException(
                $"Artifact directory for backup {manifest.BackupId:N} is missing.");

        // Reject a reparse point AT the per-backup root itself, not only on the path between a file
        // and the root: a symlinked {backupId} directory would otherwise pass every child check while
        // redirecting verification/copy/replay to a tree outside the configured backup directory.
        EnsureDirectoryNotReparsePoint(artifactPath,
            $"Backup {manifest.BackupId:N}: artifact root is a symlink/reparse point.");

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

            // A size is required for every declared artifact (current-version manifests always record
            // one); a missing size is a corrupt manifest, not a digest-only fallback.
            if (!manifest.Sizes.TryGetValue(key, out long expectedSize))
                throw new BackupArtifactException(
                    $"Backup {manifest.BackupId:N}: artifact '{key}' has no recorded size.");

            long actualSize = new FileInfo(filePath).Length;
            if (actualSize != expectedSize)
                throw new BackupArtifactException(
                    $"Backup {manifest.BackupId:N}: artifact '{key}' has size {actualSize}, expected {expectedSize}.");

            string actualDigest = ComputeSha256(filePath);
            if (!string.Equals(actualDigest, expectedDigest, StringComparison.OrdinalIgnoreCase))
                throw new BackupArtifactException(
                    $"Backup {manifest.BackupId:N}: artifact '{key}' failed digest verification.");
        }

        // The size and checksum keysets must match exactly — no size key without a checksum.
        foreach (string sizeKey in manifest.Sizes.Keys)
        {
            if (!manifest.Checksums.ContainsKey(sizeKey))
                throw new BackupArtifactException(
                    $"Backup {manifest.BackupId:N}: size recorded for '{sizeKey}' with no matching checksum.");
        }

        // No unexpected extra file may sit in the artifact directory (also rejects symlinks).
        foreach (string actual in EnumerateRegularFiles(artifactPath))
        {
            if (!expected.Contains(actual))
                throw new BackupArtifactException(
                    $"Backup {manifest.BackupId:N}: unexpected extra artifact '{actual}' not present in the manifest.");
        }

        // Artifacts are now byte-verified; validate their semantic content against the manifest.
        ValidateArtifactContent(manifest, artifactPath, ct);
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

        // The staged checkpoint directory itself must not be a reparse point — a swapped-in symlink
        // would redirect the files about to be opened outside the staging area.
        EnsureDirectoryNotReparsePoint(checkpointDir,
            $"Backup {full.BackupId:N}: staged checkpoint directory is a symlink/reparse point.");

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

            if (!full.Sizes.TryGetValue(key, out long expectedSize))
                throw new BackupArtifactException($"Backup {full.BackupId:N}: staged checkpoint file '{rel}' has no recorded size.");
            if (new FileInfo(filePath).Length != expectedSize)
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

    private const string CheckpointPrefix = "checkpoint/";

    /// <summary>
    /// Cheap, manifest-only structural validation: format version, schema (type/parent/base-cut, no
    /// duplicate ranges, valid index/HLC bounds, required artifact-name set), and checksum/size keyset
    /// equality. Reads no files and hashes nothing. Returns <c>null</c> when the manifest is
    /// structurally valid, otherwise the reason it is not — so a listing can mark a valid-JSON but
    /// semantically invalid or unsupported backup without hiding it, and without the cost of a full
    /// artifact verification.
    /// </summary>
    internal static string? GetStructuralError(BackupManifest manifest)
    {
        // A legacy (older) manifest is reported honestly by its FormatVersion, not marked invalid: it
        // predates the current schema (so validating it against current rules is meaningless) and may
        // be upgradable. A newer-than-current version, by contrast, is genuinely unreadable and is
        // flagged. Only current-version manifests get the full structural check.
        if (manifest.FormatVersion < BackupManifest.CurrentFormatVersion)
            return null;

        try
        {
            EnsureSupportedVersion(manifest); // rejects a newer-than-current (future) version
            ValidateManifestSchema(manifest);
            ValidateChecksumSizeKeysets(manifest);
            return null;
        }
        catch (Exception ex) when (ex is BackupArtifactException or BackupUnsupportedFormatException)
        {
            return ex.Message;
        }
    }

    private static void EnsureSupportedVersion(BackupManifest manifest)
    {
        // Only the current format version can be verified against the exact-file-set/coverage rules.
        // A lower version is legacy; a higher version was written by a newer reader this code cannot
        // understand — both are unsupported, never silently accepted or treated as "corrupt".
        if (manifest.FormatVersion != BackupManifest.CurrentFormatVersion)
            throw new BackupUnsupportedFormatException(
                $"Backup {manifest.BackupId:N} is in {(manifest.FormatVersion < BackupManifest.CurrentFormatVersion ? "legacy" : "unsupported newer")} " +
                $"format version {manifest.FormatVersion} (this reader supports {BackupManifest.CurrentFormatVersion}); " +
                "it cannot be verified or restored by this version without an explicit upgrade.");
    }

    /// <summary>The size and checksum keysets must match exactly (manifest-only, reads no files).</summary>
    private static void ValidateChecksumSizeKeysets(BackupManifest m)
    {
        foreach (string key in m.Checksums.Keys)
            if (!m.Sizes.ContainsKey(key))
                throw new BackupArtifactException($"Backup {m.BackupId:N}: artifact '{key}' has no recorded size.");
        foreach (string key in m.Sizes.Keys)
            if (!m.Checksums.ContainsKey(key))
                throw new BackupArtifactException($"Backup {m.BackupId:N}: size recorded for '{key}' with no matching checksum.");
    }

    /// <summary>
    /// Validates the manifest as a document, independent of the filesystem: type/parent/base-cut
    /// consistency, no duplicate partition ranges, valid index and HLC bounds per range, and the
    /// exact set of artifact names the type requires (a Full's keys are all under <c>checkpoint/</c>
    /// and include the checkpoint sidecar; an Incremental's keys are exactly one
    /// <c>partition_{id}.wal</c> per range, no more, no fewer).
    /// </summary>
    private static void ValidateManifestSchema(BackupManifest m)
    {
        switch (m.Type)
        {
            case BackupType.Full:
                if (m.ParentBackupId is not null)
                    throw new BackupArtifactException($"Backup {m.BackupId:N}: a Full backup must not record a parent.");
                if (m.BaseCut is null)
                    throw new BackupArtifactException($"Backup {m.BackupId:N}: a Full backup must record a BaseCut.");
                break;

            case BackupType.Incremental:
                if (m.ParentBackupId is null)
                    throw new BackupArtifactException($"Backup {m.BackupId:N}: an Incremental backup must record a parent.");
                if (m.BaseCut is not null)
                    throw new BackupArtifactException($"Backup {m.BackupId:N}: an Incremental backup must not record a BaseCut.");
                break;

            default:
                throw new BackupArtifactException($"Backup {m.BackupId:N}: unknown backup type {(int)m.Type}.");
        }

        HashSet<int> seenPartitions = [];
        foreach (PartitionBackupRange r in m.PartitionRanges)
        {
            if (!seenPartitions.Add(r.PartitionId))
                throw new BackupArtifactException($"Backup {m.BackupId:N}: duplicate partition range for partition {r.PartitionId}.");
            if (r.FromIndex < 1)
                throw new BackupArtifactException($"Backup {m.BackupId:N}: partition {r.PartitionId} has invalid FromIndex {r.FromIndex}.");
            if (r.ToIndex < r.FromIndex)
                throw new BackupArtifactException($"Backup {m.BackupId:N}: partition {r.PartitionId} has ToIndex {r.ToIndex} below FromIndex {r.FromIndex}.");
            if (r.ToHlc.CompareTo(r.FromHlc) < 0)
                throw new BackupArtifactException($"Backup {m.BackupId:N}: partition {r.PartitionId} has ToHlc {r.ToHlc} before FromHlc {r.FromHlc}.");
        }

        if (m.Type == BackupType.Full)
        {
            bool hasSidecar = false;
            foreach (string key in m.Checksums.Keys)
            {
                if (!key.StartsWith(CheckpointPrefix, StringComparison.Ordinal))
                    throw new BackupArtifactException(
                        $"Backup {m.BackupId:N}: Full artifact '{key}' is not under '{CheckpointPrefix}'.");
                if (key == CheckpointPrefix + CheckpointManifest.FileName)
                    hasSidecar = true;
            }
            if (m.Checksums.Count > 0 && !hasSidecar)
                throw new BackupArtifactException(
                    $"Backup {m.BackupId:N}: Full backup is missing its checkpoint manifest sidecar.");
        }
        else
        {
            // Exactly one partition_{id}.wal per range, and nothing else.
            HashSet<string> expectedSegments = new(StringComparer.Ordinal);
            foreach (PartitionBackupRange r in m.PartitionRanges)
                expectedSegments.Add($"partition_{r.PartitionId}.wal");

            if (m.Checksums.Count != expectedSegments.Count)
                throw new BackupArtifactException(
                    $"Backup {m.BackupId:N}: incremental has {m.Checksums.Count} artifact(s) for {expectedSegments.Count} partition range(s).");
            foreach (string key in m.Checksums.Keys)
                if (!expectedSegments.Contains(key))
                    throw new BackupArtifactException(
                        $"Backup {m.BackupId:N}: incremental artifact '{key}' does not match any declared partition range.");
        }
    }

    /// <summary>
    /// Validates artifact <em>content</em> against the manifest, after the files are byte-verified:
    /// a Full's checkpoint sidecar records the same applied HLC as the manifest's <c>BaseCut</c>; an
    /// Incremental's every <c>partition_{id}.wal</c> holds entries that are strictly index-ordered,
    /// HLC-monotonic, within the declared index range, and whose endpoints match the range's
    /// From/To HLC, ToIndex, and ToTerm — so replay cannot silently skip or misclassify data.
    /// </summary>
    private static void ValidateArtifactContent(BackupManifest m, string artifactPath, CancellationToken ct)
    {
        if (m.Type == BackupType.Full)
        {
            string checkpointDir = Path.Combine(artifactPath, "checkpoint");
            CheckpointManifest sidecar;
            try
            {
                sidecar = CheckpointManifest.ReadFrom(checkpointDir);
            }
            catch (Exception ex) when (ex is not BackupArtifactException)
            {
                throw new BackupArtifactException($"Backup {m.BackupId:N}: checkpoint manifest sidecar is unreadable.", ex);
            }

            if (m.BaseCut is null || sidecar.AppliedTime.CompareTo(m.BaseCut.Value) != 0)
                throw new BackupArtifactException(
                    $"Backup {m.BackupId:N}: checkpoint sidecar applied time {sidecar.AppliedTime} does not match the manifest BaseCut {m.BaseCut}.");
            return;
        }

        foreach (PartitionBackupRange r in m.PartitionRanges)
        {
            ct.ThrowIfCancellationRequested();
            string segPath = Path.Combine(artifactPath, $"partition_{r.PartitionId}.wal");

            // Stream the segment: validation only needs sequential access plus the first/last entry, so
            // the whole segment never has to be resident. A read/parse failure surfaces during iteration
            // (the reader is lazy) and is mapped to an "unreadable" error here.
            try
            {
                ValidateSegment(m.BackupId, r, WalSegmentEntry.ReadSegment(segPath));
            }
            catch (Exception ex) when (ex is not BackupArtifactException)
            {
                throw new BackupArtifactException($"Backup {m.BackupId:N}: WAL segment 'partition_{r.PartitionId}.wal' is unreadable.", ex);
            }
        }
    }

    private static void ValidateSegment(Guid backupId, PartitionBackupRange r, IEnumerable<WalSegmentEntry> entries)
    {
        long prevId = long.MinValue;
        HLCTimestamp prevTime = default;
        WalSegmentEntry? first = null;
        WalSegmentEntry? last = null;
        int i = 0;

        foreach (WalSegmentEntry e in entries)
        {
            if (e.Id <= prevId)
                throw new BackupArtifactException(
                    $"Backup {backupId:N}: partition {r.PartitionId} WAL segment is not strictly index-ordered at entry {i}.");
            if (e.Id < r.FromIndex || e.Id > r.ToIndex)
                throw new BackupArtifactException(
                    $"Backup {backupId:N}: partition {r.PartitionId} WAL segment entry index {e.Id} is outside the declared range [{r.FromIndex}, {r.ToIndex}].");
            if (i > 0 && e.Time.CompareTo(prevTime) < 0)
                throw new BackupArtifactException(
                    $"Backup {backupId:N}: partition {r.PartitionId} WAL segment HLC is not monotonic at entry {i}.");
            first ??= e;
            last = e;
            prevId = e.Id;
            prevTime = e.Time;
            i++;
        }

        if (first is null || last is null)
            throw new BackupArtifactException(
                $"Backup {backupId:N}: partition {r.PartitionId} declares a range but its WAL segment is empty.");

        if (first.Time.CompareTo(r.FromHlc) != 0)
            throw new BackupArtifactException(
                $"Backup {backupId:N}: partition {r.PartitionId} first WAL entry HLC {first.Time} does not match the declared FromHlc {r.FromHlc}.");
        if (last.Id != r.ToIndex)
            throw new BackupArtifactException(
                $"Backup {backupId:N}: partition {r.PartitionId} last WAL entry index {last.Id} does not match the declared ToIndex {r.ToIndex}.");
        if (last.Time.CompareTo(r.ToHlc) != 0)
            throw new BackupArtifactException(
                $"Backup {backupId:N}: partition {r.PartitionId} last WAL entry HLC {last.Time} does not match the declared ToHlc {r.ToHlc}.");
        if (last.Term != r.ToTerm)
            throw new BackupArtifactException(
                $"Backup {backupId:N}: partition {r.PartitionId} last WAL entry term {last.Term} does not match the declared ToTerm {r.ToTerm}.");
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

        // Check the enumeration root itself before descending — enumerating through a symlinked root
        // would follow it outside the intended tree. Children are checked as they are visited below.
        EnsureDirectoryNotReparsePoint(rootFull, $"Backup artifact root is a symlink/reparse point: '{rootFull}'.");

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
    /// Rejects a reparse point at the directory itself. Used to guard the per-backup artifact root and
    /// the staged checkpoint directory, which prior checks skipped (they walked ancestors up to, but
    /// not including, the root, and enumerated only children after pushing it). A non-existent path is
    /// not a reparse point and passes here — existence is checked by the caller.
    /// </summary>
    private static void EnsureDirectoryNotReparsePoint(string dir, string message)
    {
        if (Directory.Exists(dir) && (File.GetAttributes(dir) & FileAttributes.ReparsePoint) != 0)
            throw new BackupArtifactException(message);
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
