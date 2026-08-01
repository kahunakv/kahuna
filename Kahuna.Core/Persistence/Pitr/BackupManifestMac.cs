
using System.Globalization;
using System.Security.Cryptography;
using System.Text;

namespace Kahuna.Server.Persistence.Pitr;

/// <summary>
/// Keyed authentication (HMAC-SHA-256) of a <see cref="BackupManifest"/> with a server-held key that is
/// never written beside the artifacts. The tag covers the manifest's identity, WAL coverage, base cut,
/// and the full per-file digest+size map — so tampering with any recorded digest, coverage bound, parent
/// link, or identity, or swapping a file for another whose digest is then rewritten, breaks the tag.
/// Plain SHA-256 alone cannot detect this because an attacker who rewrites a file also rewrites its
/// (unkeyed) digest in the same plaintext manifest.
/// </summary>
internal static class BackupManifestMac
{
    /// <summary>Computes the hex HMAC-SHA-256 tag over the manifest's canonical authenticated payload.</summary>
    public static string Compute(BackupManifest m, byte[] key)
    {
        using HMACSHA256 hmac = new(key);
        byte[] tag = hmac.ComputeHash(Encoding.UTF8.GetBytes(CanonicalPayload(m)));
        return Convert.ToHexString(tag).ToLowerInvariant();
    }

    /// <summary>Stamps the MAC onto <paramref name="m"/> when a key is configured; a no-op otherwise.</summary>
    public static void Sign(BackupManifest m, byte[]? key)
    {
        if (key is not null)
            m.Mac = Compute(m, key);
    }

    /// <summary>
    /// Enforces authenticity when a key is configured: the manifest must carry a MAC that matches. A
    /// missing MAC (e.g. an unsigned/legacy backup, or one an attacker stripped) fails just like a
    /// mismatched one — so enabling the key means backups taken before it was configured must be
    /// re-taken. When no key is configured this is a no-op (authenticity is not enforced).
    /// </summary>
    /// <exception cref="BackupArtifactException">The manifest is unauthenticated or the MAC does not match.</exception>
    public static void Verify(BackupManifest m, byte[]? key)
    {
        if (key is null)
            return;

        if (string.IsNullOrEmpty(m.Mac))
            throw new BackupArtifactException(
                $"Backup {m.BackupId:N} is not authenticated (no MAC) but a backup authentication key is " +
                "configured; it may be an unsigned/legacy backup or the tag was removed.");

        byte[] expected = Encoding.UTF8.GetBytes(Compute(m, key));
        byte[] actual = Encoding.UTF8.GetBytes(m.Mac.ToLowerInvariant());
        if (!CryptographicOperations.FixedTimeEquals(expected, actual))
            throw new BackupArtifactException($"Backup {m.BackupId:N} failed MAC authentication.");
    }

    /// <summary>
    /// A deterministic, injective-enough string over every security-relevant manifest field. Field values
    /// are length-free-of-ambiguity by using fixed separators and sorting the digest/size maps by key with
    /// ordinal order, so two nodes (or a re-serialization) produce the same bytes for the same content.
    /// The <see cref="BackupManifest.Mac"/> field itself is deliberately excluded.
    /// </summary>
    private static string CanonicalPayload(BackupManifest m)
    {
        StringBuilder sb = new();
        void Field(string name, string? value)
        {
            sb.Append(name).Append('=').Append(value ?? "\0null").Append('\n');
        }

        Field("formatVersion", m.FormatVersion.ToString(CultureInfo.InvariantCulture));
        Field("backupId", m.BackupId.ToString("N"));
        Field("type", ((int)m.Type).ToString(CultureInfo.InvariantCulture));
        Field("parent", m.ParentBackupId?.ToString("N"));
        Field("createdAtUtc", m.CreatedAtUtc.ToUniversalTime().ToString("O", CultureInfo.InvariantCulture));

        Field("clusterId", m.ClusterId);
        Field("coordinatorNode", m.CoordinatorNode);
        Field("coordinatorTerm", m.CoordinatorTerm?.ToString(CultureInfo.InvariantCulture));
        Field("storageType", m.StorageType);
        Field("storageRevision", m.StorageRevision);
        Field("topologyGeneration", m.TopologyGeneration?.ToString(CultureInfo.InvariantCulture));

        Field("baseCutNode", m.BaseCutNode?.ToString(CultureInfo.InvariantCulture));
        Field("baseCutPhysical", m.BaseCutPhysical?.ToString(CultureInfo.InvariantCulture));
        Field("baseCutCounter", m.BaseCutCounter?.ToString(CultureInfo.InvariantCulture));
        Field("snapNode", m.ClusterSnapshotNode?.ToString(CultureInfo.InvariantCulture));
        Field("snapPhysical", m.ClusterSnapshotPhysical?.ToString(CultureInfo.InvariantCulture));
        Field("snapCounter", m.ClusterSnapshotCounter?.ToString(CultureInfo.InvariantCulture));

        sb.Append("ranges:\n");
        foreach (PartitionBackupRange r in m.PartitionRanges.OrderBy(x => x.PartitionId))
            sb.Append(r.PartitionId).Append(':')
              .Append(r.FromIndex).Append(',').Append(r.ToIndex).Append(',')
              .Append(r.FromHlc).Append(',').Append(r.ToHlc).Append(',').Append(r.ToTerm).Append('\n');

        sb.Append("files:\n");
        foreach (string key in m.Checksums.Keys.OrderBy(k => k, StringComparer.Ordinal))
        {
            m.Sizes.TryGetValue(key, out long size);
            sb.Append(key).Append('=').Append(m.Checksums[key]).Append(';')
              .Append(size.ToString(CultureInfo.InvariantCulture)).Append('\n');
        }

        return sb.ToString();
    }
}
