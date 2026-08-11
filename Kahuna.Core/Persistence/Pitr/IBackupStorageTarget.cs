namespace Kahuna.Server.Persistence.Pitr;

/// <summary>
/// Pluggable persistence layer for backup manifests.
/// The first implementation is a local directory; object-storage targets follow the same contract.
/// <para>
/// Every operation is asynchronous and cancellable because a target is not necessarily a filesystem:
/// a remote object store reaches it over the network, and a synchronous contract would force those
/// implementations to block a thread per call inside an otherwise-async backup path. The local
/// implementation is still I/O-bound on a disk, so it has real work to await too.
/// </para>
/// </summary>
public interface IBackupStorageTarget
{
    /// <summary>Persist <paramref name="manifest"/>, overwriting any existing entry with the same id.</summary>
    Task PutAsync(BackupManifest manifest, CancellationToken ct = default);

    /// <summary>
    /// Removes the manifest entry for <paramref name="backupId"/>. Idempotent: a no-op when the entry
    /// is already absent. Deleting the manifest tombstones the backup — it can no longer be listed,
    /// resolved, or restored — so callers remove the manifest <b>before</b> the artifacts, ensuring a
    /// crash mid-delete never leaves a manifest pointing at absent artifacts.
    /// </summary>
    Task DeleteAsync(Guid backupId, CancellationToken ct = default);

    /// <summary>Returns the manifest with <paramref name="backupId"/>, or null if not found.</summary>
    Task<BackupManifest?> GetAsync(Guid backupId, CancellationToken ct = default);

    /// <summary>Returns all manifests in the target in arbitrary order.</summary>
    Task<IReadOnlyList<BackupManifest>> ListAsync(CancellationToken ct = default);

    /// <summary>
    /// Returns the identity and a human-readable reason for every manifest entry that could not be
    /// read or parsed, so a corrupt/partially-written manifest is surfaced as invalid rather than
    /// silently omitted from <see cref="ListAsync"/>.
    /// </summary>
    Task<IReadOnlyList<(Guid backupId, string reason)>> ListCorruptAsync(CancellationToken ct = default);

    /// <summary>
    /// Returns the id of <b>every</b> manifest entry present in the target — parseable or not,
    /// readable or not — derived from the entry's identity alone without reading its contents. This is
    /// the authoritative set of "a backup exists with this id" and must be used to protect artifact
    /// directories from orphan reclamation: a directory whose manifest is corrupt or transiently
    /// unreadable is still owned and must never be swept as an orphan.
    /// </summary>
    Task<IReadOnlyList<Guid>> ListManifestIdsAsync(CancellationToken ct = default);
}
