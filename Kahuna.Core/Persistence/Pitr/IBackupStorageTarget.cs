
namespace Kahuna.Server.Persistence.Pitr;

/// <summary>
/// Pluggable persistence layer for backup manifests.
/// The first implementation is a local directory; object-storage targets follow the same contract.
/// </summary>
internal interface IBackupStorageTarget
{
    /// <summary>Persist <paramref name="manifest"/>, overwriting any existing entry with the same id.</summary>
    void Put(BackupManifest manifest);

    /// <summary>Returns the manifest with <paramref name="backupId"/>, or null if not found.</summary>
    BackupManifest? Get(Guid backupId);

    /// <summary>Returns all manifests in the target in arbitrary order.</summary>
    IReadOnlyList<BackupManifest> List(CancellationToken ct = default);

    /// <summary>
    /// Returns the identity and a human-readable reason for every manifest entry that could not be
    /// read or parsed, so a corrupt/partially-written manifest is surfaced as invalid rather than
    /// silently omitted from <see cref="List"/>.
    /// </summary>
    IReadOnlyList<(Guid backupId, string reason)> ListCorrupt(CancellationToken ct = default);
}
