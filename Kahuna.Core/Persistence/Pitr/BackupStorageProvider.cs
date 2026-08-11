namespace Kahuna.Server.Persistence.Pitr;

/// <summary>
/// Everything a storage provider needs to build a target, without reaching into the rest of the
/// configuration.
/// </summary>
/// <param name="Target">
/// The configured target name, so one provider can serve several kinds and dispatch on it.
/// </param>
/// <param name="BackupDir">
/// The configured backup root. For a local target this is where artifacts and manifests live; for a
/// remote one it is still the host-side root, and the fallback the default implementations would use.
/// </param>
/// <param name="ScratchDir">
/// Local directory that backup bytes may transit, or null when none was configured. A provider whose
/// store reports <see cref="BackupArtifactStoreCapabilities.RequiresLocalScratch"/> must be given one —
/// startup validation refuses the combination rather than letting the first backup fail.
/// </param>
public readonly record struct BackupStorageContext(string Target, string BackupDir, string? ScratchDir);

/// <summary>
/// Builds the pair of stores a backup service needs: one for manifests, one for artifact bytes.
/// <para>
/// This is the extension point for object-storage targets. <c>Kahuna.Core</c> must never reference a
/// cloud SDK — CamusDB embeds it, so a package reference here is inherited by every embedded consumer —
/// so an implementation lives in its own assembly and a host installs it by assigning
/// <c>KahunaConfiguration.BackupStorageProvider</c>.
/// </para>
/// </summary>
public delegate BackupStoragePair BackupStorageProvider(BackupStorageContext context);

/// <summary>The manifest target and artifact store a backup service runs against.</summary>
public readonly record struct BackupStoragePair(
    IBackupStorageTarget Manifests,
    IBackupArtifactStore Artifacts);
