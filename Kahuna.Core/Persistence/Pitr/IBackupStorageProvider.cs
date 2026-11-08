namespace Kahuna.Server.Persistence.Pitr;

/// <summary>
/// A place to keep backups, chosen by its <see cref="TargetName"/> (the <c>--pitr-backup-target</c> option).
/// Registered in <see cref="Providers.StorageProviders"/>: Kahuna registers <c>"local"</c>, and an extension
/// assembly or an embedding host registers the others before the node starts.
///
/// <para>A provider lives in its own assembly when it needs a cloud SDK. <c>Kahuna.Core</c> never
/// references one, because every host that embeds the core would inherit it.</para>
///
/// <para>This interface, <see cref="IBackupStorage"/>, <see cref="IBackupStorageTarget"/> and
/// <see cref="IBackupArtifactStore"/> are a binary contract: a provider is compiled apart from the server
/// that loads it. A member added to one of them needs a default body.</para>
/// </summary>
public interface IBackupStorageProvider
{
    /// <summary>The target name this provider answers to, such as <c>"local"</c> or <c>"gcs"</c>. Compared ordinally.</summary>
    string TargetName { get; }

    /// <summary>
    /// True when backup bytes pass through a local scratch directory before they reach the target. A
    /// persistence backend writes a checkpoint only through the filesystem, so every target that is not a
    /// local directory needs one. The node refuses to start with such a target and no
    /// <c>--pitr-backup-scratch-dir</c>, so the operator sizes that volume before the first backup fills it.
    /// </summary>
    bool RequiresLocalScratch { get; }

    /// <summary>
    /// Reads and checks the settings in <paramref name="context"/>, and returns the storage that
    /// <see cref="IBackupStorage.StartAsync"/> then connects. Does no I/O. Throws
    /// <see cref="KahunaServerException"/> for a setting it cannot use.
    /// </summary>
    IBackupStorage Open(BackupStorageContext context);
}
