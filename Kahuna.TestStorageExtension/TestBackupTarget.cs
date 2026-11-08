using Kahuna.Server.Persistence.Pitr;

namespace Kahuna.TestStorageExtension;

/// <summary>
/// The backup target the extension-assembly tests load as <c>--pitr-backup-target test-remote</c>. It keeps
/// manifests and artifacts in the directory that the extension option <c>test-remote.root</c> names, not in
/// the backup directory, so a test can tell that the node used this target. It needs a scratch directory,
/// like a bucket target.
/// </summary>
public sealed class TestBackupTarget : IBackupStorageProvider
{
    /// <summary>The target name.</summary>
    public const string Name = "test-remote";

    /// <summary>The extension option that names the directory the target keeps its bytes in.</summary>
    public const string RootOption = Name + ".root";

    public string TargetName => Name;

    public bool RequiresLocalScratch => true;

    public IBackupStorage Open(BackupStorageContext context)
    {
        foreach (string key in context.ExtensionOptions.Keys)
            if (key.StartsWith(Name + ".", StringComparison.Ordinal) && !string.Equals(key, RootOption, StringComparison.Ordinal))
                throw new KahunaServerException($"Unknown extension option '{key}' for backup target '{Name}'.");

        if (!context.ExtensionOptions.TryGetValue(RootOption, out string? root) || string.IsNullOrWhiteSpace(root))
            throw new KahunaServerException($"Backup target '{Name}' needs --extension-option {RootOption}=<directory>.");

        return new Storage(root);
    }

    private sealed class Storage(string root) : IBackupStorage
    {
        private IBackupStorageTarget? manifests;

        private IBackupArtifactStore? artifacts;

        public IBackupStorageTarget Manifests => manifests ?? throw new InvalidOperationException("Not started.");

        public IBackupArtifactStore Artifacts => artifacts ?? throw new InvalidOperationException("Not started.");

        public Task StartAsync(CancellationToken ct)
        {
            manifests ??= new LocalDirectoryStorageTarget(root);
            artifacts ??= new LocalDirectoryArtifactStore(root);
            return Task.CompletedTask;
        }

        public void Dispose()
        {
        }
    }
}
