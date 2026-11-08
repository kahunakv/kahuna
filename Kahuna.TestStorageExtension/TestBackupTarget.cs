using Kahuna.Server.Persistence.Pitr;

namespace Kahuna.TestStorageExtension;

/// <summary>
/// The backup target the extension-assembly tests load as <c>--pitr-backup-target test-remote</c>. It keeps
/// manifests and artifacts in the directory that the extension option <c>test-remote.root</c> names, not in
/// the backup directory, so a test can tell that the node used this target. It needs a scratch directory,
/// like a bucket target. With <c>test-remote.fail-start=true</c> its start fails, the way a bucket target
/// fails when the bucket cannot be reached, so a test can stop a node at that point without a network.
/// </summary>
public sealed class TestBackupTarget : IBackupStorageProvider
{
    /// <summary>The target name.</summary>
    public const string Name = "test-remote";

    /// <summary>The extension option that names the directory the target keeps its bytes in.</summary>
    public const string RootOption = Name + ".root";

    /// <summary>The extension option that makes the start of the storage fail.</summary>
    public const string FailStartOption = Name + ".fail-start";

    /// <summary>The message of the start failure that <see cref="FailStartOption"/> causes.</summary>
    public const string StartFailureMessage = "The test-remote backup target refused to start, as configured.";

    public string TargetName => Name;

    public bool RequiresLocalScratch => true;

    public IBackupStorage Open(BackupStorageContext context)
    {
        foreach (string key in context.ExtensionOptions.Keys)
            if (key.StartsWith(Name + ".", StringComparison.Ordinal)
                && !string.Equals(key, RootOption, StringComparison.Ordinal)
                && !string.Equals(key, FailStartOption, StringComparison.Ordinal))
                throw new KahunaServerException($"Unknown extension option '{key}' for backup target '{Name}'.");

        if (!context.ExtensionOptions.TryGetValue(RootOption, out string? root) || string.IsNullOrWhiteSpace(root))
            throw new KahunaServerException($"Backup target '{Name}' needs --extension-option {RootOption}=<directory>.");

        bool failStart = context.ExtensionOptions.TryGetValue(FailStartOption, out string? fail)
                         && string.Equals(fail, "true", StringComparison.Ordinal);

        return new Storage(root, failStart);
    }

    private sealed class Storage(string root, bool failStart) : IBackupStorage
    {
        private IBackupStorageTarget? manifests;

        private IBackupArtifactStore? artifacts;

        public IBackupStorageTarget Manifests => manifests ?? throw new InvalidOperationException("Not started.");

        public IBackupArtifactStore Artifacts => artifacts ?? throw new InvalidOperationException("Not started.");

        public Task StartAsync(CancellationToken ct)
        {
            if (failStart)
                throw new KahunaServerException(StartFailureMessage);

            manifests ??= new LocalDirectoryStorageTarget(root);
            artifacts ??= new LocalDirectoryArtifactStore(root);
            return Task.CompletedTask;
        }

        public void Dispose()
        {
        }
    }
}
