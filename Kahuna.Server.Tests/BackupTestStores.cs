
using Kahuna.Server.Persistence.Pitr;

namespace Kahuna.Server.Tests;

/// <summary>
/// Helpers for tests that drive the backup paths directly. The production code takes an
/// <see cref="IBackupArtifactStore"/> rather than a directory path, but these tests also inspect and
/// tamper with the on-disk artifacts, so they keep the root path and wrap it here at the call site.
/// </summary>
internal static class BackupTestStores
{
    /// <summary>A local artifact store rooted at <paramref name="root"/>.</summary>
    internal static LocalDirectoryArtifactStore Artifacts(string root) => new(root);

    /// <summary>The local manifest target rooted at <paramref name="root"/>.</summary>
    internal static LocalDirectoryStorageTarget Manifests(string root) => new(root);
}
