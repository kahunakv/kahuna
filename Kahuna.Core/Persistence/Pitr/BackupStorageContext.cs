using Kahuna.Server.Configuration;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Persistence.Pitr;

/// <summary>What <see cref="IBackupStorageProvider.Open"/> opens a node's backup storage with.</summary>
public sealed class BackupStorageContext
{
    /// <summary>
    /// The backup directory (<c>--pitr-backup-dir</c>). A local target keeps its manifests and artifacts
    /// here. A remote target can keep host-side state here. Kahuna checks its permissions before
    /// <see cref="IBackupStorageProvider.Open"/> runs.
    /// </summary>
    public required string BackupDir { get; init; }

    /// <summary>
    /// The local directory that backup bytes pass through, or null when none is configured. Never null for
    /// a provider whose <see cref="IBackupStorageProvider.RequiresLocalScratch"/> is true. Kahuna checks its
    /// permissions and creates it owner-only before <see cref="IBackupStorageProvider.Open"/> runs.
    /// </summary>
    public string? ScratchDir { get; init; }

    /// <summary>The node's configuration, for the store settings a provider reads.</summary>
    public required KahunaConfiguration Configuration { get; init; }

    /// <summary>The node's logger.</summary>
    public required ILogger Logger { get; init; }

    /// <summary>
    /// The node's settings for its extensions, from <c>--extension-option key=value</c>; empty when none are
    /// given. A provider reads its own keys, which by convention start with its target name, and refuses a
    /// value it cannot use.
    /// </summary>
    public IReadOnlyDictionary<string, string> ExtensionOptions { get; init; } = new Dictionary<string, string>(StringComparer.Ordinal);
}
