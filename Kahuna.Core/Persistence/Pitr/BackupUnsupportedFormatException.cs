
namespace Kahuna.Server.Persistence.Pitr;

/// <summary>
/// Thrown when a backup manifest is in a legacy or otherwise unsupported format that the current
/// verifier cannot validate against its exact-file-set / coverage rules (for example a pre-hardening
/// full backup that hashed only the checkpoint sidecar). Distinct from
/// <see cref="BackupArtifactException"/> so a legacy artifact is reported as <em>unsupported</em>,
/// never as <em>corrupt</em>.
/// </summary>
internal sealed class BackupUnsupportedFormatException : Exception
{
    public BackupUnsupportedFormatException(string message) : base(message) { }
}
