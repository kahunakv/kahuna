
namespace Kahuna.Server.Persistence.Pitr;

/// <summary>
/// Thrown when a backup's on-disk artifacts do not match its manifest: a declared file is missing,
/// truncated, padded, fails its recorded SHA-256 digest, or an unexpected extra file is present in
/// the artifact directory. Distinct from <see cref="BackupChainException"/> (structural chain
/// problems) so callers can classify integrity failures separately.
/// </summary>
internal sealed class BackupArtifactException : Exception
{
    public BackupArtifactException(string message) : base(message) { }

    public BackupArtifactException(string message, Exception innerException) : base(message, innerException) { }
}
