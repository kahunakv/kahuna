
namespace Kahuna.Server.Persistence.Pitr;

/// <summary>
/// Thrown by <see cref="BackupCatalog.Validate"/> when a chain of backup manifests is
/// structurally invalid (broken parent link, index gap, wrong ordering, etc.).
/// </summary>
internal sealed class BackupChainException : Exception
{
    public BackupChainException(string message) : base(message) { }
}
