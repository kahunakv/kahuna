namespace Kahuna.Server.Persistence.Pitr;

/// <summary>
/// The backup storage of one node, or of one PITR bootstrap, from <see cref="IBackupStorageProvider.Open"/>.
/// Its owner calls <see cref="StartAsync"/>, then uses the two stores, then disposes it.
/// <para><see cref="IDisposable.Dispose"/> can run while <see cref="StartAsync"/> is still in progress, when the
/// node stops during its start. An implementation then releases what that start builds.</para>
/// <para>See <see cref="IBackupStorageProvider"/> for the binary-contract rule.</para>
/// </summary>
public interface IBackupStorage : IDisposable
{
    /// <summary>
    /// Connects to the target and checks it: credentials, the protections a bucket must have, the permissions
    /// of a local root. A node calls it before it joins its cluster, so a target that cannot work stops the
    /// start instead of failing at the first scheduled backup. After a failure the owner can call it again.
    /// </summary>
    Task StartAsync(CancellationToken ct);

    /// <summary>The manifest store. Valid after <see cref="StartAsync"/> completes.</summary>
    IBackupStorageTarget Manifests { get; }

    /// <summary>The artifact store. Valid after <see cref="StartAsync"/> completes.</summary>
    IBackupArtifactStore Artifacts { get; }
}
