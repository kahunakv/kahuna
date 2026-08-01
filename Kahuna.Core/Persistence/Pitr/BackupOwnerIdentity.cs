
namespace Kahuna.Server.Persistence.Pitr;

/// <summary>
/// The cluster-ownership and topology identity stamped onto a <see cref="BackupManifest"/> at write
/// time. Every field is optional: the producer supplies what it genuinely knows now (the storage
/// engine type/revision and the producing node/term are always known; the cluster id and topology
/// generation are populated once the cluster-ownership coordinator and topology-capture paths supply
/// them). Fields left null are recorded as "unknown" on the manifest and are skipped by chain
/// resolution rather than forced to match, so partially-known identities never falsely reject a chain.
/// </summary>
internal readonly record struct BackupOwnerIdentity(
    string? ClusterId,
    string? CoordinatorNode,
    long? CoordinatorTerm,
    string? StorageType,
    string? StorageRevision,
    long? TopologyGeneration);
