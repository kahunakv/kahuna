
using Kahuna.Server.Persistence.Backend;

namespace Kahuna.Server.Persistence.Logging;

/// <summary>Source-generated log messages of the storage-integrity paths: the store's fail-stop, the node-level gate and the identity check.</summary>
internal static partial class StorageIntegrityLog
{
    [LoggerMessage(Level = LogLevel.Critical, Message = "RocksDb store at {Path}/{Revision} fail-stopped on storage damage; it refuses every read and write from now on and will not be reopened (the files are left untouched for forensics): {Evidence}")]
    internal static partial void LogStoreFailStop(this ILogger logger, Exception? cause, string path, string revision, string evidence);

    [LoggerMessage(Level = LogLevel.Warning, Message = "RocksDb store at {Path}/{Revision} is fail-stopped on storage damage and will not be reopened: {Reason}")]
    internal static partial void LogStoreRecoveryDeclinedOnDamage(this ILogger logger, string path, string revision, string reason);

    [LoggerMessage(Level = LogLevel.Critical, Message = "The key-value store of node {Endpoint} is fail-stopped on storage damage; the node withholds its candidacy for every partition it hosts, steps down where it leads, and answers StorageCorrupt until an operator repairs or replaces the store and restarts it: {Failure}")]
    internal static partial void LogNodeStoreFailStop(this ILogger logger, string endpoint, string failure);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Node {Endpoint} stepped down from partition #{PartitionId}: its store is fail-stopped on storage damage")]
    internal static partial void LogSteppedDownOnStoreFailStop(this ILogger logger, string endpoint, int partitionId);

    [LoggerMessage(Level = LogLevel.Error, Message = "Node {Endpoint} could not step down from partition #{PartitionId} after its store fail-stopped ({Status}); it will try again on the next writer tick")]
    internal static partial void LogStepDownOnStoreFailStopRefused(this ILogger logger, string endpoint, int partitionId, string status);

    [LoggerMessage(Level = LogLevel.Error, Message = "Containing the fail-stopped store of node {Endpoint} failed; it will be retried on the next writer tick")]
    internal static partial void LogStoreFailStopContainmentFailed(this ILogger logger, Exception exception, string endpoint);

    [LoggerMessage(Level = LogLevel.Information, Message = "The store at {StoragePath} predates the store identity; recorded it as {Identity}")]
    internal static partial void LogStoreIdentityStampedOnExistingStore(this ILogger logger, string storagePath, PersistentStoreIdentity identity);

    [LoggerMessage(Level = LogLevel.Information, Message = "Created the store at {StoragePath} as {Identity}")]
    internal static partial void LogStoreCreated(this ILogger logger, string storagePath, PersistentStoreIdentity identity);

    [LoggerMessage(Level = LogLevel.Critical, Message = "{Notice} The store was recorded as {Identity}.")]
    internal static partial void LogStoreReplacedAtStart(this ILogger logger, string notice, PersistentStoreIdentity identity);
}
