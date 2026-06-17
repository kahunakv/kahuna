
using Kahuna.Shared.Locks;

namespace Kahuna.Server.Locks.Logging;

public static partial class KahunaLoggerLocksExtensions
{
    [LoggerMessage(Level = LogLevel.Debug, Message = "LockActor Message: {Actor} {Type} {Resource} {Owner} {ExpiresMs} {Durability}")]
    public static partial void LogLocksActorEnter(this ILogger<IKahuna> logger, string actor, LockRequestType type, string resource, int? owner, int expiresMs, LockDurability durability);

    [LoggerMessage(Level = LogLevel.Debug, Message = "LockActor Took: {Actor} {Type} Key={Resource} Time={Elapsed}ms")]
    public static partial void LogLocksActorTook(this ILogger<IKahuna> logger, string actor, LockRequestType type, string resource, long elapsed);

    // ── LockLocator redirect logs ──────────────────────────────────────────

    [LoggerMessage(Level = LogLevel.Debug, Message = "LOCK Redirect {LockName} to leader partition {Partition} at {Leader}")]
    public static partial void LogLockRedirect(this ILogger<IKahuna> logger, string lockName, int partition, string leader);

    [LoggerMessage(Level = LogLevel.Debug, Message = "EXTEND-LOCK Redirect {LockName} to leader partition {Partition} at {Leader}")]
    public static partial void LogExtendLockRedirect(this ILogger<IKahuna> logger, string lockName, int partition, string leader);

    [LoggerMessage(Level = LogLevel.Debug, Message = "GET-LOCK Redirect {LockName} to leader partition {Partition} at {Leader}")]
    public static partial void LogGetLockRedirect(this ILogger<IKahuna> logger, string lockName, int partition, string leader);

    // ── LockActor eviction log ─────────────────────────────────────────────

    [LoggerMessage(Level = LogLevel.Debug, Message = "Evicted {Count} key/value pairs")]
    public static partial void LogLocksActorEviction(this ILogger<IKahuna> logger, int count);
}
