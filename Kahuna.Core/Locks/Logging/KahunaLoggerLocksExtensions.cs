
using Kahuna.Shared.Locks;

namespace Kahuna.Server.Locks.Logging;

public static partial class KahunaLoggerLocksExtensions
{
    [LoggerMessage(Level = LogLevel.Debug, Message = "LockActor Message: {Actor} {Type} {Resource} {Owner} {ExpiresMs} {Durability}")]
    public static partial void LogLocksActorEnter(this ILogger<IKahuna> logger, string actor, LockRequestType type, string resource, int? owner, int expiresMs, LockDurability durability);
    
    [LoggerMessage(Level = LogLevel.Debug, Message = "LockActor Took: {Actor} {Type} Key={Resource} Time={Elapsed}ms")]
    public static partial void LogLocksActorTook(this ILogger<IKahuna> logger, string actor, LockRequestType type, string resource, long elapsed);
}
