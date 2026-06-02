
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace Kahuna.Server.KeyValues.Logging;

public static partial class KahunaLoggerExtensions
{
    [LoggerMessage(Level = LogLevel.Debug, Message = "KeyValueActor Message: {Actor} {Type} Key={Key} {Value} Expires={ExpiresMs} Flags={Flags} Revision={Revision} TxId={TransactionId} {Durability}")]
    public static partial void LogKeyValueActorEnter(this ILogger<IKahuna> logger, string actor, KeyValueRequestType type, string key, int? value, int expiresMs, KeyValueFlags flags, long revision, HLCTimestamp transactionId, KeyValueDurability durability);
    
    [LoggerMessage(Level = LogLevel.Debug, Message = "KeyValueActor Took: {Actor} {Type} Key={Key} Response={Response} Revision={Revision} Time={Elapsed}ms")]
    public static partial void LogKeyValueActorTook(this ILogger<IKahuna> logger, string actor, KeyValueRequestType type, string key, KeyValueResponseType? response, long? revision, long elapsed);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Evicted {Count} key/value pairs (garbage={Garbage}, lru={Lru}, metadataTrimmed={MetadataTrimmed}, storeCount={StoreCount}, storeBytes={StoreBytes}, elapsedMs={ElapsedMs}, backlog={Backlog})")]
    public static partial void LogKeyValueEviction(
        this ILogger<IKahuna> logger,
        int count,
        int garbage,
        int lru,
        int metadataTrimmed,
        int storeCount,
        long storeBytes,
        long elapsedMs,
        bool backlog
    );
}