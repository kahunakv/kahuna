
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace Kahuna.Server.KeyValues.Transactions.Data;

/// <summary>
/// The cached result of a transaction write operation (set/delete/extend), stored in the operation
/// registry so a retry carrying the same operation ID receives the identical answer without applying
/// the mutation a second time. These three fields are the full response shape of a KV write.
/// </summary>
internal readonly record struct CachedOperationResponse(
    KeyValueResponseType Type,
    long Revision,
    HLCTimestamp CommitTimestamp);
