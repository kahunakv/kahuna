using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace Kahuna.Server.KeyValues;

/*public class KeyValueRequestPool
{
    
}*/

internal static class KeyValueRequestPool
{
    [ThreadStatic]
    private static Stack<KeyValueRequest>? _poolRequests;
    
    public static KeyValueRequest Rent(
        KeyValueRequestType type,
        HLCTimestamp transactionId,
        HLCTimestamp commitId,
        string key, 
        byte[]? value,
        byte[]? compareValue,
        long compareRevision,
        KeyValueFlags flags,
        int expiresMs, 
        HLCTimestamp proposalTicketId,
        KeyValueDurability durability,
        int proposalId, 
        int partitionId,
        TaskCompletionSource<KeyValueResponse?>? promise
    )
    {
        _poolRequests ??= new();
        
        if (_poolRequests.Count > 0)
        {
            KeyValueRequest request = _poolRequests.Pop();
            
            request.Reset(
                type,
                transactionId,
                commitId,
                key, 
                value,
                compareValue,
                compareRevision,
                flags,
                expiresMs, 
                proposalTicketId,
                durability,
                proposalId, 
                partitionId,
                promise
            );
            
            return request;
        }
        
        return new(
            type,
            transactionId,
            commitId,
            key, 
            value,
            compareValue,
            compareRevision,
            flags,
            expiresMs, 
            proposalTicketId,
            durability,
            proposalId, 
            partitionId,
            promise
        );
    }
    
    /// <summary>
    /// Rents a <see cref="KeyValueRequest"/> pre-populated for a GetByRange scan.
    /// </summary>
    public static KeyValueRequest RentRange(
        HLCTimestamp transactionId,
        string prefix,
        string? startKey,
        bool startInclusive,
        string? endKey,
        bool endInclusive,
        int limit,
        HLCTimestamp readTimestamp,
        KeyValueDurability durability,
        TaskCompletionSource<KeyValueResponse?>? promise)
    {
        KeyValueRequest request = Rent(
            KeyValueRequestType.GetByRange,
            transactionId,
            HLCTimestamp.Zero,
            prefix,
            null,
            null,
            0,
            KeyValueFlags.None,
            0,
            HLCTimestamp.Zero,
            durability,
            0,
            0,
            promise
        );

        request.StartKey       = startKey;
        request.StartInclusive = startInclusive;
        request.EndKey         = endKey;
        request.EndInclusive   = endInclusive;
        request.Limit          = limit;
        request.ReadTimestamp  = readTimestamp;

        return request;
    }

    public static void Return(KeyValueRequest obj)
    {
        obj.Clear();
        _poolRequests ??= new();
        _poolRequests.Push(obj);
    }
}