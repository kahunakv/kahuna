using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace Kahuna.Server.KeyValues;

/*public class KeyValueRequestPool
{
    
}*/

internal static class KeyValueRequestPool
{
    /// <summary>
    /// Upper bound on requests retained per thread. Rent and return frequently land on different
    /// thread-pool threads (a request is rented before async work and returned after), so an
    /// unbounded per-thread stack would let a traffic burst pin its peak object population on every
    /// thread indefinitely. Returns beyond this cap drop the object for the GC to reclaim.
    /// </summary>
    private const int MaxPooledPerThread = 1024;

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

        // Bounded retention: beyond the cap the object is dropped rather than pinned on this thread's
        // stack. Pooling is only an optimization, so discarding an excess request is always safe.
        if (_poolRequests.Count < MaxPooledPerThread)
            _poolRequests.Push(obj);
    }
}