
namespace Kahuna.Server.Communication.Internode.Grpc;

/// <summary>
/// Provides thread-local pooling of `GrpcServerBatcherItem` lists for optimized memory usage
/// and reduced allocations during gRPC batch processing.
/// </summary>
/// <remarks>
/// `GrpcBatcherPool` maintains a pool of reusable `List<GrpcServerBatcherItem>` instances
/// to minimize memory pressure and improve performance in scenarios where batch operations
/// are frequently created and disposed, as seen in the `GrpcBatcher` class.
/// This class leverages the `[ThreadStatic]` attribute to create thread-local storage
/// for the pool, ensuring that each thread operates independently to avoid contention
/// and synchronization overhead.
/// </remarks>
internal static class GrpcServerBatcherPool
{
    [ThreadStatic]
    private static Stack<List<GrpcServerBatcherItem>>? _poolBatcherItems;
    
    public static List<GrpcServerBatcherItem> Rent(int capacity)
    {
        _poolBatcherItems ??= new();
        if (_poolBatcherItems.Count > 0)
        {
            List<GrpcServerBatcherItem> rented = _poolBatcherItems.Pop();
            return rented;
        }
        
        return new(capacity);
    }
    
    public static void Return(List<GrpcServerBatcherItem> obj)
    {
        obj.Clear();
        _poolBatcherItems ??= new();
        _poolBatcherItems.Push(obj);
    }
}