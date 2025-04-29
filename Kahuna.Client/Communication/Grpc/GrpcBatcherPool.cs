
/**
 * This file is part of Kahuna
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace Kahuna.Client.Communication;

/// <summary>
/// Provides thread-local pooling of `GrpcBatcherItem` lists for optimized memory usage
/// and reduced allocations during gRPC batch processing.
/// </summary>
/// <remarks>
/// `GrpcBatcherPool` maintains a pool of reusable `List<GrpcBatcherItem>` instances
/// to minimize memory pressure and improve performance in scenarios where batch operations
/// are frequently created and disposed, as seen in the `GrpcBatcher` class.
/// This class leverages the `[ThreadStatic]` attribute to create thread-local storage
/// for the pool, ensuring that each thread operates independently to avoid contention
/// and synchronization overhead.
/// </remarks>
internal static class GrpcBatcherPool
{
    [ThreadStatic]
    private static Stack<List<GrpcBatcherItem>>? _poolBatcherItems;
    
    public static List<GrpcBatcherItem> Rent(int capacity)
    {
        _poolBatcherItems ??= new();
        if (_poolBatcherItems.Count > 0)
        {
            List<GrpcBatcherItem> rented = _poolBatcherItems.Pop();
            return rented;
        }
        
        return new(capacity);
    }
    
    public static void Return(List<GrpcBatcherItem> obj)
    {
        obj.Clear();
        _poolBatcherItems ??= new();
        _poolBatcherItems.Push(obj);
    }
}