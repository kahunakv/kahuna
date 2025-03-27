using System.Collections.Concurrent;

namespace Kahuna.Server.Persistence;

public class CustomIOThreadPool
{
    // Thread-safe queue to store tasks
    private readonly BlockingCollection<Action> _taskQueue;
    
    // Number of worker threads in the pool
    private readonly int _workerCount;
    
    // Cancellation support
    private readonly CancellationTokenSource _cancellationTokenSource;
    
    // Threads for processing tasks
    private readonly Thread[] _workerThreads;

    public CustomIOThreadPool(int workerCount = 4)
    {
        _workerCount = workerCount;
        _taskQueue = new();
        _cancellationTokenSource = new();
        _workerThreads = new Thread[_workerCount];
    }

    public Task<T> EnqueueTask<T>(Func<T> syncOperation)
    {
        // Create a TaskCompletionSource to provide async notification
        TaskCompletionSource<T> tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);

        // Enqueue the task
        _taskQueue.Add(() =>
        {
            try
            {
                // Execute the synchronous operation
                T result = syncOperation();
                
                // Set the result on the task completion source
                tcs.TrySetResult(result);
            }
            catch (Exception ex)
            {
                // Handle any exceptions
                tcs.TrySetException(ex);
            }
        });

        return tcs.Task;
    }

    public void Start()
    {
        for (int i = 0; i < _workerCount; i++)
        {
            int workerId = i;
            
            _workerThreads[i] = new(() =>
            {
                foreach (var task in _taskQueue.GetConsumingEnumerable(_cancellationTokenSource.Token))
                {
                    try
                    {
                        // Execute the task
                        task();
                    }
                    catch (OperationCanceledException)
                    {
                        break;
                    }
                    catch (Exception ex)
                    {
                        // Optional: Log or handle unexpected exceptions
                        Console.Error.WriteLine($"Worker {workerId} encountered an error: {ex}");
                    }
                }
            })
            {
                IsBackground = true,
                Name = $"CustomIOWorker-{workerId}"
            };

            _workerThreads[i].Start();
        }
    }

    public void Stop()
    {
        // Signal cancellation
        _cancellationTokenSource.Cancel();
        
        // Wait for all threads to complete
        foreach (Thread thread in _workerThreads)
        {
            thread.Join();
        }

        // Complete the task queue
        _taskQueue.Dispose();
    }
}