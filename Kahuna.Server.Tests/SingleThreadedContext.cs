using System.Collections.Concurrent;

namespace Kahuna.Server.Tests;

/// <summary>
/// Runs an async test body on one dedicated thread with a <see cref="SynchronizationContext"/> that
/// posts every continuation back to that thread, as the browser event loop does. A body that needs a
/// second thread to make progress while its own thread waits never completes here, so the run has a
/// deadline.
/// </summary>
internal static class SingleThreadedContext
{
    public static Task RunAsync(Func<Task> body, TimeSpan deadline)
    {
        TaskCompletionSource done = new(TaskCreationOptions.RunContinuationsAsynchronously);

        Thread thread = new(() =>
        {
            using Context context = new();
            SynchronizationContext.SetSynchronizationContext(context);

            Task task;
            try
            {
                task = body();
            }
            catch (Exception ex)
            {
                done.TrySetException(ex);
                return;
            }

            task.ContinueWith(_ => context.Complete(), TaskScheduler.Default);
            context.RunUntilComplete();

            if (task.IsFaulted)
                done.TrySetException(task.Exception!.InnerExceptions);
            else if (task.IsCanceled)
                done.TrySetCanceled();
            else
                done.TrySetResult();
        })
        {
            IsBackground = true,
            Name = "single-threaded-context"
        };

        thread.Start();

        return done.Task.WaitAsync(deadline);
    }

    private sealed class Context : SynchronizationContext, IDisposable
    {
        private readonly BlockingCollection<(SendOrPostCallback Callback, object? State)> queue = new();

        public override void Post(SendOrPostCallback d, object? state)
        {
            try
            {
                queue.Add((d, state));
            }
            catch (Exception ex) when (ex is InvalidOperationException or ObjectDisposedException)
            {
                // The body finished; a continuation posted after that has nowhere to run.
            }
        }

        public override void Send(SendOrPostCallback d, object? state) =>
            throw new NotSupportedException("A synchronous send would block the only thread of this context.");

        public override SynchronizationContext CreateCopy() => this;

        public void RunUntilComplete()
        {
            foreach ((SendOrPostCallback callback, object? state) in queue.GetConsumingEnumerable())
                callback(state);
        }

        public void Complete() => queue.CompleteAdding();

        public void Dispose() => queue.Dispose();
    }
}
