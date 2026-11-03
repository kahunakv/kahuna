/**
 * This file is part of Kahuna
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Diagnostics;

namespace Kahuna.Client.Diagnostics;

/// <summary>
/// One traced client operation: an optional span and the start time for the duration sample.
/// <para>
/// <see cref="Begin"/> makes the span the ambient <see cref="Activity.Current"/>, so the transport call
/// that follows it starts as its child. <see cref="Observe{T}"/> then puts the caller's ambient activity
/// back before it returns, because the instrumented method is not <c>async</c> and an ambient change
/// made in it would otherwise stay with the caller after the method returned.
/// </para>
/// <para>
/// When <see cref="KahunaClientTelemetry.IsEnabled"/> is false, <see cref="Begin"/> returns the default
/// value and every other member is a no-op that returns the transport task unchanged.
/// </para>
/// </summary>
internal readonly struct KahunaClientOperation
{
    private readonly string? operation;

    private readonly Activity? activity;

    private readonly Activity? previous;

    private readonly long startTimestamp;

    private KahunaClientOperation(string operation, Activity? activity, Activity? previous, long startTimestamp)
    {
        this.operation = operation;
        this.activity = activity;
        this.previous = previous;
        this.startTimestamp = startTimestamp;
    }

    public static KahunaClientOperation Begin(string operation, string url)
    {
        if (!KahunaClientTelemetry.IsEnabled)
            return default;

        Activity? previous = Activity.Current;

        Activity? activity = KahunaClientTelemetry.Source.StartActivity(operation, ActivityKind.Client);

        if (activity is not null && activity.IsAllDataRequested)
        {
            (string host, int port) = KahunaClientTelemetry.GetEndpoint(url);

            activity.SetTag(KahunaClientTelemetry.DbSystemNameTag, KahunaClientTelemetry.DbSystemName);
            activity.SetTag(KahunaClientTelemetry.DbOperationNameTag, operation);
            activity.SetTag(KahunaClientTelemetry.ServerAddressTag, host);

            if (port > 0)
                activity.SetTag(KahunaClientTelemetry.ServerPortTag, port);
        }

        return new(operation, activity, previous, Stopwatch.GetTimestamp());
    }

    public Task<T> Observe<T>(Task<T> task)
    {
        if (operation is null)
            return task;

        RestoreAmbient();

        if (task.IsCompleted)
        {
            Complete(task);
            return task;
        }

        return ObserveAsync(this, task);
    }

    public Task Observe(Task task)
    {
        if (operation is null)
            return task;

        RestoreAmbient();

        if (task.IsCompleted)
        {
            Complete(task);
            return task;
        }

        return ObserveAsync(this, task);
    }

    /// <summary>Records a transport method that threw before it returned a task.</summary>
    public void Fail(Exception exception)
    {
        if (operation is null)
            return;

        RestoreAmbient();
        Stop(exception);
    }

    private static async Task<T> ObserveAsync<T>(KahunaClientOperation self, Task<T> task)
    {
        try
        {
            T result = await task.ConfigureAwait(false);
            self.Stop(null);
            return result;
        }
        catch (Exception ex)
        {
            self.Stop(ex);
            throw;
        }
    }

    private static async Task ObserveAsync(KahunaClientOperation self, Task task)
    {
        try
        {
            await task.ConfigureAwait(false);
            self.Stop(null);
        }
        catch (Exception ex)
        {
            self.Stop(ex);
            throw;
        }
    }

    private void Complete(Task task)
    {
        if (task.IsCompletedSuccessfully)
            Stop(null);
        else if (task.IsCanceled)
            Stop(new TaskCanceledException(task));
        else
            Stop(task.Exception?.InnerException ?? task.Exception);
    }

    private void RestoreAmbient()
    {
        if (activity is not null)
            Activity.Current = previous;
    }

    private void Stop(Exception? exception)
    {
        string? errorType = exception is null ? null : KahunaClientTelemetry.GetErrorType(exception);

        if (activity is not null)
        {
            if (errorType is not null)
            {
                activity.SetStatus(ActivityStatusCode.Error, exception!.Message);
                activity.SetTag(KahunaClientTelemetry.ErrorTypeTag, errorType);
                activity.AddException(exception);
            }

            activity.Stop();
        }

        if (KahunaClientTelemetry.OperationDuration.Enabled)
        {
            TagList tags = new()
            {
                { KahunaClientTelemetry.DbSystemNameTag, KahunaClientTelemetry.DbSystemName },
                { KahunaClientTelemetry.DbOperationNameTag, operation }
            };

            if (errorType is not null)
                tags.Add(KahunaClientTelemetry.ErrorTypeTag, errorType);

            KahunaClientTelemetry.OperationDuration.Record(Stopwatch.GetElapsedTime(startTimestamp).TotalSeconds, tags);
        }
    }
}
