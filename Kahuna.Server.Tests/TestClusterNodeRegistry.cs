using Kommander;
using Nixie;

namespace Kahuna.Server.Tests;

internal static class TestClusterNodeRegistry
{
    private sealed record Runtime(IRaft Raft, IKahuna Kahuna, ActorSystem ActorSystem);

    private static readonly Dictionary<IRaft, Runtime> Runtimes = new(ReferenceEqualityComparer.Instance);
    private static readonly object Gate = new();

    public static void Register(IRaft raft, IKahuna kahuna, ActorSystem actorSystem)
    {
        lock (Gate)
            Runtimes[raft] = new(raft, kahuna, actorSystem);
    }

    public static async Task DisposeAsync(IRaft raft, CancellationToken cancellationToken = default)
    {
        Runtime? runtime;
        lock (Gate)
        {
            Runtimes.Remove(raft, out runtime);
        }

        if (runtime is null)
        {
            await DisposeRaftAsync(raft, cancellationToken);
            return;
        }

        await DisposeAsync(runtime, cancellationToken);
    }

    private static async Task DisposeAsync(Runtime runtime, CancellationToken cancellationToken)
    {
        if (runtime.Kahuna is KahunaManager kahunaManager)
        {
            runtime.Raft.OnLogRestored -= kahunaManager.OnLogRestored;
            runtime.Raft.OnReplicationReceived -= kahunaManager.OnReplicationReceived;
            runtime.Raft.OnReplicationError -= kahunaManager.OnReplicationError;
            runtime.Raft.OnLeaderChanged -= kahunaManager.OnLeaderChanged;

            try
            {
                await kahunaManager.DrainKeyValueWritesAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false);
            }
            catch (ObjectDisposedException)
            {
            }
            catch (InvalidOperationException)
            {
            }
        }

        try
        {
            await DisposeRaftAsync(runtime.Raft, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            try
            {
                await runtime.ActorSystem.GracefulShutdownAll(TimeSpan.FromSeconds(5)).ConfigureAwait(false);
            }
            catch (ObjectDisposedException)
            {
            }
            finally
            {
                runtime.ActorSystem.Dispose();

                if (runtime.Kahuna is IDisposable disposable)
                    disposable.Dispose();
            }
        }
    }

    private static async Task DisposeRaftAsync(IRaft raft, CancellationToken cancellationToken)
    {
        bool disposedByLeave = false;

        try
        {
            await raft.LeaveCluster(dispose: true, cancellationToken).ConfigureAwait(false);
            disposedByLeave = true;
        }
        catch (ObjectDisposedException)
        {
            disposedByLeave = true;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
        }
        finally
        {
            if (!disposedByLeave && raft is IDisposable disposable)
                disposable.Dispose();
        }
    }
}
