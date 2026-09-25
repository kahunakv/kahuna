using Kommander;
using Nixie;

namespace Kahuna.Server.Tests;

/// <summary>
/// Tracks the runtime (Raft node, Kahuna manager, actor system) behind every node a test assembles so
/// the node can be torn down as a unit. Two teardown flavours exist because they mean different things:
/// <list type="bullet">
/// <item><see cref="DisposeAsync(IRaft, CancellationToken)"/> throws the node away. It drains the
/// node's own writes and disposes the Raft instance <b>without</b> a membership change. This is what a
/// test wants when the whole cluster is being discarded at the end of the test: a graceful leave of every
/// voter at once cannot commit (the peers each try to reach a leader that is itself shutting down and
/// spin until Kommander's fixed 10 s leave deadline), so it only ever added that deadline to every
/// test.</item>
/// <item><see cref="LeaveAndDisposeAsync"/> performs the real graceful leave (drain, <c>RemoveMember</c>
/// commit, then dispose). Only tests whose subject is a node leaving a live cluster use it.</item>
/// </list>
/// </summary>
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

    /// <summary>
    /// Tears the node down without leaving the cluster's membership. Use for end-of-test teardown.
    /// </summary>
    public static Task DisposeAsync(IRaft raft, CancellationToken cancellationToken = default) =>
        TearDownAsync(raft, gracefulLeave: false, cancellationToken);

    /// <summary>
    /// Gracefully leaves the cluster (committing the node's removal from the roster) and then tears the
    /// node down. Use only when the test is exercising a node actually leaving a live cluster.
    /// </summary>
    public static Task LeaveAndDisposeAsync(IRaft raft, CancellationToken cancellationToken = default) =>
        TearDownAsync(raft, gracefulLeave: true, cancellationToken);

    private static async Task TearDownAsync(IRaft raft, bool gracefulLeave, CancellationToken cancellationToken)
    {
        Runtime? runtime;
        lock (Gate)
        {
            Runtimes.Remove(raft, out runtime);
        }

        if (runtime is null)
        {
            await DisposeRaftAsync(raft, gracefulLeave, cancellationToken);
            return;
        }

        await DisposeAsync(runtime, gracefulLeave, cancellationToken);
    }

    private static async Task DisposeAsync(Runtime runtime, bool gracefulLeave, CancellationToken cancellationToken)
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
            await DisposeRaftAsync(runtime.Raft, gracefulLeave, cancellationToken).ConfigureAwait(false);
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

    private static async Task DisposeRaftAsync(IRaft raft, bool gracefulLeave, CancellationToken cancellationToken)
    {
        if (!gracefulLeave)
        {
            // Dispose performs the orderly local shutdown (drain queues, stop schedulers, stop the
            // partition actors) without the membership round-trip, mirroring EmbeddedKahunaNode.
            try
            {
                if (raft is IDisposable disposable)
                    disposable.Dispose();
            }
            catch (ObjectDisposedException)
            {
            }

            return;
        }

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
