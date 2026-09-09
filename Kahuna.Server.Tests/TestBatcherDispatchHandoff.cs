using System.Collections.Concurrent;
using System.Reflection;

using Kahuna.Client.Communication;
using Kahuna.Server.Communication.Internode.Grpc;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kahuna.Server.Tests;

/// <summary>
/// Deterministic coverage of the dispatch handoff in both batchers. The dangerous interleaving:
/// the dispatcher observes an empty inbox; a producer enqueues and reads a still-claimed flag, so
/// it starts no dispatcher; the dispatcher then releases the flag and exits. Without a recheck
/// after the release, that producer's request stays in the inbox until unrelated later traffic
/// arrives — or forever on a quiet batcher. The handoff must release the flag first, recheck the
/// inbox, and reclaim ownership when an item arrived during the window.
/// </summary>
public sealed class TestBatcherDispatchHandoff
{
    // ── server-side inter-node batcher ───────────────────────────────────────

    private static readonly Type ServerBatcherType = typeof(GrpcServerBatcher);

    private static int ServerProcessing(GrpcServerBatcher batcher)
        => (int)ServerBatcherType.GetField("processing", BindingFlags.NonPublic | BindingFlags.Instance)!.GetValue(batcher)!;

    private static void SetServerProcessing(GrpcServerBatcher batcher, int value)
        => ServerBatcherType.GetField("processing", BindingFlags.NonPublic | BindingFlags.Instance)!.SetValue(batcher, value);

    private static ConcurrentQueue<GrpcServerBatcherItem> ServerInbox(GrpcServerBatcher batcher)
        => (ConcurrentQueue<GrpcServerBatcherItem>)ServerBatcherType
            .GetField("inbox", BindingFlags.NonPublic | BindingFlags.Instance)!
            .GetValue(batcher)!;

    /// <summary>
    /// The final producer enqueued while the dispatcher held the flag; no later enqueue follows.
    /// The handoff must observe the item and reclaim ownership, so the drain loop continues and
    /// the item is delivered instead of stranded.
    /// </summary>
    [Fact]
    public void ServerBatcher_ItemArrivedDuringHandoff_ReclaimsOwnership()
    {
        GrpcServerBatcher batcher = new("test://server-dispatch-handoff", NullLogger.Instance);

        // The dispatcher owns the flag and has just seen an empty inbox.
        SetServerProcessing(batcher, 0);

        // The producer's enqueue: it reads a claimed flag, so it starts no dispatcher.
        TaskCompletionSource<GrpcServerBatcherResponse> promise = new(TaskCreationOptions.RunContinuationsAsynchronously);
        ServerInbox(batcher).Enqueue(new GrpcServerBatcherItem(
            GrpcServerBatcherItemType.KeyValues, 9_960_001,
            new GrpcServerBatcherRequest(new GrpcLookupTransactionRecordRequest()), promise));

        Assert.False(batcher.TryReleaseDispatch());
        Assert.Equal(0, ServerProcessing(batcher));

        // With the inbox drained the handoff releases ownership and permits the exit.
        Assert.True(ServerInbox(batcher).TryDequeue(out _));
        Assert.True(batcher.TryReleaseDispatch());
        Assert.Equal(1, ServerProcessing(batcher));
    }

    /// <summary>
    /// End-to-end proof of the repaired handoff: the interleaving that stranded a request now
    /// delivers it. The dispatch flag is claimed (a dispatcher that saw an empty inbox), a real
    /// enqueue arrives and starts nothing, and the handoff — the code the real dispatcher runs
    /// next — reclaims and lets the drain continue.
    /// </summary>
    [Fact]
    public async Task ServerBatcher_FinalProducerWithNoLaterTraffic_IsNotStranded()
    {
        GrpcServerBatcher batcher = new("test://server-dispatch-handoff-final", NullLogger.Instance);

        // The dispatcher owns the flag and has just seen an empty inbox: exactly the state a real
        // enqueue meets in the dangerous window.
        SetServerProcessing(batcher, 0);

        Task<GrpcServerBatcherResponse> response = batcher.Enqueue(new GrpcLookupTransactionRecordRequest());

        // The producer saw a claimed flag: no dispatcher started and the item waits in the inbox.
        Assert.False(response.IsCompleted);
        Assert.False(ServerInbox(batcher).IsEmpty);

        // The dispatcher's handoff must refuse to exit while that item waits.
        Assert.False(batcher.TryReleaseDispatch());
        Assert.Equal(0, ServerProcessing(batcher));

        // Cleanup: drain the item, release the flag, and settle the admitted request through the
        // real accounting path so no pending state leaks into other tests.
        Assert.True(ServerInbox(batcher).TryDequeue(out GrpcServerBatcherItem item));
        Assert.True(batcher.TryReleaseDispatch());
        Assert.True(GrpcServerBatcher.TryTakeRequest(item.RequestId, out _));
        item.Promise.TrySetCanceled(TestContext.Current.CancellationToken);

        await Assert.ThrowsAsync<TaskCanceledException>(() => response);
    }

    // ── client-side batcher ──────────────────────────────────────────────────

    private static readonly Type ClientBatcherType = typeof(GrpcBatcher);

    private static int ClientProcessing(GrpcBatcher batcher)
        => (int)ClientBatcherType.GetField("processing", BindingFlags.NonPublic | BindingFlags.Instance)!.GetValue(batcher)!;

    private static void SetClientProcessing(GrpcBatcher batcher, int value)
        => ClientBatcherType.GetField("processing", BindingFlags.NonPublic | BindingFlags.Instance)!.SetValue(batcher, value);

    private static ConcurrentQueue<GrpcBatcherItem> ClientInbox(GrpcBatcher batcher)
        => (ConcurrentQueue<GrpcBatcherItem>)ClientBatcherType
            .GetField("inbox", BindingFlags.NonPublic | BindingFlags.Instance)!
            .GetValue(batcher)!;

    /// <summary>The client batcher shares the handoff shape and must share the repair.</summary>
    [Fact]
    public void ClientBatcher_ItemArrivedDuringHandoff_ReclaimsOwnership()
    {
        GrpcBatcher batcher = new("test://client-dispatch-handoff");

        SetClientProcessing(batcher, 0);

        TaskCompletionSource<GrpcBatcherResponse> promise = new(TaskCreationOptions.RunContinuationsAsynchronously);
        ClientInbox(batcher).Enqueue(new GrpcBatcherItem(
            GrpcBatcherItemType.KeyValues, 9_960_101,
            new GrpcBatcherRequest(new GrpcTryGetKeyValueRequest()), promise));

        Assert.False(batcher.TryReleaseDispatch());
        Assert.Equal(0, ClientProcessing(batcher));

        Assert.True(ClientInbox(batcher).TryDequeue(out _));
        Assert.True(batcher.TryReleaseDispatch());
        Assert.Equal(1, ClientProcessing(batcher));
    }
}
