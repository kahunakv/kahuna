
using Nixie;

using Kahuna.Server.Locks.Data;

namespace Kahuna.Server.Locks;

/// <summary>
/// Routes requests to a fixed set of lock actors by consistent hash, resolving the target at the
/// call site. The bucket formula matches Nixie's <c>ConsistentHashActor</c> exactly, so the
/// resource→actor mapping is identical whether a message arrives through <see cref="Ask"/> or
/// <see cref="Send"/>. Dispatching directly — instead of through a router actor — matters for
/// throughput: a router actor is one mailbox drained by one thread in front of every worker,
/// which adds an enqueue/wake hop per request and caps the whole ring at what that single thread
/// can dequeue.
/// </summary>
internal sealed class LockActorRing
{
    private readonly IActorRef<LockActor, LockRequest, LockResponse>[] instances;

    internal LockActorRing(IReadOnlyList<IActorRef<LockActor, LockRequest, LockResponse>> instances)
    {
        if (instances.Count == 0)
            throw new ArgumentException("A ring must have at least one actor.", nameof(instances));

        this.instances = [.. instances];
    }

    private IActorRef<LockActor, LockRequest, LockResponse> Route(LockRequest request) =>
        instances[(request.GetHash() & int.MaxValue) % instances.Length];

    /// <summary>
    /// Asks the resource's actor and returns its reply task.
    /// </summary>
    internal Task<LockResponse?> Ask(LockRequest request) => Route(request).Ask(request);

    /// <summary>
    /// Fire-and-forget send to the resource's actor. Lock actors have no inbox bound, so the
    /// message is always enqueued.
    /// </summary>
    internal void Send(LockRequest request) => Route(request).Send(request);
}
