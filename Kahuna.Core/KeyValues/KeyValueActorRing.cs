
using System.Diagnostics.CodeAnalysis;

using Nixie;

namespace Kahuna.Server.KeyValues;

/// <summary>
/// Routes requests to a fixed set of key-value actors by consistent hash, resolving the target at
/// the call site. The bucket formula matches Nixie's <c>ConsistentHashActor</c> exactly, so the
/// key→actor mapping is identical whether a message arrives through <see cref="Ask"/>,
/// <see cref="TryAsk"/>, or <see cref="Send"/>. Dispatching directly — instead of through a router
/// actor — matters for throughput: a router actor is one mailbox drained by one thread in front of
/// every worker, which adds an enqueue/wake hop per request and caps the whole ring at what that
/// single thread can dequeue.
/// </summary>
internal sealed class KeyValueActorRing
{
    private readonly IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse>[] instances;

    internal KeyValueActorRing(IReadOnlyList<IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse>> instances)
    {
        if (instances.Count == 0)
            throw new ArgumentException("A ring must have at least one actor.", nameof(instances));

        this.instances = [.. instances];
    }

    private IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> Route(KeyValueRequest request) =>
        instances[(request.GetHash() & int.MaxValue) % instances.Length];

    /// <summary>
    /// Admission-checked ask against the key's actor on the pooled reply path. Returns <c>false</c>
    /// when the actor's bounded inbox rejected the message (never enqueued, safe to retry) without
    /// allocating a promise or an exception. The returned ValueTask must be awaited exactly once;
    /// consuming it recycles the pooled reply promise.
    /// </summary>
    internal bool TryAsk(KeyValueRequest request, out ValueTask<KeyValueResponse?> reply) =>
        Route(request).TryAskPooled(request, out reply);

    /// <summary>
    /// Asks the key's actor and returns its reply task. A bounded-inbox rejection surfaces as an
    /// <c>ActorBusyException</c> fault on the task.
    /// </summary>
    internal Task<KeyValueResponse?> Ask(KeyValueRequest request) => Route(request).Ask(request);

    /// <summary>
    /// Fire-and-forget send to the key's actor on the promise-free admission path. An ordinary
    /// message is dropped when the bounded inbox is full; control messages are exempt from the bound.
    /// </summary>
    internal void Send(KeyValueRequest request) => Route(request).Send(request);
}
