
using Nixie;

namespace Kahuna.Server.KeyValues;

/// <summary>
/// The completion side of a key-value request's reply, unified across the two ask paths: the classic
/// task promise (<see cref="TaskCompletionSource{TResult}"/>) and the pooled
/// <see cref="ReplyHandle{TResponse}"/>. Deferral machinery — proposals, read continuations, and the
/// resume/complete control messages — stores this instead of a raw completion source, so a deferred
/// reply resolves correctly no matter which path admitted the request. Completion is first-wins and
/// idempotent on both paths. The default value is absent; every TrySet on it is a no-op.
/// </summary>
public readonly struct KeyValueReplyRef
{
    private readonly TaskCompletionSource<KeyValueResponse?>? promise;

    private readonly ReplyHandle<KeyValueResponse> handle;

    public KeyValueReplyRef(TaskCompletionSource<KeyValueResponse?>? promise, ReplyHandle<KeyValueResponse> handle)
    {
        this.promise = promise;
        this.handle = handle;
    }

    /// <summary>Captures the reply side of the message currently being delivered.</summary>
    public static KeyValueReplyRef From(in ActorMessageReply<KeyValueRequest, KeyValueResponse> reply) =>
        new(reply.Promise, reply.PooledHandle);

    /// <summary>A task completion source is a reply ref whose completion resolves that source.</summary>
    public static implicit operator KeyValueReplyRef(TaskCompletionSource<KeyValueResponse?> promise) =>
        new(promise, default);

    /// <summary>True when no reply is attached (a fire-and-forget message, or the default value).</summary>
    public bool IsDefault => promise is null && handle.IsDefault;

    /// <summary>Resolves the reply. First completion wins; repeated or stale attempts return <c>false</c>.</summary>
    public bool TrySetResult(KeyValueResponse? response) =>
        promise is not null ? promise.TrySetResult(response) : handle.TrySetResult(response);
}
