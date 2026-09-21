
namespace Kahuna.Server.KeyValues;

/// <summary>
/// Runs a request on a key-value actor from inside that actor's own turn, without going through its mailbox.
/// Only the actor implements this, and it answers only while a turn it started is running.
/// </summary>
internal interface IKeyValueInlineDispatcher
{
    /// <summary>
    /// Runs the handler of <paramref name="request"/> now, as the mailbox would have. Answers MustRetry when
    /// no turn is running on the actor, so a dispatcher that outlives its turn cannot touch actor state.
    /// </summary>
    ValueTask<KeyValueResponse?> DispatchInline(KeyValueRequest request);
}

/// <summary>
/// A unit of work that runs inside one turn of a key-value actor: every request it issues for a key the actor
/// owns is served at once, with no mailbox hop, and nothing else runs on the actor until it returns.
///
/// <para>The actor is single threaded, so a turn is as cheap as it is dangerous: while it runs, every other
/// key of the actor waits. A turn must therefore do bounded, non-blocking work, and must never send the
/// actor's own mailbox a request and wait for the answer — that answer can only come after the turn ends.</para>
/// </summary>
internal interface IKeyValueActorTurn
{
    ValueTask RunAsync(IKeyValueInlineDispatcher actor);
}

/// <summary>
/// Marks the requests rented on this thread, while the scope is set, as requests to be dispatched inline.
///
/// <para>The local operations rent their request synchronously, before their first await, so a caller sets
/// the scope, starts the operation, and clears the scope again before it awaits the result. Nothing that
/// runs later — a continuation, a detached task — can inherit it, which is the reason this is a thread
/// static and not an async local: an async local would flow into work that outlives the turn.</para>
/// </summary>
internal static class KeyValueInlineScope
{
    [ThreadStatic]
    private static IKeyValueInlineDispatcher? current;

    public static IKeyValueInlineDispatcher? Current
    {
        get => current;
        set => current = value;
    }
}
