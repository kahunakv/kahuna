
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues.Handlers;

/// <summary>
/// Handles ResumeRead messages — the actor-thread stage-3 of the resumable backend read.
///
/// A ResumeRead is sent (fire-and-forget) by the stage-2 completion callback after the
/// FairReadScheduler finishes the backend operation. The handler delegates all reconciliation
/// to the ReadContinuation carried on the message; it performs no actor-state mutations itself.
/// The return value from Execute is discarded by Nixie (ResumeRead is always sent, never asked).
/// </summary>
internal sealed class ResumeReadHandler : BaseHandler
{
    public ResumeReadHandler(KeyValueContext context) : base(context) { }

    public KeyValueResponse? Execute(KeyValueRequest message)
    {
        ReadContinuation? continuation = message.Continuation;

        if (continuation is null)
        {
            context.Logger.LogWarning(
                "KeyValueActor/ResumeRead: received message with null continuation for key {Key}",
                message.Key);
            message.Promise?.TrySetResult(KeyValueStaticResponses.ErroredResponse);
            return KeyValueStaticResponses.ErroredResponse;
        }

        // Late completion: the deadline sweep already expired this continuation, resolved its waiters
        // with a retryable result, and removed any in-flight registration. Running Execute now would
        // double-resolve (harmless via TrySetResult) but could also re-touch actor state for a read the
        // callers have already retried; drop it.
        if (continuation.Cancelled)
        {
            context.Logger.LogWarning(
                "KeyValueActor/ResumeRead: dropping late completion for expired continuation on key {Key}",
                message.Key);
            return null;
        }

        try
        {
            continuation.Execute(context);
        }
        catch (Exception ex)
        {
            // A throwing continuation must never strand its callers: the ResumeRead message is
            // fire-and-forget, so the actor's own catch would resolve only this message's (unused)
            // promise, leaving the coalesced waiters' promises unresolved and, if the throw
            // happened before the in-flight key was removed, a stale registration that later
            // arrivals would attach to. Fail() removes the key and resolves every waiter.
            context.Logger.LogError(
                ex, "KeyValueActor/ResumeRead: continuation Execute threw for key {Key}", message.Key);
            continuation.Fail(context, KeyValueStaticResponses.MustRetryResponse);
        }

        // Return value is discarded — ResumeRead is always sent fire-and-forget, never asked.
        return null;
    }
}
