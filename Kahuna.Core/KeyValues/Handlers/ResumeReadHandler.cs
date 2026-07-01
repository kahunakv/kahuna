
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

        continuation.Execute(context);

        // Return value is discarded — ResumeRead is always sent fire-and-forget, never asked.
        return null;
    }
}
