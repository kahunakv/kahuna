using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues.Handlers;

/// <summary>
/// Runs a read that parked behind a live write intent of another transaction again, when a
/// <see cref="KeyValueRequestType.ResumeIntentWait"/> message wakes it (see <see cref="IntentWaitRegistry"/>).
///
/// <para>A stale wake (a timer of an earlier park, or a second wake of the same park) is dropped. A read on a node
/// that no longer leads the key's partition answers <c>MustRetry</c>, so the caller locates the new leader. Otherwise
/// the read runs from the start: it answers what it finds, or parks again when an intent is still in its way and the
/// deadline allows. The message is fire-and-forget; the read's own deferred reply carries the answer.</para>
/// </summary>
internal sealed class ResumeIntentWaitHandler : BaseHandler
{
    private readonly TryGetHandler getHandler;

    private readonly TryProbeManyHandler probeHandler;

    public ResumeIntentWaitHandler(KeyValueContext context, TryGetHandler getHandler, TryProbeManyHandler probeHandler) : base(context)
    {
        this.getHandler = getHandler;
        this.probeHandler = probeHandler;
    }

    public async ValueTask<KeyValueResponse?> Execute(KeyValueRequest message)
    {
        if (message.ParkedRead is not { } read || !context.IntentWaits.TryAcceptResume(read, message.CompareRevision))
            return null;

        KeyValueResponse? response;
        IntentWaitExit? exit;

        try
        {
            if (!await context.Raft.AmILeaderQuickIfHosted(ResolvePartition(read.Request.Key)))
            {
                response = KeyValueStaticResponses.MustRetryResponse;
                exit = IntentWaitExit.Moved;
            }
            else if (read.Request.Type == KeyValueRequestType.TryProbeMany)
            {
                response = await probeHandler.ResumeParkedProbe(read);
                exit = probeHandler.LastWaitExit;
            }
            else
            {
                response = await getHandler.ResumeParkedRead(read);
                exit = getHandler.LastWaitExit;
            }
        }
        catch (Exception ex)
        {
            // A read that cannot run must still answer: its caller waits for this reply and nothing else.
            context.Logger.LogError(ex, "KeyValueActor/ResumeIntentWait: parked read of key {Key} failed", read.Request.Key);
            response = KeyValueStaticResponses.MustRetryResponse;
            exit = IntentWaitExit.Moved;
        }

        // Parked again: the next wake answers it.
        if (response is null)
            return null;

        // An exit taken during this run was counted by the decision that took it; a read that found no intent in its
        // way any more was served because the intent settled.
        if (exit is null)
            IntentWaitMetrics.RecordExit(IntentWaitExit.Settled);
        else if (exit == IntentWaitExit.Moved)
            IntentWaitMetrics.RecordExit(IntentWaitExit.Moved);

        IntentWaitMetrics.RecordWait(read.StartTimestamp);
        read.Reply.TrySetResult(response);
        return null;
    }
}
