
using Kommander.Time;

using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues.Handlers;

/// <summary>
/// Which step of a fused finalize produced the answer. The coordinator reports a refused prepare, a range
/// lock found at commit time, and a failed commit differently, exactly as it does when the three steps are
/// three messages, so the answer has to say which step it came from.
/// </summary>
internal enum KeyValueFinalizeStage
{
    /// <summary>The message did not reach the handler, or the answer does not say where it stopped.</summary>
    Unknown = 0,

    /// <summary>The prepare refused. Nothing was committed; an intent this transaction already held stays.</summary>
    Prepare = 1,

    /// <summary>A foreign range lock covers the key. The prepare ran and its write intent is still in place.</summary>
    RangeLock = 2,

    /// <summary>The commit step ran. The response type is the commit's own answer.</summary>
    Commit = 3
}

/// <summary>
/// Finalizes one ephemeral mutation in a single actor turn: prepare, the commit-time range-lock check, and
/// commit, in that order.
///
/// <para>A transaction whose whole write set is one ephemeral key sends those three steps to the same actor
/// one after another. Each is a mailbox round trip, and nothing can run on this actor between two steps
/// that the step after does not re-check, so running them back to back in one turn gives every answer the
/// three messages give. The steps are the existing handlers, called unchanged: this class adds no rule of
/// its own, only the order and the early exits.</para>
///
/// <para>A step that stops the sequence leaves the entry exactly as the same step leaves it on the
/// three-message path. In particular a range lock found here leaves the prepared write intent in place:
/// the coordinator rolls it back with the ordinary rollback message, as it does today.</para>
///
/// <para>The message is safe to deliver twice. A transaction that already committed here is answered
/// Committed before anything else runs — without that, a repeat would reach the prepare, find the staged
/// state gone, and report a failure for a commit that in fact happened.</para>
/// </summary>
internal sealed class TryFinalizeMutationHandler : BaseHandler
{
    private readonly TryPrepareMutationsHandler prepareHandler;

    private readonly TryCommitMutationsHandler commitHandler;

    public TryFinalizeMutationHandler(
        KeyValueContext context,
        TryPrepareMutationsHandler prepareHandler,
        TryCommitMutationsHandler commitHandler
    ) : base(context)
    {
        this.prepareHandler = prepareHandler;
        this.commitHandler = commitHandler;
    }

    public async ValueTask<KeyValueResponse> Execute(KeyValueRequest message)
    {
        // The three-message path never sends a persistent key through the manual prepare and commit; neither
        // does this one. Refused before anything is touched.
        if (message.Durability != KeyValueDurability.Ephemeral)
            return Answer(KeyValueResponseType.Errored, KeyValueFinalizeStage.Prepare);

        if (message.TransactionId != HLCTimestamp.Zero && context.WasCommittedHere(message.TransactionId))
            return Answer(KeyValueResponseType.Committed, KeyValueFinalizeStage.Commit);

        KeyValueResponse prepared = await prepareHandler.Execute(message);

        if (prepared.Type != KeyValueResponseType.Prepared)
            return Answer(prepared.Type, KeyValueFinalizeStage.Prepare);

        // The write set's decide-time fence: a range lock taken after the write was staged steps around the
        // key lock and the write intent, so only this check sees it. Same call, same clock reading rule, and
        // the same place in the order — after the prepare, before the commit — as the separate probe.
        HLCTimestamp currentTime = context.Raft.HybridLogicalClock.TrySendOrLocalEvent(context.Raft.GetLocalNodeId());

        if (RangeLockChecks.KeyCoveredByForeignRangeLock(context, message.Key, GetBucket(message.Key), message.TransactionId, currentTime))
            return Answer(KeyValueResponseType.Aborted, KeyValueFinalizeStage.RangeLock);

        KeyValueResponse committed = await commitHandler.Execute(message);

        return Answer(committed.Type, KeyValueFinalizeStage.Commit);
    }

    /// <summary>
    /// The stage travels in the revision slot of the response. A finalize answer has no revision of its own
    /// to report — the commit step answers zero there — and the response never leaves this process.
    /// </summary>
    private static KeyValueResponse Answer(KeyValueResponseType type, KeyValueFinalizeStage stage) =>
        new(type, (long)stage);

    /// <summary>Reads the stage back out of a finalize answer.</summary>
    internal static KeyValueFinalizeStage StageOf(KeyValueResponse response) =>
        response.Revision is >= (long)KeyValueFinalizeStage.Prepare and <= (long)KeyValueFinalizeStage.Commit
            ? (KeyValueFinalizeStage)response.Revision
            : KeyValueFinalizeStage.Unknown;
}
