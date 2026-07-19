using Kommander.Time;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues.Writes;

/// <summary>
/// Delivers a per-item terminal outcome after its partition batch settles, behind an interface so tests can
/// record outcomes without spawning real key actors. Production routes the existing priority-control
/// <c>CompleteProposal</c> / <c>ReleaseProposal</c> request to the item's owning key actor — the only
/// component that mutates the entry and resolves the caller's promise.
/// </summary>
internal interface IWriteCompletionRouter
{
    void Complete(KeyValueProposalRequest item);

    void Release(KeyValueProposalRequest item, bool transient);
}

/// <summary>
/// Production router: sends <c>CompleteProposal</c> (apply) or <c>ReleaseProposal</c> (unwind) to the item's
/// originating key actor, carrying its key, proposal id, partition id, durability, and caller promise. A
/// transient failure releases with <see cref="KeyValueFlags.ReplicationRetry"/> so the caller retries.
/// </summary>
internal sealed class KeyValueActorCompletionRouter : IWriteCompletionRouter
{
    public void Complete(KeyValueProposalRequest item) =>
        item.KeyValueActor.Send(new(
            KeyValueRequestType.CompleteProposal,
            HLCTimestamp.Zero,
            HLCTimestamp.Zero,
            item.Key,
            null,
            null,
            -1,
            KeyValueFlags.None,
            0,
            HLCTimestamp.Zero,
            item.Durability,
            item.ProposalId,
            item.PartitionId,
            item.Promise
        ));

    public void Release(KeyValueProposalRequest item, bool transient) =>
        item.KeyValueActor.Send(new(
            KeyValueRequestType.ReleaseProposal,
            HLCTimestamp.Zero,
            HLCTimestamp.Zero,
            item.Key,
            null,
            null,
            -1,
            transient ? KeyValueFlags.ReplicationRetry : KeyValueFlags.None,
            0,
            HLCTimestamp.Zero,
            item.Durability,
            item.ProposalId,
            item.PartitionId,
            item.Promise
        ));
}
