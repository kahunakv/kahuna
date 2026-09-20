
using Nixie;
using Kommander.Data;
using Kahuna.Shared.Locks;
using Kahuna.Server.Replication;
using Kahuna.Server.KeyValues.Writes;

namespace Kahuna.Server.Locks.Data;

/// <summary>
/// A staged persistent lock mutation on its way to Raft through the shared partition write scheduler.
/// It carries the already-serialized lock record and its resolved partition; the <see cref="LockProposal"/>
/// itself stays in the owning lock actor's proposal dictionary until <c>CompleteProposal</c> /
/// <c>ReleaseProposal</c> applies or releases it. As an <see cref="IProposalSubmission"/> the record shares
/// one heterogeneous proposal — one AppendEntries round trip, one group-committed WAL flush — with every
/// other lock and key/value write queued for the same partition, instead of paying a whole Raft proposal
/// (two fsync-bearing WAL writes) per lock mutation.
/// </summary>
internal sealed class LockProposalSubmission : IProposalSubmission
{
    public string Resource { get; }

    public int PartitionId { get; }

    /// <summary>A lock mutation adds new pending state and is bounded strictly by the base admission budget.</summary>
    public WriteAdmissionClass AdmissionClass => WriteAdmissionClass.Ordinary;

    /// <summary>A lock mutation belongs to no transaction stage.</summary>
    public WriteSubmissionStage Stage => WriteSubmissionStage.Other;

    public int ProposalId { get; }

    public LockDurability Durability { get; }

    public int ByteLength { get; }

    /// <summary>A lock mutation is a single-entry bundle: one auto-commit, generation-zero entry of type
    /// <see cref="ReplicationTypes.Locks"/>. Lock resources route purely by key-space hash, so no key-range
    /// generation fence applies.</summary>
    public IReadOnlyList<RaftProposalEntry> Entries { get; }

    public IActorRef<LockActor, LockRequest, LockResponse> LockActor { get; }

    public TaskCompletionSource<LockResponse?>? Promise { get; }

    /// <summary>Millisecond tick when the aggregator admitted this mutation, stamped from the aggregator's
    /// <see cref="System.TimeProvider"/>. Set by the aggregator at admission.</summary>
    public long EnqueueTicks { get; set; }

    public LockProposalSubmission(
        string resource,
        int partitionId,
        int proposalId,
        LockDurability durability,
        byte[] serializedMessage,
        IActorRef<LockActor, LockRequest, LockResponse> lockActor,
        TaskCompletionSource<LockResponse?>? promise,
        long expectedTerm = 0
    )
    {
        Resource = resource;
        PartitionId = partitionId;
        ProposalId = proposalId;
        Durability = durability;
        ByteLength = serializedMessage.Length;
        LockActor = lockActor;
        Promise = promise;
        // Term fence: the Raft executor refuses the batch with TermMismatch when the partition's term
        // moved since the actor judged this lock transition (see IRaft.GetPartitionTerm).
        Entries = [new RaftProposalEntry(ReplicationTypes.Locks, serializedMessage, AutoCommit: true, ExpectedGeneration: 0, ExpectedTerm: expectedTerm)];
    }

    /// <summary>Hash-routed resources never move between admission and flush.</summary>
    public bool IsStale(IWriteRangeFence fence) => false;

    /// <summary>Batch committed: send <c>CompleteProposal</c> to the owning lock actor — the only component that
    /// mutates the entry and resolves the caller's promise. The entry's own committed log index travels with it so
    /// the background write can be resolved against the durability floor once it flushes. The pooled request
    /// transfers ownership to the actor, which recycles it after handling.</summary>
    public void Complete(IReadOnlyList<long>? entryLogIndices) =>
        LockActor.Send(LockRequestPool.RentCompleteProposal(
            Resource,
            Durability,
            ProposalId,
            PartitionId,
            Promise,
            entryLogIndices is { Count: > 0 } ? entryLogIndices[0] : -1
        ));

    /// <summary>Batch did not commit (or the submission was released before dispatch): send
    /// <c>ReleaseProposal</c> to the owning lock actor. A transient failure asks the caller to retry
    /// (<c>MustRetry</c>); a definite failure answers <c>Errored</c>. The pooled request transfers ownership to
    /// the actor, which recycles it after handling.</summary>
    public void Release(bool transient) =>
        LockActor.Send(LockRequestPool.RentReleaseProposal(
            Resource,
            Durability,
            ProposalId,
            PartitionId,
            Promise,
            transient
        ));
}
