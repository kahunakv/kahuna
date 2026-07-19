
using Nixie;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues;

/// <summary>
/// Immutable envelope for a staged direct (auto-commit, non-transactional) key/value write on its way to
/// Raft. It carries the already-serialized log record and its resolved partition rather than the full
/// <see cref="KeyValueProposal"/> — the proposal itself stays in the owning key actor's proposal dictionary
/// until <c>CompleteProposal</c>/<c>ReleaseProposal</c> applies or releases it, so the value graph is not
/// retained twice. The fence generation, cached byte length, and enqueue tick are carried for a later
/// partition-level aggregator (flush-time re-fence, byte budgeting, linger); the current single-key dispatch
/// path uses only the bytes, partition, ids, and completion correlation.
/// </summary>
internal sealed class KeyValueProposalRequest
{
    public string Key { get; }

    public int PartitionId { get; }

    public int ProposalId { get; }

    public KeyValueDurability Durability { get; }

    /// <summary>The live key-range descriptor generation captured at admission (0 for hash spaces, which never
    /// move). The flush-time fence releases the write if the descriptor covering <see cref="Key"/> no longer
    /// matches this generation and <see cref="PartitionId"/>.</summary>
    public long FenceGeneration { get; }

    public byte[] SerializedMessage { get; }

    public int ByteLength { get; }

    public IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> KeyValueActor { get; }

    public TaskCompletionSource<KeyValueResponse?> Promise { get; }

    /// <summary>Millisecond tick when the aggregator admitted this write, stamped from the aggregator's
    /// <see cref="System.TimeProvider"/> so queue-age math and its wake timers share one clock. Set by the
    /// facade at admission; the constructor value is a placeholder.</summary>
    public long EnqueueTicks { get; internal set; }

    public KeyValueProposalRequest(
        string key,
        int partitionId,
        int proposalId,
        KeyValueDurability durability,
        long fenceGeneration,
        byte[] serializedMessage,
        IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> keyValueActor,
        TaskCompletionSource<KeyValueResponse?> promise,
        long enqueueTicks
    )
    {
        Key = key;
        PartitionId = partitionId;
        ProposalId = proposalId;
        Durability = durability;
        FenceGeneration = fenceGeneration;
        SerializedMessage = serializedMessage;
        ByteLength = serializedMessage.Length;
        KeyValueActor = keyValueActor;
        Promise = promise;
        EnqueueTicks = enqueueTicks;
    }
}
