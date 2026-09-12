using Kahuna.Shared.Sequences;
using Kommander;
using Nixie.Routers;

namespace Kahuna.Server.Sequencer.Data;

/// <summary>
/// A message addressed to the <see cref="SequenceActor"/> that owns a sequence name. The name is the
/// consistent-hash key, so every request for one sequence lands on the same actor and is processed
/// single-threaded — which is what makes the in-memory reserved block safe without any lock.
/// </summary>
internal sealed class SequenceRequest : IConsistentHashable
{
    public SequenceRequestType Type { get; }

    /// <summary>Normalized (trimmed) sequence name. Empty for <see cref="SequenceRequestType.Invalidate"/>.</summary>
    public string Name { get; }

    /// <summary>Number of consecutive values requested. Only meaningful for <see cref="SequenceRequestType.Reserve"/>.</summary>
    public int Count { get; }

    /// <summary>Trimmed idempotency key, or null when the caller did not supply one.</summary>
    public string? IdempotencyKey { get; }

    public long InitialValue { get; }

    public long Increment { get; }

    public long? MaxValue { get; }

    /// <summary>
    /// Per-sequence values reserved per commit. Only meaningful for <see cref="SequenceRequestType.Create"/>;
    /// null leaves the sequence on the server-wide setting.
    /// </summary>
    public int? BlockSize { get; }

    /// <summary>Change set for an <see cref="SequenceRequestType.Update"/>; default for every other type.</summary>
    public SequenceUpdate Update { get; }

    /// <summary>
    /// Partition whose reserved blocks an <see cref="SequenceRequestType.Invalidate"/> surrenders.
    /// Negative means every partition. Ignored by other request types.
    /// </summary>
    public int PartitionId { get; }

    /// <summary>Caller's token, carried through so a cancelled request stops waiting on store round trips.</summary>
    public CancellationToken CancellationToken { get; }

    public SequenceRequest(
        SequenceRequestType type,
        string name,
        int count = 0,
        string? idempotencyKey = null,
        long initialValue = 0,
        long increment = 0,
        long? maxValue = null,
        int? blockSize = null,
        SequenceUpdate update = default,
        int partitionId = -1,
        CancellationToken cancellationToken = default
    )
    {
        Type = type;
        Name = name;
        Count = count;
        IdempotencyKey = idempotencyKey;
        InitialValue = initialValue;
        Increment = increment;
        MaxValue = maxValue;
        BlockSize = blockSize;
        Update = update;
        PartitionId = partitionId;
        CancellationToken = cancellationToken;
    }

    public int GetHash()
    {
        return (int)HashUtils.SimpleHash(Name);
    }
}
