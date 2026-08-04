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
        CancellationToken = cancellationToken;
    }

    public int GetHash()
    {
        return (int)HashUtils.SimpleHash(Name);
    }
}
