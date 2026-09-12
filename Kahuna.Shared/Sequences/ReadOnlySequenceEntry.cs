using Kommander.Time;

namespace Kahuna.Shared.Sequences;

public sealed class ReadOnlySequenceEntry
{
    public string Name { get; }

    public long CurrentValue { get; }

    public long InitialValue { get; }

    public long Increment { get; }

    public long? MaxValue { get; }

    /// <summary>Per-sequence values reserved per commit, or null when the server-wide setting applies.</summary>
    public int? BlockSize { get; }

    /// <summary>
    /// How many times this sequence's value stream has been deliberately broken by an update. Two
    /// allocations carrying different incarnations came from streams the caller chose to separate, so
    /// uniqueness across them is the caller's decision rather than a guarantee.
    /// </summary>
    public long Incarnation { get; }

    public long Revision { get; }

    public SequenceDurability Durability { get; }

    public HLCTimestamp CreatedAt { get; }

    public HLCTimestamp UpdatedAt { get; }

    public ReadOnlySequenceEntry(
        string name,
        long currentValue,
        long initialValue,
        long increment,
        long? maxValue,
        long revision,
        SequenceDurability durability,
        HLCTimestamp createdAt,
        HLCTimestamp updatedAt,
        int? blockSize = null,
        long incarnation = 0
    )
    {
        Name = name;
        CurrentValue = currentValue;
        InitialValue = initialValue;
        Increment = increment;
        MaxValue = maxValue;
        BlockSize = blockSize;
        Incarnation = incarnation;
        Revision = revision;
        Durability = durability;
        CreatedAt = createdAt;
        UpdatedAt = updatedAt;
    }
}
