using Kommander.Time;

namespace Kahuna.Shared.Sequences;

public sealed class ReadOnlySequenceEntry
{
    public string Name { get; }

    public long CurrentValue { get; }

    public long InitialValue { get; }

    public long Increment { get; }

    public long? MaxValue { get; }

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
        HLCTimestamp updatedAt
    )
    {
        Name = name;
        CurrentValue = currentValue;
        InitialValue = initialValue;
        Increment = increment;
        MaxValue = maxValue;
        Revision = revision;
        Durability = durability;
        CreatedAt = createdAt;
        UpdatedAt = updatedAt;
    }
}
