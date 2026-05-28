using Kahuna.Shared.Sequences;
using Kommander.Time;

namespace Kahuna.Client;

public sealed class KahunaSequence
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

    public KahunaSequence(ReadOnlySequenceEntry entry)
    {
        Name = entry.Name;
        CurrentValue = entry.CurrentValue;
        InitialValue = entry.InitialValue;
        Increment = entry.Increment;
        MaxValue = entry.MaxValue;
        Revision = entry.Revision;
        Durability = entry.Durability;
        CreatedAt = entry.CreatedAt;
        UpdatedAt = entry.UpdatedAt;
    }
}
