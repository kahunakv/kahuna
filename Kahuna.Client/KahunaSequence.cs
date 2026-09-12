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

    /// <summary>Values this sequence reserves per commit, or null when the server-wide setting applies.</summary>
    public int? BlockSize { get; }

    /// <summary>
    /// How many times an update has deliberately broken this sequence's value stream. Values carrying
    /// different incarnations come from streams the caller chose to separate, so whether they overlap
    /// is the caller's decision rather than a guarantee.
    /// </summary>
    public long Incarnation { get; }

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
        BlockSize = entry.BlockSize;
        Incarnation = entry.Incarnation;
        Revision = entry.Revision;
        Durability = entry.Durability;
        CreatedAt = entry.CreatedAt;
        UpdatedAt = entry.UpdatedAt;
    }
}
