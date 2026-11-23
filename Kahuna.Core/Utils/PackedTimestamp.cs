using Kommander.Time;

namespace Kahuna.Utils;

/// <summary>
/// An <see cref="HLCTimestamp"/> in 16 bytes, for a field of an object that a node holds in large numbers.
/// <see cref="HLCTimestamp"/> declares its node id, a 4-byte number, before its 8-byte physical time, so the
/// runtime pads it to 24 bytes. This type holds the same three numbers with the physical time first.
/// </summary>
internal readonly struct PackedTimestamp : IEquatable<PackedTimestamp>
{
    private readonly long physical;

    private readonly uint counter;

    private readonly int node;

    public PackedTimestamp(HLCTimestamp value)
    {
        physical = value.L;
        counter = value.C;
        node = value.N;
    }

    public HLCTimestamp Value => new(node, physical, counter);

    /// <summary>The physical time of the timestamp, <see cref="HLCTimestamp.L"/>.</summary>
    public long L => physical;

    public bool Equals(PackedTimestamp other) =>
        physical == other.physical && counter == other.counter && node == other.node;

    public override bool Equals(object? obj) => obj is PackedTimestamp other && Equals(other);

    public override int GetHashCode() => HashCode.Combine(physical, counter, node);

    public override string ToString() => Value.ToString();
}
