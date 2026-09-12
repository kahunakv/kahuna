namespace Kahuna.Shared.Sequences;

/// <summary>
/// The change an update applies to a sequence. Every field is optional: a null leaves that parameter
/// exactly as the record already has it.
///
/// <para><see cref="MaxValue"/> and <see cref="BlockSize"/> are nullable at rest too, so a null on
/// them is ambiguous on its own. Each therefore carries a companion flag that removes the setting:
/// null plus the flag clears it, null without the flag leaves it alone. The alternative — a bit mask
/// naming the fields being written — is unambiguous as well, but it makes every caller compute a
/// number before it can set one field, and it stops the REST body being something a person can
/// type.</para>
/// </summary>
/// <param name="CurrentValue">New reserved high-water mark. The next value issued is this plus the increment.</param>
/// <param name="Increment">New step between values. Must be positive.</param>
/// <param name="InitialValue">New recorded starting value. Descriptive only; it does not move the counter.</param>
/// <param name="MaxValue">New maximum. Ignored when <paramref name="RemoveMaxValue"/> is set.</param>
/// <param name="RemoveMaxValue">Removes the maximum, leaving <see cref="long.MaxValue"/> as the only ceiling.</param>
/// <param name="BlockSize">New per-sequence block size. Must be at least 1. Ignored when <paramref name="RemoveBlockSize"/> is set.</param>
/// <param name="RemoveBlockSize">Removes the per-sequence block size, returning the sequence to the server-wide setting.</param>
public readonly record struct SequenceUpdate(
    long? CurrentValue = null,
    long? Increment = null,
    long? InitialValue = null,
    long? MaxValue = null,
    bool RemoveMaxValue = false,
    int? BlockSize = null,
    bool RemoveBlockSize = false
)
{
    /// <summary>True when the change set would leave every field of the record as it is.</summary>
    public bool IsEmpty =>
        CurrentValue is null
        && Increment is null
        && InitialValue is null
        && MaxValue is null
        && !RemoveMaxValue
        && BlockSize is null
        && !RemoveBlockSize;
}
