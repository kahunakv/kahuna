
using System.Diagnostics.CodeAnalysis;

namespace Kahuna.Extensibility;

/// <summary>
/// The kind of value a <see cref="KahunaValue"/> carries. These are the seven value shapes the
/// Kahuna script language has, so a user-defined function sees exactly what a built-in sees.
/// </summary>
// The members name the script language's own types, which is the whole point of the enum: a function
// author reads Long and Double as the script kinds they are, not as the CLR types they resemble.
[SuppressMessage("Microsoft.Naming", "CA1720:IdentifiersShouldNotContainTypeNames", Justification = "The members name script value kinds, not CLR types")]
public enum KahunaValueKind
{
    /// <summary>No value. A key that does not exist reads as this.</summary>
    Null,

    /// <summary>A boolean.</summary>
    Bool,

    /// <summary>A 64-bit signed integer.</summary>
    Long,

    /// <summary>A double-precision floating point number.</summary>
    Double,

    /// <summary>A string.</summary>
    String,

    /// <summary>A byte buffer. A value read from a key arrives in this kind.</summary>
    Bytes,

    /// <summary>An ordered list of values.</summary>
    Array,
}
