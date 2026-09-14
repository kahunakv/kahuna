
using System.Runtime.InteropServices;

namespace Kahuna.Extensibility;

/// <summary>
/// Supplies the elements of an array value only when a function actually asks for them.
/// <para>The engine hands arguments to a function eagerly, but an array argument can hold an
/// arbitrary number of nested values, and most functions ignore the arguments they do not use. The
/// engine therefore wraps an array in an implementation of this interface instead of converting it,
/// and the conversion happens on the first <see cref="KahunaValue.AsArray"/> call. An implementation
/// must cache its result, because a <see cref="KahunaValue"/> is a struct and cannot cache anything
/// itself.</para>
/// </summary>
internal interface IKahunaArraySource
{
    IReadOnlyList<KahunaValue> Materialize();
}

/// <summary>
/// An immutable value passed to, and returned from, a user-defined function.
///
/// <para>The layout is three fields and no allocation. <see cref="Kind"/> says which of the other two
/// is meaningful. <c>scalar</c> carries the bool (0 or 1), the long, the double (through its raw
/// bits), and the offset and length of a byte buffer. <c>reference</c> carries the string, the byte
/// array, and the array source. A scalar value therefore costs nothing to build or to pass.</para>
///
/// <para>The double case is packed through <see cref="BitConverter.DoubleToInt64Bits(double)"/> and
/// never through a shared floating-point slot, because a 64-bit long does not survive a round trip
/// through a double.</para>
///
/// <para>Revision and expiry metadata are deliberately absent. A value a function returns is a plain
/// value, exactly like the value <c>concat</c> or <c>upper</c> returns. To read a key's revision or
/// expiry, apply the built-in <c>rev()</c> or <c>expires()</c> to a key read in the script, and pass
/// the result in as an argument.</para>
/// </summary>
public readonly struct KahunaValue
{
    private readonly long scalar;

    private readonly object? reference;

    /// <summary>The kind of value this instance carries.</summary>
    public KahunaValueKind Kind { get; }

    private KahunaValue(KahunaValueKind kind, long scalar, object? reference)
    {
        Kind = kind;
        this.scalar = scalar;
        this.reference = reference;
    }

    /// <summary>The null value. This is also what <c>default(KahunaValue)</c> is.</summary>
    public static KahunaValue Null => new(KahunaValueKind.Null, 0, null);

    /// <summary>Builds a boolean value.</summary>
    public static KahunaValue From(bool value) => new(KahunaValueKind.Bool, value ? 1 : 0, null);

    /// <summary>Builds a 64-bit integer value.</summary>
    public static KahunaValue From(long value) => new(KahunaValueKind.Long, value, null);

    /// <summary>Builds a double-precision value.</summary>
    public static KahunaValue From(double value) => new(KahunaValueKind.Double, BitConverter.DoubleToInt64Bits(value), null);

    /// <summary>
    /// Builds a string value. A null string builds <see cref="Null"/>, so a function can pass a
    /// missing string through without a branch.
    /// </summary>
    public static KahunaValue From(string? value) => value is null ? Null : new(KahunaValueKind.String, 0, value);

    /// <summary>
    /// Builds a byte-buffer value.
    ///
    /// <para>Memory backed by an array is referenced in place and costs no allocation. Memory backed
    /// by anything else is copied, because the value must stay readable after the caller's memory
    /// manager releases it.</para>
    ///
    /// <para>The buffer is not copied on the way in. A caller that overwrites the array afterwards
    /// changes this value too. The engine copies on the way out, when a returned value enters a
    /// transaction's write set, so a function may return a buffer it reuses.</para>
    /// </summary>
    public static KahunaValue From(ReadOnlyMemory<byte> value)
    {
        if (MemoryMarshal.TryGetArray(value, out ArraySegment<byte> segment) && segment.Array is not null)
            return new(KahunaValueKind.Bytes, Pack(segment.Offset, segment.Count), segment.Array);

        byte[] copy = value.ToArray();

        return new(KahunaValueKind.Bytes, Pack(0, copy.Length), copy);
    }

    /// <summary>
    /// Builds a byte-buffer value over a whole array. A null array builds <see cref="Null"/>.
    /// </summary>
    public static KahunaValue From(byte[]? value) => value is null ? Null : new(KahunaValueKind.Bytes, Pack(0, value.Length), value);

    /// <summary>Builds an array value. A null list builds an empty array.</summary>
    public static KahunaValue FromArray(IReadOnlyList<KahunaValue>? values) => new(KahunaValueKind.Array, 0, values ?? Array.Empty<KahunaValue>());

    /// <summary>
    /// Builds an array value whose elements are converted on first use. The engine calls this for an
    /// array argument, so a function that ignores the argument never pays for the conversion.
    /// </summary>
    internal static KahunaValue FromArraySource(IKahunaArraySource source) => new(KahunaValueKind.Array, 0, source);

    /// <summary>True when this value is <see cref="KahunaValueKind.Null"/>.</summary>
    public bool IsNull => Kind == KahunaValueKind.Null;

    /// <summary>Returns the boolean. Throws when the kind is not <see cref="KahunaValueKind.Bool"/>.</summary>
    public bool AsBool() => Kind == KahunaValueKind.Bool ? scalar != 0 : throw Mismatch(KahunaValueKind.Bool);

    /// <summary>Returns the integer. Throws when the kind is not <see cref="KahunaValueKind.Long"/>.</summary>
    public long AsLong() => Kind == KahunaValueKind.Long ? scalar : throw Mismatch(KahunaValueKind.Long);

    /// <summary>Returns the double. Throws when the kind is not <see cref="KahunaValueKind.Double"/>.</summary>
    public double AsDouble() => Kind == KahunaValueKind.Double ? BitConverter.Int64BitsToDouble(scalar) : throw Mismatch(KahunaValueKind.Double);

    /// <summary>Returns the string. Throws when the kind is not <see cref="KahunaValueKind.String"/>.</summary>
    public string AsString() => Kind == KahunaValueKind.String ? (string)reference! : throw Mismatch(KahunaValueKind.String);

    /// <summary>Returns the byte buffer. Throws when the kind is not <see cref="KahunaValueKind.Bytes"/>.</summary>
    public ReadOnlyMemory<byte> AsBytes()
    {
        if (Kind != KahunaValueKind.Bytes)
            throw Mismatch(KahunaValueKind.Bytes);

        return new((byte[])reference!, Offset, Length);
    }

    /// <summary>
    /// Returns the array elements. Throws when the kind is not <see cref="KahunaValueKind.Array"/>.
    /// The first call on an argument converts the underlying elements; later calls reuse that result.
    /// </summary>
    public IReadOnlyList<KahunaValue> AsArray()
    {
        if (Kind != KahunaValueKind.Array)
            throw Mismatch(KahunaValueKind.Array);

        if (reference is IKahunaArraySource source)
            return source.Materialize();

        return (IReadOnlyList<KahunaValue>)reference!;
    }

    /// <summary>Returns the boolean without throwing. False when the kind does not match.</summary>
    public bool TryGetBool(out bool value)
    {
        value = Kind == KahunaValueKind.Bool && scalar != 0;

        return Kind == KahunaValueKind.Bool;
    }

    /// <summary>Returns the integer without throwing. False when the kind does not match.</summary>
    public bool TryGetLong(out long value)
    {
        value = Kind == KahunaValueKind.Long ? scalar : 0;

        return Kind == KahunaValueKind.Long;
    }

    /// <summary>Returns the double without throwing. False when the kind does not match.</summary>
    public bool TryGetDouble(out double value)
    {
        value = Kind == KahunaValueKind.Double ? BitConverter.Int64BitsToDouble(scalar) : 0;

        return Kind == KahunaValueKind.Double;
    }

    /// <summary>Returns the string without throwing. False when the kind does not match.</summary>
    public bool TryGetString(out string value)
    {
        value = Kind == KahunaValueKind.String ? (string)reference! : string.Empty;

        return Kind == KahunaValueKind.String;
    }

    /// <summary>Returns the byte buffer without throwing. False when the kind does not match.</summary>
    public bool TryGetBytes(out ReadOnlyMemory<byte> value)
    {
        if (Kind != KahunaValueKind.Bytes)
        {
            value = default;

            return false;
        }

        value = new((byte[])reference!, Offset, Length);

        return true;
    }

    /// <summary>Returns the array elements without throwing. False when the kind does not match.</summary>
    public bool TryGetArray(out IReadOnlyList<KahunaValue> value)
    {
        if (Kind != KahunaValueKind.Array)
        {
            value = Array.Empty<KahunaValue>();

            return false;
        }

        value = AsArray();

        return true;
    }

    public override string ToString()
    {
        return Kind switch
        {
            KahunaValueKind.Null   => "(null)",
            KahunaValueKind.Bool   => AsBool() ? "true" : "false",
            KahunaValueKind.Long   => AsLong().ToString(System.Globalization.CultureInfo.InvariantCulture),
            KahunaValueKind.Double => AsDouble().ToString(System.Globalization.CultureInfo.InvariantCulture),
            KahunaValueKind.String => AsString(),
            KahunaValueKind.Bytes  => "(bytes:" + Length + ")",
            KahunaValueKind.Array  => "(array)",
            _ => "(unknown)"
        };
    }

    /// <summary>Start of the byte buffer inside the array held in <c>reference</c>.</summary>
    private int Offset => (int)(scalar >> 32);

    /// <summary>Length of the byte buffer inside the array held in <c>reference</c>.</summary>
    private int Length => (int)(scalar & 0xFFFFFFFFL);

    private static long Pack(int offset, int length) => ((long)offset << 32) | (uint)length;

    private KahunaFunctionException Mismatch(KahunaValueKind expected)
    {
        return new("Expected a " + expected + " value but the value is " + Kind);
    }
}
