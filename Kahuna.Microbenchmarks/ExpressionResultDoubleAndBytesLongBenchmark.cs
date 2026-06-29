using System.Buffers.Text;
using System.Globalization;
using System.Text;
using BenchmarkDotNet.Attributes;

namespace Kahuna.Microbenchmarks;

/// <summary>
/// Before/after comparison for allocation improvements in <c>KeyValueExpressionResult</c>:
///
/// <list type="bullet">
///   <item><b>Double → bytes</b>: Old = <c>Encoding.UTF8.GetBytes(d.ToString(InvariantCulture))</c>
///   — allocates a string and a <c>byte[]</c>. New = <c>double.TryFormat(Span&lt;byte&gt;, ...)</c>
///   via <c>IUtf8SpanFormattable</c> into <c>stackalloc byte[32]</c>, one final
///   <c>ToArray()</c> — no intermediate string.</item>
///   <item><b>Bytes → long</b>: Old = <c>long.Parse(Encoding.UTF8.GetString(bytes), ...)</c>
///   — allocates a transient string. New = <c>Utf8Parser.TryParse</c> directly on the
///   <c>byte[]</c> span; falls back to the old path only for malformed input.</item>
/// </list>
///
/// The production helpers (<c>EncodeDouble</c>, <c>ParseLongFromBytes</c>) are <c>private</c>,
/// so both paths are reproduced verbatim — matching the <see cref="ExpressionResultBytesBenchmark"/>
/// convention.
///
/// <para><b>Result</b> (net10.0, Release; measured — double <c>3.14159265358979</c>, long bytes
/// <c>"-9223372036854775808"</c>):
/// <list type="bullet">
///   <item>OldDouble → 59.9 ns / 96 B; NewDouble → 51.0 ns / 40 B (~1.18× faster, ~2.4× less alloc).
///   The speedup is modest because <c>TryFormat</c> and <c>ToString</c> do the same expensive
///   shortest-round-trippable formatting; the only saving is the ~56 B intermediate string, leaving
///   just the owned result <c>byte[]</c>.</item>
///   <item>OldLong → 21.8 ns / 64 B; NewLong → 8.9 ns / 0 B (~2.45× faster, allocation eliminated).
///   <c>Utf8Parser.TryParse</c> skips both the UTF-8 decode and the transient parse string, and the
///   method returns a value type — so the fast path allocates nothing.</item>
/// </list>
/// </para>
/// </summary>
[MemoryDiagnoser]
public class ExpressionResultDoubleAndBytesLongBenchmark
{
    private const double DoubleValue = 3.14159265358979;
    private static readonly byte[] LongBytes = "-9223372036854775808"u8.ToArray();

    // ── double → bytes ───────────────────────────────────────────────────────────

    [Benchmark(Baseline = true)]
    public byte[] OldDouble() =>
        Encoding.UTF8.GetBytes(DoubleValue.ToString(CultureInfo.InvariantCulture));

    [Benchmark]
    public byte[] NewDouble()
    {
        Span<byte> buf = stackalloc byte[32];
        DoubleValue.TryFormat(buf, out int written, default, CultureInfo.InvariantCulture);
        return buf[..written].ToArray();
    }

    // ── bytes → long ─────────────────────────────────────────────────────────────

    [Benchmark]
    public long OldLong() =>
        long.Parse(Encoding.UTF8.GetString(LongBytes), System.Globalization.NumberStyles.Integer, CultureInfo.InvariantCulture);

    [Benchmark]
    public long NewLong()
    {
        if (Utf8Parser.TryParse(LongBytes, out long value, out int consumed) && consumed == LongBytes.Length)
            return value;
        return long.Parse(Encoding.UTF8.GetString(LongBytes), System.Globalization.NumberStyles.Integer, CultureInfo.InvariantCulture);
    }
}
