using System.Buffers.Text;
using System.Text;
using BenchmarkDotNet.Attributes;

namespace Kahuna.Microbenchmarks;

/// <summary>
/// Before/after comparison for <c>KeyValueExpressionResult.ToBytes()</c>.
///
/// <para><b>Bool</b>: Old = <c>"true"u8.ToArray()</c> (fresh array per call) vs New = a shared
/// <c>static readonly byte[]</c>. <b>Long</b>: Old = <c>Encoding.UTF8.GetBytes(v.ToString())</c>
/// (string + byte[]) vs New = <see cref="Utf8Formatter"/> into a <c>stackalloc</c> + one
/// <c>ToArray()</c>.</para>
///
/// The production type is <c>internal</c>, so both paths are reproduced verbatim here.
/// </summary>
[MemoryDiagnoser]
public class ExpressionResultBytesBenchmark
{
    private static readonly byte[] TrueBytes = [.. "true"u8];

    private const long Value = -9_223_372_036_854L;

    [Benchmark(Baseline = true)]
    public byte[] OldBool() => "true"u8.ToArray();

    [Benchmark]
    public byte[] NewBool() => TrueBytes;

    [Benchmark]
    public byte[] OldLong() => Encoding.UTF8.GetBytes(Value.ToString());

    [Benchmark]
    public byte[] NewLong()
    {
        Span<byte> buf = stackalloc byte[32];
        Utf8Formatter.TryFormat(Value, buf, out int written);
        return buf[..written].ToArray();
    }
}
