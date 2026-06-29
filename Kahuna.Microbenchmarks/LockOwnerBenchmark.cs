using System.Text;
using BenchmarkDotNet.Attributes;

namespace Kahuna.Microbenchmarks;

/// <summary>
/// Before/after comparison for lock-owner byte generation in <c>KahunaClient</c>.
///
/// <para><b>Old</b>: <c>Encoding.UTF8.GetBytes(Guid.NewGuid().ToString("N"))</c> — allocates a
/// 32-character string and then a 32-byte array.</para>
///
/// <para><b>New</b>: formats the GUID directly into <c>stackalloc char[32]</c> via
/// <c>Guid.TryFormat</c>, then encodes into a pre-allocated <c>new byte[32]</c> with
/// <c>Encoding.ASCII.GetBytes</c>. Eliminates the intermediate string.</para>
///
/// <para>The production helper (<c>KahunaClient.NewLockOwner</c>) is <c>private</c>, so both
/// paths are reproduced verbatim here — the same approach used by
/// <see cref="ExpressionResultBytesBenchmark"/>.</para>
///
/// <para><b>Result</b> (net10.0, Release): 237.8 ns/144 B → 234.5 ns/56 B (0.99× time, 0.39× alloc).
/// The win is allocation, not speed: the new path drops the 32-char intermediate string (~88 B),
/// leaving only the owned <c>byte[32]</c> result (56 B). Time is flat because <c>Guid.NewGuid()</c>'s
/// RNG dominates both paths — so this matters under lock churn (one fewer allocation per acquire),
/// not in per-call latency.</para>
/// </summary>
[MemoryDiagnoser]
public class LockOwnerBenchmark
{
    /// <summary>Old path: Guid → string → byte[].</summary>
    [Benchmark(Baseline = true)]
    public byte[] Old() => Encoding.UTF8.GetBytes(Guid.NewGuid().ToString("N"));

    /// <summary>New path: Guid → stackalloc char[32] → byte[32], no intermediate string.</summary>
    [Benchmark]
    public byte[] New()
    {
        byte[] owner = new byte[32];
        Span<char> chars = stackalloc char[32];
        Guid.NewGuid().TryFormat(chars, out _, "N");
        Encoding.ASCII.GetBytes(chars, owner);
        return owner;
    }
}
