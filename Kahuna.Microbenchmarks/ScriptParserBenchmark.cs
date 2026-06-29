using System.Buffers;
using System.Text;
using BenchmarkDotNet.Attributes;

namespace Kahuna.Microbenchmarks;

/// <summary>
/// Before/after comparison for the string-to-UTF8 transcoding in
/// <c>ScriptParserProcessor.Parse(string)</c>.
///
/// <para><b>Old</b>: <c>stackalloc byte[Encoding.UTF8.GetByteCount(script)]</c> — the stack frame
/// grows with the script length, which is caller-controlled. A 1 MB script yields a 1 MB stack
/// allocation, risking stack overflow on threads with limited stack space.</para>
///
/// <para><b>New</b>: <c>stackalloc</c> for ≤ 4096 bytes (short/typical scripts), then
/// <see cref="ArrayPool{T}"/> rent for larger scripts. The parser itself is not exercised here;
/// only the transcoding buffer strategy is benchmarked.</para>
///
/// <para>Both methods encode the script string into a <c>Span&lt;byte&gt;</c> and fold the bytes
/// into a checksum so the JIT cannot elide the work. The sizes here all run safely on both paths;
/// the unbounded path's actual hazard is MB-scale scripts, which would overflow the stack and so
/// cannot be benchmarked against <c>Old</c>.</para>
///
/// <para><b>Result</b> (net10.0, Release; both paths 0 B allocated — stackalloc and a
/// rented-then-returned pooled buffer are both off-heap):
/// <list type="bullet">
///   <item>32 B: 17.6 ns → 17.65 ns (1.00×) — parity; both take the stack path.</item>
///   <item>512 B: 435 ns → 445 ns (1.02×) — parity, both still stack (within noise).</item>
///   <item>8192 B: 7009 ns → 6849 ns (0.98×) — the pooled path is marginally faster (skips
///     zero-initializing an 8 KB stack buffer; the gain is small here because the per-byte checksum
///     fold dominates the timing).</item>
/// </list>
/// The bound is free: parity at every size with zero allocation. The point is robustness — the old
/// path's stack frame grew without limit on caller-controlled input; the new one is capped at the
/// 4096-byte threshold.</para>
/// </summary>
[MemoryDiagnoser]
public class ScriptParserBenchmark
{
    private const int StackAllocThreshold = 4096;

    /// <summary>Short script ≈ 32 B, typical ≈ 512 B, large ≈ 8192 B.</summary>
    [Params(32, 512, 8192)]
    public int ScriptByteLength;

    private string _script = "";

    [GlobalSetup]
    public void Setup()
    {
        // ASCII content — 1 char == 1 UTF-8 byte — so ScriptByteLength drives the byte count.
        _script = new string('x', ScriptByteLength);
    }

    /// <summary>
    /// Old path: unbounded <c>stackalloc</c> sized from the script's UTF-8 byte count.
    /// </summary>
    [Benchmark(Baseline = true)]
    public int Old()
    {
        Span<byte> buf = stackalloc byte[Encoding.UTF8.GetByteCount(_script)];
        Encoding.UTF8.GetBytes(_script.AsSpan(), buf);
        return Checksum(buf);
    }

    /// <summary>
    /// New path: stack for ≤ 4096 bytes, <see cref="ArrayPool{T}"/> for larger scripts.
    /// </summary>
    [Benchmark]
    public int New()
    {
        int byteCount = Encoding.UTF8.GetByteCount(_script);
        byte[]? rented = byteCount > StackAllocThreshold ? ArrayPool<byte>.Shared.Rent(byteCount) : null;
        Span<byte> buf = rented is not null ? rented.AsSpan(0, byteCount) : stackalloc byte[byteCount];
        try
        {
            Encoding.UTF8.GetBytes(_script.AsSpan(), buf);
            return Checksum(buf);
        }
        finally
        {
            if (rented is not null) ArrayPool<byte>.Shared.Return(rented);
        }
    }

    private static int Checksum(ReadOnlySpan<byte> buf)
    {
        int sum = 0;
        foreach (byte b in buf)
            sum = sum * 31 + b;
        return sum;
    }
}
