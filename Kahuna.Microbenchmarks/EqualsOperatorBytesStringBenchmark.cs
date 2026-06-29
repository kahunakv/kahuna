using System.Buffers;
using System.Text;
using BenchmarkDotNet.Attributes;

namespace Kahuna.Microbenchmarks;

/// <summary>
/// Before/after comparison for the bytes-vs-string equality branches in
/// <c>EqualsOperator</c>.
///
/// <para><b>Old</b>: <c>stackalloc byte[Encoding.UTF8.GetByteCount(str)]</c> — unbounded stack
/// allocation sized directly from the string operand (script or stored value). The danger is not the
/// sizes measured here (65536 bytes is a 64 KB single frame, well under a 1 MB stack) but operands in
/// the hundreds-of-KB-to-MB range, which overflow the stack and crash the process. Those sizes are
/// deliberately excluded — the unbounded path cannot survive them, so the benchmark caps at sizes both
/// paths run safely.</para>
///
/// <para><b>New</b>: stack for ≤ 256 bytes, <see cref="ArrayPool{T}"/> rent for larger values.
/// The comparison itself (<see cref="ReadOnlySpan{T}.SequenceEqual"/>) is identical.</para>
///
/// <para>Both methods perform the bytes-vs-string path (the <c>left=BytesType, right=StringType</c>
/// branch). Results are folded into a <c>bool</c> so the JIT cannot elide the work.</para>
///
/// <para><b>Result</b> (net10.0, Release; both paths 0 B allocated at every size — stackalloc and a
/// rented-then-returned pooled buffer are both off-heap):
/// <list type="bullet">
///   <item>8 B: 5.54 ns → 5.55 ns (1.00×) — parity; tiny operands take the stack path in both.</item>
///   <item>128 B: 15.0 ns → 14.8 ns (0.98×) — parity, still stack on both.</item>
///   <item>4096 B: 409 ns → 318 ns (0.78×) — the pooled path is <i>faster</i>.</item>
///   <item>65536 B: 5413 ns → 4349 ns (0.80×) — same effect, larger.</item>
/// </list>
/// The bound is not merely free above the 256-byte threshold, it is a speedup: <c>stackalloc</c>
/// zero-initializes the whole buffer on every call, whereas <c>ArrayPool.Rent</c> returns a dirty
/// buffer that <c>GetBytes</c> fully overwrites — so the new path skips a large memset. The headline
/// win is still robustness (no unbounded stack growth on caller-controlled sizes); the speedup at
/// large sizes is a bonus.</para>
/// </summary>
[MemoryDiagnoser]
public class EqualsOperatorBytesStringBenchmark
{
    private const int StackAllocThreshold = 256;

    [Params(8, 128, 4096, 65536)]
    public int ByteCount;

    private byte[] _inputBytes = [];
    private string _inputString = "";

    [GlobalSetup]
    public void Setup()
    {
        _inputString = new string('a', ByteCount);
        _inputBytes = Encoding.UTF8.GetBytes(_inputString);
    }

    /// <summary>
    /// Old path: unbounded <c>stackalloc</c> sized from the string's UTF-8 byte count.
    /// Safe only for small inputs; risks stack overflow for large values.
    /// </summary>
    [Benchmark(Baseline = true)]
    public bool Old()
    {
        Span<byte> buf = stackalloc byte[Encoding.UTF8.GetByteCount(_inputString)];
        Encoding.UTF8.GetBytes(_inputString.AsSpan(), buf);
        return ((ReadOnlySpan<byte>)_inputBytes).SequenceEqual(buf);
    }

    /// <summary>
    /// New path: bounded stackalloc for ≤ 256 bytes, pooled array for larger values.
    /// </summary>
    [Benchmark]
    public bool New()
    {
        int byteCount = Encoding.UTF8.GetByteCount(_inputString);
        byte[]? rented = byteCount > StackAllocThreshold ? ArrayPool<byte>.Shared.Rent(byteCount) : null;
        Span<byte> buf = rented is not null ? rented.AsSpan(0, byteCount) : stackalloc byte[byteCount];
        try
        {
            Encoding.UTF8.GetBytes(_inputString.AsSpan(), buf);
            return ((ReadOnlySpan<byte>)_inputBytes).SequenceEqual(buf);
        }
        finally
        {
            if (rented is not null) ArrayPool<byte>.Shared.Return(rented);
        }
    }
}
