using System.Buffers;
using BenchmarkDotNet.Attributes;

namespace Kahuna.Microbenchmarks;

/// <summary>
/// Allocation comparison for <c>TryGetByRangeHandler.GetPrefixUpperBound</c> that records why the
/// T1 span/stackalloc rewrite was rejected.
///
/// <para><b>Old</b> = the original (and current, after revert)
/// <c>string.Concat(prefix.AsSpan(0, i), ((char)(prefix[i]+1)).ToString())</c> form.
/// <b>New</b> = the proposed span/stackalloc rewrite.</para>
///
/// The measured result: <b>New</b> saves a flat 24 B/call (the eliminated single-char string) but
/// runs 2–37% slower because <c>stackalloc char[128]</c> zeroes 256 bytes every call and the
/// rent path pays <see cref="ArrayPool{T}"/> overhead. Since this method is called once per scan
/// page (not per item), the constant 24 B is negligible and the time regression isn't worth it, so
/// <b>Old</b> was kept. This benchmark stays as the evidence behind that call.
/// </summary>
[MemoryDiagnoser]
public class GetPrefixUpperBoundBenchmark
{
    /// <summary>
    /// Prefix lengths chosen to exercise both code paths of the new implementation:
    /// 8/64/128 stay on the <c>stackalloc</c> path (&lt;= 128 chars); 512 forces the
    /// <see cref="ArrayPool{T}"/> fallback.
    /// </summary>
    [Params(8, 64, 128, 512)]
    public int PrefixLength;

    private string _prefix = "";

    [GlobalSetup]
    public void Setup()
    {
        // A prefix whose last char is incrementable, so both variants take the
        // common (non-null) return path rather than short-circuiting.
        _prefix = new string('a', PrefixLength);
    }

    [Benchmark(Baseline = true)]
    public string? Old() => OldGetPrefixUpperBound(_prefix);

    [Benchmark]
    public string? New() => NewGetPrefixUpperBound(_prefix);

    // ── original implementation (pre-T1) ───────────────────────────────────────

    private static string? OldGetPrefixUpperBound(string prefix)
    {
        for (int i = prefix.Length - 1; i >= 0; i--)
        {
            if (prefix[i] < char.MaxValue)
                return string.Concat(prefix.AsSpan(0, i), ((char)(prefix[i] + 1)).ToString());
        }

        return null;
    }

    // ── new implementation (T1, as shipped in TryGetByRangeHandler) ─────────────

    private static string? NewGetPrefixUpperBound(string prefix)
    {
        const int StackThreshold = 128; // chars = 256 bytes on stack
        char[]? rented = null;
        Span<char> buf = prefix.Length <= StackThreshold
            ? stackalloc char[StackThreshold]
            : (rented = ArrayPool<char>.Shared.Rent(prefix.Length));
        try
        {
            for (int i = prefix.Length - 1; i >= 0; i--)
            {
                if (prefix[i] < char.MaxValue)
                {
                    prefix.AsSpan(0, i).CopyTo(buf);
                    buf[i] = (char)(prefix[i] + 1);
                    return new string(buf[..(i + 1)]);
                }
            }

            return null;
        }
        finally
        {
            if (rented is not null)
                ArrayPool<char>.Shared.Return(rented);
        }
    }
}
