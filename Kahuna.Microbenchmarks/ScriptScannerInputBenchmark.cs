using System.Buffers;
using System.Text;
using BenchmarkDotNet.Attributes;
using Microsoft.IO;

namespace Kahuna.Microbenchmarks;

/// <summary>
/// Before/after for how the transaction-script scanner materializes its input, in
/// <c>scriptParser.Parse(ReadOnlySpan&lt;byte&gt;)</c>.
///
/// <para><b>Old</b> = a <c>RecyclableMemoryStream</c> built from the UTF-8 bytes, fed to the
/// stream-based GPLEX scanner. That path wraps the stream in a <see cref="StreamReader"/> (which
/// sniffs the encoding) and buffers decoded characters into <see cref="StringBuilder"/> blocks
/// (GPLEX's <c>BuildBuffer</c>) before the scanner reads a single character.</para>
///
/// <para><b>New</b> = <c>Encoding.UTF8.GetString(bytes)</c> once, scanned directly by the
/// string-based GPLEX scanner (<c>StringBuffer</c>), which holds the string by reference and reads
/// characters with a plain index. One decoded-string allocation replaces the stream + reader +
/// builder chain (and drops the <c>Microsoft.IO.RecyclableMemoryStream</c> dependency).</para>
///
/// <para>Only the input materialization differs between the two scanners; the token state machine
/// consumes the same characters either way. Both methods fold every character into a checksum so
/// the JIT cannot elide the decoding/reading work. Sizes span short, typical, large, and a
/// caller-scale 1 MiB script.</para>
///
/// <para><b>Result</b> (net10.0, Release, Arm64; Mean / Allocated, StringBacked ratio in parens):
/// <list type="bullet">
///   <item>32 B: 551 ns → 21 ns (0.04×); 3768 B → 88 B (0.02×) — the common case; ~26× faster,
///     ~43× less allocated.</item>
///   <item>512 B: 1137 ns → 448 ns (0.39×); 4728 B → 1048 B (0.22×).</item>
///   <item>8192 B: 12.59 us → 7.57 us (0.60×); 20160 B → 16408 B (0.81×).</item>
///   <item>1 MiB: 79.9 ms → 1.04 ms (0.01×); 2013360 B → 2000023 B (0.99×) — the stream path's
///     StreamReader + StringBuilder buffering degrades pathologically at caller scale; the direct
///     decode is ~77× faster for near-identical allocation.</item>
/// </list>
/// The string path wins on both time and allocation at every size, decisively so for the short
/// scripts that dominate. The remaining allocation is the single decoded string; eliminating it
/// would require a pooled-char <c>ScanBuff</c> and a new <c>SetSource</c> overload, which the
/// numbers do not justify — the per-token <c>yytext</c> still allocates and parse runs only on a
/// script-cache miss.</para>
/// </summary>
[MemoryDiagnoser]
public class ScriptScannerInputBenchmark
{
    private static readonly RecyclableMemoryStreamManager Manager = new();

    /// <summary>Short ≈ 32 B, typical ≈ 512 B, large ≈ 8 KiB, caller-scale ≈ 1 MiB.</summary>
    [Params(32, 512, 8192, 1_000_000)]
    public int ScriptByteLength;

    private byte[] _bytes = [];

    [GlobalSetup]
    public void Setup()
    {
        // ASCII content — 1 char == 1 UTF-8 byte — so ScriptByteLength drives the byte count.
        _bytes = Encoding.UTF8.GetBytes(new string('x', ScriptByteLength));
    }

    /// <summary>
    /// Old path: RecyclableMemoryStream + StreamReader (with encoding detection) + StringBuilder
    /// block buffering, mirroring the stream-based scanner's <c>BuildBuffer</c>.
    /// </summary>
    [Benchmark(Baseline = true)]
    public int StreamBacked()
    {
        using RecyclableMemoryStream stream = Manager.GetStream(_bytes);
        using StreamReader reader = new(stream, Encoding.UTF8, detectEncodingFromByteOrderMarks: true);

        StringBuilder builder = new();
        char[] block = ArrayPool<char>.Shared.Rent(4096);
        try
        {
            int read;
            while ((read = reader.Read(block, 0, block.Length)) > 0)
                builder.Append(block, 0, read);
        }
        finally
        {
            ArrayPool<char>.Shared.Return(block);
        }

        int sum = 0;
        for (int i = 0; i < builder.Length; i++)
            sum = sum * 31 + builder[i];

        return sum;
    }

    /// <summary>
    /// New path: a single UTF-8 decode into a string, scanned by index (StringBuffer).
    /// </summary>
    [Benchmark]
    public int StringBacked()
    {
        string script = Encoding.UTF8.GetString(_bytes);

        int sum = 0;
        for (int i = 0; i < script.Length; i++)
            sum = sum * 31 + script[i];

        return sum;
    }
}
