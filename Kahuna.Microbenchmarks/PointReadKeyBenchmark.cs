using System.Buffers;
using System.Text;
using BenchmarkDotNet.Attributes;

namespace Kahuna.Microbenchmarks;

/// <summary>
/// Before/after for the point-read key construction in
/// <c>RocksDbPersistenceBackend.GetLock</c>/<c>GetKeyValue</c>.
///
/// <para><b>Old</b> = <c>resource + CurrentMarker</c> (heap string) + an unbounded
/// <c>stackalloc byte[GetByteCount(currentKey)]</c> + encode. <b>New</b> = bounded
/// <c>stackalloc byte[512]</c>/<see cref="ArrayPool{T}"/> + encode of <c>resource</c> + marker
/// <c>CopyTo</c>. The change removes the string allocation and bounds the stack frame.</para>
///
/// To keep the buffer work observable (and not elided), each method folds the key bytes into a
/// checksum instead of calling RocksDB.
/// </summary>
[MemoryDiagnoser]
public class PointReadKeyBenchmark
{
    private const string CurrentMarker = "~CURRENT";
    private static ReadOnlySpan<byte> CurrentMarkerUtf8 => "~CURRENT"u8;
    private const int KeyStackThreshold = 512;

    // Typical resource/key name for a lock or KV entry.
    [Params("locks/orders/0000000042", "a")]
    public string Resource = "";

    [Benchmark(Baseline = true)]
    public int Old()
    {
        string currentKey = Resource + CurrentMarker;
        Span<byte> buffer = stackalloc byte[Encoding.UTF8.GetByteCount(currentKey)];
        Encoding.UTF8.GetBytes(currentKey.AsSpan(), buffer);
        return Checksum(buffer);
    }

    [Benchmark]
    public int New()
    {
        int keyLen = Encoding.UTF8.GetByteCount(Resource);
        int totalLen = keyLen + CurrentMarkerUtf8.Length;

        byte[]? rented = null;
        Span<byte> buffer = totalLen <= KeyStackThreshold
            ? stackalloc byte[KeyStackThreshold]
            : (rented = ArrayPool<byte>.Shared.Rent(totalLen));
        try
        {
            buffer = buffer[..totalLen];
            Encoding.UTF8.GetBytes(Resource.AsSpan(), buffer);
            CurrentMarkerUtf8.CopyTo(buffer[keyLen..]);
            return Checksum(buffer);
        }
        finally
        {
            if (rented is not null)
                ArrayPool<byte>.Shared.Return(rented);
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
