using BenchmarkDotNet.Attributes;
using Kahuna.Utils;

namespace Kahuna.Microbenchmarks;

/// <summary>
/// Cost of <see cref="SharedKeyStrings.Share(string)"/>, which each store of the retained transaction state
/// calls for a key before it keeps it. Each benchmark shares a batch of 1,024 keys of 30 characters.
///
/// <para><b>Shape:</b> <see cref="Held"/> is the common case: the cache holds an equal string, and the caller
/// passes a string of its own, as a store that decoded a log entry does. <see cref="Forgotten"/> passes keys
/// from a set four times the size of the table, so most lookups find no equal string and store theirs.</para>
/// </summary>
[MemoryDiagnoser]
public class SharedKeyStringsBenchmark
{
    private const int Batch = 1_024;

    private SharedKeyStrings held = null!;
    private SharedKeyStrings forgotten = null!;

    private string[] heldKeys = null!;
    private string[] manyKeys = null!;
    private int next;

    private static string Key(int n) => $"t/accounts/{n:D19}";

    [GlobalSetup]
    public void Setup()
    {
        held = new SharedKeyStrings();
        heldKeys = new string[Batch];

        // The cache holds one instance of each key of a space of 10,000, and the benchmark passes another.
        for (int i = 0; i < 10_000; i++)
            held.Share(Key(i));

        for (int i = 0; i < Batch; i++)
            heldKeys[i] = Key(i * 9);

        forgotten = new SharedKeyStrings();
        manyKeys = new string[8 * SharedKeyStrings.DefaultSlotPairs];

        for (int i = 0; i < manyKeys.Length; i++)
            manyKeys[i] = Key(1_000_000 + i);
    }

    [Benchmark]
    public int Held()
    {
        int length = 0;
        foreach (string key in heldKeys)
            length += held.Share(key).Length;

        return length;
    }

    [Benchmark]
    public int Forgotten()
    {
        int length = 0;
        for (int i = 0; i < Batch; i++)
        {
            length += forgotten.Share(manyKeys[next]).Length;
            next = (next + 1) % manyKeys.Length;
        }

        return length;
    }
}
