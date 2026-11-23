using Kahuna.Utils;

namespace Kahuna.Server.Tests;

/// <summary>
/// <see cref="SharedKeyStrings"/>: the cache that gives one string instance to the equal key strings a node
/// keeps in its transaction records, its completion receipts and its settled identities.
/// </summary>
public sealed class TestSharedKeyStrings
{
    // A new string with the characters of the argument, as a decoder makes for each log entry.
    private static string Copy(string value) => new(value.AsSpan());

    [Fact]
    public void EqualStrings_GetOneInstance_AndOnlyTheFirstIsNewToTheCache()
    {
        SharedKeyStrings strings = new();

        string first = Copy("accounts/000000000000000001");
        string second = Copy("accounts/000000000000000001");
        Assert.NotSame(first, second);

        Assert.Same(first, strings.Share(first, out bool firstAdded));
        Assert.True(firstAdded);

        Assert.Same(first, strings.Share(second, out bool secondAdded));
        Assert.False(secondAdded);

        // The shared instance itself is not new the second time.
        Assert.Same(first, strings.Share(first, out bool again));
        Assert.False(again);
    }

    [Fact]
    public void DifferentStrings_KeepTheirOwnInstances()
    {
        SharedKeyStrings strings = new();

        string a = strings.Share(Copy("accounts/a"));
        string b = strings.Share(Copy("accounts/b"));

        Assert.Equal("accounts/a", a);
        Assert.Equal("accounts/b", b);
        Assert.Same(a, strings.Share(Copy("accounts/a")));
        Assert.Same(b, strings.Share(Copy("accounts/b")));
    }

    [Fact]
    public void TheEmptyString_IsTheSingletonAndIsNeverNew()
    {
        SharedKeyStrings strings = new();

        Assert.Same(string.Empty, strings.Share(Copy(""), out bool added));
        Assert.False(added);
    }

    [Fact]
    public void TwoStringsOfOnePair_BothStayShared()
    {
        // One pair of slots: every string has the same hash value for the table.
        SharedKeyStrings strings = new(slotPairs: 1);

        string a = strings.Share(Copy("a"));
        string b = strings.Share(Copy("b"));

        // Each lookup finds its string, whichever of the two was used last.
        for (int i = 0; i < 4; i++)
        {
            Assert.Same(a, strings.Share(Copy("a"), out bool aAdded));
            Assert.False(aAdded);
            Assert.Same(b, strings.Share(Copy("b"), out bool bAdded));
            Assert.False(bAdded);
        }
    }

    [Fact]
    public void AThirdStringOfOnePair_ReplacesTheLeastRecentlyUsed()
    {
        SharedKeyStrings strings = new(slotPairs: 1);

        string a = strings.Share(Copy("a"));
        string b = strings.Share(Copy("b"));

        // "a" is used again, so "b" is the older of the two when "c" arrives.
        Assert.Same(a, strings.Share(Copy("a")));
        string c = strings.Share(Copy("c"), out bool cAdded);
        Assert.True(cAdded);

        Assert.Same(a, strings.Share(Copy("a")));
        Assert.Same(c, strings.Share(Copy("c")));

        // "b" left the table: an equal string is new again, and becomes the shared one.
        string newB = Copy("b");
        Assert.Same(newB, strings.Share(newB, out bool bAdded));
        Assert.True(bAdded);
        Assert.NotSame(b, newB);
    }

    [Fact]
    public void AStringTheTableForgot_IsStillReturnedEqual()
    {
        SharedKeyStrings strings = new(slotPairs: 2);

        // Far more strings than slots: every result must equal its argument, shared or not.
        for (int round = 0; round < 3; round++)
        {
            for (int i = 0; i < 1_000; i++)
            {
                string key = $"ranged1/accounts/k{i:D12}";
                Assert.Equal(key, strings.Share(Copy(key)));
            }
        }
    }

    [Fact]
    public void ConcurrentCallers_AlwaysGetAStringEqualToTheirOwn()
    {
        // A small table and many threads on the same keys: the writes to one pair of slots race.
        SharedKeyStrings strings = new(slotPairs: 8);
        string[] keys = [.. Enumerable.Range(0, 64).Select(i => $"ranged1/accounts/k{i:D12}")];
        int mismatches = 0;

        Parallel.For(0, 8, new ParallelOptions { MaxDegreeOfParallelism = 8 }, worker =>
        {
            for (int round = 0; round < 20_000; round++)
            {
                string key = keys[(round + worker) % keys.Length];
                string shared = strings.Share(Copy(key));

                if (!string.Equals(shared, key, StringComparison.Ordinal))
                    Interlocked.Increment(ref mismatches);
            }
        });

        Assert.Equal(0, mismatches);
    }

    [Fact]
    public void TheNumberOfSlotPairs_MustBeAPowerOfTwo()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new SharedKeyStrings(slotPairs: 0));
        Assert.Throws<ArgumentOutOfRangeException>(() => new SharedKeyStrings(slotPairs: 3));
    }
}
