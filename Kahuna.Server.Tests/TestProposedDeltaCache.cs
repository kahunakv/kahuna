using Kahuna.Server.KeyValues.Transactions;

namespace Kahuna.Server.Tests;

/// <summary>The proposed-delta cache hands out a registered batch once per budgeted take, so every
/// co-hosted node's single local apply can reuse the producer's decoded commands; the entry is released
/// once the budget is spent. These tests drive the budget explicitly, because the default budget reads
/// the live-node census, which other tests in the process move.</summary>
public sealed class TestProposedDeltaCache
{
    [Fact]
    public void TakesExactlyTheBudget_ThenMisses()
    {
        ProposedDeltaCache<string> cache = new();
        byte[] bytes = [1, 2, 3];
        string[] commands = ["a", "b"];

        cache.Register(bytes, commands, takeBudget: 2);

        Assert.True(cache.TryTake(bytes, out string[]? first));
        Assert.Same(commands, first);
        Assert.True(cache.TryTake(bytes, out string[]? second));
        Assert.Same(commands, second);
        Assert.False(cache.TryTake(bytes, out _));
    }

    [Fact]
    public void EqualContentInADifferentArray_DoesNotHit()
    {
        ProposedDeltaCache<string> cache = new();
        byte[] registered = [1, 2, 3];
        byte[] lookalike = [1, 2, 3];

        cache.Register(registered, ["a"], takeBudget: 5);

        // The cache keys on array identity — a fresh materialization of the same bytes (WAL replay,
        // network receive) must decode, never inherit another array's commands.
        Assert.False(cache.TryTake(lookalike, out _));
        Assert.True(cache.TryTake(registered, out _));
    }
}
