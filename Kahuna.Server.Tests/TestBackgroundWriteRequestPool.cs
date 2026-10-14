
using Kahuna.Server.Persistence;
using Kommander.Time;

namespace Kahuna.Server.Tests;

/// <summary>
/// The request pool's bound follows what the writer found queued at a flush tick: sized to an interval's
/// population, the pool hands every returned request back on the next interval's rents; sized down, it trims
/// to the new bound at once; the bound never leaves its clamp. Each test owns its pool, so the process-wide
/// one keeps serving the other tests' writers untouched.
/// </summary>
public sealed class TestBackgroundWriteRequestPool
{
    private static BackgroundWriteRequest Rent(BackgroundWriteRequestPool pool, int i) =>
        pool.Rent(BackgroundWriteType.QueueStoreKeyValue, 0, "k", null, i,
            HLCTimestamp.Zero, HLCTimestamp.Zero, HLCTimestamp.Zero, 0);

    /// <summary>Rents one interval's population from an empty pool, so every request is a fresh object.</summary>
    private static List<BackgroundWriteRequest> RentInterval(BackgroundWriteRequestPool pool, int size)
    {
        List<BackgroundWriteRequest> interval = new(size);
        for (int i = 0; i < size; i++)
            interval.Add(Rent(pool, i));
        return interval;
    }

    [Fact]
    public void SizedToTheQueuedPopulation_EveryReturnedRequestComesBack()
    {
        BackgroundWriteRequestPool pool = new();
        const int Queued = 3 * BackgroundWriteRequestPool.MinCapacity;

        List<BackgroundWriteRequest> interval = RentInterval(pool, Queued);

        // The tick finds the whole interval queued, sizes the pool, then the drain returns it.
        pool.SizeToQueued(Queued);
        foreach (BackgroundWriteRequest request in interval)
            pool.Return(request);

        Assert.Equal(Queued, pool.Capacity);
        Assert.Equal(Queued, pool.PooledCount);

        // The next interval rents the same population back: no rent falls through to a fresh object.
        HashSet<BackgroundWriteRequest> returned = new(interval);
        for (int i = 0; i < Queued; i++)
            Assert.True(returned.Remove(Rent(pool, i)), $"rent {i} produced a fresh object with the pool sized to {Queued}");

        Assert.Empty(returned);
        Assert.Equal(0, pool.PooledCount);
    }

    [Fact]
    public void ReturnBeyondTheBound_DropsTheRequest()
    {
        BackgroundWriteRequestPool pool = new();
        const int Over = BackgroundWriteRequestPool.MinCapacity + 64;

        foreach (BackgroundWriteRequest request in RentInterval(pool, Over))
            pool.Return(request);

        Assert.Equal(BackgroundWriteRequestPool.MinCapacity, pool.PooledCount);
    }

    [Fact]
    public void SizedDown_TrimsThePooledExcessAtOnce()
    {
        BackgroundWriteRequestPool pool = new();
        const int Peak = 2 * BackgroundWriteRequestPool.MinCapacity;

        List<BackgroundWriteRequest> interval = RentInterval(pool, Peak);
        pool.SizeToQueued(Peak);
        foreach (BackgroundWriteRequest request in interval)
            pool.Return(request);

        Assert.Equal(Peak, pool.PooledCount);

        // A quiet tick: the bound falls to the floor and the pool drops what no producer will rent.
        pool.SizeToQueued(0);

        Assert.Equal(BackgroundWriteRequestPool.MinCapacity, pool.Capacity);
        Assert.Equal(BackgroundWriteRequestPool.MinCapacity, pool.PooledCount);
    }

    [Fact]
    public void Bound_StaysWithinTheClamp()
    {
        BackgroundWriteRequestPool pool = new();

        Assert.Equal(BackgroundWriteRequestPool.MinCapacity, pool.Capacity);

        pool.SizeToQueued(1);
        Assert.Equal(BackgroundWriteRequestPool.MinCapacity, pool.Capacity);

        pool.SizeToQueued(BackgroundWriteRequestPool.MinCapacity + 1);
        Assert.Equal(BackgroundWriteRequestPool.MinCapacity + 1, pool.Capacity);

        pool.SizeToQueued(int.MaxValue);
        Assert.Equal(BackgroundWriteRequestPool.MaxCapacity, pool.Capacity);
    }

    [Fact]
    public void RecycledRequest_CarriesOnlyTheNewFields()
    {
        BackgroundWriteRequestPool pool = new();

        BackgroundWriteRequest first = pool.Rent(BackgroundWriteType.QueueStoreKeyValue, 7, "old", [1, 2, 3], 5,
            new HLCTimestamp(1, 2, 3), new HLCTimestamp(4, 5, 6), new HLCTimestamp(7, 8, 9), 1, noRevision: true, logIndex: 42);
        pool.Return(first);

        BackgroundWriteRequest second = pool.Rent(BackgroundWriteType.QueueStoreLock, 3, "new", null, 9,
            HLCTimestamp.Zero, HLCTimestamp.Zero, HLCTimestamp.Zero, 0);

        Assert.Same(first, second);
        Assert.Equal(BackgroundWriteType.QueueStoreLock, second.Type);
        Assert.Equal(3, second.PartitionId);
        Assert.Equal("new", second.Key);
        Assert.Null(second.Value);
        Assert.Equal(9, second.Revision);
        Assert.Equal(HLCTimestamp.Zero, second.Expires);
        Assert.Equal(HLCTimestamp.Zero, second.LastUsed);
        Assert.Equal(HLCTimestamp.Zero, second.LastModified);
        Assert.Equal(0, second.State);
        Assert.False(second.NoRevision);
        Assert.Equal(-1, second.LogIndex);
        Assert.Null(second.CompletionSource);
    }
}
