
namespace Kahuna.Server.KeyValues.Transactions;

/// <summary>
/// A small pool of reusable <see cref="CancellationTokenSource"/> instances for the script executor.
///
/// <para>Every script allocated two of them: one for the admission wait and one for the execution
/// deadline. The first is armed only when the caller asked to queue, so below the admission ceiling it
/// was allocated, never used, and thrown away. The second always carries a timer registration.</para>
///
/// <para>A source can be reused because <see cref="CancellationTokenSource.TryReset"/> clears its state
/// and unschedules any pending cancellation. A source that was cancelled cannot be reset, so a script
/// that timed out — and an admission wait that was abandoned — drops its instance instead of returning
/// it. That is the whole failure mode: the pool helps the paths that finish normally, which is most of
/// them, and a failed reset costs nothing but an allocation.</para>
///
/// <para><b>A rented source's token must not outlive the rental.</b> Reuse issues a fresh token, so a
/// token captured by something still running would be reset under it, and a later cancellation of an
/// unrelated script would arrive at the wrong place. The executor awaits every call it hands the token
/// to, and stores it nowhere, which is what makes the reuse safe.</para>
/// </summary>
internal static class CancellationSourcePool
{
    /// <summary>
    /// How many idle sources are kept. A rental scans the slots, so this trades the scan against the hit
    /// rate; the number only has to cover the scripts a node runs at the same time, not its whole
    /// throughput, because a source goes back as soon as its script ends.
    /// </summary>
    private const int Capacity = 16;

    private static readonly CancellationTokenSource?[] Slots = new CancellationTokenSource?[Capacity];

    /// <summary>
    /// Takes an idle source, or builds one when none is free.
    /// </summary>
    internal static PooledCancellationSource Rent()
    {
        for (int i = 0; i < Capacity; i++)
        {
            CancellationTokenSource? source = Interlocked.Exchange(ref Slots[i], null);

            if (source is not null)
                return new PooledCancellationSource(source);
        }

        return new PooledCancellationSource(new CancellationTokenSource());
    }

    /// <summary>
    /// Resets a source and parks it, or disposes it when it cannot be reset or the pool is full.
    /// </summary>
    internal static void Return(CancellationTokenSource source)
    {
        if (!source.TryReset())
        {
            source.Dispose();
            return;
        }

        for (int i = 0; i < Capacity; i++)
        {
            if (Interlocked.CompareExchange(ref Slots[i], source, null) is null)
                return;
        }

        source.Dispose();
    }
}

/// <summary>
/// A rented <see cref="CancellationTokenSource"/> that goes back to the pool when its scope ends.
///
/// <para>A struct, and used through <c>using</c>, so the return runs on every exit — including an
/// exception — exactly as disposing an owned source did, and without an allocation of its own.</para>
/// </summary>
internal readonly struct PooledCancellationSource : IDisposable
{
    internal CancellationTokenSource Source { get; }

    internal PooledCancellationSource(CancellationTokenSource source) => Source = source;

    public void Dispose() => CancellationSourcePool.Return(Source);
}
