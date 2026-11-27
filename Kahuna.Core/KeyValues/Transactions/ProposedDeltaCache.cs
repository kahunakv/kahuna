using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Kahuna.Utils;

namespace Kahuna.Server.KeyValues.Transactions;

/// <summary>
/// Remembers the decoded form of a locally serialized replication record, keyed by the exact byte
/// array that was handed to Raft. The node that proposes a record serialized it from live objects
/// moments before Raft hands the same array back to the local apply path, so that apply — and, when an
/// in-process transport shares the array, the applies of the other nodes hosted in this process — can
/// reuse the decoded form instead of parsing and rebuilding it. The payload is whatever the producer
/// had in hand: a command batch for a transaction delta, the proposal itself for a direct write.
///
/// <para>Each entry carries a take budget equal to the live in-process node count at registration
/// (<see cref="InProcessNodeCensus"/>). With the redundant-apply ledger each node runs one local apply
/// per committed entry, so the budget bounds how long an entry can pin its payload. The entry is
/// removed once the budget is spent. A reader that misses — a node in another process, WAL replay on
/// restart, state transfer, all of which see freshly materialized arrays — decodes the bytes instead,
/// so correctness never depends on a hit. Reusing the producer's instances across nodes is safe only
/// because the payload and everything it references are immutable.</para>
///
/// <para>The table holds its keys strongly. A weak table (<see cref="ConditionalWeakTable{TKey, TValue}"/>)
/// allocates a dependent GC handle for each registration and frees it only when its finalizable container is
/// collected, after a gen2 collection; each young collection scans those handles and containers. Here the
/// table is split into stripes by the identity hash of the bytes, each with its own lock and two generations.
/// A registration goes to the current generation. When the current generation holds
/// <see cref="DefaultGenerationCapacity"/> entries or <see cref="DefaultGenerationBytes"/> of keys, or is
/// <see cref="DefaultGenerationAgeMs"/> old, the next registration in the stripe drops the previous
/// generation and starts a new one. This bounds the entries that no apply takes: a proposal that is not
/// committed, a record forwarded to a leader in another process, a partition that a co-hosted node does not
/// replicate, and a take budget above the number of takers. Such an entry is released by a later
/// registration in its stripe; with no registrations it stays, inside the bounds above. Bytes longer than
/// <see cref="DefaultMaxRegisteredBytes"/> are not registered, so the cache never holds a large handoff
/// chunk or a large value.</para>
///
/// <para>A producer that puts registered bytes into other bytes, which go to Raft in their place, removes
/// the entry of the inner bytes at once (<see cref="TryClaim"/>, <see cref="Forget"/>): a one-phase bundle
/// entry for its three parts, and a merged settle entry for its members. No apply sees the inner bytes, so
/// their entry would stay until its generation is dropped.</para>
/// </summary>
internal sealed class ProposedDeltaCache<TPayload> where TPayload : class
{
    /// <summary>Stripes of a cache made with the default constructor. A power of two.</summary>
    internal const int DefaultStripeCount = 16;

    /// <summary>Entries of one generation of one stripe before the next registration starts a new generation.</summary>
    internal const int DefaultGenerationCapacity = 1024;

    /// <summary>Key bytes of one generation of one stripe before the next registration starts a new generation.</summary>
    internal const long DefaultGenerationBytes = 1024 * 1024;

    /// <summary>Age of the current generation of a stripe at which the next registration starts a new one. An entry
    /// that no capacity limit pushes out is kept at least this long, which is far above the time from a proposal to
    /// its local apply.</summary>
    internal const long DefaultGenerationAgeMs = 1_000;

    /// <summary>Longest bytes that are registered. Longer bytes are decoded at their apply.</summary>
    internal const int DefaultMaxRegisteredBytes = 256 * 1024;

    private struct Slot
    {
        internal TPayload Payload;
        internal int RemainingTakes;
    }

    private sealed class Stripe
    {
        internal readonly Lock Gate = new();
        internal Dictionary<byte[], Slot> Current = new(ReferenceEqualityComparer.Instance);
        internal Dictionary<byte[], Slot> Previous = new(ReferenceEqualityComparer.Instance);
        internal long CurrentBytes;
        internal long PreviousBytes;
        internal long CurrentStartedAt;
    }

    private readonly Stripe[] stripes;
    private readonly int stripeMask;
    private readonly int generationCapacity;
    private readonly long generationBytes;
    private readonly long generationAgeMs;
    private readonly int maxRegisteredBytes;
    private readonly Func<long>? clock;

    internal ProposedDeltaCache()
        : this(DefaultStripeCount, DefaultGenerationCapacity, DefaultGenerationBytes, DefaultGenerationAgeMs, DefaultMaxRegisteredBytes)
    {
    }

    /// <summary>A cache with explicit bounds, for tests that drive the generations. <paramref name="clock"/> gives the
    /// time in milliseconds; when it is null the cache reads <see cref="Environment.TickCount64"/>.</summary>
    internal ProposedDeltaCache(int stripeCount, int generationCapacity, long generationBytes, long generationAgeMs, int maxRegisteredBytes, Func<long>? clock = null)
    {
        if (stripeCount <= 0 || (stripeCount & (stripeCount - 1)) != 0)
            throw new ArgumentOutOfRangeException(nameof(stripeCount), stripeCount, "The stripe count must be a power of two.");

        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(generationCapacity);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(generationBytes);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(generationAgeMs);
        ArgumentOutOfRangeException.ThrowIfNegative(maxRegisteredBytes);

        this.generationCapacity = generationCapacity;
        this.generationBytes = generationBytes;
        this.generationAgeMs = generationAgeMs;
        this.maxRegisteredBytes = maxRegisteredBytes;
        this.clock = clock;

        stripeMask = stripeCount - 1;
        stripes = new Stripe[stripeCount];

        long now = Now();
        for (int i = 0; i < stripes.Length; i++)
            stripes[i] = new() { CurrentStartedAt = now };
    }

    /// <summary>Registers the decoded form of freshly produced record bytes, with a take budget of the
    /// current live node count. The floor of one preserves the single-take behavior for callers that
    /// serialize outside any node's lifetime (tests, tools).</summary>
    internal void Register(byte[] data, TPayload payload) =>
        Register(data, payload, Math.Max(1, InProcessNodeCensus.Count));

    /// <summary>Budget-explicit form for tests that exercise the take accounting directly. A registration of bytes
    /// that already have an entry replaces it.</summary>
    internal void Register(byte[] data, TPayload payload, int takeBudget)
    {
        if (data.Length > maxRegisteredBytes)
            return;

        Stripe stripe = StripeOf(data);

        lock (stripe.Gate)
        {
            RemoveFrom(stripe.Previous, ref stripe.PreviousBytes, data);
            RemoveFrom(stripe.Current, ref stripe.CurrentBytes, data);

            long now = Now();
            if (stripe.Current.Count >= generationCapacity
                || stripe.CurrentBytes + data.Length > generationBytes
                || now - stripe.CurrentStartedAt >= generationAgeMs)
            {
                (stripe.Previous, stripe.Current) = (stripe.Current, stripe.Previous);
                stripe.Current.Clear();
                stripe.PreviousBytes = stripe.CurrentBytes;
                stripe.CurrentBytes = 0;
                stripe.CurrentStartedAt = now;
            }

            stripe.Current.Add(data, new() { Payload = payload, RemainingTakes = takeBudget });
            stripe.CurrentBytes += data.Length;
        }
    }

    /// <summary>Whether bytes of <paramref name="length"/> are registered: a producer can skip a decode that it makes only to
    /// register its result.</summary>
    internal bool Accepts(int length) => length <= maxRegisteredBytes;

    /// <summary>The payload registered for <paramref name="data"/>, without taking a reuse of it: for a reader ahead
    /// of the apply, which takes its own.</summary>
    internal bool TryPeek(byte[] data, [NotNullWhen(true)] out TPayload? payload)
    {
        Stripe stripe = StripeOf(data);

        lock (stripe.Gate)
        {
            if (stripe.Current.TryGetValue(data, out Slot slot) || stripe.Previous.TryGetValue(data, out slot))
            {
                payload = slot.Payload;
                return true;
            }
        }

        payload = null;
        return false;
    }

    /// <summary>Takes one reuse of the payload registered for <paramref name="data"/>. The entry is
    /// removed when its budget is spent.</summary>
    internal bool TryTake(byte[] data, [NotNullWhen(true)] out TPayload? payload)
    {
        Stripe stripe = StripeOf(data);

        lock (stripe.Gate)
        {
            return TakeFrom(stripe.Current, ref stripe.CurrentBytes, data, out payload)
                   || TakeFrom(stripe.Previous, ref stripe.PreviousBytes, data, out payload);
        }
    }

    /// <summary>Takes the payload registered for <paramref name="data"/> and removes the entry, whatever its take
    /// budget is: for a producer that registers the payload again with the bytes that replace
    /// <paramref name="data"/> in the log.</summary>
    internal bool TryClaim(byte[] data, [NotNullWhen(true)] out TPayload? payload)
    {
        Stripe stripe = StripeOf(data);

        lock (stripe.Gate)
        {
            if (stripe.Current.Remove(data, out Slot slot))
            {
                stripe.CurrentBytes -= data.Length;
                payload = slot.Payload;
                return true;
            }

            if (stripe.Previous.Remove(data, out slot))
            {
                stripe.PreviousBytes -= data.Length;
                payload = slot.Payload;
                return true;
            }
        }

        payload = null;
        return false;
    }

    /// <summary>Removes the entry of <paramref name="data"/>, whatever its take budget is: for a producer that put the
    /// payload into other bytes, which replace <paramref name="data"/> in the log.</summary>
    internal void Forget(byte[] data)
    {
        Stripe stripe = StripeOf(data);

        lock (stripe.Gate)
        {
            RemoveFrom(stripe.Current, ref stripe.CurrentBytes, data);
            RemoveFrom(stripe.Previous, ref stripe.PreviousBytes, data);
        }
    }

    /// <summary>The entries that the cache holds now, in all stripes and both generations: for tests.</summary>
    internal int Count
    {
        get
        {
            int count = 0;

            foreach (Stripe stripe in stripes)
            {
                lock (stripe.Gate)
                    count += stripe.Current.Count + stripe.Previous.Count;
            }

            return count;
        }
    }

    private Stripe StripeOf(byte[] data) => stripes[RuntimeHelpers.GetHashCode(data) & stripeMask];

    private long Now() => clock is null ? Environment.TickCount64 : clock();

    private static bool TakeFrom(Dictionary<byte[], Slot> generation, ref long generationKeyBytes, byte[] data, [NotNullWhen(true)] out TPayload? payload)
    {
        ref Slot slot = ref CollectionsMarshal.GetValueRefOrNullRef(generation, data);
        if (Unsafe.IsNullRef(ref slot))
        {
            payload = null;
            return false;
        }

        payload = slot.Payload;

        // The reference to the slot is not used after the removal, which can move the entries of the dictionary.
        if (--slot.RemainingTakes <= 0)
        {
            generation.Remove(data);
            generationKeyBytes -= data.Length;
        }

        return true;
    }

    private static void RemoveFrom(Dictionary<byte[], Slot> generation, ref long generationKeyBytes, byte[] data)
    {
        if (generation.Remove(data))
            generationKeyBytes -= data.Length;
    }
}
