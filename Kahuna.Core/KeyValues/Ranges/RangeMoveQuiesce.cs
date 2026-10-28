using Kahuna.Server.Configuration;
using Kommander.Time;

namespace Kahuna.Server.KeyValues.Ranges;

/// <summary>
/// The quiesce deadline of a range move (split or merge), shared by both drivers. The quiesce is held by two
/// guards with one TTL — the write-fence range lock on the moving range and the deadline the replicated
/// descriptor carries — and both reopen the range on their own once the TTL passes. A move that is still
/// running at that point no longer excludes writers: a write admitted on the source after the catch-up copy
/// commits there, and a cutover after it would route the range away from that write for good. So the move
/// must prove the quiesce still holds before it cuts over, with a margin for the cutover's own commit and
/// for clock skew between the driver and the source leader that judges the guards.
/// </summary>
internal static class RangeMoveQuiesce
{
    /// <summary>Floor on the configured TTL: below this the settle barrier, the copies and the hand-off cannot
    /// complete, and the move would only ever refuse itself.</summary>
    internal const int MinTtlMs = 1_000;

    /// <summary>
    /// The most the cutover margin grows to: the quiesce must still have this much left when the cutover is
    /// proposed at the default TTL. It covers the cutover's own meta-partition commit and the bounded clock
    /// difference between the driver's clock, which judges the check, and the source leader's clock, which
    /// judges the lock's TTL and the descriptor deadline.
    /// </summary>
    internal const int MaxCutoverMarginMs = 2_000;

    /// <summary>The least the margin shrinks to under a short TTL (tests, or an operator who shortened it).</summary>
    internal const int MinCutoverMarginMs = 200;

    /// <summary>The cutover margin for a TTL: a fifteenth of it, between the two bounds, so a short TTL still
    /// leaves the in-window steps most of the window.</summary>
    internal static int MarginMsOf(int ttlMs) => Math.Clamp(ttlMs / 15, MinCutoverMarginMs, MaxCutoverMarginMs);

    internal static int TtlMsOf(KahunaConfiguration configuration)
    {
        double ms = configuration.RangeMoveQuiesceTtl.TotalMilliseconds;
        return ms < MinTtlMs ? MinTtlMs : (int)Math.Min(ms, int.MaxValue);
    }

    /// <summary>Milliseconds of the quiesce left at <paramref name="now"/>, negative once it has passed.</summary>
    internal static long RemainingMs(HLCTimestamp quiescedUntil, HLCTimestamp now) => quiescedUntil.L - now.L;

    /// <summary>Whether a cutover proposed at <paramref name="now"/> still lands inside the quiesce, margin
    /// included.</summary>
    internal static bool AllowsCutover(HLCTimestamp quiescedUntil, HLCTimestamp now, int ttlMs) =>
        RemainingMs(quiescedUntil, now) > MarginMsOf(ttlMs);

    /// <summary>The time the in-window steps (settle barrier, catch-up copy, hand-off) may still use at
    /// <paramref name="now"/> before the cutover's margin begins; zero once the window is spent.</summary>
    internal static int WindowBudgetMs(HLCTimestamp quiescedUntil, HLCTimestamp now, int ttlMs)
    {
        long budget = RemainingMs(quiescedUntil, now) - MarginMsOf(ttlMs);
        return budget <= 0 ? 0 : (int)Math.Min(budget, int.MaxValue);
    }
}
