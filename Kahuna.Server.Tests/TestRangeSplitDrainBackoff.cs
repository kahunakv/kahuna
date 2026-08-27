using Kahuna.Server.KeyValues.Ranges;

namespace Kahuna.Server.Tests;

/// <summary>
/// The backoff applied after a move is refused because the moving half still holds unsettled durable
/// intents.
///
/// <para>
/// The refusal repeats by nature: the range is refused because it is being written, and a range worth
/// splitting is written continuously. Every re-attempt takes the quiesce and refuses writes into the
/// moving half for the whole drain window, so a checker that retries on each pass makes a busy range
/// pay that cost forever while never dividing. The delay therefore lengthens per consecutive refusal,
/// and these assertions pin the shape of that lengthening: it doubles, it saturates, and it never
/// overflows into a nonsensical value for a range that has been refused for a long time.
/// </para>
/// </summary>
public sealed class TestRangeSplitDrainBackoff
{
    private const double BaseMs = 60_000;

    private const double MaxMs = 300_000;

    [Fact]
    public void FirstRefusalCostsOneBaseDelay()
    {
        Assert.Equal(BaseMs, RangeSplitTrigger.ComputeDrainBackoffMs(BaseMs, MaxMs, 1));
    }

    [Fact]
    public void EachConsecutiveRefusalDoublesTheDelay()
    {
        Assert.Equal(BaseMs * 2, RangeSplitTrigger.ComputeDrainBackoffMs(BaseMs, MaxMs, 2));
        Assert.Equal(BaseMs * 4, RangeSplitTrigger.ComputeDrainBackoffMs(BaseMs, MaxMs, 3));
    }

    [Fact]
    public void TheDelaySaturatesAtTheCeiling()
    {
        // 60s doubled three times is 480s, past the 300s ceiling.
        Assert.Equal(MaxMs, RangeSplitTrigger.ComputeDrainBackoffMs(BaseMs, MaxMs, 4));
        Assert.Equal(MaxMs, RangeSplitTrigger.ComputeDrainBackoffMs(BaseMs, MaxMs, 9));
    }

    [Fact]
    public void ALongRefusalStreakStaysAtTheCeiling()
    {
        // The multiplier is 2^(consecutive-1). Without a bound on the shift a streak this long
        // produces a negative or wrapped delay, which reads as "no backoff at all" — the exact
        // failure this backoff exists to prevent, arriving only after a range has been busy for
        // hours.
        foreach (int consecutive in new[] { 20, 21, 64, 1_000, int.MaxValue })
            Assert.Equal(MaxMs, RangeSplitTrigger.ComputeDrainBackoffMs(BaseMs, MaxMs, consecutive));
    }

    [Fact]
    public void AStreakContinuesWhileTheRefusalsKeepComing()
    {
        // The entry outlives its own delay on purpose: the count is what makes the delay grow, so
        // discarding it at expiry would pin the backoff at the second step and the range would be
        // re-attempted every other pass for as long as it stays busy.
        Assert.Equal(2, RangeSplitTrigger.NextConsecutive(1, elapsedSinceLastMs: MaxMs, maxBackoffMs: MaxMs));
        Assert.Equal(5, RangeSplitTrigger.NextConsecutive(4, elapsedSinceLastMs: 2 * MaxMs, maxBackoffMs: MaxMs));
    }

    [Fact]
    public void AStreakStartsOverAfterALongQuietGap()
    {
        // Past twice the ceiling the range has drained, or nobody has asked, for long enough that the
        // old episode says nothing about now. Carrying the count forward would make one refusal cost
        // the full delay for a range that has behaved for ten minutes.
        Assert.Equal(1, RangeSplitTrigger.NextConsecutive(4, elapsedSinceLastMs: 2 * MaxMs + 1, maxBackoffMs: MaxMs));
        Assert.Equal(1, RangeSplitTrigger.NextConsecutive(9, elapsedSinceLastMs: 60 * MaxMs, maxBackoffMs: MaxMs));
    }

    [Fact]
    public void ACeilingBelowTheBaseDelayIsRaisedToIt()
    {
        // Both values are derived from configuration (the checker interval and the indivisibility
        // cooldown), so an operator can set a cooldown shorter than one checker pass. The first
        // refusal must still cost a full pass rather than nothing.
        Assert.Equal(BaseMs, RangeSplitTrigger.ComputeDrainBackoffMs(BaseMs, 1_000, 1));
        Assert.Equal(BaseMs, RangeSplitTrigger.ComputeDrainBackoffMs(BaseMs, 1_000, 5));
    }
}
