/**
 * This file is part of Kahuna
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Kahuna.Benchmark;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Benchmark.Tests;

/// <summary>
/// Unit coverage for the rate-limit workload's key naming and expiry arithmetic, and for the two
/// statements of its counter script that the script engine is strict about. No network or running
/// cluster is involved; <c>Kahuna.Server.Tests.TestRateLimitingScripts</c> runs the same script
/// against a real cluster and asserts the budget it enforces.
/// </summary>
public sealed class TestRateLimitWorkload
{
    // ── key naming ────────────────────────────────────────────────────────────

    /// <summary>
    /// A fixed window puts the window start in the key, so the next window reads a counter that does
    /// not exist yet and starts from zero.
    /// </summary>
    [Fact]
    public void FixedWindowKey_CarriesTheWindowStart()
    {
        string first = RateLimitWorkload.CounterKey(RateLimitMode.FixedWindow, "bench:", 42, 60_000);
        string second = RateLimitWorkload.CounterKey(RateLimitMode.FixedWindow, "bench:", 42, 120_000);

        Assert.Equal("bench:rate-limit/42/60000", first);
        Assert.Equal("bench:rate-limit/42/120000", second);
        Assert.NotEqual(first, second);
    }

    /// <summary>
    /// A sliding counter has one key per subject, so every window start maps to the same counter and
    /// the count carries over.
    /// </summary>
    [Fact]
    public void SlidingKey_IsOnePerSubject()
    {
        string first = RateLimitWorkload.CounterKey(RateLimitMode.SlidingExpiration, "bench:", 42, 60_000);
        string second = RateLimitWorkload.CounterKey(RateLimitMode.SlidingExpiration, "bench:", 42, 120_000);

        Assert.Equal("bench:rate-limit/42", first);
        Assert.Equal(first, second);
    }

    [Fact]
    public void SubjectsGetDistinctKeys()
    {
        Assert.NotEqual(
            RateLimitWorkload.CounterKey(RateLimitMode.FixedWindow, "bench:", 1, 60_000),
            RateLimitWorkload.CounterKey(RateLimitMode.FixedWindow, "bench:", 2, 60_000));
    }

    // ── window quantisation ───────────────────────────────────────────────────

    [Theory]
    [InlineData(60_000, 1000, 60_000)] // exactly on a boundary
    [InlineData(60_001, 1000, 60_000)]
    [InlineData(60_999, 1000, 60_000)]
    [InlineData(61_000, 1000, 61_000)] // the next boundary
    public void WindowStart_QuantisesToTheWindow(long nowMs, int windowMs, long expected) =>
        Assert.Equal(expected, RateLimitWorkload.WindowStart(nowMs, windowMs));

    /// <summary>
    /// Every reading inside one window quantises to the same start, so every request of that window
    /// names the same counter.
    /// </summary>
    [Fact]
    public void WindowStart_IsStableInsideOneWindow()
    {
        long start = RateLimitWorkload.WindowStart(1_718_391_900_123, 60_000);

        for (long offset = 0; offset < 60_000; offset += 7_919)
            Assert.Equal(start, RateLimitWorkload.WindowStart(start + offset, 60_000));
    }

    // ── expiry ────────────────────────────────────────────────────────────────

    /// <summary>
    /// A fixed window passes the time left in the current window plus the grace, never the whole
    /// window. Passing the whole window on every request would push the expiry out each time one
    /// arrived, which turns a fixed window into a sliding one.
    /// </summary>
    [Theory]
    [InlineData(60_000, 1000, 1100)] // on the boundary: the whole window is still ahead
    [InlineData(60_400, 600, 700)]
    [InlineData(60_999, 1, 101)] // the last millisecond of the window
    public void FixedWindowExpiry_IsTheTimeLeftPlusGrace(long nowMs, long expectedRemaining, long expected)
    {
        long windowStartMs = RateLimitWorkload.WindowStart(nowMs, 1000);

        Assert.Equal(
            expected,
            RateLimitWorkload.ExpiresMs(RateLimitMode.FixedWindow, nowMs, windowStartMs, 1000, graceMs: 100));

        // Stated separately so a failure says whether the window maths or the grace was wrong.
        Assert.Equal(expected - 100, expectedRemaining);
    }

    /// <summary>
    /// The expiry a fixed window asks for never grows as the window runs down, which is what keeps
    /// the counter tied to the window it belongs to.
    /// </summary>
    [Fact]
    public void FixedWindowExpiry_NeverGrowsWithinOneWindow()
    {
        long windowStartMs = RateLimitWorkload.WindowStart(500_000, 10_000);
        long previous = long.MaxValue;

        for (long offset = 0; offset < 10_000; offset += 250)
        {
            long expires = RateLimitWorkload.ExpiresMs(
                RateLimitMode.FixedWindow, windowStartMs + offset, windowStartMs, 10_000, graceMs: 100);

            Assert.True(expires < previous, $"expiry rose at offset {offset}: {previous} then {expires}");
            previous = expires;
        }
    }

    /// <summary>
    /// A sliding counter asks for the whole window on every admitted request. That is exactly the
    /// extension a fixed window must avoid, and it is what makes this mode stricter for a caller that
    /// never goes quiet.
    /// </summary>
    [Fact]
    public void SlidingExpiry_IsAlwaysTheWholeWindow()
    {
        long windowStartMs = RateLimitWorkload.WindowStart(500_000, 10_000);

        for (long offset = 0; offset < 10_000; offset += 250)
            Assert.Equal(
                10_000,
                RateLimitWorkload.ExpiresMs(
                    RateLimitMode.SlidingExpiration, windowStartMs + offset, windowStartMs, 10_000, graceMs: 100));
    }

    /// <summary>
    /// The server is never asked for an expiry of zero or less, even for a window the clock reading
    /// has already left.
    /// </summary>
    [Fact]
    public void FixedWindowExpiry_StaysPositiveForAStaleWindow()
    {
        Assert.Equal(
            1,
            RateLimitWorkload.ExpiresMs(RateLimitMode.FixedWindow, 90_000, windowStartMs: 60_000, windowMs: 1000, graceMs: 0));
    }

    // ── parameters ────────────────────────────────────────────────────────────

    /// <summary>
    /// The budget is the same for every request of a run, so it is written once and the worker
    /// rewrites only the key and the expiry per request.
    /// </summary>
    [Fact]
    public void Parameters_CarryTheBudgetOnceAndTheRestPerRequest()
    {
        List<KeyValueParameter> parameters = RateLimitWorkload.CreateParameters(budget: 250);

        Assert.Equal(3, parameters.Count);
        Assert.Equal("@counter_key", parameters[0].Key);
        Assert.Equal("@limit", parameters[1].Key);
        Assert.Equal("@expires_ms", parameters[2].Key);
        Assert.Equal("250", parameters[1].Value);

        RateLimitWorkload.FillParameters(parameters, "bench:rate-limit/7/60000", 1100);

        Assert.Equal("bench:rate-limit/7/60000", parameters[0].Value);
        Assert.Equal("250", parameters[1].Value);
        Assert.Equal("1100", parameters[2].Value);

        // Reused in place for the next request rather than rebuilt.
        RateLimitWorkload.FillParameters(parameters, "bench:rate-limit/8/61000", 100);

        Assert.Equal("bench:rate-limit/8/61000", parameters[0].Value);
        Assert.Equal("250", parameters[1].Value);
        Assert.Equal("100", parameters[2].Value);
    }

    // ── script form ───────────────────────────────────────────────────────────

    /// <summary>
    /// The script engine has one strict truthiness model: only a boolean is a condition. A key that
    /// was read but not found binds null, so the absence test must be an explicit comparison. The
    /// published recipe writes <c>NOT current</c>, which throws, and this asserts the shipped script
    /// does not drift back to it.
    /// </summary>
    [Theory]
    [InlineData(KeyValueDurability.Ephemeral)]
    [InlineData(KeyValueDurability.Persistent)]
    public void Script_TestsAbsenceByComparingAgainstNull(KeyValueDurability durability)
    {
        string script = RateLimitWorkload.ScriptFor(durability);

        Assert.Contains("IF current = null THEN", script, StringComparison.Ordinal);
        Assert.DoesNotContain("NOT current", script, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// A script parameter always arrives as a string, and <c>EX</c> demands a number, so the expiry
    /// must be cast. The budget is cast too, so the comparison never leans on an implicit conversion.
    /// </summary>
    [Theory]
    [InlineData(KeyValueDurability.Ephemeral)]
    [InlineData(KeyValueDurability.Persistent)]
    public void Script_CastsEveryNumericParameter(KeyValueDurability durability)
    {
        string script = RateLimitWorkload.ScriptFor(durability);

        Assert.Contains("EX to_int(@expires_ms)", script, StringComparison.Ordinal);
        Assert.Contains("to_int(@limit)", script, StringComparison.Ordinal);
        Assert.DoesNotContain("EX @expires_ms", script, StringComparison.Ordinal);
    }

    /// <summary>
    /// The durability the run asks for decides which key space the counter lives in.
    /// </summary>
    [Fact]
    public void Script_UsesTheKeySpaceTheDurabilityNames()
    {
        string ephemeral = RateLimitWorkload.ScriptFor(KeyValueDurability.Ephemeral);
        string persistent = RateLimitWorkload.ScriptFor(KeyValueDurability.Persistent);

        Assert.Contains("EGET @counter_key", ephemeral, StringComparison.Ordinal);
        Assert.Contains("ESET @counter_key", ephemeral, StringComparison.Ordinal);

        Assert.Contains("GET @counter_key", persistent, StringComparison.Ordinal);
        Assert.DoesNotContain("EGET", persistent, StringComparison.Ordinal);
        Assert.DoesNotContain("ESET", persistent, StringComparison.Ordinal);
    }

    /// <summary>
    /// Both answers the workload maps to an outcome are the ones the script returns.
    /// </summary>
    [Theory]
    [InlineData(KeyValueDurability.Ephemeral)]
    [InlineData(KeyValueDurability.Persistent)]
    public void Script_AnswersAllowedOrRefused(KeyValueDurability durability)
    {
        string script = RateLimitWorkload.ScriptFor(durability);

        Assert.Contains("RETURN " + RateLimitWorkload.Allowed, script, StringComparison.Ordinal);
        Assert.Contains("RETURN " + RateLimitWorkload.Refused, script, StringComparison.Ordinal);
    }
}
