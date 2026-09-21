/**
 * This file is part of Kahuna
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Kahuna.Shared.KeyValue;

namespace Kahuna.Benchmark;

/// <summary>
/// How a rate-limit counter decides when it expires.
/// </summary>
internal enum RateLimitMode
{
    /// <summary>
    /// One counter per subject and per quantised window. The counter lives until the end of the
    /// window it belongs to, so the budget resets on the window boundary regardless of how busy the
    /// caller is.
    /// </summary>
    FixedWindow,

    /// <summary>
    /// One counter per subject whose expiry is pushed out to the full window on every admitted
    /// request. This is stricter than a fixed window for a caller that never goes quiet, because the
    /// counter keeps extending while the caller stays active.
    /// </summary>
    SlidingExpiration
}

/// <summary>
/// The server-side counter that the rate-limit workload drives, and the arithmetic that decides the
/// counter's key and expiry for one request.
///
/// The whole admission decision is one script transaction: read the counter, refuse the caller if it
/// already reached the budget, otherwise write the counter back. Doing it in one round trip is the
/// point of the pattern — a read followed by a separate write would let two callers both observe the
/// same count and both be admitted.
/// </summary>
internal static class RateLimitWorkload
{
    /// <summary>Answer the script returns for a request the budget admits.</summary>
    public const string Allowed = "1";

    /// <summary>Answer the script returns for a request the budget refuses.</summary>
    public const string Refused = "0";

    private const string CounterKeyParameter = "@counter_key";
    private const string BudgetParameter = "@limit";
    private const string ExpiresParameter = "@expires_ms";

    /// <summary>
    /// The counter script, over the ephemeral key space. A rate-limit counter is temporary state
    /// that nothing needs to survive a restart, so this is the durability the pattern is written for.
    ///
    /// Two details differ from the published recipe, because the script engine rejects the published
    /// form. The condition is written as an explicit comparison against <c>null</c>: the language has
    /// a single strict truthiness model, and <c>not</c> demands a boolean operand rather than
    /// treating an absent value as false. The expiry is wrapped in <c>to_int</c>: a script parameter
    /// always arrives as a string, and <c>EX</c> demands a number. The budget is wrapped for the same
    /// reason, even though the comparison operators would coerce it, so the script never leans on an
    /// implicit conversion.
    /// </summary>
    private const string EphemeralScript = """
    LET current = EGET @counter_key
    IF current = null THEN
      ESET @counter_key 1 EX to_int(@expires_ms)
      RETURN 1
    END
    LET count = to_int(current)
    IF count >= to_int(@limit) THEN
      RETURN 0
    END
    ESET @counter_key count + 1 EX to_int(@expires_ms)
    RETURN 1
    """;

    /// <summary>
    /// The same counter over the persistent key space, for a limit that must survive a process
    /// restart. Every write is replicated and persisted, so this costs far more per request than the
    /// ephemeral form.
    /// </summary>
    private const string PersistentScript = """
    LET current = GET @counter_key
    IF current = null THEN
      SET @counter_key 1 EX to_int(@expires_ms)
      RETURN 1
    END
    LET count = to_int(current)
    IF count >= to_int(@limit) THEN
      RETURN 0
    END
    SET @counter_key count + 1 EX to_int(@expires_ms)
    RETURN 1
    """;

    public static string ScriptFor(KeyValueDurability durability) =>
        durability == KeyValueDurability.Ephemeral ? EphemeralScript : PersistentScript;

    /// <summary>
    /// Builds the three parameters the script takes, in a list the caller owns and reuses. The budget
    /// is the same for every request of a run, so it is written once here; a worker rewrites only the
    /// other two per request rather than building a new list per operation.
    /// </summary>
    public static List<KeyValueParameter> CreateParameters(int budget) =>
    [
        new() { Key = CounterKeyParameter },
        new() { Key = BudgetParameter, Value = budget.ToString() },
        new() { Key = ExpiresParameter }
    ];

    /// <summary>
    /// Rewrites a list from <see cref="CreateParameters"/> for one request.
    /// </summary>
    public static void FillParameters(List<KeyValueParameter> parameters, string counterKey, long expiresMs)
    {
        parameters[0].Value = counterKey;
        parameters[2].Value = expiresMs.ToString();
    }

    /// <summary>
    /// Names the counter for one subject.
    ///
    /// A fixed window puts the window start in the key, so the next window reads a key that does not
    /// exist yet and starts from zero. The counter of the window that just ended is never read again
    /// and expires on its own. A sliding counter has one key per subject and keeps using it.
    /// </summary>
    public static string CounterKey(RateLimitMode mode, string keyPrefix, int subject, long windowStartMs) =>
        mode == RateLimitMode.FixedWindow
            ? $"{keyPrefix}rate-limit/{subject}/{windowStartMs}"
            : $"{keyPrefix}rate-limit/{subject}";

    /// <summary>
    /// Quantises the current time to the start of the window it falls in.
    /// </summary>
    public static long WindowStart(long nowMs, int windowMs) => nowMs - (nowMs % windowMs);

    /// <summary>
    /// Decides how long the counter written by this request should live.
    ///
    /// A fixed window passes the time left in the current window, never the whole window. Passing the
    /// whole window on every request would push the expiry out each time a request arrived, which
    /// turns a fixed window into a sliding one. The grace is added on top so a counter cannot expire
    /// a moment before the window it guards ends, which would let a caller open a second counter
    /// inside one window and spend its budget twice.
    ///
    /// A sliding counter passes the whole window every time, which is exactly the extension a fixed
    /// window must avoid.
    ///
    /// The caller passes the window start it already computed, rather than a second clock reading, so
    /// the key and the expiry always describe the same window.
    /// </summary>
    public static long ExpiresMs(RateLimitMode mode, long nowMs, long windowStartMs, int windowMs, int graceMs)
    {
        if (mode == RateLimitMode.SlidingExpiration)
            return windowMs;

        // Never ask the server for an expiry of zero or less. For a window start taken from this same
        // clock reading the remaining time is at least 1 ms, so this only guards a caller that passed
        // a window the reading has already left.
        return Math.Max(1, windowStartMs + windowMs - nowMs + graceMs);
    }
}
