
namespace Kahuna.Server.KeyValues;

/// <summary>
/// The poll a read runs while a key or page answers transient: <c>WaitingForReplication</c> behind a live
/// foreign write intent whose commit may land inside the read's snapshot, or a replication intent still in
/// flight. The actor does not park the read; the caller asks it again after a sleep, and the sleep decides
/// how long after the obstruction clears the read is served.
///
/// <para>The sleeps double from one millisecond and stop at <see cref="MaxDelayMs"/>. A cap of one second
/// used to apply, and with it a read was served at the next power-of-two boundary past the moment the
/// intent cleared: a writer that held its intent for 700 ms cost the read 1,023 ms, one that held for 1.5 s
/// cost 2,023 ms, and a hold just past the scan page budget's last boundary failed the page. With the short
/// cap a read is served within <see cref="MaxDelayMs"/> of the clear. A sleep never runs past the caller's
/// deadline, so the last retry lands at the deadline and a hold inside the budget never fails.</para>
///
/// <para>The cost is more asks per second of wait: a blocked page is asked again every 50 ms instead of
/// every second. Each ask is one actor turn over a resident page, and a read only waits while a writer
/// holds the key, so the extra turns are bounded by the writer's hold.</para>
/// </summary>
internal static class ReadWaitBackoff
{
    /// <summary>The longest sleep between two retries of a waiting read.</summary>
    internal const int MaxDelayMs = 50;

    /// <summary>
    /// The sleep before the next retry, with the back-off advanced for the retry after it.
    /// <paramref name="deadlineTick"/> is the <see cref="Environment.TickCount64"/> at which the caller gives
    /// up; the sleep is cut so that it ends at the deadline, and is never below one millisecond.
    /// </summary>
    internal static int Next(ref int backoffMs, long deadlineTick)
    {
        long remaining = deadlineTick - Environment.TickCount64;
        int delay = (int)Math.Clamp(Math.Min((long)backoffMs, remaining), 1L, MaxDelayMs);
        backoffMs = Math.Min(backoffMs * 2, MaxDelayMs);
        return delay;
    }

    /// <summary>The sleep before the next retry when the caller counts attempts instead of time.</summary>
    internal static int Next(ref int backoffMs)
    {
        int delay = Math.Min(backoffMs, MaxDelayMs);
        backoffMs = Math.Min(backoffMs * 2, MaxDelayMs);
        return delay;
    }
}
