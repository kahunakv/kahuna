
using System.Diagnostics.Metrics;

namespace Kahuna.Server.Diagnostics;

/// <summary>Instruments of the process-fault policy: faults a node cannot survive and the breaker that stops it from looping on one.</summary>
internal static class ProcessFaultMetrics
{
    internal static readonly Meter Meter = new("Kahuna", "1.0");

    /// <summary>
    /// Times this process tripped its out-of-memory restart breaker at start: once per process at most. A
    /// non-zero value is the alarm; the readiness probe carries the reason.
    /// </summary>
    internal static readonly Counter<long> OutOfMemoryRestartBreakerTrips =
        Meter.CreateCounter<long>("kahuna.process.out_of_memory_restart_breaker_trips_total",
            description: "Times the node tripped its out-of-memory restart breaker at start: its last starts all ended in an out-of-memory fail-fast early in recovery under the same heap limit.");
}
