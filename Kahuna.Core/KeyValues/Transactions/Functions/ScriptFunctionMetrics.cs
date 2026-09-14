
using System.Diagnostics.Metrics;

namespace Kahuna.Server.KeyValues.Transactions.Functions;

/// <summary>
/// Instruments for the user-defined functions a node makes callable from Kahuna script.
///
/// <para>Two questions these answer. First, which node is running which extension build: the
/// fingerprint gauge is tagged with the node and the hash of the registered surface, so an operator
/// who sees one node answer <c>Errored</c> for a function that works elsewhere can diff the nodes
/// without reading a log. The registry is not replicated, so a mismatch is a deployment fault that
/// has to be visible.</para>
///
/// <para>Second, what a function costs. A call is synchronous and blocks a request path while the
/// transaction holds its locks and write intents, so an expensive function shows up as latency on
/// keys it never touches. Call count and total elapsed time per function turn that into a number.</para>
/// </summary>
internal static class ScriptFunctionMetrics
{
    /// <summary>
    /// Registers the gauges on a caller-owned meter. The meter is returned so a disposed node's table
    /// is not kept reachable by a gauge callback for the lifetime of the process.
    /// </summary>
    internal static Meter RegisterGauges(ScriptFunctionTable table)
    {
        Meter meter = new("Kahuna", "1.0");

        meter.CreateObservableGauge(
            "kahuna.script_functions.registered",
            () => new Measurement<int>(
                table.CustomCount,
                new KeyValuePair<string, object?>("node", table.NodeName),
                new KeyValuePair<string, object?>("fingerprint", table.Fingerprint)),
            description: "User-defined functions registered on this node. The fingerprint tag identifies the registered surface; every node of a cluster must report the same one.");

        meter.CreateObservableGauge(
            "kahuna.script_functions.calls",
            () => Observe(table, static stats => stats.Calls),
            description: "Invocations of each user-defined function since the node started.");

        meter.CreateObservableGauge(
            "kahuna.script_functions.elapsed_ms",
            () => Observe(table, static stats => (long)stats.ElapsedMilliseconds),
            description: "Total time spent inside each user-defined function, in milliseconds. It is time a request path spent blocked while a transaction held its locks.");

        return meter;
    }

    private static Measurement<long>[] Observe(ScriptFunctionTable table, Func<ScriptFunctionStats, long> read)
    {
        IReadOnlyList<ScriptFunctionStats> all = table.CustomStats;

        Measurement<long>[] measurements = new Measurement<long>[all.Count];

        for (int i = 0; i < all.Count; i++)
            measurements[i] = new(read(all[i]), new KeyValuePair<string, object?>("function", all[i].Name));

        return measurements;
    }
}
