
/**
 * This file is part of Kahuna
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CommandLine;
using Kahuna.Benchmark;
using Kahuna.Client;
using Kahuna.Client.Routing;
using Spectre.Console;

ParserResult<BenchmarkOptions> result = Parser.Default.ParseArguments<BenchmarkOptions>(args);

BenchmarkOptions? opts = result.Value;
if (opts is null)
    return 1;

// ── validation ────────────────────────────────────────────────────────────────

string[] endpoints = opts.ConnectionSource
    .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

if (endpoints.Length == 0)
{
    AnsiConsole.MarkupLine("[red]--connection-source must specify at least one endpoint.[/]");
    return 1;
}

string[] validWorkloads = ["set", "get", "mixed", "delete", "set-many", "delete-many", "txn", "bank", "lock", "sequence", "script"];
if (!validWorkloads.Contains(opts.Workload, StringComparer.OrdinalIgnoreCase))
{
    AnsiConsole.MarkupLine($"[red]--workload must be one of: {string.Join(", ", validWorkloads)}[/]");
    return 1;
}

if (opts.Workload.Equals("script", StringComparison.OrdinalIgnoreCase) &&
    string.IsNullOrWhiteSpace(opts.Script))
{
    AnsiConsole.MarkupLine("[red]--workload script requires --script <path>[/]");
    return 1;
}

if (opts.Duration <= 0)
{
    AnsiConsole.MarkupLine("[red]--duration must be > 0[/]");
    return 1;
}

if (opts.Concurrency <= 0)
{
    AnsiConsole.MarkupLine("[red]--concurrency must be > 0[/]");
    return 1;
}

if (opts.KeySpace <= 0)
{
    AnsiConsole.MarkupLine("[red]--key-space must be > 0[/]");
    return 1;
}

if (opts.ValueSize <= 0)
{
    AnsiConsole.MarkupLine("[red]--value-size must be > 0[/]");
    return 1;
}

if (opts.ReadPct is < 0 or > 100)
{
    AnsiConsole.MarkupLine("[red]--read-pct must be between 0 and 100[/]");
    return 1;
}

if (opts.Timeout <= 0)
{
    AnsiConsole.MarkupLine("[red]--timeout must be > 0[/]");
    return 1;
}

string[] validDurabilities = ["persistent", "ephemeral"];
if (!validDurabilities.Contains(opts.Durability, StringComparer.OrdinalIgnoreCase))
{
    AnsiConsole.MarkupLine("[red]--durability must be persistent or ephemeral[/]");
    return 1;
}

KahunaRoutingMode routingMode;

switch (opts.Routing.ToLowerInvariant())
{
    case "auto": routingMode = KahunaRoutingMode.Auto; break;
    case "roundrobin": routingMode = KahunaRoutingMode.RoundRobin; break;
    case "learned": routingMode = KahunaRoutingMode.Learned; break;
    case "metadata": routingMode = KahunaRoutingMode.Metadata; break;
    default:
        AnsiConsole.MarkupLine("[red]--routing must be auto, roundrobin, learned or metadata[/]");
        return 1;
}

// A node advertises the address its peers route on, which is not always the one this client dials —
// container port mapping is the usual reason. Without the mapping every hint is refused and the run
// silently measures round-robin under another name, so a malformed pair is rejected rather than skipped.
Dictionary<string, string>? routingEndpointMap = null;

if (!string.IsNullOrWhiteSpace(opts.RoutingEndpointMap))
{
    routingEndpointMap = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

    foreach (string pair in opts.RoutingEndpointMap.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
    {
        int separator = pair.IndexOf('=');

        if (separator <= 0 || separator == pair.Length - 1)
        {
            AnsiConsole.MarkupLine($"[red]--routing-endpoint-map entry '{pair}' is not advertised=dialled[/]");
            return 1;
        }

        routingEndpointMap[pair[..separator].Trim()] = pair[(separator + 1)..].Trim();
    }
}

string[] validFormats = ["console", "json", "csv"];
if (!validFormats.Contains(opts.Format, StringComparer.OrdinalIgnoreCase))
{
    AnsiConsole.MarkupLine("[red]--format must be console, json, or csv[/]");
    return 1;
}

// ── diagnostic writer ─────────────────────────────────────────────────────────
// For json/csv formats stdout must be pure machine output. Route every human-
// readable banner and progress line to stderr so piping works correctly:
//   kahuna-bench --format json | jq
// For console format all output goes to stdout as before.

bool isConsoleFormat = opts.Format.Equals("console", StringComparison.OrdinalIgnoreCase);
TextWriter diag = isConsoleFormat ? Console.Out : Console.Error;

// ── client construction ───────────────────────────────────────────────────────

bool insecure = opts.Insecure || endpoints.All(IsLocalhost);
KahunaOptions kahunaOptions = new()
{
    AllowInsecureCertificateValidation = insecure,
    GrpcChannelPoolSize = Math.Max(1, opts.GrpcChannels),
    BatchCoalescingThreshold = Math.Max(1, opts.BatchCoalescingThreshold),
    BatchCoalescingDelayMs = Math.Max(0, opts.BatchCoalescingDelayMs),
    Routing = routingMode,
    RoutingEndpointMap = routingEndpointMap,
    AllowUnlistedRoutingEndpoints = opts.AllowUnlistedRoutingEndpoints
};
KahunaClient client = new(endpoints, null, null, kahunaOptions);

// Totalled for the whole run so the report can show whether the chosen mode was actually in effect.
//
// Off unless asked for. Subscribing to a meter turns every counter the client publishes into a
// listener callback, and only the routing modes publish any — so leaving it on would tax one arm of
// an A/B comparison and not the other, and report the tax as the feature's cost.
using RoutingCounters? routingCounters = opts.RoutingCounters ? new RoutingCounters() : null;

// ── run ───────────────────────────────────────────────────────────────────────

string rateLabel   = opts.Rate > 0 ? $"{opts.Rate} req/s" : "unbounded";
string warmupLabel = opts.Warmup > 0 ? $" + {opts.Warmup}s warmup" : "";

string tlsLabel = insecure
    ? (opts.Insecure ? "disabled (--insecure)" : "disabled (localhost)")
    : "enabled";

if (isConsoleFormat)
{
    AnsiConsole.MarkupLine(
        $"[bold]Kahuna Benchmark[/] — [cyan]{opts.Workload}[/], " +
        $"{opts.Duration}s{warmupLabel}, concurrency={opts.Concurrency}, target={rateLabel}");
    AnsiConsole.MarkupLine($"  endpoints : {string.Join(", ", endpoints)}");
    AnsiConsole.MarkupLine($"  tls       : {tlsLabel}");
    AnsiConsole.MarkupLine(
        $"  routing   : {client.EffectiveRouting}" + (routingMode == KahunaRoutingMode.Auto ? " (auto)" : "") +
        (routingEndpointMap is null ? "" : $"   endpoint-map : {routingEndpointMap.Count} entries"));
    AnsiConsole.MarkupLine(
        $"  key-space : {opts.KeySpace}   value-size : {opts.ValueSize}B   durability : {opts.Durability}");
}
else
{
    diag.WriteLine(
        $"Kahuna Benchmark — {opts.Workload}, " +
        $"{opts.Duration}s{warmupLabel}, concurrency={opts.Concurrency}, target={rateLabel}");
    diag.WriteLine($"  endpoints : {string.Join(", ", endpoints)}");
    diag.WriteLine($"  tls       : {tlsLabel}");
    diag.WriteLine(
        $"  routing   : {client.EffectiveRouting}" + (routingMode == KahunaRoutingMode.Auto ? " (auto)" : "") +
        (routingEndpointMap is null ? "" : $"   endpoint-map : {routingEndpointMap.Count} entries"));
    diag.WriteLine(
        $"  key-space : {opts.KeySpace}   value-size : {opts.ValueSize}B   durability : {opts.Durability}");
}

await BenchmarkRunner.RunAsync(client, opts, diag);

List<KeyValuePair<string, long>> routingTotals = routingCounters?.Snapshot() ?? [];

if (routingTotals.Count > 0)
{
    diag.WriteLine();
    diag.WriteLine("Routing counters");

    foreach (KeyValuePair<string, long> row in routingTotals)
        diag.WriteLine($"  {row.Key,-58} {row.Value,12:N0}");
}

return 0;

static bool IsLocalhost(string url)
{
    if (!Uri.TryCreate(url, UriKind.Absolute, out Uri? uri))
        return false;
    string host = uri.Host;
    return host is "localhost" or "127.0.0.1" or "::1" or "[::1]";
}
