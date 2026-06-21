
/**
 * This file is part of Kahuna
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CommandLine;
using Kahuna.Benchmark;
using Kahuna.Client;
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

string[] validWorkloads = ["set", "get", "mixed", "lock", "sequence", "script"];
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

string[] validFormats = ["console", "json", "csv"];
if (!validFormats.Contains(opts.Format, StringComparer.OrdinalIgnoreCase))
{
    AnsiConsole.MarkupLine("[red]--format must be console, json, or csv[/]");
    return 1;
}

if (!opts.Format.Equals("console", StringComparison.OrdinalIgnoreCase))
{
    AnsiConsole.MarkupLine($"[red]--format {opts.Format} is not yet available (Phase C). Only --format console is supported.[/]");
    return 1;
}

// ── client construction ───────────────────────────────────────────────────────

bool insecure = opts.Insecure || endpoints.All(IsLocalhost);
KahunaOptions kahunaOptions = new() { AllowInsecureCertificateValidation = insecure };
KahunaClient client = new(endpoints, null, null, kahunaOptions);

// ── run ───────────────────────────────────────────────────────────────────────

string rateLabel = opts.Rate > 0 ? $"{opts.Rate} req/s" : "unbounded";
string warmupLabel = opts.Warmup > 0 ? $" + {opts.Warmup}s warmup" : "";

string tlsLabel = insecure
    ? (opts.Insecure ? "disabled (--insecure)" : "disabled (localhost)")
    : "enabled";

AnsiConsole.MarkupLine(
    $"[bold]Kahuna Benchmark[/] — [cyan]{opts.Workload}[/], " +
    $"{opts.Duration}s{warmupLabel}, concurrency={opts.Concurrency}, target={rateLabel}");
AnsiConsole.MarkupLine(
    $"  endpoints : {string.Join(", ", endpoints)}");
AnsiConsole.MarkupLine(
    $"  tls       : {tlsLabel}");
AnsiConsole.MarkupLine(
    $"  key-space : {opts.KeySpace}   value-size : {opts.ValueSize}B   durability : {opts.Durability}");

await BenchmarkRunner.RunAsync(client, opts);

return 0;

static bool IsLocalhost(string url)
{
    if (!Uri.TryCreate(url, UriKind.Absolute, out Uri? uri))
        return false;
    string host = uri.Host;
    return host is "localhost" or "127.0.0.1" or "::1" or "[::1]";
}
