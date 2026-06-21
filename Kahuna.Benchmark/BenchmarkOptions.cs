
/**
 * This file is part of Kahuna
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CommandLine;

namespace Kahuna.Benchmark;

internal sealed class BenchmarkOptions
{
    [Option('c', "connection-source", Required = true,
        HelpText = "Comma-separated cluster endpoints, e.g. https://h1:8082,https://h2:8084")]
    public string ConnectionSource { get; set; } = "";

    [Option("workload", Default = "mixed",
        HelpText = "set | get | mixed | lock | sequence | script")]
    public string Workload { get; set; } = "mixed";

    [Option("duration", Default = 30,
        HelpText = "Measurement window in seconds (excludes warmup)")]
    public int Duration { get; set; } = 30;

    [Option("warmup", Default = 5,
        HelpText = "Warmup seconds before measurement; samples discarded")]
    public int Warmup { get; set; } = 5;

    [Option("concurrency", Default = 64,
        HelpText = "Number of concurrent in-flight workers (closed-loop degree of parallelism)")]
    public int Concurrency { get; set; } = 64;

    [Option("rate", Default = 0,
        HelpText = "Target aggregate req/sec (0 = unbounded closed-loop; >0 = open-loop)")]
    public int Rate { get; set; } = 0;

    [Option("key-space", Default = 10000,
        HelpText = "Number of distinct keys cycled through (controls hit/contention rate)")]
    public int KeySpace { get; set; } = 10000;

    [Option("value-size", Default = 128,
        HelpText = "Value payload size in bytes for write ops")]
    public int ValueSize { get; set; } = 128;

    [Option("read-pct", Default = 50,
        HelpText = "For mixed: percentage of reads (the rest are writes)")]
    public int ReadPct { get; set; } = 50;

    [Option("durability", Default = "persistent",
        HelpText = "persistent | ephemeral")]
    public string Durability { get; set; } = "persistent";

    [Option("script",
        HelpText = "Path to a .4gl transaction script used by --workload script")]
    public string? Script { get; set; }

    [Option("timeout", Default = 10,
        HelpText = "Per-request client timeout in seconds")]
    public int Timeout { get; set; } = 10;

    [Option("format", Default = "console",
        HelpText = "console | json | csv")]
    public string Format { get; set; } = "console";

    [Option("output",
        HelpText = "File path for json/csv output (defaults to stdout)")]
    public string? Output { get; set; }

    [Option("insecure", Default = false,
        HelpText = "Skip TLS validation for self-signed dev/standalone certs")]
    public bool Insecure { get; set; }

    [Option("seed", Default = 0,
        HelpText = "RNG seed for reproducible key/value selection (0 = time-based)")]
    public int Seed { get; set; } = 0;
}
