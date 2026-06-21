
/**
 * This file is part of Kahuna
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text.Json.Serialization;

namespace Kahuna.Benchmark;

internal sealed class RunParameters
{
    [JsonPropertyName("workload")]    public string   Workload    { get; init; } = "";
    [JsonPropertyName("duration")]    public int      Duration    { get; init; }
    [JsonPropertyName("warmup")]      public int      Warmup      { get; init; }
    [JsonPropertyName("concurrency")] public int      Concurrency { get; init; }
    [JsonPropertyName("rate")]        public int      Rate        { get; init; }
    [JsonPropertyName("keySpace")]    public int      KeySpace    { get; init; }
    [JsonPropertyName("valueSize")]   public int      ValueSize   { get; init; }
    [JsonPropertyName("durability")]  public string   Durability  { get; init; } = "";
    [JsonPropertyName("endpoints")]   public string[] Endpoints   { get; init; } = [];
    [JsonPropertyName("seed")]        public int      Seed        { get; init; }
    [JsonPropertyName("timeout")]     public int      Timeout     { get; init; }
}

/// <summary>Per-operation statistics row; used for both JSON and CSV output.</summary>
internal sealed class OpStats
{
    [JsonPropertyName("operation")] public string Operation { get; init; } = "";
    [JsonPropertyName("count")]     public long   Count     { get; init; }
    [JsonPropertyName("rps")]       public double Rps       { get; init; }
    [JsonPropertyName("p50Ms")]     public double P50Ms     { get; init; }
    [JsonPropertyName("p90Ms")]     public double P90Ms     { get; init; }
    [JsonPropertyName("p95Ms")]     public double P95Ms     { get; init; }
    [JsonPropertyName("p99Ms")]     public double P99Ms     { get; init; }
    [JsonPropertyName("p999Ms")]    public double P999Ms    { get; init; }
    [JsonPropertyName("maxMs")]     public double MaxMs     { get; init; }
    [JsonPropertyName("meanMs")]    public double MeanMs    { get; init; }
    [JsonPropertyName("errors")]    public long   Errors    { get; init; }
    [JsonPropertyName("timeouts")]  public long   Timeouts  { get; init; }
    [JsonPropertyName("misses")]    public long   Misses    { get; init; }
}

/// <summary>Top-level machine-readable benchmark result for JSON/CSV output.</summary>
internal sealed class BenchmarkOutput
{
    [JsonPropertyName("parameters")] public RunParameters Parameters { get; init; } = new();
    [JsonPropertyName("elapsedSec")] public double        ElapsedSec { get; init; }
    [JsonPropertyName("operations")] public List<OpStats> Operations { get; init; } = [];
    [JsonPropertyName("aggregate")]  public OpStats       Aggregate  { get; init; } = new();
}
