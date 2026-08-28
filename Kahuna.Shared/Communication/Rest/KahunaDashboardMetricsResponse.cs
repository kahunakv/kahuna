using System.Text.Json.Serialization;

namespace Kahuna.Shared.Communication.Rest;

/// <summary>
/// One aggregated instrument + tag-set, as served by <c>GET /v1/dashboard/metrics</c>.
///
/// <para>Which fields carry a value depends on <see cref="Kind"/>. A counter fills
/// <see cref="Total"/> only. A gauge fills <see cref="Last"/> only. A histogram fills
/// <see cref="Count"/>, <see cref="Total"/>, <see cref="Min"/>, <see cref="Max"/> and
/// <see cref="Last"/>, so a mean is <c>total / count</c>.</para>
/// </summary>
public sealed class KahunaDashboardMetricRow
{
    /// <summary>The meter that published the instrument, lowercased: <c>kahuna</c> or <c>kommander</c>.</summary>
    [JsonPropertyName("source")]
    public string Source { get; set; } = "";

    /// <summary>The instrument name, for example <c>kahuna.kv.write.batches</c>.</summary>
    [JsonPropertyName("metric")]
    public string Metric { get; set; } = "";

    /// <summary>The tag-set in canonical <c>k1=v1,k2=v2</c> form, keys sorted ordinally. Empty when untagged.</summary>
    [JsonPropertyName("tags")]
    public string Tags { get; set; } = "";

    /// <summary>How the samples combine: <c>Counter</c>, <c>Histogram</c> or <c>Gauge</c>.</summary>
    [JsonPropertyName("kind")]
    public string Kind { get; set; } = "";

    /// <summary>Samples recorded. For a counter this equals the total; for a gauge it is 1.</summary>
    [JsonPropertyName("count")]
    public long Count { get; set; }

    /// <summary>Sum of every recorded sample. Null for a gauge, which keeps no history.</summary>
    [JsonPropertyName("total")]
    public double? Total { get; set; }

    /// <summary>Smallest sample seen. Histogram only.</summary>
    [JsonPropertyName("min")]
    public double? Min { get; set; }

    /// <summary>Largest sample seen. Histogram only.</summary>
    [JsonPropertyName("max")]
    public double? Max { get; set; }

    /// <summary>Most recent sample. Null for a counter.</summary>
    [JsonPropertyName("last")]
    public double? Last { get; set; }
}

/// <summary>
/// The curated instrument set this node has recorded since process start.
///
/// <para><b>No rate is computed here.</b> The collector accumulates from process start and never
/// resets, and this endpoint holds no previous sample, so a rate is the browser's job: it keeps the
/// last value per instrument and divides by elapsed monotonic time. <see cref="MonotonicMs"/> is
/// served for exactly that division — a wall clock can step, and a rate measured across a step is
/// wrong.</para>
/// </summary>
public sealed class KahunaDashboardMetricsResponse
{
    /// <summary>The rows, ordered by source, then metric, then tags, so two readings diff cleanly.</summary>
    [JsonPropertyName("rows")]
    public List<KahunaDashboardMetricRow> Rows { get; set; } = [];

    /// <summary>
    /// Curated rows the cap dropped from this payload. Reported rather than silently trimmed, so a
    /// truncated panel never reads as a complete one.
    /// </summary>
    [JsonPropertyName("omitted")]
    public int Omitted { get; set; }

    /// <summary>Wall-clock sampling time in Unix milliseconds. For display; never for a rate.</summary>
    [JsonPropertyName("sampledAtUnixMs")]
    public long SampledAtUnixMs { get; set; }

    /// <summary>Monotonic clock reading at sampling time. This is the denominator for a browser-side rate.</summary>
    [JsonPropertyName("monotonicMs")]
    public long MonotonicMs { get; set; }
}
