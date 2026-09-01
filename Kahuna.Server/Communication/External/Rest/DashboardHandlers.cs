using System.Diagnostics;
using System.Reflection;
using System.Text.Json;

using Kommander;
using Kommander.Data;
using Kommander.System;
using Microsoft.Net.Http.Headers;

using Kahuna.Server;
using Kahuna.Server.Configuration;
using Kahuna.Server.Diagnostics;
using Kahuna.Shared.Communication.Rest;

namespace Kahuna.Communication.External.Rest;

/// <summary>
/// The browser operator dashboard: the page itself, plus the two read-only endpoints behind it that
/// no other route already answers.
///
/// <para><b>Nothing here mutates anything.</b> No endpoint writes, opens a transaction, takes a
/// lock, splits a range, or starts a backup. That is not a stylistic preference. The page polls on a
/// timer, so every open browser tab multiplies whatever one poll costs; and Kahuna's REST surface
/// carries no authentication, so an action route reached from this page would be an unauthenticated
/// mutation available to anyone who can open the port.</para>
///
/// <para><b>The page reads the existing endpoints for everything else.</b> Roster, placement, key
/// ranges and the backup catalog are already served by <see cref="ClusterHandlers"/>,
/// <see cref="RangesHandlers"/> and <see cref="BackupsHandlers"/>. Restating them here would produce
/// two code paths answering the same question, which is how the two come to disagree.</para>
/// </summary>
public static class DashboardHandlers
{
    /// <summary>
    /// Instruments the engine panel shows. It is an allowlist rather than every instrument the two
    /// meters publish: the full set runs to hundreds of rows once per-partition tags are expanded,
    /// which is a fine answer for a scrape endpoint and an unreadable one for an operator glancing
    /// at a card. A name absent here is still reachable through OpenTelemetry or dotnet-counters.
    /// </summary>
    private static readonly HashSet<string> CuratedMetrics = new(StringComparer.Ordinal)
    {
        // Key-value write path.
        "kahuna.kv.write.admitted",
        "kahuna.kv.write.batches",
        "kahuna.kv.write.entries",
        "kahuna.kv.write.batch_items",
        "kahuna.kv.write.batch_bytes",
        "kahuna.kv.write.queue_age",
        "kahuna.kv.write.raft_duration",
        "kahuna.kv.write.rejections",
        "kahuna.kv.write.outcomes",

        // Scans and read amplification.
        "kahuna.scan.snapshot_prefix_rows_examined_total",
        "kahuna.scan.snapshot_prefix_entries_returned_total",
        "kahuna.scan.abandoned_cancelled_total",

        // Replica placement.
        "kahuna.placement.replicas_gained",
        "kahuna.placement.replicas_lost",
        "kahuna.placement.forwards_resolved",
        "kahuna.placement.forwards_unresolved",
        "kahuna.placement.leader_hint_hits",
        "kahuna.placement.leader_hint_misses",
        "kahuna.placement.chained_forwards_refused",

        // Consensus, from Kommander.
        "raft.executor.operations_total",
        "raft.executor.rejections_total",
        "raft.executor.operation_duration_ms",
        "raft.executor.client_queue_depth",
        "raft.wal.operations_total",
        "raft.wal.batches_total",
        "raft.wal.batch_size",
        "raft.wal.queue_depth",
        "raft.wal.durability_floor_lag",
        "raft.heartbeat_delay_ms",
        "raft.heartbeats_sent_total",
        "raft.elections_started_total",
        "raft.election_delay_ms",
        "raft.stale_completions_total",
        "raft.backfill.no_progress_episodes_total",
        "raft.snapshot.transfer_failures_total",
    };

    /// <summary>
    /// Ceiling on returned metric rows. A curated instrument still splits into many rows by tag —
    /// one per partition, for several of the Kommander instruments — and an unbounded payload polled
    /// every few seconds is its own load. What the cap drops is reported in <c>omitted</c>.
    /// </summary>
    private const int MaxMetricRows = 400;

    /// <summary>Bounds on the served refresh interval, so a typo cannot produce a hot poll loop.</summary>
    private const int MinRefreshSeconds = 1;
    private const int MaxRefreshSeconds = 300;

    /// <summary>
    /// Process start, captured once. It is read from the operating system where that works, and
    /// otherwise falls back to first use of this type — which under-reports uptime rather than
    /// throwing, because an approximate uptime is worth more on this page than a failed band.
    /// </summary>
    private static readonly DateTime ProcessStartUtc = ResolveProcessStart();

    private static readonly string ServerVersion = ResolveVersion();

    public static void MapDashboardRoutes(WebApplication app, KahunaCommandLineOptions opts)
    {
        if (!opts.GetDashboard())
        {
            // The dashboard is off. Keep the response the root has always given, so a health check
            // or a smoke test that greps for it still passes.
            app.MapGet("/", () => "Kahuna.Server");
            return;
        }

        app.MapGet("/", () => ServeAsset(DashboardAssets.Page, "text/html; charset=utf-8"));
        app.MapGet("/dashboard/dashboard.css", () => ServeAsset(DashboardAssets.Stylesheet, "text/css; charset=utf-8"));
        app.MapGet("/dashboard/dashboard.js", () => ServeAsset(DashboardAssets.Script, "text/javascript; charset=utf-8"));

        app.MapGet("/v1/dashboard/summary", (IRaft raft, IKahuna kahuna) => Results.Text(
            JsonSerializer.Serialize(
                BuildSummary(raft, kahuna, opts), KahunaJsonContext.Default.KahunaDashboardSummaryResponse),
            "application/json"));

        app.MapGet("/v1/dashboard/metrics", (EngineMetricsCollector collector) => Results.Text(
            JsonSerializer.Serialize(
                BuildMetrics(collector), KahunaJsonContext.Default.KahunaDashboardMetricsResponse),
            "application/json"));
    }

    /// <summary>
    /// Node identity, readiness and storage, in one payload.
    ///
    /// <para>Readiness comes from <see cref="ClusterHandlers.BuildHealthResponse"/> rather than being
    /// recomputed here, so the band and the readiness probe cannot drift into disagreeing about
    /// whether this node can serve.</para>
    /// </summary>
    private static KahunaDashboardSummaryResponse BuildSummary(IRaft raft, IKahuna kahuna, KahunaCommandLineOptions opts)
    {
        KahunaClusterHealthResponse health = ClusterHandlers.BuildHealthResponse(raft);

        string endpoint;
        long membershipVersion = 0;
        int memberCount = 0;
        int totalPartitions = 0;

        try
        {
            endpoint = raft.GetLocalEndpoint();
        }
        catch (Exception)
        {
            // A node early in boot has no endpoint yet. The band shows everything else regardless.
            endpoint = "";
        }

        try
        {
            ClusterMembership membership = raft.GetMembership();
            membershipVersion = membership.MembershipVersion;
            memberCount = membership.Members.Count;
        }
        catch (Exception)
        {
            // Membership state is not constructed yet. A zero member count reads as "not known".
        }

        if (health.Initialized)
        {
            try
            {
                foreach (RaftPartitionRange range in raft.GetPartitionMap())
                    if (range.State != RaftPartitionState.Removed)
                        totalPartitions++;
            }
            catch (Exception)
            {
                totalPartitions = 0;
            }
        }

        return new()
        {
            LocalEndpoint = endpoint,
            NodeName = opts.RaftNodeName,
            LocalRole = health.LocalRole,
            Initialized = health.Initialized,
            Ready = health.Ready,
            HostedPartitions = health.HostedPartitions,
            TotalPartitions = totalPartitions,

            ClusterMode = opts.InitialCluster is not null && opts.InitialCluster.Any(),
            MemberCount = memberCount,
            MembershipVersion = membershipVersion,
            ReplicationFactor = raft.Configuration.ReplicationFactor,

            Storage = opts.Storage,
            StoragePath = DataPathResolver.IsInMemory(opts.Storage) ? "" : opts.StoragePath,
            WalStorage = opts.WalStorage,
            WalPath = DataPathResolver.IsInMemory(opts.WalStorage) ? "" : opts.WalPath,
            BackupConfigured = kahuna.IsBackupConfigured,

            Version = ServerVersion,
            UptimeSeconds = (long)(DateTime.UtcNow - ProcessStartUtc).TotalSeconds,
            HeapBytes = GC.GetTotalMemory(false),
            ThreadCount = ThreadCount(),

            RefreshSeconds = Math.Clamp(opts.DashboardRefreshSeconds, MinRefreshSeconds, MaxRefreshSeconds),
        };
    }

    /// <summary>
    /// The curated instrument set, as raw cumulative values. Rates are the browser's job — see
    /// <see cref="KahunaDashboardMetricsResponse"/> for why.
    /// </summary>
    private static KahunaDashboardMetricsResponse BuildMetrics(EngineMetricsCollector collector)
    {
        KahunaDashboardMetricsResponse response = new()
        {
            SampledAtUnixMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            MonotonicMs = Environment.TickCount64,
        };

        int omitted = 0;

        foreach (EngineMetricRow row in collector.Snapshot())
        {
            if (!CuratedMetrics.Contains(row.Metric))
                continue;

            if (response.Rows.Count >= MaxMetricRows)
            {
                omitted++;
                continue;
            }

            response.Rows.Add(new KahunaDashboardMetricRow
            {
                Source = row.Source,
                Metric = row.Metric,
                Tags = row.Tags,
                Kind = row.Kind.ToString(),
                Count = row.Count,
                Total = row.Total,
                Min = row.Min,
                Max = row.Max,
                Last = row.Last,
            });
        }

        response.Omitted = omitted;
        return response;
    }

    /// <summary>
    /// Serves one embedded asset. The bytes are immutable for the process lifetime, so the version
    /// is a sufficient entity tag: a browser holding the previous release's stylesheet revalidates
    /// once and is told to replace it, while a page reloaded against the same build pays nothing.
    /// The quotes are not decoration — an entity tag is only valid quoted, and
    /// <see cref="EntityTagHeaderValue"/> rejects a bare value at request time rather than at build.
    /// </summary>
    private static IResult ServeAsset(byte[] content, string contentType) =>
        Results.Bytes(content, contentType, entityTag: new("\"" + ServerVersion + "\"", isWeak: true));

    private static int ThreadCount()
    {
        try
        {
            return Process.GetCurrentProcess().Threads.Count;
        }
        catch (Exception)
        {
            // Enumerating threads is refused in some sandboxes. 0 reads as "not known".
            return 0;
        }
    }

    private static DateTime ResolveProcessStart()
    {
        try
        {
            return Process.GetCurrentProcess().StartTime.ToUniversalTime();
        }
        catch (Exception)
        {
            return DateTime.UtcNow;
        }
    }

    private static string ResolveVersion() =>
        typeof(DashboardHandlers).Assembly.GetName().Version?.ToString() ?? "unknown";
}

/// <summary>
/// The three dashboard assets, read once from the server assembly.
///
/// <para>They are embedded resources rather than files under <c>wwwroot</c> because
/// <c>Kahuna.Server</c> ships as a .NET global tool. Embedded resources travel with the assembly,
/// so the tool, the container image and a plain <c>dotnet run</c> all serve the same bytes, with no
/// static-web-asset manifest to be absent at the wrong moment.</para>
/// </summary>
internal static class DashboardAssets
{
    internal static readonly byte[] Page = Read("index.html");
    internal static readonly byte[] Stylesheet = Read("dashboard.css");
    internal static readonly byte[] Script = Read("dashboard.js");

    private static byte[] Read(string name)
    {
        Assembly assembly = typeof(DashboardAssets).Assembly;

        using Stream? stream = assembly.GetManifestResourceStream("Kahuna.Server.Dashboard." + name);

        if (stream is null)
            throw new KahunaServerException(
                $"The dashboard asset '{name}' is missing from the server assembly. It is embedded by " +
                "an EmbeddedResource entry in Kahuna.Server.csproj.");

        using MemoryStream buffer = new();
        stream.CopyTo(buffer);
        return buffer.ToArray();
    }
}
