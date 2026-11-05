
using System.Buffers.Text;
using System.Diagnostics;
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Text;
using Kahuna.Server.Persistence.Backend;
using Kommander;
using Kommander.Data;
using Kommander.System;
using Microsoft.Extensions.Logging;
using Microsoft.Win32.SafeHandles;

namespace Kahuna.Server.Diagnostics;

/// <summary>
/// One line of the out-of-memory exit ledger: when the process failed fast, how long it had been up, the
/// managed heap limit it ran under, and the site that raised the fault.
/// </summary>
internal readonly record struct OutOfMemoryExitRecord(DateTimeOffset At, TimeSpan Uptime, long HeapLimitBytes, string Site);

/// <summary>
/// Writes one line to the exit ledger right before an out-of-memory fail-fast. Everything it needs — the file
/// handle and the line buffer — is prepared when it is armed, so the write at crash time allocates nothing on a
/// heap that has just run out. The line is <c>{unixMs} {uptimeMs} {heapLimitBytes} {site}\n</c>, written through
/// to the device, appended at the file's current end.
/// </summary>
internal sealed class OutOfMemoryExitRecorder : IDisposable
{
    private const int LineCapacity = 512;

    private readonly SafeFileHandle handle;

    private readonly byte[] line = new byte[LineCapacity];

    private readonly long armedAt;

    private readonly long heapLimitBytes;

    private readonly object writeGate = new();

    internal OutOfMemoryExitRecorder(string path, long heapLimitBytes)
    {
        handle = File.OpenHandle(path, FileMode.OpenOrCreate, FileAccess.Write, FileShare.Read, FileOptions.WriteThrough);
        armedAt = Stopwatch.GetTimestamp();
        this.heapLimitBytes = heapLimitBytes;
    }

    /// <summary>Appends the exit line. Allocation-free; a line whose site does not fit is truncated, never dropped.</summary>
    internal void RecordExit(string site)
    {
        lock (writeGate)
        {
            Span<byte> buffer = line;
            int written = 0;

            Utf8Formatter.TryFormat(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(), buffer[written..], out int n);
            written += n;
            buffer[written++] = (byte)' ';

            Utf8Formatter.TryFormat((long)Stopwatch.GetElapsedTime(armedAt).TotalMilliseconds, buffer[written..], out n);
            written += n;
            buffer[written++] = (byte)' ';

            Utf8Formatter.TryFormat(heapLimitBytes, buffer[written..], out n);
            written += n;
            buffer[written++] = (byte)' ';

            int room = buffer.Length - written - 1;
            ReadOnlySpan<char> siteChars = site.AsSpan();
            while (siteChars.Length > 0 && Encoding.UTF8.GetByteCount(siteChars) > room)
                siteChars = siteChars[..^1];

            written += Encoding.UTF8.GetBytes(siteChars, buffer[written..]);
            buffer[written++] = (byte)'\n';

            long offset = RandomAccess.GetLength(handle);
            RandomAccess.Write(handle, buffer[..written], offset);
        }
    }

    public void Dispose() => handle.Dispose();
}

/// <summary>
/// The circuit breaker for a node that cannot finish recovering: every start ends in an out-of-memory
/// fail-fast while it restores and installs, so under an orchestrator it crash-loops, and each loop costs the
/// cluster a snapshot export it never gets to use. The fail-fast policy records each such exit in a ledger
/// beside the node's data; at start the breaker reads the ledger and counts the trailing run of exits that
/// happened early in the process' life (<see cref="RecoveryUptime"/>), recently (<see cref="ConsecutiveWindow"/>)
/// and under the heap limit this process runs with. A run at or over the threshold trips the breaker: the node
/// still starts — its partitions are restored and it keeps voting, so it costs its peers nothing — but it
/// withholds its candidacy for every partition it hosts and reports itself not ready with the reason, so the
/// orchestrator stops routing to it and an operator sees what to change. Raising the memory limit changes the
/// heap limit the ledger lines are matched against, which clears the count on its own; so does an hour without
/// a crash, or removing the ledger file.
///
/// <para>The breaker does not defer the restore or refuse snapshot installs: a refused install still makes the
/// leader export and stream the snapshot before the refusal, and a restore without the durable-2PC metadata
/// would serve the partition's idempotency and fence questions wrongly. What it stops is the node competing
/// for leadership between two crashes, and the probe reporting it healthy in between.</para>
///
/// <para>One breaker per backend instance, looked up through <see cref="For"/> like the storage fail-stop gate:
/// the background writer re-asserts the containment on its tick for partitions hosted after the trip.</para>
/// </summary>
internal sealed class OutOfMemoryRestartBreaker
{
    internal const string LedgerFileName = "out-of-memory-exits.log";

    /// <summary>An exit this early in the process' life counts as a recovery exit: the node had not finished
    /// restoring and installing, or had only just. Later exits are steady-state faults, not a crash loop.</summary>
    internal static readonly TimeSpan RecoveryUptime = TimeSpan.FromMinutes(10);

    /// <summary>Exits older than this are not consecutive with the present start and are dropped from the ledger.</summary>
    internal static readonly TimeSpan ConsecutiveWindow = TimeSpan.FromHours(1);

    private static readonly ConditionalWeakTable<IPersistenceBackend, OutOfMemoryRestartBreaker> breakers = new();

    private IRaft? raft;

    private ILogger? logger;

    private string endpoint = "(unattached)";

    private string? notice;

    private int containing;

    /// <summary>The reason the breaker tripped at start, or null while it is open.</summary>
    internal string? Notice => Volatile.Read(ref notice);

    internal bool IsTripped => Notice is not null;

    /// <summary>The trailing run of recovery exits the ledger held at start, under this process' heap limit.</summary>
    internal int ConsecutiveRecoveryExits { get; private set; }

    /// <summary>The breaker of <paramref name="backend"/>, created on first use.</summary>
    internal static OutOfMemoryRestartBreaker For(IPersistenceBackend backend) => breakers.GetValue(backend, static _ => new OutOfMemoryRestartBreaker());

    /// <summary>
    /// Reads the ledger under <paramref name="storagePath"/>, trips on a run of <paramref name="threshold"/> or more
    /// consecutive recovery exits, rewrites the ledger to the window, and arms the fail-fast recorder. A node
    /// without durable storage, or a threshold of zero, keeps no ledger and never trips. Returns the run length.
    /// </summary>
    internal int Arm(string? storagePath, int threshold, IRaft raft, ILogger logger)
    {
        this.raft = raft;
        this.logger = logger;

        try
        {
            endpoint = raft.GetLocalEndpoint();
        }
        catch (Exception)
        {
            endpoint = "(unknown)";
        }

        if (string.IsNullOrWhiteSpace(storagePath))
            return 0;

        string path = Path.Combine(storagePath, LedgerFileName);
        long heapLimit = HeapLimitBytes();

        LedgerVerdict verdict = EvaluateLedger(path, threshold, heapLimit, DateTimeOffset.UtcNow, logger);
        ConsecutiveRecoveryExits = verdict.Run;

        try
        {
            Directory.CreateDirectory(storagePath);
            // Owned by the process-wide policy from here on: it must outlive every node and be reachable
            // from the fail-fast path, which has no node in hand.
            ProcessFaults.ExitRecorder = new OutOfMemoryExitRecorder(path, heapLimit);
        }
        catch (Exception ex)
        {
            logger.LogOutOfMemoryLedgerUnavailable(ex, endpoint, path);
        }

        if (verdict.Run == 0)
            return 0;

        if (verdict.Reason is { } reason)
        {
            Interlocked.CompareExchange(ref notice, reason, null);
            ProcessFaultMetrics.OutOfMemoryRestartBreakerTrips.Add(1);
            logger.LogOutOfMemoryRestartBreakerTripped(endpoint, verdict.Run, reason, path);

            _ = ContainAsync();
        }
        else
        {
            logger.LogOutOfMemoryRecoveryExitsRecorded(endpoint, verdict.Run, threshold, verdict.Last.At, verdict.Last.Site, path);
        }

        return verdict.Run;
    }

    /// <summary>What the ledger says at start: the trailing run of recovery exits, its last record, and the
    /// reason to trip when the run reaches the threshold (null below it).</summary>
    internal readonly record struct LedgerVerdict(int Run, OutOfMemoryExitRecord Last, string? Reason);

    /// <summary>
    /// Reads the ledger at <paramref name="path"/>, drops what is outside <see cref="ConsecutiveWindow"/> of
    /// <paramref name="now"/> (rewriting the file when anything was dropped, so it stays a few lines long), and
    /// judges the trailing run against <paramref name="threshold"/> under <paramref name="heapLimitBytes"/>. A
    /// threshold of zero never produces a reason. Pure apart from the file, so the decision is testable without
    /// a crash.
    /// </summary>
    internal static LedgerVerdict EvaluateLedger(string path, int threshold, long heapLimitBytes, DateTimeOffset now, ILogger? logger)
    {
        List<OutOfMemoryExitRecord> records = ReadLedger(path, logger);
        List<OutOfMemoryExitRecord> recent = WithinWindow(records, now);

        if (recent.Count != records.Count)
        {
            try
            {
                File.WriteAllLines(path, recent.ConvertAll(FormatLine));
            }
            catch (Exception ex)
            {
                logger?.LogOutOfMemoryLedgerUnavailable(ex, "(trimming)", path);
            }
        }

        int run = CountTrailingRecoveryExits(recent, heapLimitBytes);

        if (run == 0)
            return new LedgerVerdict(0, default, null);

        OutOfMemoryExitRecord last = recent[^1];

        string? reason = threshold > 0 && run >= threshold
            ? $"{run} consecutive starts ended in an out-of-memory fail-fast within {RecoveryUptime.TotalMinutes:0} minutes of starting, " +
              $"at a managed heap limit of {heapLimitBytes:N0} bytes (last at {last.At:O} in {last.Site}); the next start would too"
            : null;

        return new LedgerVerdict(run, last, reason);
    }

    /// <summary>
    /// Re-applies the containment while tripped: a partition this node started hosting after the trip is gated
    /// too. Called from the background writer's tick; a no-op while the breaker is open or a containment runs.
    /// </summary>
    internal Task ReassertAsync() => IsTripped ? ContainAsync() : Task.CompletedTask;

    private async Task ContainAsync()
    {
        if (raft is null || Interlocked.Exchange(ref containing, 1) == 1)
            return;

        try
        {
            IReadOnlyList<RaftPartitionRange> map;
            try
            {
                map = raft.GetPartitionMap();
            }
            catch (Exception)
            {
                // Too early in boot for a partition map; the writer's tick re-asserts once there is one.
                return;
            }

            foreach (RaftPartitionRange range in map)
            {
                if (range.State == RaftPartitionState.Removed || !raft.HostsPartition(range.PartitionId))
                    continue;

                int partitionId = range.PartitionId;

                // Withhold before stepping down, as the storage fail-stop gate does: a replica that still
                // campaigns would win the election it just forced.
                raft.SetCandidacyWithheld(partitionId, true);

                using CancellationTokenSource timeout = new(TimeSpan.FromSeconds(10));

                if (!await raft.AmILeaderIfHosted(partitionId, timeout.Token).ConfigureAwait(false))
                    continue;

                RaftOperationStatus status = await raft.StepDownAsync(partitionId, timeout.Token).ConfigureAwait(false);

                if (status != RaftOperationStatus.Success)
                    logger?.LogOutOfMemoryBreakerStepDownRefused(endpoint, partitionId, status.ToString());
            }
        }
        catch (Exception ex)
        {
            logger?.LogOutOfMemoryBreakerContainmentFailed(ex, endpoint);
        }
        finally
        {
            Volatile.Write(ref containing, 0);
        }
    }

    /// <summary>The managed heap limit this process runs under (the hard limit, else the machine's memory); 0 when unknown.</summary>
    internal static long HeapLimitBytes()
    {
        GCMemoryInfo info = GC.GetGCMemoryInfo();
        return info.TotalAvailableMemoryBytes > 0 ? info.TotalAvailableMemoryBytes : 0;
    }

    /// <summary>The records of <paramref name="records"/> written within <see cref="ConsecutiveWindow"/> of <paramref name="now"/>, in order.</summary>
    internal static List<OutOfMemoryExitRecord> WithinWindow(IReadOnlyList<OutOfMemoryExitRecord> records, DateTimeOffset now)
    {
        List<OutOfMemoryExitRecord> recent = new(records.Count);

        foreach (OutOfMemoryExitRecord record in records)
        {
            if (now - record.At <= ConsecutiveWindow && record.At <= now + TimeSpan.FromMinutes(5))
                recent.Add(record);
        }

        return recent;
    }

    /// <summary>
    /// The length of the trailing run of recovery exits in <paramref name="records"/>: exits within
    /// <see cref="RecoveryUptime"/> of their process' start, under <paramref name="heapLimitBytes"/>. The run
    /// ends at the first older record that does not qualify — a steady-state exit, or one under another heap
    /// limit (the operator changed the memory limit since; what happened under the old one says nothing about
    /// the new one). Pure, so the decision is testable without a file or a crash.
    /// </summary>
    internal static int CountTrailingRecoveryExits(IReadOnlyList<OutOfMemoryExitRecord> records, long heapLimitBytes)
    {
        int run = 0;

        for (int i = records.Count - 1; i >= 0; i--)
        {
            OutOfMemoryExitRecord record = records[i];

            if (record.Uptime > RecoveryUptime || record.HeapLimitBytes != heapLimitBytes)
                break;

            run++;
        }

        return run;
    }

    /// <summary>Parses the ledger at <paramref name="path"/>; an absent file is empty, a malformed line is skipped.</summary>
    internal static List<OutOfMemoryExitRecord> ReadLedger(string path, ILogger? logger)
    {
        List<OutOfMemoryExitRecord> records = [];

        if (!File.Exists(path))
            return records;

        string[] lines;
        try
        {
            lines = File.ReadAllLines(path);
        }
        catch (Exception ex)
        {
            logger?.LogOutOfMemoryLedgerUnavailable(ex, "(reading)", path);
            return records;
        }

        foreach (string line in lines)
        {
            if (TryParseLine(line, out OutOfMemoryExitRecord record))
                records.Add(record);
        }

        return records;
    }

    internal static bool TryParseLine(string line, out OutOfMemoryExitRecord record)
    {
        record = default;

        ReadOnlySpan<char> span = line.AsSpan().Trim();
        if (span.IsEmpty)
            return false;

        int first = span.IndexOf(' ');
        if (first < 0)
            return false;

        ReadOnlySpan<char> rest = span[(first + 1)..];
        int second = rest.IndexOf(' ');
        if (second < 0)
            return false;

        ReadOnlySpan<char> rest2 = rest[(second + 1)..];
        int third = rest2.IndexOf(' ');

        ReadOnlySpan<char> heapText = third < 0 ? rest2 : rest2[..third];
        string site = third < 0 ? "" : rest2[(third + 1)..].ToString();

        if (!long.TryParse(span[..first], NumberStyles.Integer, CultureInfo.InvariantCulture, out long unixMs)
            || !long.TryParse(rest[..second], NumberStyles.Integer, CultureInfo.InvariantCulture, out long uptimeMs)
            || !long.TryParse(heapText, NumberStyles.Integer, CultureInfo.InvariantCulture, out long heapLimit)
            || unixMs < 0 || uptimeMs < 0)
            return false;

        record = new OutOfMemoryExitRecord(DateTimeOffset.FromUnixTimeMilliseconds(unixMs), TimeSpan.FromMilliseconds(uptimeMs), heapLimit, site);
        return true;
    }

    internal static string FormatLine(OutOfMemoryExitRecord record) =>
        string.Create(CultureInfo.InvariantCulture, $"{record.At.ToUnixTimeMilliseconds()} {(long)record.Uptime.TotalMilliseconds} {record.HeapLimitBytes} {record.Site}");
}
