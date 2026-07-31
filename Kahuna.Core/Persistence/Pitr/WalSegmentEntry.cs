
using System.Runtime.CompilerServices;
using System.Text.Json;
using Kommander.Data;
using Kommander.Time;

namespace Kahuna.Server.Persistence.Pitr;

/// <summary>
/// Serialisable representation of a single committed WAL entry stored inside an incremental
/// backup segment file. The <see cref="Kommander.Data.RaftLogType"/> is not persisted because
/// every entry in a segment is committed by construction.
/// </summary>
internal sealed class WalSegmentEntry
{
    public long Id { get; set; }
    public long Term { get; set; }
    public int TimeNode { get; set; }
    public long TimePhysical { get; set; }
    public uint TimeCounter { get; set; }
    public string? LogType { get; set; }
    public byte[]? LogData { get; set; }

    public HLCTimestamp Time => new(TimeNode, TimePhysical, TimeCounter);

    public static WalSegmentEntry From(RaftLog log) => new()
    {
        Id = log.Id,
        Term = log.Term,
        TimeNode = log.Time.N,
        TimePhysical = log.Time.L,
        TimeCounter = log.Time.C,
        LogType = log.LogType,
        LogData = log.LogData
    };

    public RaftLog ToRaftLog() => new()
    {
        Id = Id,
        Term = Term,
        Time = Time,
        Type = RaftLogType.Committed,
        LogType = LogType,
        LogData = LogData
    };

    private static readonly JsonSerializerOptions SegmentJsonOptions = new() { WriteIndented = false };

    /// <summary>
    /// Writes a segment as JSON Lines — one compact entry per line — to a temp file, then atomically
    /// renames it into place. Compact JSON never contains an embedded newline (the payload is base64),
    /// so a line boundary is always a record boundary, which is what lets <see cref="ReadSegment"/>
    /// stream one entry at a time with memory bounded by a single record rather than the whole segment.
    /// </summary>
    public static void WriteSegment(string path, IReadOnlyList<WalSegmentEntry> entries)
    {
        string tmp = path + ".tmp_" + Guid.NewGuid().ToString("N")[..8];
        using (FileStream fs = new(tmp, FileMode.Create, FileAccess.Write, FileShare.None))
        using (StreamWriter writer = new(fs) { NewLine = "\n" })
        {
            foreach (WalSegmentEntry entry in entries)
                writer.WriteLine(JsonSerializer.Serialize(entry, SegmentJsonOptions));
        }
        File.Move(tmp, path, overwrite: true);
    }

    /// <summary>
    /// Streams a segment file's entries lazily, so only a single entry is resident at a time. Current
    /// segments are JSON Lines; a legacy segment written as one JSON array (first non-whitespace byte
    /// <c>[</c>) is still read, via a whole-file parse, for backward compatibility.
    /// </summary>
    public static IEnumerable<WalSegmentEntry> ReadSegment(string path)
    {
        if (IsJsonArray(path))
        {
            List<WalSegmentEntry>? all = JsonSerializer.Deserialize<List<WalSegmentEntry>>(
                File.ReadAllText(path), SegmentJsonOptions);
            if (all is not null)
                foreach (WalSegmentEntry entry in all)
                    yield return entry;
            yield break;
        }

        foreach (string line in File.ReadLines(path))
        {
            if (line.Length == 0)
                continue;
            WalSegmentEntry? entry = JsonSerializer.Deserialize<WalSegmentEntry>(line, SegmentJsonOptions);
            if (entry is not null)
                yield return entry;
        }
    }

    /// <summary>
    /// Async, cancellable streaming read — the same one-record-at-a-time semantics as
    /// <see cref="ReadSegment"/>, but with asynchronous file I/O so a caller (e.g. an offline restore)
    /// does not block a thread on disk reads. Legacy JSON-array segments are read whole (asynchronously).
    /// </summary>
    public static async IAsyncEnumerable<WalSegmentEntry> ReadSegmentAsync(
        string path, [EnumeratorCancellation] CancellationToken ct = default)
    {
        if (IsJsonArray(path))
        {
            string text = await File.ReadAllTextAsync(path, ct).ConfigureAwait(false);
            List<WalSegmentEntry>? all = JsonSerializer.Deserialize<List<WalSegmentEntry>>(text, SegmentJsonOptions);
            if (all is not null)
                foreach (WalSegmentEntry entry in all)
                    yield return entry;
            yield break;
        }

        await using FileStream fs = new(
            path, FileMode.Open, FileAccess.Read, FileShare.Read, bufferSize: 65536, useAsync: true);
        using StreamReader reader = new(fs);
        string? line;
        while ((line = await reader.ReadLineAsync(ct).ConfigureAwait(false)) is not null)
        {
            if (line.Length == 0)
                continue;
            WalSegmentEntry? entry = JsonSerializer.Deserialize<WalSegmentEntry>(line, SegmentJsonOptions);
            if (entry is not null)
                yield return entry;
        }
    }

    /// <summary>True when the file's first non-whitespace byte is <c>[</c> — a legacy JSON-array segment.</summary>
    private static bool IsJsonArray(string path)
    {
        using FileStream stream = File.OpenRead(path);
        int b;
        while ((b = stream.ReadByte()) != -1)
        {
            if (b is (byte)' ' or (byte)'\t' or (byte)'\r' or (byte)'\n')
                continue;
            return b == '[';
        }
        return false; // empty file — treat as an empty JSON-Lines stream
    }
}
