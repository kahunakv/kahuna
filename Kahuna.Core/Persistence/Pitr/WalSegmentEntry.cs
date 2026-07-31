
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
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
    /// Byte-length, digest, and endpoint metadata of a segment written by
    /// <see cref="WriteSegmentStreaming"/>. <see cref="EntryCount"/> is 0 when the source produced no
    /// committed entries — in that case no file was published and the caller should skip the partition.
    /// </summary>
    internal readonly record struct SegmentWriteResult(
        long EntryCount,
        long ByteLength,
        string Sha256Hex,
        long ToId,
        HLCTimestamp ToHlc,
        long ToTerm,
        HLCTimestamp FromHlc);

    /// <summary>
    /// Streams entries from <paramref name="entries"/> straight to a temp file as JSON Lines — one
    /// compact record per line — hashing the exact bytes as they are written and capturing the segment's
    /// endpoint metadata (first/last id, term, HLC) on the fly, then atomically renames the temp into
    /// place. Peak memory is a single entry plus one line buffer, never the whole segment, so capture is
    /// bounded no matter how large the WAL range is. Cancellation is observed between records; a cancelled
    /// or failed write removes the temp file and never publishes a partial segment. When the source yields
    /// no entries, nothing is published and <see cref="SegmentWriteResult.EntryCount"/> is 0.
    ///
    /// <para>The digest is computed over the same bytes the file contains, so it matches an independent
    /// <c>SHA256</c> of the published file (what the verifier recomputes) exactly.</para>
    /// </summary>
    public static SegmentWriteResult WriteSegmentStreaming(
        string path, IEnumerable<WalSegmentEntry> entries, CancellationToken ct = default)
    {
        string tmp = path + ".tmp_" + Guid.NewGuid().ToString("N")[..8];

        long count = 0;
        long toId = 0;
        long toTerm = 0;
        HLCTimestamp toHlc = default;
        HLCTimestamp fromHlc = default;
        bool first = true;
        byte[] hash;

        try
        {
            using (FileStream fs = new(tmp, FileMode.Create, FileAccess.Write, FileShare.None, bufferSize: 65536))
            using (SHA256 sha = SHA256.Create())
            using (CryptoStream cs = new(fs, sha, CryptoStreamMode.Write, leaveOpen: true))
            {
                // StreamWriter defaults to UTF-8 without a BOM and NewLine "\n" — byte-identical to the
                // previous list-based writer, so existing manifests/digests stay compatible.
                using (StreamWriter writer = new(cs, leaveOpen: true) { NewLine = "\n" })
                {
                    foreach (WalSegmentEntry entry in entries)
                    {
                        ct.ThrowIfCancellationRequested();

                        writer.WriteLine(JsonSerializer.Serialize(entry, SegmentJsonOptions));

                        count++;
                        if (first)
                        {
                            fromHlc = entry.Time;
                            first = false;
                        }
                        toId = entry.Id;
                        toHlc = entry.Time;
                        toTerm = entry.Term;
                    }
                }

                cs.FlushFinalBlock();
                hash = sha.Hash!;
            }

            if (count == 0)
            {
                TryDelete(tmp);
                return default;
            }

            File.Move(tmp, path, overwrite: true);
        }
        catch
        {
            TryDelete(tmp);
            throw;
        }

        long length = new FileInfo(path).Length;
        return new SegmentWriteResult(count, length, Convert.ToHexString(hash).ToLowerInvariant(),
            toId, toHlc, toTerm, fromHlc);
    }

    /// <summary>
    /// Streams a segment file's entries lazily, so only a single entry is resident at a time. Current
    /// segments are JSON Lines; a legacy segment written as one JSON array (first non-whitespace byte
    /// <c>[</c>) is streamed too, via a bounded incremental parser — never a whole-file load.
    /// </summary>
    public static IEnumerable<WalSegmentEntry> ReadSegment(string path)
    {
        if (IsJsonArray(path))
        {
            foreach (WalSegmentEntry entry in ReadJsonArrayStreaming(path))
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
    /// does not block a thread on disk reads. Legacy JSON-array segments are streamed incrementally via
    /// <see cref="JsonSerializer.DeserializeAsyncEnumerable{TValue}(Stream, JsonSerializerOptions, CancellationToken)"/>,
    /// so a multi-gigabyte legacy array is bounded to the deserializer's internal buffer, not the whole file.
    /// </summary>
    public static async IAsyncEnumerable<WalSegmentEntry> ReadSegmentAsync(
        string path, [EnumeratorCancellation] CancellationToken ct = default)
    {
        if (IsJsonArray(path))
        {
            await using FileStream arrayStream = new(
                path, FileMode.Open, FileAccess.Read, FileShare.Read, bufferSize: 65536, useAsync: true);
            await foreach (WalSegmentEntry? entry in JsonSerializer
                .DeserializeAsyncEnumerable<WalSegmentEntry>(arrayStream, SegmentJsonOptions, ct)
                .ConfigureAwait(false))
            {
                if (entry is not null)
                    yield return entry;
            }
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

    /// <summary>
    /// Incrementally parses a legacy single-JSON-array segment, yielding one entry at a time with memory
    /// bounded by a growable read buffer (grown only to fit a single oversized record), never the whole
    /// file. The buffer is refilled as tokens are consumed; a record straddling a buffer boundary is left
    /// unconsumed and re-read after the next refill.
    /// </summary>
    private static IEnumerable<WalSegmentEntry> ReadJsonArrayStreaming(string path)
    {
        using FileStream stream = File.OpenRead(path);

        byte[] buffer = new byte[32 * 1024];
        int bytesInBuffer = 0;
        JsonReaderState state = new(new JsonReaderOptions());
        bool isFinalBlock = false;

        while (!isFinalBlock)
        {
            int bytesRead = stream.Read(buffer, bytesInBuffer, buffer.Length - bytesInBuffer);
            bytesInBuffer += bytesRead;
            isFinalBlock = bytesRead == 0;

            (List<WalSegmentEntry> entries, int consumed, JsonReaderState nextState) =
                ParseJsonArrayChunk(buffer, bytesInBuffer, isFinalBlock, state);
            state = nextState;

            foreach (WalSegmentEntry entry in entries)
                yield return entry;

            int leftover = bytesInBuffer - consumed;
            if (leftover > 0 && consumed > 0)
                Buffer.BlockCopy(buffer, consumed, buffer, 0, leftover);
            bytesInBuffer = leftover;

            // A single record larger than the buffer consumed nothing and filled it — grow so it fits.
            if (!isFinalBlock && consumed == 0 && bytesInBuffer == buffer.Length)
                Array.Resize(ref buffer, buffer.Length * 2);
        }
    }

    /// <summary>
    /// Parses as many complete objects as are fully present in <paramref name="buffer"/>[0..<paramref name="length"/>),
    /// returning them plus the number of bytes consumed (so the caller can retain the trailing partial
    /// record) and the reader state to resume from. An object that is not yet fully buffered is left
    /// unconsumed. Uses a ref-struct <see cref="Utf8JsonReader"/> in a non-iterator method so it never
    /// crosses a yield boundary.
    /// </summary>
    private static (List<WalSegmentEntry> entries, int consumed, JsonReaderState state) ParseJsonArrayChunk(
        byte[] buffer, int length, bool isFinalBlock, JsonReaderState state)
    {
        Utf8JsonReader reader = new(buffer.AsSpan(0, length), isFinalBlock, state);
        List<WalSegmentEntry> entries = [];

        while (true)
        {
            // Snapshot before Read so an incomplete object can be rewound and re-read after the next refill.
            Utf8JsonReader beforeRead = reader;
            if (!reader.Read())
            {
                reader = beforeRead;
                break;
            }

            if (reader.TokenType is JsonTokenType.StartArray or JsonTokenType.EndArray)
                continue;

            if (reader.TokenType == JsonTokenType.StartObject)
            {
                Utf8JsonReader skipCheck = reader;
                if (!skipCheck.TrySkip())
                {
                    // The object is not fully buffered yet; rewind to before it and wait for more bytes.
                    reader = beforeRead;
                    break;
                }

                WalSegmentEntry? entry = JsonSerializer.Deserialize<WalSegmentEntry>(ref reader, SegmentJsonOptions);
                if (entry is not null)
                    entries.Add(entry);
            }
        }

        return (entries, (int)reader.BytesConsumed, reader.CurrentState);
    }

    private static void TryDelete(string path)
    {
        try
        {
            if (File.Exists(path))
                File.Delete(path);
        }
        catch
        {
            // Best-effort cleanup of a temp artifact; a leftover .tmp_ file is reclaimed by the orphan sweep.
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
