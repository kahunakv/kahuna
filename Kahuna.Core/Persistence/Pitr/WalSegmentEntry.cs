
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
    /// Streams entries from <paramref name="entries"/> straight into <paramref name="destination"/> as
    /// JSON Lines — one compact record per line — hashing the exact bytes as they are written and
    /// capturing the segment's endpoint metadata (first/last id, term, HLC) on the fly. Peak memory is a
    /// single entry plus one line buffer, never the whole segment, so capture is bounded no matter how
    /// large the WAL range is. Cancellation is observed between records.
    ///
    /// <para>Publication is the caller's decision and deliberately not done here: when the source yields
    /// no entries the result's <see cref="SegmentWriteResult.EntryCount"/> is 0 and the caller abandons
    /// the write rather than publishing an empty segment. The byte length is counted as it is written
    /// rather than stat-ed afterwards, since the destination is not necessarily a file.</para>
    ///
    /// <para>The digest is computed over the same bytes the destination receives, so it matches an
    /// independent <c>SHA256</c> of the published artifact (what the verifier recomputes) exactly.</para>
    /// </summary>
    public static async Task<SegmentWriteResult> WriteSegmentStreamingAsync(
        Stream destination, IEnumerable<WalSegmentEntry> entries, CancellationToken ct = default)
    {
        long count = 0;
        long toId = 0;
        long toTerm = 0;
        HLCTimestamp toHlc = default;
        HLCTimestamp fromHlc = default;
        bool first = true;
        byte[] hash;
        long byteLength;

        await using (CountingStream counter = new(destination))
        {
            using (SHA256 sha = SHA256.Create())
            await using (CryptoStream cs = new(counter, sha, CryptoStreamMode.Write, leaveOpen: true))
            {
                // StreamWriter defaults to UTF-8 without a BOM and NewLine "\n" — byte-identical to the
                // previous file-based writer, so existing manifests/digests stay compatible.
                await using (StreamWriter writer = new(cs, leaveOpen: true) { NewLine = "\n" })
                {
                    foreach (WalSegmentEntry entry in entries)
                    {
                        ct.ThrowIfCancellationRequested();

                        await writer.WriteLineAsync(JsonSerializer.Serialize(entry, SegmentJsonOptions))
                            .ConfigureAwait(false);

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

            byteLength = counter.BytesWritten;
        }

        if (count == 0)
            return default;

        return new SegmentWriteResult(count, byteLength, Convert.ToHexString(hash).ToLowerInvariant(),
            toId, toHlc, toTerm, fromHlc);
    }

    /// <summary>
    /// Streams a segment file's entries lazily, so only a single entry is resident at a time. Current
    /// segments are JSON Lines; a legacy segment written as one JSON array (first non-whitespace byte
    /// <c>[</c>) is streamed too, via a bounded incremental parser — never a whole-file load.
    /// </summary>
    public static IEnumerable<WalSegmentEntry> ReadSegment(Stream source)
    {
        if (StartsJsonArray(source))
        {
            foreach (WalSegmentEntry entry in ReadJsonArrayStreaming(source))
                yield return entry;
            yield break;
        }

        using StreamReader reader = new(source, leaveOpen: true);
        string? line;
        while ((line = reader.ReadLine()) is not null)
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
        Stream source, [EnumeratorCancellation] CancellationToken ct = default)
    {
        if (StartsJsonArray(source))
        {
            await foreach (WalSegmentEntry? entry in JsonSerializer
                .DeserializeAsyncEnumerable<WalSegmentEntry>(source, SegmentJsonOptions, ct)
                .ConfigureAwait(false))
            {
                if (entry is not null)
                    yield return entry;
            }
            yield break;
        }

        using StreamReader reader = new(source, leaveOpen: true);
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
    private static IEnumerable<WalSegmentEntry> ReadJsonArrayStreaming(Stream stream)
    {
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

    /// <summary>
    /// True when the segment's first non-whitespace byte is <c>[</c> — a legacy JSON-array segment.
    /// <para>
    /// Sniffing consumes bytes, and an artifact store's read stream is forward-only in general, so the
    /// stream is rewound afterwards. A non-seekable source therefore cannot be sniffed; callers hand in
    /// a seekable stream (the local store returns one, and a remote store buffers or re-opens), and a
    /// non-seekable one is treated as the current JSON-Lines format rather than silently misparsed.
    /// </para>
    /// </summary>
    private static bool StartsJsonArray(Stream source)
    {
        if (!source.CanSeek)
            return false;

        long origin = source.Position;
        try
        {
            int b;
            while ((b = source.ReadByte()) != -1)
            {
                if (b is (byte)' ' or (byte)'\t' or (byte)'\r' or (byte)'\n')
                    continue;
                return b == '[';
            }
            return false; // empty segment — treat as an empty JSON-Lines stream
        }
        finally
        {
            source.Position = origin;
        }
    }

    /// <summary>
    /// Passes writes through while counting them, so a segment's byte length is known without stat-ing
    /// a file the destination may not be. Disposal does not dispose the wrapped stream — the artifact
    /// writer owns its own lifetime.
    /// </summary>
    private sealed class CountingStream(Stream inner) : Stream
    {
        internal long BytesWritten { get; private set; }

        public override bool CanRead => false;
        public override bool CanSeek => false;
        public override bool CanWrite => true;
        public override long Length => BytesWritten;
        public override long Position { get => BytesWritten; set => throw new NotSupportedException(); }

        public override void Flush() => inner.Flush();
        public override Task FlushAsync(CancellationToken ct) => inner.FlushAsync(ct);
        public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();

        public override void Write(byte[] buffer, int offset, int count)
        {
            inner.Write(buffer, offset, count);
            BytesWritten += count;
        }

        public override void Write(ReadOnlySpan<byte> buffer)
        {
            inner.Write(buffer);
            BytesWritten += buffer.Length;
        }

        public override async Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken ct)
        {
            await inner.WriteAsync(buffer.AsMemory(offset, count), ct).ConfigureAwait(false);
            BytesWritten += count;
        }

        public override async ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken ct = default)
        {
            await inner.WriteAsync(buffer, ct).ConfigureAwait(false);
            BytesWritten += buffer.Length;
        }
    }
}
