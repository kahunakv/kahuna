using System.Collections.Concurrent;
using Google.Protobuf;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.KeyValues.Writes;
using Kahuna.Server.Replication;
using Kahuna.Server.Replication.Protos;
using Kahuna.Shared.KeyValue;
using Kommander.Data;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.KeyValues.Transactions;

/// <summary>The prepared-intent delta wire codec of <see cref="PreparedIntentStore"/>.</summary>
internal sealed partial class PreparedIntentStore
{
    // Wire tags of the delta envelope (field number << 3 | wire type 2, length-delimited), matching
    // prepared_intent_message.proto: field 1 is the repeated command, field 2 the shared header.
    private const uint DeltaCommandEntryTag = 1 << 3 | 2;
    private const uint DeltaHeaderTag = 2 << 3 | 2;

    // Scratch slice list for DecodeDelta on this thread. Decode is synchronous, so entries never
    // survive a call; the backing array persists across deltas.
    [ThreadStatic]
    private static List<(int Offset, int Length)>? scratchCommandSlices;

    /// <summary>
    /// Decodes a prepared-intent delta's transitions without applying them — the single codec every reader
    /// of these bytes must use (live apply, WAL replay, point-in-time restore), so a second decoder can
    /// never drift from the apply path.
    ///
    /// <para>Reads the wire directly instead of materializing the generated proto messages: a delta decode
    /// on this path allocates only the commands themselves plus their strings and value bytes, not a
    /// parallel message graph, and this is the hottest decode in the replication pipeline — every node
    /// decodes every delta it did not itself propose. The shared header is written after the commands, so
    /// the envelope walk records each command's slice first and the commands are built afterward against
    /// the header. When a proto field is added, update <see cref="ReadCommand"/> and the reference decoder
    /// <see cref="ToCommand"/> together — the encoding test decodes every delta both ways and fails on any
    /// disagreement.</para>
    /// </summary>
    internal static PreparedIntentCommand[] DecodeDelta(byte[] data)
    {
        ReadOnlySpan<byte> bytes = data;

        List<(int Offset, int Length)> slices = scratchCommandSlices ??= [];
        slices.Clear();

        int headerOffset = -1;
        int headerLength = 0;

        int pos = 0;
        while (pos < bytes.Length)
        {
            ulong tag = ReadWireTag(bytes, ref pos);
            if (tag == DeltaCommandEntryTag)
            {
                int length = ReadLengthPrefix(bytes, ref pos);
                slices.Add((pos, length));
                pos += length;
            }
            else if (tag == DeltaHeaderTag)
            {
                headerLength = ReadLengthPrefix(bytes, ref pos);
                headerOffset = pos;
                pos += headerLength;
            }
            else
                SkipField(bytes, ref pos, tag);
        }

        SharedHeaderFields header = default;
        bool hasHeader = headerOffset >= 0;
        if (hasHeader)
            ReadSharedHeader(bytes.Slice(headerOffset, headerLength), ref header);

        PreparedIntentCommand[] commands = new PreparedIntentCommand[slices.Count];
        for (int i = 0; i < commands.Length; i++)
        {
            (int offset, int length) = slices[i];
            commands[i] = ReadCommand(bytes.Slice(offset, length), in header, hasHeader);
        }

        return commands;
    }

    // ── wire primitives for the direct delta reader ───────────────────────────────

    /// <summary>Reads one base-128 varint (up to ten bytes, matching the protobuf limit). Callers narrow
    /// the result to the field's declared width, which reproduces protobuf's truncation semantics for
    /// int32-family fields encoded as 64-bit varints.</summary>
    private static ulong ReadVarint(ReadOnlySpan<byte> data, ref int pos)
    {
        ulong result = 0;
        for (int shift = 0; shift < 70 && pos < data.Length; shift += 7)
        {
            byte b = data[pos++];
            result |= (ulong)(b & 0x7F) << shift;
            if ((b & 0x80) == 0)
                return result;
        }

        throw new InvalidDataException("Malformed varint in a prepared-intent delta.");
    }

    private static ulong ReadWireTag(ReadOnlySpan<byte> data, ref int pos)
    {
        ulong tag = ReadVarint(data, ref pos);
        if (tag >> 3 == 0)
            throw new InvalidDataException("Zero field number in a prepared-intent delta.");
        return tag;
    }

    private static int ReadLengthPrefix(ReadOnlySpan<byte> data, ref int pos)
    {
        ulong length = ReadVarint(data, ref pos);
        if (length > (ulong)(data.Length - pos))
            throw new InvalidDataException("Length prefix overruns the prepared-intent delta.");
        return (int)length;
    }

    private static string ReadLengthDelimitedString(ReadOnlySpan<byte> data, ref int pos)
    {
        int length = ReadLengthPrefix(data, ref pos);
        string result = System.Text.Encoding.UTF8.GetString(data.Slice(pos, length));
        pos += length;
        return result;
    }

    private static byte[] ReadLengthDelimitedBytes(ReadOnlySpan<byte> data, ref int pos)
    {
        int length = ReadLengthPrefix(data, ref pos);
        byte[] result = data.Slice(pos, length).ToArray();
        pos += length;
        return result;
    }

    /// <summary>Skips one unknown field by its wire type, so a delta written by a newer schema still
    /// decodes (the unknown data is dropped, never re-serialized — decoded commands are applied, not
    /// forwarded). Groups are rejected: proto3 never writes them.</summary>
    private static void SkipField(ReadOnlySpan<byte> data, ref int pos, ulong tag)
    {
        switch ((int)(tag & 7))
        {
            case 0: ReadVarint(data, ref pos); break;
            case 1:
                if (data.Length - pos < 8)
                    throw new InvalidDataException("Truncated fixed64 field in a prepared-intent delta.");
                pos += 8;
                break;
            case 2: pos += ReadLengthPrefix(data, ref pos); break;
            case 5:
                if (data.Length - pos < 4)
                    throw new InvalidDataException("Truncated fixed32 field in a prepared-intent delta.");
                pos += 4;
                break;
            default:
                throw new InvalidDataException($"Unsupported wire type {tag & 7} in a prepared-intent delta.");
        }
    }

    /// <summary>Raw fields of one delta's shared header, exactly as read from the wire. Absent fields keep
    /// their proto3 defaults, which is what the generated reader would also report.</summary>
    private struct SharedHeaderFields
    {
        internal int TxNode; internal long TxPhysical; internal uint TxCounter;
        internal long Epoch;
        internal long ManifestHash;
        internal string? AnchorKey;
        internal int CommitNode; internal long CommitPhysical; internal uint CommitCounter;
        internal int DeadlineNode; internal long DeadlinePhysical; internal uint DeadlineCounter;
    }

    // Field numbers follow PreparedIntentDeltaHeaderMessage in prepared_intent_message.proto.
    private static void ReadSharedHeader(ReadOnlySpan<byte> data, ref SharedHeaderFields header)
    {
        const int Varint = 0, Len = 2;

        int pos = 0;
        while (pos < data.Length)
        {
            ulong tag = ReadWireTag(data, ref pos);
            switch (tag)
            {
                case 1 << 3 | Varint: header.TxNode = (int)(uint)ReadVarint(data, ref pos); break;
                case 2 << 3 | Varint: header.TxPhysical = (long)ReadVarint(data, ref pos); break;
                case 3 << 3 | Varint: header.TxCounter = (uint)ReadVarint(data, ref pos); break;
                case 4 << 3 | Varint: header.Epoch = (long)ReadVarint(data, ref pos); break;
                case 5 << 3 | Varint: header.ManifestHash = (long)ReadVarint(data, ref pos); break;
                case 6 << 3 | Len: header.AnchorKey = ReadLengthDelimitedString(data, ref pos); break;
                case 7 << 3 | Varint: header.CommitNode = (int)(uint)ReadVarint(data, ref pos); break;
                case 8 << 3 | Varint: header.CommitPhysical = (long)ReadVarint(data, ref pos); break;
                case 9 << 3 | Varint: header.CommitCounter = (uint)ReadVarint(data, ref pos); break;
                case 10 << 3 | Varint: header.DeadlineNode = (int)(uint)ReadVarint(data, ref pos); break;
                case 11 << 3 | Varint: header.DeadlinePhysical = (long)ReadVarint(data, ref pos); break;
                case 12 << 3 | Varint: header.DeadlineCounter = (uint)ReadVarint(data, ref pos); break;
                default: SkipField(data, ref pos, tag); break;
            }
        }
    }

    /// <summary>Reads one command from its wire slice and builds the final command object, binding the
    /// header-hoisted fields (transaction identity, epoch, manifest hash, anchor key, commit timestamp,
    /// recovery deadline) from the shared header when the delta carries one. Field numbers follow
    /// PreparedIntentCommandMessage in prepared_intent_message.proto, and the header-fallback semantics
    /// mirror <see cref="ToCommand"/> exactly; the encoding test compares the two decoders so they cannot
    /// drift apart.</summary>
    private static PreparedIntentCommand ReadCommand(ReadOnlySpan<byte> data, in SharedHeaderFields header, bool hasHeader)
    {
        const int Varint = 0, Len = 2;

        PreparedIntentCommandKindMessage kind = PreparedIntentCommandKindMessage.PreparedIntentPrepare;
        int txNode = 0; long txPhysical = 0; uint txCounter = 0;
        long epoch = 0;
        string key = string.Empty;
        bool commit = false;
        long manifestHash = 0;
        string anchorKey = string.Empty;
        int commitNode = 0; long commitPhysical = 0; uint commitCounter = 0;
        int state = 0;
        byte[] value = [];
        bool valueNull = false;
        string bucket = string.Empty;
        bool bucketNull = false;
        long revision = 0;
        int expiresNode = 0; long expiresPhysical = 0; uint expiresCounter = 0;
        bool noRevision = false;
        long baseRevision = 0;
        int baseState = 0;
        int deadlineNode = 0; long deadlinePhysical = 0; uint deadlineCounter = 0;
        int resolution = 0;
        bool materializeOnResolve = false;
        long admissionLagMs = 0;
        string? origin = null;

        int pos = 0;
        while (pos < data.Length)
        {
            ulong tag = ReadWireTag(data, ref pos);
            switch (tag)
            {
                case 1 << 3 | Varint: kind = (PreparedIntentCommandKindMessage)(int)(uint)ReadVarint(data, ref pos); break;
                case 2 << 3 | Varint: txNode = (int)(uint)ReadVarint(data, ref pos); break;
                case 3 << 3 | Varint: txPhysical = (long)ReadVarint(data, ref pos); break;
                case 4 << 3 | Varint: txCounter = (uint)ReadVarint(data, ref pos); break;
                case 5 << 3 | Varint: epoch = (long)ReadVarint(data, ref pos); break;
                case 6 << 3 | Len: key = ReadLengthDelimitedString(data, ref pos); break;
                case 7 << 3 | Varint: commit = ReadVarint(data, ref pos) != 0; break;
                case 8 << 3 | Varint: manifestHash = (long)ReadVarint(data, ref pos); break;
                case 9 << 3 | Len: anchorKey = ReadLengthDelimitedString(data, ref pos); break;
                case 10 << 3 | Varint: commitNode = (int)(uint)ReadVarint(data, ref pos); break;
                case 11 << 3 | Varint: commitPhysical = (long)ReadVarint(data, ref pos); break;
                case 12 << 3 | Varint: commitCounter = (uint)ReadVarint(data, ref pos); break;
                case 13 << 3 | Varint: state = (int)(uint)ReadVarint(data, ref pos); break;
                case 14 << 3 | Len: value = ReadLengthDelimitedBytes(data, ref pos); break;
                case 15 << 3 | Varint: valueNull = ReadVarint(data, ref pos) != 0; break;
                case 16 << 3 | Len: bucket = ReadLengthDelimitedString(data, ref pos); break;
                case 17 << 3 | Varint: bucketNull = ReadVarint(data, ref pos) != 0; break;
                case 18 << 3 | Varint: revision = (long)ReadVarint(data, ref pos); break;
                case 19 << 3 | Varint: expiresNode = (int)(uint)ReadVarint(data, ref pos); break;
                case 20 << 3 | Varint: expiresPhysical = (long)ReadVarint(data, ref pos); break;
                case 21 << 3 | Varint: expiresCounter = (uint)ReadVarint(data, ref pos); break;
                case 22 << 3 | Varint: noRevision = ReadVarint(data, ref pos) != 0; break;
                case 23 << 3 | Varint: baseRevision = (long)ReadVarint(data, ref pos); break;
                case 24 << 3 | Varint: baseState = (int)(uint)ReadVarint(data, ref pos); break;
                case 25 << 3 | Varint: deadlineNode = (int)(uint)ReadVarint(data, ref pos); break;
                case 26 << 3 | Varint: deadlinePhysical = (long)ReadVarint(data, ref pos); break;
                case 27 << 3 | Varint: deadlineCounter = (uint)ReadVarint(data, ref pos); break;
                case 28 << 3 | Varint: resolution = (int)(uint)ReadVarint(data, ref pos); break;
                case 29 << 3 | Varint: materializeOnResolve = ReadVarint(data, ref pos) != 0; break;
                case 30 << 3 | Varint: admissionLagMs = (long)ReadVarint(data, ref pos); break;
                case 31 << 3 | Len: origin = ReadLengthDelimitedString(data, ref pos); break;
                default: SkipField(data, ref pos, tag); break;
            }
        }

        HLCTimestamp txId = hasHeader
            ? new(header.TxNode, header.TxPhysical, header.TxCounter)
            : new(txNode, txPhysical, txCounter);
        long effectiveEpoch = hasHeader ? header.Epoch : epoch;

        switch (kind)
        {
            case PreparedIntentCommandKindMessage.PreparedIntentPrepare:
                return new PrepareIntentCommand(new PreparedIntent(
                    txId, effectiveEpoch, key,
                    hasHeader ? header.ManifestHash : manifestHash,
                    hasHeader ? header.AnchorKey ?? string.Empty : anchorKey,
                    hasHeader
                        ? new HLCTimestamp(header.CommitNode, header.CommitPhysical, header.CommitCounter)
                        : new HLCTimestamp(commitNode, commitPhysical, commitCounter),
                    (KeyValueState)state,
                    valueNull ? null : value,
                    bucketNull ? null : bucket,
                    revision,
                    new HLCTimestamp(expiresNode, expiresPhysical, expiresCounter),
                    noRevision,
                    baseRevision, (KeyValueState)baseState,
                    hasHeader
                        ? new HLCTimestamp(header.DeadlineNode, header.DeadlinePhysical, header.DeadlineCounter)
                        : new HLCTimestamp(deadlineNode, deadlinePhysical, deadlineCounter),
                    (PreparedIntentResolution)resolution,
                    origin),
                    admissionLagMs);

            case PreparedIntentCommandKindMessage.PreparedIntentResolve:
                // A materializing resolve's commit timestamp is its own, never the header's: the header hoists
                // the timestamp only from prepares.
                return new ResolveIntentCommand(
                    txId, effectiveEpoch, key, commit, materializeOnResolve,
                    materializeOnResolve ? new HLCTimestamp(commitNode, commitPhysical, commitCounter) : HLCTimestamp.Zero);

            case PreparedIntentCommandKindMessage.PreparedIntentRemove:
                return new RemoveIntentCommand(txId, effectiveEpoch, key);

            default:
                throw new ArgumentOutOfRangeException(nameof(kind), kind, "unknown prepared-intent command kind");
        }
    }

    /// <summary>Serializes a batch of transitions for one atomic data-partition log entry. The produced bytes
    /// remember their decoded commands so the local apply of this same entry can skip re-parsing them.</summary>
    public static byte[] SerializeDelta(IEnumerable<PreparedIntentCommand> commands)
    {
        PreparedIntentCommand[] batch = commands as PreparedIntentCommand[] ?? [.. commands];

        PreparedIntentDeltaMessage delta = scratchDelta ??= new();

        // Entry-side clear rather than trusting the previous call's cleanup: if an earlier serialization threw
        // mid-batch, its half-built commands are still parked here and must never leak into this delta. Discarded
        // (not pooled) because only messages that went through ResetCommandMessage may re-enter the pool.
        delta.Commands.Clear();
        delta.Header = null;

        foreach (PreparedIntentCommand command in batch)
            delta.Commands.Add(ToProto(command));

        HoistSharedHeader(delta);

        byte[] data = ReplicationSerializer.Serialize(delta);
        locallyProposedDeltas.Register(data, batch);

        // The proto layer is scaffolding: the bytes are final and the delta cache above holds the decoded
        // commands, never these messages, so they can be recycled for the next serialization on this thread.
        ReturnCommandMessages(delta);

        return data;
    }

    /// <summary>
    /// Upper bound on proto command messages retained per thread; a burst beyond the cap is dropped for the GC.
    /// Rent and return both happen inside the synchronous <see cref="SerializeDelta"/>, so unlike request pooling
    /// the pool has perfect thread locality.
    /// </summary>
    private const int MaxPooledCommandMessages = 1024;

    [ThreadStatic]
    private static Stack<PreparedIntentCommandMessage>? pooledCommandMessages;

    // Reused delta envelope for this thread's serializations; its repeated field keeps its backing array across
    // calls. Safe because SerializeDelta is synchronous with no awaits between first touch and last use.
    [ThreadStatic]
    private static PreparedIntentDeltaMessage? scratchDelta;

    private static PreparedIntentCommandMessage RentCommandMessage()
    {
        Stack<PreparedIntentCommandMessage>? pool = pooledCommandMessages;
        return pool is { Count: > 0 } ? pool.Pop() : new PreparedIntentCommandMessage();
    }

    private static void ReturnCommandMessages(PreparedIntentDeltaMessage delta)
    {
        Stack<PreparedIntentCommandMessage> pool = pooledCommandMessages ??= new();

        foreach (PreparedIntentCommandMessage message in delta.Commands)
        {
            if (pool.Count >= MaxPooledCommandMessages)
                break;

            ResetCommandMessage(message);
            pool.Push(message);
        }

        delta.Commands.Clear();
        delta.Header = null;
    }

    /// <summary>
    /// Returns every field to its proto3 default so a recycled message is indistinguishable from a fresh one.
    /// The fill paths (<see cref="FillPrepareProto"/> and the resolve/remove arms of <see cref="ToProto"/>) rely
    /// on this: each sets only the fields its kind carries, so a stale field surviving here would leak one
    /// command's payload into the next delta on this thread. Every field of
    /// <see cref="PreparedIntentCommandMessage"/> must be listed; the encoding test's descriptor sweep fails if a
    /// newly added proto field is missed. Internal for that test.
    /// </summary>
    internal static void ResetCommandMessage(PreparedIntentCommandMessage m)
    {
        m.Kind = PreparedIntentCommandKindMessage.PreparedIntentPrepare;
        m.TransactionIdNode = 0; m.TransactionIdPhysical = 0; m.TransactionIdCounter = 0;
        m.Epoch = 0;
        m.Key = string.Empty;
        m.Commit = false;
        m.ManifestHash = 0;
        m.RecordAnchorKey = string.Empty;
        m.CommitTimestampNode = 0; m.CommitTimestampPhysical = 0; m.CommitTimestampCounter = 0;
        m.State = 0;
        m.Value = ByteString.Empty; m.ValueNull = false;
        m.Bucket = string.Empty; m.BucketNull = false;
        m.Revision = 0;
        m.ExpiresNode = 0; m.ExpiresPhysical = 0; m.ExpiresCounter = 0;
        m.NoRevision = false;
        m.BaseRevision = 0; m.BaseState = 0;
        m.RecoveryDeadlineNode = 0; m.RecoveryDeadlinePhysical = 0; m.RecoveryDeadlineCounter = 0;
        m.Resolution = 0;
        m.MaterializeOnResolve = false;
        m.AdmissionLagMs = 0;
        m.ClearOrigin();
    }

    /// <summary>
    /// Moves the fields every transition in the delta agrees on into one delta-level header and clears them from the
    /// commands, so the transaction identity, manifest hash, commit timestamp, recovery deadline and record-anchor
    /// key are encoded once for the batch instead of once per key. A cleared field is at its proto3 default and so is
    /// not written at all. Does nothing when the commands disagree, which keeps the per-command form correct for any
    /// delta this can't compact.
    /// </summary>
    private static void HoistSharedHeader(PreparedIntentDeltaMessage delta)
    {
        if (delta.Commands.Count < 2)
            return;

        PreparedIntentCommandMessage first = delta.Commands[0];

        // Every command's identity must agree. The mutation fields only exist on PREPARE commands, so they need only
        // agree among those — a resolve/remove never reads them back.
        PreparedIntentCommandMessage? firstPrepare = null;

        foreach (PreparedIntentCommandMessage command in delta.Commands)
        {
            if (command.TransactionIdNode != first.TransactionIdNode
                || command.TransactionIdPhysical != first.TransactionIdPhysical
                || command.TransactionIdCounter != first.TransactionIdCounter
                || command.Epoch != first.Epoch)
                return;

            if (command.Kind != PreparedIntentCommandKindMessage.PreparedIntentPrepare)
                continue;

            if (firstPrepare is null)
            {
                firstPrepare = command;
                continue;
            }

            if (command.ManifestHash != firstPrepare.ManifestHash
                || !string.Equals(command.RecordAnchorKey, firstPrepare.RecordAnchorKey, StringComparison.Ordinal)
                || command.CommitTimestampNode != firstPrepare.CommitTimestampNode
                || command.CommitTimestampPhysical != firstPrepare.CommitTimestampPhysical
                || command.CommitTimestampCounter != firstPrepare.CommitTimestampCounter
                || command.RecoveryDeadlineNode != firstPrepare.RecoveryDeadlineNode
                || command.RecoveryDeadlinePhysical != firstPrepare.RecoveryDeadlinePhysical
                || command.RecoveryDeadlineCounter != firstPrepare.RecoveryDeadlineCounter)
                return;
        }

        delta.Header = new()
        {
            TransactionIdNode = first.TransactionIdNode,
            TransactionIdPhysical = first.TransactionIdPhysical,
            TransactionIdCounter = first.TransactionIdCounter,
            Epoch = first.Epoch,
            ManifestHash = firstPrepare?.ManifestHash ?? 0,
            RecordAnchorKey = firstPrepare?.RecordAnchorKey ?? string.Empty,
            CommitTimestampNode = firstPrepare?.CommitTimestampNode ?? 0,
            CommitTimestampPhysical = firstPrepare?.CommitTimestampPhysical ?? 0,
            CommitTimestampCounter = firstPrepare?.CommitTimestampCounter ?? 0,
            RecoveryDeadlineNode = firstPrepare?.RecoveryDeadlineNode ?? 0,
            RecoveryDeadlinePhysical = firstPrepare?.RecoveryDeadlinePhysical ?? 0,
            RecoveryDeadlineCounter = firstPrepare?.RecoveryDeadlineCounter ?? 0
        };

        foreach (PreparedIntentCommandMessage command in delta.Commands)
        {
            command.TransactionIdNode = 0;
            command.TransactionIdPhysical = 0;
            command.TransactionIdCounter = 0;
            command.Epoch = 0;

            if (command.Kind != PreparedIntentCommandKindMessage.PreparedIntentPrepare)
                continue;

            command.ManifestHash = 0;
            command.RecordAnchorKey = string.Empty;
            command.CommitTimestampNode = 0;
            command.CommitTimestampPhysical = 0;
            command.CommitTimestampCounter = 0;
            command.RecoveryDeadlineNode = 0;
            command.RecoveryDeadlinePhysical = 0;
            command.RecoveryDeadlineCounter = 0;
        }
    }

    // Fills a pooled message; every rented message starts at proto3 defaults (see ResetCommandMessage), so each
    // arm only sets the fields its kind carries.
    private static PreparedIntentCommandMessage ToProto(PreparedIntentCommand command)
    {
        PreparedIntentCommandMessage m = RentCommandMessage();

        switch (command)
        {
            case PrepareIntentCommand prepare:
                FillPrepareProto(m, prepare.Intent);
                m.AdmissionLagMs = prepare.AdmissionLagMs;
                return m;

            case ResolveIntentCommand resolve:
                m.Kind = PreparedIntentCommandKindMessage.PreparedIntentResolve;
                m.TransactionIdNode = resolve.TransactionId.N; m.TransactionIdPhysical = resolve.TransactionId.L; m.TransactionIdCounter = resolve.TransactionId.C;
                m.Epoch = resolve.Epoch; m.Key = resolve.Key; m.Commit = resolve.Commit;

                // A plain resolve stays byte-identical to the form every older node reads.
                if (resolve.MaterializeOnResolve)
                {
                    m.MaterializeOnResolve = true;
                    m.CommitTimestampNode = resolve.CommitTimestamp.N;
                    m.CommitTimestampPhysical = resolve.CommitTimestamp.L;
                    m.CommitTimestampCounter = resolve.CommitTimestamp.C;
                }

                return m;

            case RemoveIntentCommand remove:
                m.Kind = PreparedIntentCommandKindMessage.PreparedIntentRemove;
                m.TransactionIdNode = remove.TransactionId.N; m.TransactionIdPhysical = remove.TransactionId.L; m.TransactionIdCounter = remove.TransactionId.C;
                m.Epoch = remove.Epoch; m.Key = remove.Key;
                return m;

            default:
                throw new ArgumentOutOfRangeException(nameof(command), command.GetType().Name, "unknown prepared-intent command");
        }
    }

    /// <summary>Rebuilds one transition, taking the fields the batch shares from <paramref name="header"/> when the
    /// writer hoisted them there and from the command itself when it did not.</summary>
    /// <summary>Reference decoder over the generated proto messages, kept as the readable statement of the
    /// wire semantics. Production decoding is <see cref="DecodeDelta"/>, which reads the same fields directly
    /// off the wire without the message graph; the encoding test decodes every delta through both and fails
    /// on any disagreement, so a change here without the matching <see cref="ReadCommand"/> change (or the
    /// reverse) cannot land silently. Internal for that test.</summary>
    internal static PreparedIntentCommand ToCommand(PreparedIntentCommandMessage m, PreparedIntentDeltaHeaderMessage? header)
    {
        HLCTimestamp txId = header is null
            ? new(m.TransactionIdNode, m.TransactionIdPhysical, m.TransactionIdCounter)
            : new(header.TransactionIdNode, header.TransactionIdPhysical, header.TransactionIdCounter);

        long epoch = header?.Epoch ?? m.Epoch;

        switch (m.Kind)
        {
            case PreparedIntentCommandKindMessage.PreparedIntentPrepare:
                return new PrepareIntentCommand(IntentOf(m, header), m.AdmissionLagMs);

            case PreparedIntentCommandKindMessage.PreparedIntentResolve:
                return new ResolveIntentCommand(
                    txId, epoch, m.Key, m.Commit, m.MaterializeOnResolve,
                    m.MaterializeOnResolve
                        ? new HLCTimestamp(m.CommitTimestampNode, m.CommitTimestampPhysical, m.CommitTimestampCounter)
                        : HLCTimestamp.Zero);

            case PreparedIntentCommandKindMessage.PreparedIntentRemove:
                return new RemoveIntentCommand(txId, epoch, m.Key);

            default:
                throw new ArgumentOutOfRangeException(nameof(m), m.Kind, "unknown prepared-intent command kind");
        }
    }

    // Fresh-message form for the snapshot/state-transfer paths, which run at checkpoint cadence and don't pool.
    private static PreparedIntentCommandMessage PrepareProtoOf(PreparedIntent i) => FillPrepareProto(new PreparedIntentCommandMessage(), i);

    // A prepared intent maps to (and from) a PREPARE-kind command message, which carries every intent field plus
    // its resolution — reused for both delta commands (pooled messages) and full snapshot entries (fresh
    // messages). Sets every prepare-carried field, so the target may be a recycled message.
    private static PreparedIntentCommandMessage FillPrepareProto(PreparedIntentCommandMessage m, PreparedIntent i)
    {
        m.Kind = PreparedIntentCommandKindMessage.PreparedIntentPrepare;
        m.TransactionIdNode = i.TransactionId.N; m.TransactionIdPhysical = i.TransactionId.L; m.TransactionIdCounter = i.TransactionId.C;
        m.Epoch = i.Epoch; m.Key = i.Key;
        m.ManifestHash = i.ManifestHash; m.RecordAnchorKey = i.RecordAnchorKey;
        m.CommitTimestampNode = i.CommitTimestamp.N; m.CommitTimestampPhysical = i.CommitTimestamp.L; m.CommitTimestampCounter = i.CommitTimestamp.C;
        m.State = (int)i.State;
        // Wrap the intent's value array without copying: the committed value is immutable and the message is
        // serialized synchronously by the caller before the array could change, so aliasing it here is safe and
        // avoids a full value copy per intent on every prepare/settle serialization.
        m.Value = i.Value is null ? ByteString.Empty : UnsafeByteOperations.UnsafeWrap(i.Value); m.ValueNull = i.Value is null;
        m.Bucket = i.Bucket ?? string.Empty; m.BucketNull = i.Bucket is null;
        m.Revision = i.Revision;
        m.ExpiresNode = i.Expires.N; m.ExpiresPhysical = i.Expires.L; m.ExpiresCounter = i.Expires.C;
        m.NoRevision = i.NoRevision;
        m.BaseRevision = i.BaseRevision; m.BaseState = (int)i.BaseState;
        m.RecoveryDeadlineNode = i.RecoveryDeadline.N; m.RecoveryDeadlinePhysical = i.RecoveryDeadline.L; m.RecoveryDeadlineCounter = i.RecoveryDeadline.C;
        m.Resolution = (int)i.Resolution;
        if (i.Origin is not null)
            m.Origin = i.Origin;
        return m;
    }

    private static PreparedIntent IntentOf(PreparedIntentCommandMessage m, PreparedIntentDeltaHeaderMessage? header = null) => new(
        header is null
            ? new HLCTimestamp(m.TransactionIdNode, m.TransactionIdPhysical, m.TransactionIdCounter)
            : new HLCTimestamp(header.TransactionIdNode, header.TransactionIdPhysical, header.TransactionIdCounter),
        header?.Epoch ?? m.Epoch, m.Key, header?.ManifestHash ?? m.ManifestHash, header?.RecordAnchorKey ?? m.RecordAnchorKey,
        header is null
            ? new HLCTimestamp(m.CommitTimestampNode, m.CommitTimestampPhysical, m.CommitTimestampCounter)
            : new HLCTimestamp(header.CommitTimestampNode, header.CommitTimestampPhysical, header.CommitTimestampCounter),
        (KeyValueState)m.State,
        m.ValueNull ? null : m.Value.ToByteArray(),
        m.BucketNull ? null : m.Bucket,
        m.Revision,
        new HLCTimestamp(m.ExpiresNode, m.ExpiresPhysical, m.ExpiresCounter),
        m.NoRevision,
        m.BaseRevision, (KeyValueState)m.BaseState,
        header is null
            ? new HLCTimestamp(m.RecoveryDeadlineNode, m.RecoveryDeadlinePhysical, m.RecoveryDeadlineCounter)
            : new HLCTimestamp(header.RecoveryDeadlineNode, header.RecoveryDeadlinePhysical, header.RecoveryDeadlineCounter),
        (PreparedIntentResolution)m.Resolution,
        m.HasOrigin ? m.Origin : null);

}
