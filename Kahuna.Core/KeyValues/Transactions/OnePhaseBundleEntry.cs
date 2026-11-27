using System.Diagnostics.CodeAnalysis;

using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.Replication;

namespace Kahuna.Server.KeyValues.Transactions;

/// <summary>
/// The wire codec of a <see cref="ReplicationTypes.OnePhaseBundle"/> entry: the record init, the prepare and the commit
/// decision of a one-phase commit in one log entry.
///
/// <para>The entry is three length-delimited protobuf fields, in apply order: field 1 is the record init delta, field 2
/// the prepared-intent delta and field 3 the decision delta. Each part keeps the encoding it has as an entry of its
/// own, so the stores apply it with the bodies they use for that entry. A reader parses the parts from spans of the
/// entry and copies nothing. A field that a newer writer adds is skipped.</para>
///
/// <para>The node that serializes the entry moves the decoded commands of the three parts from the registrations of the
/// part bytes to one registration of the entry, so its own apply parses nothing. A node that did not serialize it
/// decodes the parts.</para>
/// </summary>
internal static class OnePhaseBundleEntry
{
    private const uint InitTag = 1 << 3 | 2;

    private const uint PrepareTag = 2 << 3 | 2;

    private const uint DecisionTag = 3 << 3 | 2;

    /// <summary>The decoded commands of the parts that this node serialized. A part this node did not have decoded is
    /// null, and the apply decodes it from the entry.</summary>
    internal sealed record Commands(
        TransactionRecordCommand[]? Init,
        PreparedIntentCommand[] Prepare,
        TransactionRecordCommand[]? Decision);

    /// <summary>Where each part lies in the entry.</summary>
    internal readonly record struct Parts(Range Init, Range Prepare, Range Decision);

    private static readonly ProposedDeltaCache<Commands> locallyProposed = new();

    /// <summary>
    /// Builds the entry from the three deltas of a one-phase commit. The decoded commands of each part that this node
    /// registered or can decode go with the bytes, for the apply of the entry on this node. The registrations of the
    /// three part arrays are removed, because the entry goes to the log in their place and no apply sees them. A part
    /// array that goes to the log later as an entry of its own is decoded at its apply.
    /// </summary>
    internal static byte[] Serialize(byte[] recordInit, byte[] prepare, byte[] decision)
    {
        int length = FieldLength(InitTag, recordInit.Length) + FieldLength(PrepareTag, prepare.Length) + FieldLength(DecisionTag, decision.Length);
        byte[] data = new byte[length];

        int position = 0;
        WriteField(data, ref position, InitTag, recordInit);
        WriteField(data, ref position, PrepareTag, prepare);
        WriteField(data, ref position, DecisionTag, decision);

        TransactionRecordStore.TryClaimProposedCommands(recordInit, out TransactionRecordCommand[]? init);
        TransactionRecordStore.TryClaimProposedCommands(decision, out TransactionRecordCommand[]? decided);

        if (PreparedIntentStore.TryClaimProposedCommands(prepare, out PreparedIntentCommand[]? prepared))
            locallyProposed.Register(data, new Commands(init, prepared, decided));

        return data;
    }

    /// <summary>Reads where the three parts lie. Throws <see cref="InvalidDataException"/> for an entry that misses a
    /// part, repeats one, or whose lengths overrun it.</summary>
    internal static Parts ReadParts(ReadOnlySpan<byte> data)
    {
        Range init = default, prepare = default, decision = default;
        bool hasInit = false, hasPrepare = false, hasDecision = false;

        int position = 0;
        while (position < data.Length)
        {
            ulong tag = ReadVarint(data, ref position);
            int wireType = (int)(tag & 7);

            if (wireType != 2)
            {
                SkipScalar(data, ref position, wireType);
                continue;
            }

            int fieldLength = ReadLength(data, ref position);
            Range field = new(position, position + fieldLength);
            position += fieldLength;

            switch (tag)
            {
                case InitTag:
                    if (hasInit)
                        throw new InvalidDataException("A one-phase bundle entry carries two record init parts.");
                    init = field;
                    hasInit = true;
                    break;

                case PrepareTag:
                    if (hasPrepare)
                        throw new InvalidDataException("A one-phase bundle entry carries two prepare parts.");
                    prepare = field;
                    hasPrepare = true;
                    break;

                case DecisionTag:
                    if (hasDecision)
                        throw new InvalidDataException("A one-phase bundle entry carries two decision parts.");
                    decision = field;
                    hasDecision = true;
                    break;
            }
        }

        if (!hasInit || !hasPrepare || !hasDecision)
            throw new InvalidDataException("A one-phase bundle entry misses its record init, prepare or decision part.");

        return new Parts(init, prepare, decision);
    }

    /// <summary>
    /// Applies the three parts of the entry <paramref name="log"/> of <paramref name="partitionId"/>, whose bytes are
    /// <paramref name="data"/>: the record init to <paramref name="records"/>, the prepare to <paramref name="intents"/>
    /// and the decision to <paramref name="records"/>, in that order, with the index, the term and the time of the
    /// entry. These are the applies of the three entries of the same commit. Returns whether every prepare took its key,
    /// as the live apply of a prepare entry does, and true for a replay (<paramref name="replay"/>), which answers
    /// nothing.
    /// <para>The entry is open on the record and the intent channels of <paramref name="tracker"/> from before the
    /// apply until both store snapshots cover it, so the durability floor of the partition does not pass the entry
    /// while one of the two stores holds its part only in memory.</para>
    /// </summary>
    internal static bool Apply(
        TransactionRecordStore records, PreparedIntentStore intents, Persistence.PartitionDurabilityTracker? tracker,
        int partitionId, Kommander.Data.RaftLog log, byte[] data, bool replay)
    {
        tracker?.RegisterPending(partitionId, log.Id, Persistence.DurabilityChannel.TransactionRecords, Persistence.DurabilityChannel.PreparedIntents);

        Parts parts = ReadParts(data);
        TryTakeCommands(data, out Commands? commands);

        records.ApplyDelta(partitionId, log.Term, commands?.Init, data.AsSpan(parts.Init));

        PreparedIntentCommand[] prepare = commands?.Prepare ?? PreparedIntentStore.DecodeDelta(data.AsSpan(parts.Prepare));

        bool acknowledged = true;
        if (replay)
            intents.ApplyCommands(partitionId, log.Id, log.Time, prepare, replay: true);
        else
            acknowledged = intents.ApplyCommandsAckPrepares(partitionId, log.Id, log.Time, prepare, out _);

        records.ApplyDelta(partitionId, log.Term, commands?.Decision, data.AsSpan(parts.Decision));

        tracker?.MarkApplied(partitionId, log.Id, Persistence.DurabilityChannel.TransactionRecords);
        tracker?.MarkApplied(partitionId, log.Id, Persistence.DurabilityChannel.PreparedIntents);
        return acknowledged;
    }

    /// <summary>Takes the decoded commands that this node registered for <paramref name="data"/>, for the apply of the
    /// entry. False when this node did not serialize it, or when the reuses of the registration are spent.</summary>
    internal static bool TryTakeCommands(byte[] data, [NotNullWhen(true)] out Commands? commands) =>
        locallyProposed.TryTake(data, out commands);

    /// <summary>The commands of the prepare part of <paramref name="data"/>: the registered ones, or a decode of the
    /// part. For a reader of the log that follows prepared intents and applies nothing.</summary>
    internal static PreparedIntentCommand[] PrepareCommandsOf(byte[] data)
    {
        if (locallyProposed.TryPeek(data, out Commands? commands))
            return commands.Prepare;

        Parts parts = ReadParts(data);
        return PreparedIntentStore.DecodeDelta(data.AsSpan(parts.Prepare));
    }

    /// <summary>
    /// The prepared-intent commands that an entry of <paramref name="logType"/> carries: the commands of a
    /// <see cref="ReplicationTypes.PreparedIntent"/> delta, or those of the prepare part of a
    /// <see cref="ReplicationTypes.OnePhaseBundle"/> entry. False for any other entry, and for an entry with no data.
    /// Every reader of the log that follows prepared intents (a watch replay, a restore, the intent frontier of a
    /// backup) reads them through here.
    /// </summary>
    internal static bool TryGetIntentCommands(string? logType, byte[]? data, [NotNullWhen(true)] out PreparedIntentCommand[]? commands)
    {
        commands = null;

        if (data is null || data.Length == 0)
            return false;

        if (logType == ReplicationTypes.PreparedIntent)
        {
            commands = PreparedIntentStore.DecodeDelta(data);
            return true;
        }

        if (logType == ReplicationTypes.OnePhaseBundle)
        {
            commands = PrepareCommandsOf(data);
            return true;
        }

        return false;
    }

    private static int FieldLength(uint tag, int payload) => VarintLength(tag) + VarintLength((uint)payload) + payload;

    private static int VarintLength(uint value)
    {
        int length = 1;
        while (value >= 0x80)
        {
            value >>= 7;
            length++;
        }

        return length;
    }

    private static void WriteField(byte[] data, ref int position, uint tag, byte[] payload)
    {
        WriteVarint(data, ref position, tag);
        WriteVarint(data, ref position, (uint)payload.Length);
        payload.CopyTo(data, position);
        position += payload.Length;
    }

    private static void WriteVarint(byte[] data, ref int position, uint value)
    {
        while (value >= 0x80)
        {
            data[position++] = (byte)(value | 0x80);
            value >>= 7;
        }

        data[position++] = (byte)value;
    }

    private static ulong ReadVarint(ReadOnlySpan<byte> data, ref int position)
    {
        ulong result = 0;
        for (int shift = 0; shift < 70 && position < data.Length; shift += 7)
        {
            byte b = data[position++];
            result |= (ulong)(b & 0x7F) << shift;
            if ((b & 0x80) == 0)
                return result;
        }

        throw new InvalidDataException("Malformed varint in a one-phase bundle entry.");
    }

    private static int ReadLength(ReadOnlySpan<byte> data, ref int position)
    {
        ulong length = ReadVarint(data, ref position);
        if (length > (ulong)(data.Length - position))
            throw new InvalidDataException("A length prefix overruns the one-phase bundle entry.");

        return (int)length;
    }

    private static void SkipScalar(ReadOnlySpan<byte> data, ref int position, int wireType)
    {
        switch (wireType)
        {
            case 0:
                ReadVarint(data, ref position);
                break;

            case 1 when data.Length - position >= 8:
                position += 8;
                break;

            case 5 when data.Length - position >= 4:
                position += 4;
                break;

            default:
                throw new InvalidDataException($"Unsupported wire type {wireType} in a one-phase bundle entry.");
        }
    }
}
