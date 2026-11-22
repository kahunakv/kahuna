using System.Collections;
using System.Collections.Concurrent;
using Google.Protobuf;
using Google.Protobuf.Collections;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.KeyValues.Writes;
using Kahuna.Server.Replication;
using Kahuna.Server.Replication.Protos;
using Kahuna.Shared.KeyValue;
using Kahuna.Utils;
using Kommander.Data;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.KeyValues.Transactions;

/// <summary>Durable snapshot persistence, load, and partition transfer of <see cref="PreparedIntentStore"/>.</summary>
internal sealed partial class PreparedIntentStore
{
    // ── durable snapshot ──────────────────────────────────────────────────────────

    /// <summary>Atomically rewrites this partition's on-disk intent snapshot. Returns true (durable) so the WAL
    /// checkpoint may discard the covered tail; false on write failure gates the checkpoint. No-op (true) when
    /// persistence or the resolver is not configured.</summary>
    /// <param name="appliedThroughIndex">
    /// The partition's log position the caller certifies as applied through this store BEFORE the call: every
    /// prepared-intent entry at or below it has completed its apply (the durability tracker's prepared-intent
    /// ceiling, which is raised after each apply returns). The file records it as the slice's fully reflected
    /// position, and the position read after the intent walk as its reflected one, exactly as a whole-partition
    /// section carries the exporter's positions — so a restart's replay of the entries at or below them is
    /// history the reloaded set already reflects, not live catch-up.
    ///
    /// <para>Without it a restart replayed history through the live path against an intent set captured later
    /// than the replay's start: the application-durability floor the WAL replays from can sit hundreds of
    /// thousands of entries below the checkpoint (a flusher behind on a stalled disk), and a prepare that the
    /// live apply had rejected because a competitor held its key found the key free in the replay — the holder
    /// had settled before the checkpoint — and installed a phantom holder no replica ever had. Every later
    /// transaction of the key was then refused here as foreign-held, its by-reference materialization found no
    /// intent, and the key's head froze below its peers'; when the node led next it served the frozen value
    /// (CamusDB fault soak sn1, 2026-09-30: two rows lost ~100 s of commits). Zero means the caller certifies
    /// nothing, and the file keeps only whatever positions the slice was installed with.</para>
    /// </param>
    public bool PersistSnapshot(int partitionId, long appliedThroughIndex = 0)
    {
        if (snapshotDirectory is null || snapshotPrefix is null || resolvePartition is null)
            return true;

        // Unchanged since this partition's last durable write: the file already holds exactly this content, so
        // the checkpoint may proceed without scanning or rewriting anything. The stamp is captured before the
        // scan and recorded only after a successful write, so a failed write or a mutation racing the scan
        // always leaves the partition due for a rewrite. The certified position is part of the stamp: an
        // unchanged set at a later position still needs its file to say so, or the entries in between replay
        // as live history on the next restart.
        long observedVersion = Math.Max(
            Interlocked.Read(ref allPartitionsVersion),
            partitionVersion.TryGetValue(partitionId, out long dirtyTick) ? dirtyTick : 0);
        long observedRouting = routingVersion?.Invoke() ?? 0;
        long certifiedThroughIndex = Math.Max(0, appliedThroughIndex);

        if (persistedVersion.TryGetValue(partitionId, out PersistedStamp last)
            && last.Version == observedVersion && last.RoutingVersion == observedRouting
            && last.AppliedThroughIndex == certifiedThroughIndex)
            return true;

        string path = Path.Combine(snapshotDirectory, $"{snapshotPrefix}_p{partitionId}.snapshot");

        try
        {
            // Entries stream straight into the temp file through one reused PREPARE-kind message: each entry
            // is written length-delimited under the repeated field's tag, and a reader parses the file as one
            // PreparedIntentSnapshotMessage whatever the order of its fields. Materializing one protobuf object
            // per retained intent plus one byte[] for the whole set made every checkpoint's allocation
            // proportional to the store size, which dominated the node's allocation profile whenever the
            // retained set was large.
            //
            // The partition's committed-head ledger slice rides in the same file, captured BEFORE the intent
            // scan: a bundled commit replayed from the WAL after a restart is judged against the reloaded
            // ledger, and it is exact only if the ledger's position is no later than the intent set's (and
            // the record store's, which the checkpoint captures after this file) — every entry between the
            // ledger's position and the replayed commit is then re-applied on top of it in order.
            lock (fileLock)
            {
                string tmp = path + ".tmp";

                ChunkedList<PreparedIntent> pendingBeforeCapture = OwnedIntentsBeforeCapture(key => resolvePartition(key) == partitionId);

                using (FileStream file = new(tmp, FileMode.Create, FileAccess.Write, FileShare.None, 64 * 1024))
                using (CodedOutputStream output = new(file))
                {
                    // The capture writes the slice's settled identities to the file as it reads them, so they
                    // are the first field of the file and the capture holds one chunk of them at a time.
                    LedgerCapture? ledger = CaptureLedger(partitionId, SettledIdentityWriter(output));

                    // The position the file certifies as fully reflected: what the caller certified, never below
                    // what the slice was installed with. Read BEFORE the walk (the caller's ceiling is raised only
                    // after an apply returns, so every intent an entry at or below it prepared is in the map when
                    // the walk starts) — the same cut a whole-partition export takes (see WritePartitionSection).
                    long fullyReflected = Math.Max(certifiedThroughIndex, ledger?.FullyReflectedThroughIndex ?? 0);

                    // Settled intents whose materialized row is still queued for the flush ride in the same file, in
                    // their own field: a reader must never install them as live intents.
                    List<PreparedIntent>? settledAwaitingFlushSlice = CollectSettledAwaitingFlush(partitionId, resolvePartition);

                    PreparedIntentCommandMessage entry = new();

                    // The map is walked directly, as the whole-partition export walks it (see
                    // WritePartitionSection). Its Values property copies every intent reference into one list
                    // under every bucket lock. Each key is written in a state it had at some instant of the
                    // walk, and an intent prepared or settled during the walk is written or not. Every entry
                    // applied during the walk is above the position this file certifies as fully reflected, so
                    // a restart replays it on top of the file: a prepare the walk missed installs, a settle
                    // of an intent the walk still wrote settles it, and the head of an intent that settled
                    // before the walk reached it is added below.
                    foreach (KeyValuePair<string, PreparedIntent> kv in intents)
                    {
                        PreparedIntent intent = kv.Value;

                        if (resolvePartition(intent.Key) != partitionId)
                            continue;

                        FillPrepareProto(entry, intent);
                        output.WriteTag(PreparedIntentSnapshotMessage.IntentsFieldNumber, WireFormat.WireType.LengthDelimited);
                        output.WriteMessage(entry);
                    }

                    if (settledAwaitingFlushSlice is not null)
                    {
                        foreach (PreparedIntent intent in settledAwaitingFlushSlice)
                        {
                            FillPrepareProto(entry, intent);
                            output.WriteTag(PreparedIntentSnapshotMessage.SettledIntentsAwaitingFlushFieldNumber, WireFormat.WireType.LengthDelimited);
                            output.WriteMessage(entry);
                        }
                    }

                    // The checkpoint persists the slice's positions: the ones it was installed with (so a restart
                    // mid catch-up after a snapshot install keeps treating the remaining window as history) raised
                    // to this node's own — the certified cut as fully reflected, and the position applied through
                    // this store by the end of the walk as reflected. Entries between the two are the walk's tear
                    // window: an intent prepared there may be missing from the file, so its replayed prepare must
                    // still install, while the advisory fence stays silent on it.
                    AddHeadsSettledDuringCapture(partitionId, ledger, pendingBeforeCapture);
                    long reflected = Math.Max(fullyReflected, Math.Max(AppliedLogIndexOf(partitionId), ledger?.ReflectedThroughIndex ?? 0));
                    WriteLedgerSection(output, partitionId, ledger, reflected, fullyReflected);
                }

                File.Move(tmp, path, overwrite: true);
            }

            persistedVersion[partitionId] = new PersistedStamp(observedVersion, observedRouting, certifiedThroughIndex);
            return true;
        }
        catch (Exception ex)
        {
            logger?.LogError(ex, "Failed to persist prepared-intent snapshot to {Path}", path);
            return false;
        }
    }

    private void LoadFromDisk()
    {
        if (snapshotDirectory is null || snapshotPrefix is null || !Directory.Exists(snapshotDirectory))
            return;

        string[] files;
        lock (fileLock)
            files = Directory.GetFiles(snapshotDirectory, $"{snapshotPrefix}_p*.snapshot");

        // Each file is decoded one entry at a time straight off the file, and each entry goes to its place
        // before the next is read. The whole-message parser held one protobuf object for every intent, head and
        // settled identity of the file at once, in repeated fields whose arrays grow by doubling: at a million
        // settled identities, arrays of up to 8 MiB next to the slice they were about to become. A failure
        // part-way through leaves a partial set, which is harmless: the constructor throws and the node refuses
        // to start, as it did when the whole file failed to parse up front.
        foreach (string path in files)
        {
            int filePartition = TryParsePartitionFromSnapshotPath(path, out int parsedPartition) ? parsedPartition : UnattributedPartition;

            try
            {
                lock (fileLock)
                {
                    using FileStream file = new(path, FileMode.Open, FileAccess.Read, FileShare.Read, 64 * 1024);

                    // The slice is filled as its heads and settled identities are decoded, and it takes its
                    // scalar state (the watermark, the positions, the size floor) at the end of the file: a
                    // writer can put those fields anywhere, and one pass over the file is enough this way. The
                    // slice has no watermark while it is filled, so every settled identity of the file is
                    // loaded; the sweep removes one that the watermark does not retain.
                    PartitionLedger ledger = new();

                    SnapshotHeader header = ReadSnapshot(
                        file, verify: false,
                        onIntent: loaded =>
                        {
                            MergeLoad(loaded, filePartition);
                            (loadedIntentPartitions ??= new(IntentMapShards, concurrencyLevel: 1))[loaded.Key] = filePartition;
                        },
                        // Retained settled intents reload into their own set — never into the live set, where a
                        // settled intent would hold its key as a phantom. Nothing is queued yet, so no probe
                        // applies here; the partition's restore completion re-checks them (see
                        // ReleaseSettledIntentsWithDurableRows).
                        onSettledAwaitingFlush: retained =>
                        {
                            lock (settledGate)
                                AddSettledAwaitingFlushLocked(retained);
                        },
                        onHead: (key, head) => AddInstalledHead(ledger, key, head),
                        onSettled: entry => AddInstalledSettled(ledger, entry));

                    if (header.LedgerPresent)
                    {
                        SetInstalledState(ledger, header.LedgerWatermark, header.ReflectedThroughIndex, header.FullyReflectedThroughIndex, header.LedgerSizeFloor);

                        lock (applyGate)
                            InstallLedgerLocked(header.LedgerPartitionId, ledger);
                    }
                    else if (filePartition != UnattributedPartition)
                        ledgerMissingPartitions.Add(filePartition);
                }
            }
            catch (InvalidProtocolBufferException ex)
            {
                throw new InvalidDataException($"Corrupt prepared-intent snapshot {path}; refusing to start empty and lose a prepared intent", ex);
            }
            catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
            {
                throw new IOException($"Failed to read prepared-intent snapshot {path}; refusing to start with a possibly incomplete intent set", ex);
            }
        }
    }

    /// <summary>The scalar fields of a prepared-intent snapshot stream and the number of its entries: what a
    /// reader knows after one pass, and what an install needs before it decodes the entries.</summary>
    internal readonly record struct SnapshotHeader(
        bool LedgerPresent, int LedgerPartitionId, HLCTimestamp LedgerWatermark,
        long ReflectedThroughIndex, long FullyReflectedThroughIndex, long LedgerSizeFloor,
        int Intents, int Heads, int SettledIdentities);

    /// <summary>
    /// Reads a snapshot stream to its end, one field at a time, and returns its scalar fields. The stream is a
    /// <see cref="PreparedIntentSnapshotMessage"/> with its fields in any order: a checkpoint file, or the
    /// prepared-intent payload of a whole-partition section.
    ///
    /// <para>Each entry is decoded into a message of its own and handed to the handler of its kind before the
    /// next is read, so a pass holds one entry at a time whatever the size of the stream. An entry kind with no
    /// handler is decoded and dropped when <paramref name="verify"/> is set (a pass that checks that every
    /// entry decodes), and skipped with no decode when it is not (a pass that needs the scalar fields only).</para>
    ///
    /// <para>The scalar fields are written by the generated serializer as varints. The reader copies each one
    /// to a small buffer and parses that buffer with the generated parser, so a scalar field added to the
    /// message later needs no change here.</para>
    /// </summary>
    private static SnapshotHeader ReadSnapshot(
        Stream payload, bool verify,
        Action<PreparedIntent>? onIntent, Action<PreparedIntent>? onSettledAwaitingFlush,
        Action<string, CommittedHead>? onHead, Action<SettledIdentityEntry>? onSettled)
    {
        CodedInputStream input = new(payload, leaveOpen: true);

        using MemoryStream scalarBytes = new(64);
        CodedOutputStream scalars = new(scalarBytes, leaveOpen: true);

        int intents = 0;
        int heads = 0;
        int settled = 0;

        uint tag;
        while ((tag = input.ReadTag()) != 0)
        {
            WireFormat.WireType wireType = WireFormat.GetTagWireType(tag);

            if (wireType == WireFormat.WireType.Varint)
            {
                scalars.WriteTag(tag);
                scalars.WriteUInt64(input.ReadUInt64());
                continue;
            }

            if (wireType != WireFormat.WireType.LengthDelimited)
            {
                input.SkipLastField();
                continue;
            }

            switch (WireFormat.GetTagFieldNumber(tag))
            {
                case PreparedIntentSnapshotMessage.IntentsFieldNumber:
                    intents++;
                    ReadIntentEntry(input, verify, onIntent);
                    break;

                case PreparedIntentSnapshotMessage.SettledIntentsAwaitingFlushFieldNumber:
                    ReadIntentEntry(input, verify, onSettledAwaitingFlush);
                    break;

                case PreparedIntentSnapshotMessage.CommittedHeadsFieldNumber:
                    heads++;

                    if (onHead is null && !verify)
                    {
                        input.SkipLastField();
                        break;
                    }

                    CommittedHeadLedgerEntryMessage head = new();
                    input.ReadMessage(head);
                    onHead?.Invoke(
                        head.Key,
                        new CommittedHead(head.Revision, (KeyValueState)head.State,
                            new HLCTimestamp(head.CommittedAtNode, head.CommittedAtPhysical, head.CommittedAtCounter)));
                    break;

                case PreparedIntentSnapshotMessage.SettledTransactionsFieldNumber:
                    settled++;

                    if (onSettled is null && !verify)
                    {
                        input.SkipLastField();
                        break;
                    }

                    SettledTransactionLedgerEntryMessage identity = new();
                    input.ReadMessage(identity);
                    onSettled?.Invoke(new SettledIdentityEntry(
                        new HLCTimestamp(identity.TransactionIdNode, identity.TransactionIdPhysical, identity.TransactionIdCounter),
                        identity.Epoch,
                        identity.Key,
                        new HLCTimestamp(identity.SettledAtNode, identity.SettledAtPhysical, identity.SettledAtCounter)));
                    break;

                default:
                    input.SkipLastField();
                    break;
            }
        }

        scalars.Flush();

        PreparedIntentSnapshotMessage message = PreparedIntentSnapshotMessage.Parser.ParseFrom(scalarBytes.GetBuffer(), 0, (int)scalarBytes.Length);

        return new SnapshotHeader(
            message.LedgerPresent, message.LedgerPartitionId, LedgerWatermarkOf(message),
            message.LedgerReflectedThroughIndex, message.LedgerFullyReflectedThroughIndex, message.LedgerSizeFloorPhysical,
            intents, heads, settled);
    }

    private static void ReadIntentEntry(CodedInputStream input, bool verify, Action<PreparedIntent>? handler)
    {
        if (handler is null && !verify)
        {
            input.SkipLastField();
            return;
        }

        PreparedIntentCommandMessage entry = new();
        input.ReadMessage(entry);

        // The conversion runs in a verify pass too: it is part of what a later pass does with the entry.
        PreparedIntent intent = IntentOf(entry);
        handler?.Invoke(intent);
    }

    // A per-partition snapshot file is named "{prefix}_p{partitionId}.snapshot".
    private static bool TryParsePartitionFromSnapshotPath(string path, out int partitionId)
    {
        partitionId = 0;
        string name = Path.GetFileNameWithoutExtension(path);
        int marker = name.LastIndexOf("_p", StringComparison.Ordinal);
        return marker >= 0 && int.TryParse(name.AsSpan(marker + 2), out partitionId);
    }

    /// <summary>Streams a partition's ledger slice (or an explicitly empty one) after the intents: each head
    /// under the repeated field's tag, then the scalar fields. The settled identities of the slice are already
    /// in the stream: the capture wrote them (see <see cref="SettledIdentityWriter"/>). The writer always marks
    /// the ledger present so a reader can tell "empty ledger" from "written before the ledger existed".</summary>
    private static void WriteLedgerSection(CodedOutputStream output, int partitionId, LedgerCapture? ledger, long reflectedThroughIndex, long fullyReflectedThroughIndex)
    {
        HLCTimestamp watermark = HLCTimestamp.Zero;

        if (ledger is not null)
        {
            watermark = ledger.Watermark;

            CommittedHeadLedgerEntryMessage head = new();

            foreach (KeyValuePair<string, CommittedHead> entry in ledger.Heads)
            {
                if (ledger.IsReplaced(entry.Key))
                    continue;

                FillLedgerEntry(head, entry.Key, entry.Value);
                output.WriteTag(PreparedIntentSnapshotMessage.CommittedHeadsFieldNumber, WireFormat.WireType.LengthDelimited);
                output.WriteMessage(head);
            }

            if (ledger.ReplacedHeads is not null)
            {
                foreach (KeyValuePair<string, CommittedHead> entry in ledger.ReplacedHeads)
                {
                    FillLedgerEntry(head, entry.Key, entry.Value);
                    output.WriteTag(PreparedIntentSnapshotMessage.CommittedHeadsFieldNumber, WireFormat.WireType.LengthDelimited);
                    output.WriteMessage(head);
                }
            }
        }

        long reflected = Math.Max(0, reflectedThroughIndex);

        // A zero is omitted exactly as the message serializer omits it.
        PreparedIntentSnapshotMessage trailer = new()
        {
            LedgerPresent = true,
            LedgerPartitionId = partitionId,
            LedgerWatermarkNode = watermark.N,
            LedgerWatermarkPhysical = watermark.L,
            LedgerWatermarkCounter = watermark.C,
            LedgerReflectedThroughIndex = reflected,
            LedgerFullyReflectedThroughIndex = Math.Clamp(fullyReflectedThroughIndex, 0, reflected),
            LedgerSizeFloorPhysical = ledger?.SizeFloor ?? 0
        };

        trailer.WriteTo(output);
    }

    /// <summary>The sink a capture hands its settled identities to when the slice goes to a stream: each
    /// identity is written under the repeated field's tag through one reused message.</summary>
    private static Action<List<SettledIdentityEntry>> SettledIdentityWriter(CodedOutputStream output)
    {
        SettledTransactionLedgerEntryMessage message = new();

        return chunk =>
        {
            foreach (SettledIdentityEntry entry in chunk)
            {
                FillSettledEntry(message, entry);
                output.WriteTag(PreparedIntentSnapshotMessage.SettledTransactionsFieldNumber, WireFormat.WireType.LengthDelimited);
                output.WriteMessage(message);
            }
        };
    }

    private static void FillLedgerEntry(CommittedHeadLedgerEntryMessage m, string key, CommittedHead head)
    {
        m.Key = key;
        m.Revision = head.Revision;
        m.State = (int)head.State;
        m.CommittedAtNode = head.CommittedAt.N;
        m.CommittedAtPhysical = head.CommittedAt.L;
        m.CommittedAtCounter = head.CommittedAt.C;
    }

    /// <summary>The heads of a decoded message. Each head is converted when it is read: a list of every head
    /// of a full slice is one array of tens of megabytes next to the decoded message.</summary>
    private static IReadOnlyList<(string Key, long Revision, KeyValueState State, HLCTimestamp CommittedAt)> LedgerEntriesOf(PreparedIntentSnapshotMessage message) =>
        new DecodedList<CommittedHeadLedgerEntryMessage, (string Key, long Revision, KeyValueState State, HLCTimestamp CommittedAt)>(
            message.CommittedHeads,
            static head => (head.Key, head.Revision, (KeyValueState)head.State,
                new HLCTimestamp(head.CommittedAtNode, head.CommittedAtPhysical, head.CommittedAtCounter)));

    /// <summary>A read-only list over a decoded repeated field that converts a message each time it is read.
    /// It holds no copy of the field.</summary>
    private sealed class DecodedList<TMessage, T>(RepeatedField<TMessage> messages, Func<TMessage, T> convert) : IReadOnlyList<T>
    {
        public int Count => messages.Count;

        public T this[int index] => convert(messages[index]);

        public IEnumerator<T> GetEnumerator()
        {
            foreach (TMessage message in messages)
                yield return convert(message);
        }

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }

    private static HLCTimestamp LedgerWatermarkOf(PreparedIntentSnapshotMessage message) =>
        new(message.LedgerWatermarkNode, message.LedgerWatermarkPhysical, message.LedgerWatermarkCounter);

    private static void FillSettledEntry(SettledTransactionLedgerEntryMessage m, SettledIdentityEntry entry)
    {
        m.TransactionIdNode = entry.TransactionId.N;
        m.TransactionIdPhysical = entry.TransactionId.L;
        m.TransactionIdCounter = entry.TransactionId.C;
        m.Epoch = entry.Epoch;
        m.Key = entry.Key;
        m.SettledAtNode = entry.SettledAt.N;
        m.SettledAtPhysical = entry.SettledAt.L;
        m.SettledAtCounter = entry.SettledAt.C;
    }

    /// <summary>The settled identities of a decoded message, converted when they are read (see
    /// <see cref="LedgerEntriesOf"/>).</summary>
    private static IReadOnlyList<SettledIdentityEntry> SettledEntriesOf(PreparedIntentSnapshotMessage message) =>
        new DecodedList<SettledTransactionLedgerEntryMessage, SettledIdentityEntry>(
            message.SettledTransactions,
            static m => new SettledIdentityEntry(
                new HLCTimestamp(m.TransactionIdNode, m.TransactionIdPhysical, m.TransactionIdCounter),
                m.Epoch,
                m.Key,
                new HLCTimestamp(m.SettledAtNode, m.SettledAtPhysical, m.SettledAtCounter)));

    // Load-time merge across (possibly overlapping) per-partition files: at most one live intent per key. A more
    // resolved intent for the same identity is authoritative; a different transaction on the same key is a
    // conflict that keeps the existing intent and is logged rather than silently overwritten.
    private void MergeLoad(PreparedIntent incoming, int partitionId)
    {
        if (!intents.TryGetValue(incoming.Key, out PreparedIntent? existing))
        {
            InstallLive(incoming.Key, incoming, partitionId);
            ObserveInstalled(incoming);
            Interlocked.Add(ref totalBytes, IntentBytes(incoming));
            StampDirty(incoming.Key);
            return;
        }

        if (existing.TransactionId != incoming.TransactionId || existing.Epoch != incoming.Epoch)
        {
            logger?.LogError("Conflicting prepared intents on load for key {Key}: {A} vs {B}",
                incoming.Key, existing.TransactionId, incoming.TransactionId);
            return;
        }

        if (existing.IsPending && incoming.IsResolved)
        {
            InstallLive(incoming.Key, incoming, partitionId);
            ObserveInstalled(incoming);
            Interlocked.Add(ref totalBytes, IntentBytes(incoming) - IntentBytes(existing));
            StampDirty(incoming.Key);
        }
    }

    // ── state transfer (split/merge) ────────────────────────────────────────────────

    /// <summary>Intents whose key routes into <c>[startKey, endKey)</c> (ordinal, half-open) — the set a range
    /// split/merge hands to the destination partition. Deliberately a consistent cut (the walk runs under the
    /// apply gate, which every writer of the map holds): the moved keys' log entries stay on the source partition, so the destination has
    /// no replay tail to converge a torn capture with. The whole-partition export does have that tail and streams
    /// its slice lock-free instead (<see cref="WritePartitionSection"/>); the scan captures read the ordered key
    /// index per key instead — see <see cref="SnapshotScanWindow"/>.</summary>
    public IReadOnlyList<PreparedIntent> SnapshotRange(string? startKey, string? endKey)
    {
        List<PreparedIntent> result = [];

        // Under the apply gate, which every writer of the map holds: the walk is then one cut across keys.
        lock (applyGate)
        {
            foreach (KeyValuePair<string, PreparedIntent> kv in intents)
            {
                PreparedIntent intent = kv.Value;

                if (startKey is not null && string.CompareOrdinal(intent.Key, startKey) < 0)
                    continue;

                if (endKey is not null && string.CompareOrdinal(intent.Key, endKey) >= 0)
                    continue;

                result.Add(intent);
            }
        }

        return result;
    }

    /// <summary>
    /// Writes one partition's intents together with its committed-head ledger slice to <paramref name="output"/> in
    /// the <see cref="SerializePartitionIntents"/> wire format and returns how many intents were written. The
    /// whole-partition export streams the slice through this instead of copying the store: the filter runs inside
    /// one lock-free walk of the map and each owned intent goes through one reused entry message, so the cost is
    /// the walk plus the owned rows — never a copy of every intent the node holds, and never one protobuf object
    /// per intent.
    ///
    /// <para>The ledger is captured before the walk, so its position is no later than any intent's (the order
    /// <see cref="PersistSnapshot"/> keeps for the same reason). The walk itself is not a point-in-time cut: each
    /// key's intent is written in a state that key really had at some instant during the walk, and a key prepared
    /// or settled during the walk may or may not appear. The export tolerates that per-key tear because the
    /// receiver replays every log entry above the snapshot boundary — which precedes the walk — on top of the
    /// installed set in order: an already-reflected prepare or settle folds idempotently, a missed one is applied,
    /// so by the time any entry is judged against a key's intent that intent is exact at its position. The
    /// committed-head ledger and the transaction records are captured at or after the intents, which is what the
    /// bundled-commit gate needs (see the record-side ordering in the exporter).</para>
    /// </summary>
    public int WritePartitionSection(Stream output, int partitionId, Func<string, bool> isOwnedKey)
    {
        // Read BEFORE the ledger capture and the walk: every head recorded and every intent prepared at or below this
        // position is in the maps when both start, so the section holds them unless a removal took an intent out, and
        // that removal is replayed (and its head added below). Entries at or below it are therefore fully reflected on
        // the receiver (see IsFullyReflectedApply), where a replayed prepare that finds no intent installs nothing.
        // Read any later, an entry applied during the capture of a large ledger could prepare and settle a transaction
        // on a key the capture had already passed: the section would hold neither its head nor its intent, and its replay
        // would restore neither. The entries between this position and the one read after the walk are the tear window,
        // where a missed intent must still install on replay.
        long fullyReflected = Math.Max(AppliedLogIndexOf(partitionId), GetLedgerReflectedThroughIndex(partitionId));

        ChunkedList<PreparedIntent> pendingBeforeCapture = OwnedIntentsBeforeCapture(isOwnedKey);

        using CodedOutputStream coded = new(output, leaveOpen: true);

        // The capture writes the slice's settled identities to the section as it reads them.
        LedgerCapture? ledger = CaptureLedger(partitionId, SettledIdentityWriter(coded));

        AfterLedgerCaptureForTesting?.Invoke();

        int written = 0;

        PreparedIntentCommandMessage entry = new();

        foreach (KeyValuePair<string, PreparedIntent> kv in intents)
        {
            if (!isOwnedKey(kv.Key))
                continue;

            FillPrepareProto(entry, kv.Value);
            coded.WriteTag(PreparedIntentSnapshotMessage.IntentsFieldNumber, WireFormat.WireType.LengthDelimited);
            coded.WriteMessage(entry);
            written++;
        }

        // The reflected position is read AFTER the walk: every entry at or below it was applied through this
        // store before the section was complete, so the installing replica may treat its replay of those
        // entries as history (see IsHistoricalApply). An entry applied during the walk may or may not be in the
        // captured ledger or intents; either way its replay folds idempotently, and the only thing withheld
        // from it on the receiver is an advisory verdict on a decision that predates the transfer.
        AddHeadsSettledDuringCapture(partitionId, ledger, pendingBeforeCapture);
        WriteLedgerSection(coded, partitionId, ledger, ReflectedPositionForExport(partitionId, ledger), fullyReflected);

        return written;
    }

    /// <summary>Test seam, invoked between a section's ledger capture and its intent walk. Never set in production.</summary>
    internal Action? AfterLedgerCaptureForTesting { get; set; }

    // A chunked list: at the live intents of a restart under load, one list is an array above 1 MiB.
    private ChunkedList<PreparedIntent> OwnedIntentsBeforeCapture(Func<string, bool> isOwnedKey)
    {
        ChunkedList<PreparedIntent> owned = new();

        foreach (KeyValuePair<string, PreparedIntent> kv in intents)
        {
            if (isOwnedKey(kv.Key))
                owned.Add(kv.Value);
        }

        return owned;
    }

    /// <summary>
    /// Closes the tear between the ledger capture and the intent walk. A transaction settled between the two records
    /// its head after the capture read that key and removes its intent before the walk reaches it, so the section
    /// would hold neither, and its settle, replayed on a replica with no intent to act on, would record nothing: the
    /// replica would be short of that head for as long as the key does not change, and its fence would admit a prepare
    /// against the base it replaced. The capture of a large ledger takes long enough for this to happen under load.
    ///
    /// <para>The capture must stay ahead of the walk (a replayed bundled commit is exact only against a ledger no later
    /// than the intent set), so the heads of exactly those transactions are added to it. Each of those keys had a
    /// pending intent from before the capture until its settle, so no other commit of the key lies between the capture's
    /// position and that head, and no replayed entry is judged against a head from its future.</para>
    /// </summary>
    private void AddHeadsSettledDuringCapture(int partitionId, LedgerCapture? ledger, ChunkedList<PreparedIntent> pendingBeforeCapture)
    {
        if (ledger is null || !ledgers.TryGetValue(partitionId, out PartitionLedger? slice))
            return;

        Dictionary<string, CommittedHead>? settled = null;

        foreach (PreparedIntent intent in pendingBeforeCapture)
        {
            if (intents.TryGetValue(intent.Key, out PreparedIntent? current)
                && current.TransactionId == intent.TransactionId
                && current.Epoch == intent.Epoch)
                continue;

            if (slice.Heads.TryGetValue(intent.Key, out CommittedHead head))
                (settled ??= new(StringComparer.Ordinal))[intent.Key] = head;
        }

        // The writers take these heads in place of the scanned heads of the same keys.
        ledger.ReplacedHeads = settled;
    }

    /// <summary>The reflected position a whole-partition section advertises: the highest log index applied through
    /// this store for the partition, never below what the slice itself was installed with.</summary>
    private long ReflectedPositionForExport(int partitionId, LedgerCapture? ledger) =>
        Math.Max(AppliedLogIndexOf(partitionId), ledger?.ReflectedThroughIndex ?? 0);

    /// <summary>Intents whose key starts with <paramref name="prefix"/> (ordinal) — the set a bucket scan
    /// (<c>GetByBucket</c>) reconciles against. Matched on the key with the same prefix predicate the scan applies to
    /// its resident and persisted rows, so a prefix spelled with a trailing slash, a bare key-space name, or a
    /// partial key selects exactly the intents the scan's own page can contain; an intent-only committed key is then
    /// included/overridden/excluded in a bucket scan exactly as it would be in the equivalent range scan.</summary>
    public IReadOnlyList<PreparedIntent> SnapshotPrefix(string prefix)
    {
        List<PreparedIntent>? result = null;

        lock (orderedKeysGate)
        {
            if (orderedKeys.Count == 0)
                return [];

            // Every key with the prefix sorts at or after the prefix and before any key without it that sorts
            // after the prefix, so the keys with the prefix are one run at the head of this view.
            string last = orderedKeys.Max!;
            if (string.CompareOrdinal(prefix, last) > 0)
                return [];

            foreach (string key in orderedKeys.GetViewBetween(prefix, last))
            {
                if (!key.StartsWith(prefix, StringComparison.Ordinal))
                    break;

                // Per-key capture of the live state; the argument is the one on SnapshotScanWindow.
                if (intents.TryGetValue(key, out PreparedIntent? intent))
                    (result ??= []).Add(intent);
            }
        }

        return result ?? [];
    }

    /// <summary>Intents covering a scan page's window with the scan's own boundary semantics: start-exclusivity for a
    /// continuation cursor (<paramref name="startInclusive"/> false skips an intent exactly at <paramref name="startKey"/>,
    /// which the prior page already emitted) and end-inclusivity (<paramref name="endInclusive"/> true keeps an intent
    /// exactly at <paramref name="endKey"/>). This is distinct from <see cref="SnapshotRange"/>'s fixed half-open
    /// <c>[start, end)</c> so the overlaid intent set matches exactly the window the scan's KV rows were drawn from —
    /// without it a boundary intent is re-emitted across pages or an inclusive-end intent is missed. The intents come
    /// back in ordinal key order.
    ///
    /// <para>The window's keys come from the ordered key index (<see cref="orderedKeys"/>): a descent to the first
    /// key at or after the start, then the keys up to the end, under the index gate. Each key's intent is then read
    /// from the map lock-free. The cost is the window, not the live intent population, and an empty store answers
    /// from the index count alone. Before, every page enumerated the whole map: O(live intents) per page, and a map
    /// that had grown for a bulk load kept walking its empty buckets after the load settled.</para>
    ///
    /// <para>This is not a consistent cut across keys: an intent present for the whole call is always returned
    /// exactly once, while one installed, replaced or removed during it may be seen in either state (a key the index
    /// names whose intent is already gone is skipped; a key installed after the descent is missed). That is
    /// sufficient here because the merge judges each key independently — a key's outcome depends only on its own
    /// intent, so any state read for a key is a state that key really had at some instant after the KV rows were
    /// read, exactly as a copy taken at that instant would show it. The only cross-key effect, a pending intent
    /// making the whole page retry, is monotone-safe: observing a stale pending node causes a retry, never a wrong
    /// row. State-transfer captures need a true cut and keep <c>Values</c>.</para>
    /// </summary>
    public IReadOnlyList<PreparedIntent> SnapshotScanWindow(string? startKey, bool startInclusive, string? endKey, bool endInclusive)
    {
        List<PreparedIntent>? result = null;

        lock (orderedKeysGate)
        {
            if (orderedKeys.Count == 0)
                return [];

            // The view is inclusive at both ends; the exclusive bounds skip the one key that can equal them.
            string lower = startKey ?? orderedKeys.Min!;
            string upper = endKey ?? orderedKeys.Max!;

            if (string.CompareOrdinal(lower, upper) > 0)
                return [];

            foreach (string key in orderedKeys.GetViewBetween(lower, upper))
            {
                if (!startInclusive && startKey is not null && string.CompareOrdinal(key, startKey) == 0)
                    continue;

                if (!endInclusive && endKey is not null && string.CompareOrdinal(key, endKey) == 0)
                    break;

                if (intents.TryGetValue(key, out PreparedIntent? intent))
                    (result ??= []).Add(intent);
            }
        }

        return result ?? [];
    }

    /// <summary>
    /// Drops every intent whose key satisfies <paramref name="shouldRemove"/> and returns how many were
    /// removed. This is the un-host purge: when this node stops being a replica of the keys' partition, the
    /// intents' resolution lives with the partition's replicas (and returns in a seeding snapshot on any
    /// re-gain), so the local copies are dead retention. Byte accounting is adjusted and the change stamp
    /// bumped so the next per-partition snapshot rewrites the emptied slice.
    /// </summary>
    public int PurgeWhere(Func<string, bool> shouldRemove)
    {
        int removedCount;

        lock (applyGate)
        {
            // The retained settled intents of the range go with it, whether or not a live intent matches.
            bool settledPurged = PurgeSettledAwaitingFlushLocked(shouldRemove);

            List<string>? toRemove = null;

            foreach (KeyValuePair<string, PreparedIntent> kv in intents)
            {
                if (shouldRemove(kv.Key))
                    (toRemove ??= []).Add(kv.Key);
            }

            if (toRemove is null)
            {
                if (settledPurged)
                    StampAllDirty();

                return 0;
            }

            foreach (string key in toRemove)
            {
                if (RemoveLive(key, out PreparedIntent? removed))
                    Interlocked.Add(ref totalBytes, -IntentBytes(removed!));
            }

            StampAllDirty();
            removedCount = toRemove.Count;
        }

        SignalFenceWaiters();
        return removedCount;
    }

    /// <summary>Folds transferred intents into this partition's set (idempotent by key + resolution authority).</summary>
    public void ImportIntents(IEnumerable<PreparedIntent> incoming) => ImportIntents(incoming, UnattributedPartition);

    /// <summary>Folds transferred intents into <paramref name="partitionId"/>'s set (idempotent by key + resolution
    /// authority).</summary>
    public void ImportIntents(IEnumerable<PreparedIntent> incoming, int partitionId)
    {
        // Under the apply gate, as every other writer of the map: a copy taken under the gate is then one cut
        // across keys. The gate is taken for each intent, so an import of many does not hold it for long.
        foreach (PreparedIntent intent in incoming)
        {
            lock (applyGate)
                MergeLoad(intent, partitionId);
        }

        SignalFenceWaiters();
    }

    /// <summary>
    /// The intents and the committed-head ledger slice of one partition as a whole-partition snapshot carries
    /// them, decoded by <see cref="DeserializePartitionIntents"/>. The ledger is captured before the intents
    /// so its position is no later than theirs (see <see cref="PersistSnapshot"/>). A section written by a
    /// build that predates the ledger decodes with a null ledger.
    /// </summary>
    internal sealed record PartitionIntentSection(
        IReadOnlyList<PreparedIntent> Intents,
        IReadOnlyList<(string Key, long Revision, KeyValueState State, HLCTimestamp CommittedAt)>? Ledger,
        HLCTimestamp LedgerWatermark,
        long ReflectedThroughIndex = 0,
        IReadOnlyList<SettledIdentityEntry>? Settled = null,
        long FullyReflectedThroughIndex = 0,
        long LedgerSizeFloor = 0);

    /// <summary>Serializes one partition's intents together with its committed-head ledger slice, for
    /// whole-partition state transfer. The ledger is captured first so its position is no later than the
    /// intents' — a seeded replica then re-applies every entry after the snapshot boundary on top of a ledger
    /// that is exact at or before each of them.</summary>
    /// <param name="partitionId">The partition whose slice rides with the intents.</param>
    /// <param name="partitionIntents">The intents, cut by the caller before this call.</param>
    /// <param name="fullyReflectedThroughIndex">The partition's applied log index read by the caller BEFORE it
    /// cut <paramref name="partitionIntents"/> (see <see cref="GetAppliedLogIndex"/>): every intent prepared at or
    /// below it is in the cut, so a replay there installs nothing the cut lacks. Zero when the cut was not taken
    /// against a position; the section then advertises no fully reflected window.</param>
    public byte[] SerializePartitionIntents(int partitionId, IReadOnlyList<PreparedIntent> partitionIntents, long fullyReflectedThroughIndex = 0)
    {
        PreparedIntentSnapshotMessage message = new()
        {
            LedgerPresent = true,
            LedgerPartitionId = partitionId
        };

        LedgerCapture? ledger = CaptureLedger(partitionId, chunk =>
        {
            foreach (SettledIdentityEntry entry in chunk)
            {
                SettledTransactionLedgerEntryMessage settled = new();
                FillSettledEntry(settled, entry);
                message.SettledTransactions.Add(settled);
            }
        });

        foreach (PreparedIntent intent in partitionIntents)
            message.Intents.Add(PrepareProtoOf(intent));

        if (ledger is not null)
        {
            message.LedgerWatermarkNode = ledger.Watermark.N;
            message.LedgerWatermarkPhysical = ledger.Watermark.L;
            message.LedgerWatermarkCounter = ledger.Watermark.C;
            message.LedgerSizeFloorPhysical = ledger.SizeFloor;

            foreach (KeyValuePair<string, CommittedHead> entry in ledger.Heads)
            {
                CommittedHeadLedgerEntryMessage head = new();
                FillLedgerEntry(head, entry.Key, entry.Value);
                message.CommittedHeads.Add(head);
            }
        }

        message.LedgerReflectedThroughIndex = ReflectedPositionForExport(partitionId, ledger);
        message.LedgerFullyReflectedThroughIndex = Math.Clamp(fullyReflectedThroughIndex, 0, message.LedgerReflectedThroughIndex);

        return ReplicationSerializer.Serialize(message);
    }

    /// <summary>The highest log index applied through this store for <paramref name="partitionId"/> (0 when none):
    /// the position a caller reads before cutting the intents it hands to <see cref="SerializePartitionIntents"/>.</summary>
    internal long GetAppliedLogIndex(int partitionId) => AppliedLogIndexOf(partitionId);

    /// <summary>Decodes a section written by <see cref="SerializePartitionIntents"/> (or, with a null ledger,
    /// by the ledgerless <see cref="SerializeIntents"/> of an older build) into one object that holds every
    /// entry. The install of a whole-partition snapshot does not use it: it reads the section one entry at a
    /// time (see <see cref="ReplacePartitionIntents(int, Stream, SnapshotHeader, bool, Func{string, bool})"/>).</summary>
    public static PartitionIntentSection DeserializePartitionIntents(byte[] data) =>
        PartitionIntentSectionOf(ReplicationSerializer.UnserializePreparedIntentSnapshotMessage(data));

    private static PartitionIntentSection PartitionIntentSectionOf(PreparedIntentSnapshotMessage message)
    {
        List<PreparedIntent> result = new(message.Intents.Count);
        foreach (PreparedIntentCommandMessage entry in message.Intents)
            result.Add(IntentOf(entry));

        return new PartitionIntentSection(
            result,
            message.LedgerPresent ? LedgerEntriesOf(message) : null,
            message.LedgerPresent ? LedgerWatermarkOf(message) : HLCTimestamp.Zero,
            message.LedgerPresent ? message.LedgerReflectedThroughIndex : 0,
            message.LedgerPresent ? SettledEntriesOf(message) : null,
            message.LedgerPresent ? message.LedgerFullyReflectedThroughIndex : 0,
            message.LedgerPresent ? message.LedgerSizeFloorPhysical : 0);
    }

    /// <summary>
    /// Installs a whole-partition snapshot's intents and ledger slice (idempotent for the intents; the slice
    /// replaces this node's slice for the partition, which a seeding install has made dead retention). A
    /// section without a ledger — written by a build that predates it — leaves the slice empty exactly as
    /// that build would hold it; a node that applies the extended bundled commit gate must refuse such a
    /// section (<paramref name="requireLedger"/>), because judging with an empty slice where the exporter's
    /// peers hold heads would fork the record state machine.
    /// </summary>
    public void ImportPartitionIntents(int partitionId, PartitionIntentSection section, bool requireLedger)
    {
        if (section.Ledger is null)
        {
            if (requireLedger)
                throw new InvalidDataException(
                    $"Partition {partitionId} snapshot carries no committed-head ledger (written by a build that predates it); " +
                    "refusing to install it while OnePhaseApplyTimeValidation is enabled, because this node would judge bundled commits differently from its replicas.");

            PurgePartitionLedger(partitionId);
        }
        else
            InstallLedger(partitionId, section.Ledger, section.LedgerWatermark, section.ReflectedThroughIndex, section.Settled, section.FullyReflectedThroughIndex, section.LedgerSizeFloor);

        ImportIntents(section.Intents, partitionId);
    }

    /// <summary>
    /// Installs a whole-partition snapshot's intents and ledger slice as a REPLACEMENT of everything this node
    /// holds for the partition — the seeding install's purge-then-apply discipline applied to the intent set.
    /// Every intent whose key <paramref name="isOwned"/> assigns to the partition is dropped and the section's
    /// intents and ledger are installed in the same critical section, so no verdict read or apply can observe
    /// the half-way state.
    ///
    /// <para>Why a merge is wrong here: an intent this node still held as pending — a transaction whose
    /// settlement applied while this node was down or behind, below the snapshot boundary and therefore
    /// compacted away, never to be replayed here — survived a merging install as a phantom holder of its key.
    /// From then on every prepare of the key by a later transaction was rejected on this node as a foreign
    /// holder while its replicas admitted it, every bundled commit of the key was refused at apply, every
    /// by-reference materialization of it found no intent, the key's row and committed head froze here, and the
    /// node answered <c>NotApplied</c> to every fence ask about the key — which kept the leader's commits
    /// waiting on a replica that could never attest. Idempotent for a re-delivered snapshot. The ledger
    /// contract is that of <see cref="ImportPartitionIntents"/>.</para>
    /// </summary>
    public void ReplacePartitionIntents(int partitionId, PartitionIntentSection section, bool requireLedger, Func<string, bool> isOwned)
    {
        if (section.Ledger is null && requireLedger)
            throw new InvalidDataException(
                $"Partition {partitionId} snapshot carries no committed-head ledger (written by a build that predates it); " +
                "refusing to install it while OnePhaseApplyTimeValidation is enabled, because this node would judge bundled commits differently from its replicas.");

        int purged = PurgeBeforeInstall(partitionId, isOwned, out PartitionLedger? previousLedger);

        PartitionLedger? ledger = section.Ledger is null
            ? null
            : BuildLedger(section.Ledger, section.LedgerWatermark, section.ReflectedThroughIndex, section.Settled, section.FullyReflectedThroughIndex, section.LedgerSizeFloor);

        CompleteInstall(partitionId, ledger, previousLedger, section.Intents, purged, section.Intents.Count, section.ReflectedThroughIndex);
    }

    /// <summary>Decodes every entry of a whole-partition section and keeps none: the pass of an install that
    /// checks the section before anything is purged. Returns the section's scalar fields and entry counts,
    /// which <see cref="ReplacePartitionIntents(int, Stream, SnapshotHeader, bool, Func{string, bool})"/> takes.
    /// Throws <see cref="InvalidProtocolBufferException"/> for a section that does not decode.</summary>
    internal static SnapshotHeader VerifyPartitionSection(Stream payload) =>
        ReadSnapshot(payload, verify: true, null, null, null, null);

    /// <summary>
    /// The form of <see cref="ReplacePartitionIntents(int, PartitionIntentSection, bool, Func{string, bool})"/>
    /// that reads the section from <paramref name="payload"/> one entry at a time: each head and each settled
    /// identity goes into the new slice as it is decoded, so the install holds the slice it builds and one entry,
    /// and never a decoded copy of the section next to it. <paramref name="header"/> is what
    /// <see cref="VerifyPartitionSection"/> returned for the same bytes. The contract and the order of the steps
    /// (purge, build outside the gate, swap) are those of the other form.
    /// </summary>
    public void ReplacePartitionIntents(int partitionId, Stream payload, SnapshotHeader header, bool requireLedger, Func<string, bool> isOwned)
    {
        if (!header.LedgerPresent && requireLedger)
            throw new InvalidDataException(
                $"Partition {partitionId} snapshot carries no committed-head ledger (written by a build that predates it); " +
                "refusing to install it while OnePhaseApplyTimeValidation is enabled, because this node would judge bundled commits differently from its replicas.");

        int purged = PurgeBeforeInstall(partitionId, isOwned, out PartitionLedger? previousLedger);

        PartitionLedger? ledger = header.LedgerPresent
            ? NewInstalledLedger(header.LedgerWatermark, header.ReflectedThroughIndex, header.FullyReflectedThroughIndex, header.LedgerSizeFloor)
            : null;

        // The intents wait in a chunked list: they are installed with the slice, in one critical section.
        ChunkedList<PreparedIntent> installed = new();

        ReadSnapshot(
            payload, verify: false,
            onIntent: installed.Add,
            onSettledAwaitingFlush: null,
            onHead: ledger is null ? null : (key, head) => AddInstalledHead(ledger, key, head),
            onSettled: ledger is null ? null : entry => AddInstalledSettled(ledger, entry));

        CompleteInstall(partitionId, ledger, previousLedger, installed, purged, installed.Count, header.ReflectedThroughIndex);
    }

    /// <summary>The first step of a replacing install: drops every intent of the partition and takes its ledger
    /// slice out. Returns how many intents were dropped.</summary>
    private int PurgeBeforeInstall(int partitionId, Func<string, bool> isOwned, out PartitionLedger? previousLedger)
    {
        int purged = 0;

        // The slice this node held for the partition is released BEFORE the installed one is built: the
        // install's purge-then-apply discipline, applied to the intent side in the same order the receipts and
        // the records follow. Building the new ledger first kept both slices resident at once — on a partition
        // with hundreds of thousands of settled identities, two multi-megabyte dictionary arrays plus every
        // key string — at the one moment a re-seeded node is nearest its heap limit (the soak that ran out of
        // memory inside this build had the old slice still live). A reader between the two gate sections finds
        // the partition without a slice, exactly as it finds the receipt and record stores between their purge
        // and their import; the partition's apply stream is quiescent for the whole install (the install owns
        // its executor), so no replicated verdict is judged in that window. The reflected positions are
        // carried over by the swap below, which never regresses them.
        lock (applyGate)
        {
            // A chunked list: a node that restarted under load holds enough intents to make one list an array
            // above 1 MiB, allocated here under the gate.
            ChunkedList<string>? owned = null;
            foreach (KeyValuePair<string, PreparedIntent> kv in intents)
            {
                if (isOwned(kv.Key))
                    (owned ??= new()).Add(kv.Key);
            }

            if (owned is not null)
            {
                foreach (string key in owned)
                {
                    if (!RemoveLive(key, out PreparedIntent? removed))
                        continue;

                    Interlocked.Add(ref totalBytes, -IntentBytes(removed!));
                    fenceRefusalStreaks.TryRemove(key, out _);
                    purged++;
                }
            }

            // The install replaces this node's rows for the partition too, so a retained settled intent that was
            // waiting on a locally queued row no longer certifies anything the installed state needs.
            PurgeSettledAwaitingFlushLocked(isOwned);

            ledgers.TryRemove(partitionId, out previousLedger);
        }

        return purged;
    }

    /// <summary>The last step of a replacing install: swaps the built slice in and installs the intents, in one
    /// critical section.</summary>
    private void CompleteInstall(
        int partitionId, PartitionLedger? ledger, PartitionLedger? previousLedger, IEnumerable<PreparedIntent> installedIntents,
        int purged, int installedCount, long reflectedThroughIndex)
    {
        lock (applyGate)
        {
            if (ledger is not null)
            {
                // The previous slice was taken out above so its memory could go before the build; its reflected
                // positions still fence the swap, as they did when the slice was replaced in place.
                if (previousLedger is not null)
                {
                    if (previousLedger.ReflectedThroughIndex > ledger.ReflectedThroughIndex)
                        ledger.ReflectedThroughIndex = previousLedger.ReflectedThroughIndex;
                    if (previousLedger.FullyReflectedThroughIndex > ledger.FullyReflectedThroughIndex)
                        ledger.FullyReflectedThroughIndex = previousLedger.FullyReflectedThroughIndex;
                }

                InstallLedgerLocked(partitionId, ledger);
            }
            else
                RecomputeLedgerSizeFloorLocked();

            foreach (PreparedIntent intent in installedIntents)
                MergeLoad(intent, partitionId);

            StampAllDirty();
        }

        SignalFenceWaiters();

        if (purged > 0 && logger is not null && logger.IsEnabled(LogLevel.Information))
            logger.LogInformation(
                "Snapshot install for partition {PartitionId} replaced {Purged} locally held intent(s) with the {Installed} the snapshot carries (reflected through log index {ReflectedThroughIndex})",
                partitionId, purged, installedCount, reflectedThroughIndex);
    }

    /// <summary>
    /// Encodes the intents a backup's restore must know at the start of its replay: the live intents with their
    /// values, and the identities already settled (no settled-at position is recorded for them).
    /// </summary>
    internal static byte[] SerializeBackupFrontier(
        IEnumerable<PreparedIntent> live, IEnumerable<(HLCTimestamp TransactionId, long Epoch, string Key)> settled)
    {
        PreparedIntentSnapshotMessage message = new();
        foreach (PreparedIntent intent in live)
            message.Intents.Add(PrepareProtoOf(intent));

        foreach ((HLCTimestamp transactionId, long epoch, string key) in settled)
        {
            SettledTransactionLedgerEntryMessage entry = new();
            FillSettledEntry(entry, new SettledIdentityEntry(transactionId, epoch, key, HLCTimestamp.Zero));
            message.SettledTransactions.Add(entry);
        }

        return ReplicationSerializer.Serialize(message);
    }

    /// <summary>Decodes a frontier written by <see cref="SerializeBackupFrontier"/>, read to the end of
    /// <paramref name="payload"/>.</summary>
    internal static (List<PreparedIntent> Live, List<(HLCTimestamp TransactionId, long Epoch, string Key)> Settled) DeserializeBackupFrontier(Stream payload)
    {
        PreparedIntentSnapshotMessage message = PreparedIntentSnapshotMessage.Parser.ParseFrom(payload);

        List<PreparedIntent> live = new(message.Intents.Count);
        foreach (PreparedIntentCommandMessage entry in message.Intents)
            live.Add(IntentOf(entry));

        List<(HLCTimestamp TransactionId, long Epoch, string Key)> settled = new(message.SettledTransactions.Count);
        foreach (SettledIdentityEntry entry in SettledEntriesOf(message))
            settled.Add((entry.TransactionId, entry.Epoch, entry.Key));

        return (live, settled);
    }

    public static byte[] SerializeIntents(IEnumerable<PreparedIntent> intents)
    {
        PreparedIntentSnapshotMessage message = new();
        foreach (PreparedIntent intent in intents)
            message.Intents.Add(PrepareProtoOf(intent));

        return ReplicationSerializer.Serialize(message);
    }

    public static IReadOnlyList<PreparedIntent> DeserializeIntents(byte[] data)
    {
        PreparedIntentSnapshotMessage message = ReplicationSerializer.UnserializePreparedIntentSnapshotMessage(data);
        List<PreparedIntent> result = new(message.Intents.Count);
        foreach (PreparedIntentCommandMessage entry in message.Intents)
            result.Add(IntentOf(entry));

        return result;
    }

    /// <summary>Serializes one gathered page of a moving range's committed heads (the range transaction-state
    /// gather's wire form of the hand-off set).</summary>
    internal static byte[] SerializeCommittedHeads(IReadOnlyList<CommittedHeadLedgerEntry> heads)
    {
        CommittedHeadLedgerPageMessage message = new();

        foreach (CommittedHeadLedgerEntry head in heads)
        {
            message.Heads.Add(new CommittedHeadLedgerEntryMessage
            {
                Key = head.Key,
                Revision = head.Revision,
                State = (int)head.State,
                CommittedAtNode = head.CommittedAt.N,
                CommittedAtPhysical = head.CommittedAt.L,
                CommittedAtCounter = head.CommittedAt.C
            });
        }

        return message.ToByteArray();
    }

    /// <summary>Decodes a page written by <see cref="SerializeCommittedHeads"/>.</summary>
    internal static IReadOnlyList<CommittedHeadLedgerEntry> DeserializeCommittedHeads(byte[] data)
    {
        CommittedHeadLedgerPageMessage message = CommittedHeadLedgerPageMessage.Parser.ParseFrom(data);
        List<CommittedHeadLedgerEntry> result = new(message.Heads.Count);

        foreach (CommittedHeadLedgerEntryMessage entry in message.Heads)
        {
            result.Add(new CommittedHeadLedgerEntry(
                entry.Key, entry.Revision, (KeyValueState)entry.State,
                new HLCTimestamp(entry.CommittedAtNode, entry.CommittedAtPhysical, entry.CommittedAtCounter)));
        }

        return result;
    }
}
