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
            // Entries stream straight into the temp file through one reused PREPARE-kind message, producing
            // the same bytes as serializing a whole PreparedIntentSnapshotMessage: each entry is written
            // length-delimited under the repeated field's tag. Materializing one protobuf object per retained
            // intent plus one byte[] for the whole set made every checkpoint's allocation proportional to the
            // store size, which dominated the node's allocation profile whenever the retained set was large.
            //
            // The partition's committed-head ledger slice rides in the same file, captured BEFORE the intent
            // scan: a bundled commit replayed from the WAL after a restart is judged against the reloaded
            // ledger, and it is exact only if the ledger's position is no later than the intent set's (and
            // the record store's, which the checkpoint captures after this file) — every entry between the
            // ledger's position and the replayed commit is then re-applied on top of it in order.
            lock (fileLock)
            {
                string tmp = path + ".tmp";

                LedgerCapture? ledger = CaptureLedger(partitionId);

                // The position the file certifies as fully reflected: what the caller certified, never below
                // what the slice was installed with. Read BEFORE the walk (the caller's ceiling is raised only
                // after an apply returns, so every intent an entry at or below it prepared is in the map when
                // the walk starts) — the same cut a whole-partition export takes (see WritePartitionSection).
                long fullyReflected = Math.Max(certifiedThroughIndex, ledger?.FullyReflectedThroughIndex ?? 0);

                // Settled intents whose materialized row is still queued for the flush ride in the same file, in
                // their own field: a reader must never install them as live intents.
                List<PreparedIntent>? settledAwaitingFlushSlice = CollectSettledAwaitingFlush(partitionId, resolvePartition);

                using (FileStream file = new(tmp, FileMode.Create, FileAccess.Write, FileShare.None, 64 * 1024))
                using (CodedOutputStream output = new(file))
                {
                    PreparedIntentCommandMessage entry = new();

                    foreach (PreparedIntent intent in intents.Values)
                    {
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

        foreach (string path in files)
        {
            // Parsed straight off the file rather than read into one array first, so loading a file never holds
            // its bytes and its decoded message at once. The message itself is kept whole: the ledger slice it
            // carries is installed as one unit, and the file's size is bounded by in-flight intents and the
            // ledger's retention window rather than by throughput.
            PreparedIntentSnapshotMessage message;

            try
            {
                lock (fileLock)
                {
                    using FileStream file = new(path, FileMode.Open, FileAccess.Read, FileShare.Read, 64 * 1024);
                    message = PreparedIntentSnapshotMessage.Parser.ParseFrom(file);
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

            int filePartition = TryParsePartitionFromSnapshotPath(path, out int parsedPartition) ? parsedPartition : UnattributedPartition;

            foreach (PreparedIntentCommandMessage entry in message.Intents)
            {
                PreparedIntent loaded = IntentOf(entry);
                MergeLoad(loaded, filePartition);
                (loadedIntentPartitions ??= new(StringComparer.Ordinal))[loaded.Key] = filePartition;
            }

            // Retained settled intents reload into their own set — never into the live set, where a settled
            // intent would hold its key as a phantom. Nothing is queued yet, so no probe applies here; the
            // partition's restore completion re-checks them (see ReleaseSettledIntentsWithDurableRows).
            if (message.SettledIntentsAwaitingFlush.Count > 0)
            {
                lock (settledGate)
                {
                    foreach (PreparedIntentCommandMessage entry in message.SettledIntentsAwaitingFlush)
                        AddSettledAwaitingFlushLocked(IntentOf(entry));
                }
            }

            if (message.LedgerPresent)
                InstallLedger(message.LedgerPartitionId, LedgerEntriesOf(message), LedgerWatermarkOf(message), message.LedgerReflectedThroughIndex, SettledEntriesOf(message), message.LedgerFullyReflectedThroughIndex);
            else if (TryParsePartitionFromSnapshotPath(path, out int partitionId))
                ledgerMissingPartitions.Add(partitionId);
        }
    }

    // A per-partition snapshot file is named "{prefix}_p{partitionId}.snapshot".
    private static bool TryParsePartitionFromSnapshotPath(string path, out int partitionId)
    {
        partitionId = 0;
        string name = Path.GetFileNameWithoutExtension(path);
        int marker = name.LastIndexOf("_p", StringComparison.Ordinal);
        return marker >= 0 && int.TryParse(name.AsSpan(marker + 2), out partitionId);
    }

    /// <summary>Streams a partition's ledger slice (or an explicitly empty one) after the intents, in the
    /// message's field order so the bytes equal a whole-message serialization: each head under the repeated
    /// field's tag, the scalar trailer, each settled identity under its tag, then the fully reflected position.
    /// The writer always marks the ledger present so a reader can tell "empty ledger" from "written before the
    /// ledger existed".</summary>
    private static void WriteLedgerSection(CodedOutputStream output, int partitionId, LedgerCapture? ledger, long reflectedThroughIndex, long fullyReflectedThroughIndex)
    {
        HLCTimestamp watermark = HLCTimestamp.Zero;

        if (ledger is { } captured)
        {
            watermark = captured.Watermark;

            CommittedHeadLedgerEntryMessage head = new();
            foreach (KeyValuePair<string, CommittedHead> entry in captured.Heads)
            {
                FillLedgerEntry(head, entry.Key, entry.Value);
                output.WriteTag(PreparedIntentSnapshotMessage.CommittedHeadsFieldNumber, WireFormat.WireType.LengthDelimited);
                output.WriteMessage(head);
            }
        }

        long reflected = Math.Max(0, reflectedThroughIndex);

        PreparedIntentSnapshotMessage trailer = new()
        {
            LedgerPresent = true,
            LedgerPartitionId = partitionId,
            LedgerWatermarkNode = watermark.N,
            LedgerWatermarkPhysical = watermark.L,
            LedgerWatermarkCounter = watermark.C,
            LedgerReflectedThroughIndex = reflected
        };

        trailer.WriteTo(output);

        if (ledger is { } withSettled)
        {
            SettledTransactionLedgerEntryMessage settled = new();
            foreach (SettledIdentityEntry entry in withSettled.Settled)
            {
                FillSettledEntry(settled, entry);
                output.WriteTag(PreparedIntentSnapshotMessage.SettledTransactionsFieldNumber, WireFormat.WireType.LengthDelimited);
                output.WriteMessage(settled);
            }
        }

        // Written last, as the highest-numbered field; a zero is omitted exactly as the message serializer omits it.
        PreparedIntentSnapshotMessage positionTrailer = new()
        {
            LedgerFullyReflectedThroughIndex = Math.Clamp(fullyReflectedThroughIndex, 0, reflected)
        };

        positionTrailer.WriteTo(output);
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

    private static List<(string Key, long Revision, KeyValueState State, HLCTimestamp CommittedAt)> LedgerEntriesOf(PreparedIntentSnapshotMessage message)
    {
        List<(string, long, KeyValueState, HLCTimestamp)> entries = new(message.CommittedHeads.Count);
        foreach (CommittedHeadLedgerEntryMessage head in message.CommittedHeads)
            entries.Add((head.Key, head.Revision, (KeyValueState)head.State,
                new HLCTimestamp(head.CommittedAtNode, head.CommittedAtPhysical, head.CommittedAtCounter)));
        return entries;
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

    private static List<SettledIdentityEntry> SettledEntriesOf(PreparedIntentSnapshotMessage message)
    {
        List<SettledIdentityEntry> entries = new(message.SettledTransactions.Count);
        foreach (SettledTransactionLedgerEntryMessage m in message.SettledTransactions)
            entries.Add(new SettledIdentityEntry(
                new HLCTimestamp(m.TransactionIdNode, m.TransactionIdPhysical, m.TransactionIdCounter),
                m.Epoch,
                m.Key,
                new HLCTimestamp(m.SettledAtNode, m.SettledAtPhysical, m.SettledAtCounter)));
        return entries;
    }

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
    /// split/merge hands to the destination partition. Deliberately a consistent cut (<c>Values</c> copies the map
    /// under every bucket lock): the moved keys' log entries stay on the source partition, so the destination has
    /// no replay tail to converge a torn capture with. The whole-partition export does have that tail and streams
    /// its slice lock-free instead (<see cref="WritePartitionSection"/>); the scan captures read the ordered key
    /// index per key instead — see <see cref="SnapshotScanWindow"/>.</summary>
    public IReadOnlyList<PreparedIntent> SnapshotRange(string? startKey, string? endKey)
    {
        List<PreparedIntent> result = [];

        foreach (PreparedIntent intent in intents.Values)
        {
            if (startKey is not null && string.CompareOrdinal(intent.Key, startKey) < 0)
                continue;

            if (endKey is not null && string.CompareOrdinal(intent.Key, endKey) >= 0)
                continue;

            result.Add(intent);
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
        LedgerCapture? ledger = CaptureLedger(partitionId);

        // Read BEFORE the walk: every intent prepared at or below this position is in the map when the walk
        // starts, so the section holds it unless a removal during the walk took it out — and that removal is
        // replayed. Entries at or below it are therefore fully reflected on the receiver (see
        // IsFullyReflectedApply); the ones between it and the position read after the walk are the tear window,
        // where a missed intent must still install on replay.
        long fullyReflected = ReflectedPositionForExport(partitionId, ledger);

        int written = 0;

        using CodedOutputStream coded = new(output, leaveOpen: true);
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
        WriteLedgerSection(coded, partitionId, ledger, ReflectedPositionForExport(partitionId, ledger), fullyReflected);

        return written;
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
        foreach (PreparedIntent intent in incoming)
            MergeLoad(intent, partitionId);

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
        long FullyReflectedThroughIndex = 0);

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
        LedgerCapture? ledger = CaptureLedger(partitionId);

        PreparedIntentSnapshotMessage message = new()
        {
            LedgerPresent = true,
            LedgerPartitionId = partitionId
        };

        foreach (PreparedIntent intent in partitionIntents)
            message.Intents.Add(PrepareProtoOf(intent));

        if (ledger is { } captured)
        {
            message.LedgerWatermarkNode = captured.Watermark.N;
            message.LedgerWatermarkPhysical = captured.Watermark.L;
            message.LedgerWatermarkCounter = captured.Watermark.C;

            foreach (KeyValuePair<string, CommittedHead> entry in captured.Heads)
            {
                CommittedHeadLedgerEntryMessage head = new();
                FillLedgerEntry(head, entry.Key, entry.Value);
                message.CommittedHeads.Add(head);
            }

            foreach (SettledIdentityEntry entry in captured.Settled)
            {
                SettledTransactionLedgerEntryMessage settled = new();
                FillSettledEntry(settled, entry);
                message.SettledTransactions.Add(settled);
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
    /// by the ledgerless <see cref="SerializeIntents"/> of an older build).</summary>
    public static PartitionIntentSection DeserializePartitionIntents(byte[] data) =>
        PartitionIntentSectionOf(ReplicationSerializer.UnserializePreparedIntentSnapshotMessage(data));

    /// <summary>Decodes a section written by <see cref="SerializePartitionIntents"/> straight off
    /// <paramref name="payload"/> (read to its end), without first copying the section into one array.</summary>
    public static PartitionIntentSection DeserializePartitionIntents(Stream payload) =>
        PartitionIntentSectionOf(PreparedIntentSnapshotMessage.Parser.ParseFrom(payload));

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
            message.LedgerPresent ? message.LedgerFullyReflectedThroughIndex : 0);
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
            InstallLedger(partitionId, section.Ledger, section.LedgerWatermark, section.ReflectedThroughIndex, section.Settled, section.FullyReflectedThroughIndex);

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

        PartitionLedger? ledger = section.Ledger is null
            ? null
            : BuildLedger(section.Ledger, section.LedgerWatermark, section.ReflectedThroughIndex, section.Settled, section.FullyReflectedThroughIndex);

        int purged = 0;

        lock (applyGate)
        {
            List<string>? owned = null;
            foreach (KeyValuePair<string, PreparedIntent> kv in intents)
            {
                if (isOwned(kv.Key))
                    (owned ??= []).Add(kv.Key);
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

            if (ledger is null)
                ledgers.TryRemove(partitionId, out _);
            else
                InstallLedgerLocked(partitionId, ledger);

            foreach (PreparedIntent intent in section.Intents)
                MergeLoad(intent, partitionId);

            StampAllDirty();
        }

        SignalFenceWaiters();

        if (purged > 0 && logger is not null && logger.IsEnabled(LogLevel.Information))
            logger.LogInformation(
                "Snapshot install for partition {PartitionId} replaced {Purged} locally held intent(s) with the {Installed} the snapshot carries (reflected through log index {ReflectedThroughIndex})",
                partitionId, purged, section.Intents.Count, section.ReflectedThroughIndex);
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
