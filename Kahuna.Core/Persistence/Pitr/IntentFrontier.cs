using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.Replication;
using Kahuna.Server.Replication.Protos;
using Kommander.Data;
using Kommander.WAL;

namespace Kahuna.Server.Persistence.Pitr;

/// <summary>
/// The prepared intents a full backup hands to the restore of its chain: the intents live at the end of each
/// partition's range, with their values, and the identities settled shortly before it.
///
/// <para><b>Why the restore needs it.</b> A durable transaction's committed value lives in its prepared intent: a
/// by-reference materialization record and a materializing settle carry no value. The restore starts from the
/// checkpoint and replays only the incrementals, which begin after the full's range. So a transaction prepared
/// inside the range and settled after it has its value in no replayed segment, and without this frontier the
/// restore of every chain built on the full aborts.</para>
///
/// <para><b>How it is captured.</b> The node's live intents are walked before the range ends are read (see
/// <see cref="LiveIntentWalk"/>), and each partition's WAL is folded up to its range end from the walk's applied
/// position (and from at least <see cref="SettledLookbackEntries"/> entries back). An intent live at the range end
/// was then either prepared after the walk's position, so the fold sees its prepare, or prepared at or before it
/// and still live during the walk, so the walk holds it — unless the range end is below the walk's position (a
/// coordinated cut) and the intent settled in between. That one case is caught by checking every entry after the
/// range end, up to the WAL end, against the frontier the way the restore will replay them: a materialization the
/// frontier cannot expand fails the backup closed instead of publishing a chain base no restore can use.</para>
/// </summary>
internal sealed class IntentFrontier
{
    /// <summary>The full backup's artifact holding the frontier. Absent when the frontier is empty, and in every
    /// full backup written before the frontier existed.</summary>
    internal const string ArtifactName = "intent_frontier.pb";

    /// <summary>
    /// How many entries before a range end the fold always covers, so a second copy of a settle or of a
    /// by-reference materialization replayed after the range end is recognized when the intent settled inside the
    /// range. The same bound as the restore's own memory of recently settled identities within a replay.
    /// </summary>
    internal const int SettledLookbackEntries = 65_536;

    private const int PageSize = 256;

    // How many unexpandable identities the refusal names.
    private const int ReportedMissingLimit = 8;

    public IReadOnlyList<PreparedIntent> Live { get; }

    public IReadOnlyList<PreparedIntentIdentity> Settled { get; }

    public bool IsEmpty => Live.Count == 0 && Settled.Count == 0;

    private IntentFrontier(IReadOnlyList<PreparedIntent> live, IReadOnlyList<PreparedIntentIdentity> settled)
    {
        Live = live;
        Settled = settled;
    }

    internal static IntentFrontier Empty { get; } = new([], []);

    /// <summary>
    /// Captures the frontier at each range's <see cref="PartitionBackupRange.ToIndex"/> and proves it expands every
    /// materialization the WAL holds after it.
    /// </summary>
    /// <param name="wal">The WAL the ranges were read from.</param>
    /// <param name="ranges">The full backup's per-partition ranges.</param>
    /// <param name="walk">The node's live intents, walked before the ranges were read; <c>null</c> when this node
    /// has no durable intent store, and the fold then starts at the compaction floor.</param>
    /// <param name="ct">Cancellation.</param>
    /// <exception cref="BackupDriverException">A materialization after a range end names an intent the frontier
    /// cannot expand (<see cref="BackupDriverException.CutUnverified"/>).</exception>
    internal static IntentFrontier Capture(
        IWAL wal, IReadOnlyList<PartitionBackupRange> ranges, LiveIntentWalk? walk, CancellationToken ct)
    {
        Dictionary<PreparedIntentIdentity, PreparedIntent> walked = [];
        if (walk is { } liveWalk)
        {
            foreach (PreparedIntent intent in liveWalk.Intents)
                walked[IdentityOf(intent)] = intent;
        }

        // The fold's view at the range ends: the intents prepared and not removed inside the folded windows, the
        // walked intents a folded entry removed, and every identity a folded entry removed.
        Dictionary<PreparedIntentIdentity, PreparedIntent> folded = [];
        HashSet<PreparedIntentIdentity> walkedButRemoved = [];
        HashSet<PreparedIntentIdentity> settled = [];

        foreach (PartitionBackupRange range in ranges)
        {
            long from = FoldStart(wal, range, walk);
            ForEachEntry(wal, range.PartitionId, from, range.ToIndex, log =>
            {
                if (log.LogType != ReplicationTypes.PreparedIntent || log.LogData is null || log.LogData.Length == 0)
                    return;

                foreach (PreparedIntentCommand command in PreparedIntentStore.DecodeDelta(log.LogData))
                {
                    switch (command)
                    {
                        case PrepareIntentCommand prepare:
                        {
                            PreparedIntentIdentity identity = IdentityOf(prepare.Intent);
                            folded[identity] = prepare.Intent;
                            walkedButRemoved.Remove(identity);
                            break;
                        }

                        case RemoveIntentCommand remove:
                        {
                            PreparedIntentIdentity identity = new(remove.TransactionId, remove.Epoch, remove.Key);
                            folded.Remove(identity);
                            if (walked.ContainsKey(identity))
                                walkedButRemoved.Add(identity);
                            settled.Add(identity);
                            break;
                        }
                    }
                }
            }, ct);
        }

        // A folded prepare is the copy the restore would have replayed; a walked intent fills in what the fold
        // started too late to see.
        Dictionary<PreparedIntentIdentity, PreparedIntent> live = new(folded);
        foreach ((PreparedIntentIdentity identity, PreparedIntent intent) in walked)
        {
            if (!walkedButRemoved.Contains(identity))
                live.TryAdd(identity, intent);
        }

        foreach (PreparedIntentIdentity identity in live.Keys)
            settled.Remove(identity);

        VerifyExpandsTail(wal, ranges, live, settled, ct);

        return new IntentFrontier([.. live.Values], [.. settled]);
    }

    /// <summary>
    /// The first index the fold reads: the entry after the walk's applied position (every intent prepared at or
    /// below it and still live is in the walk), lowered to the settled lookback, and never below the compaction
    /// floor. Without a walk the fold covers everything the WAL still holds.
    /// </summary>
    private static long FoldStart(IWAL wal, PartitionBackupRange range, LiveIntentWalk? walk)
    {
        long from = 1;
        if (walk is { } liveWalk)
        {
            long appliedThrough = liveWalk.AppliedThrough.TryGetValue(range.PartitionId, out long applied) ? applied : 0;
            from = Math.Min(appliedThrough + 1, range.ToIndex + 1 - SettledLookbackEntries);
        }

        long floor = wal.GetLastCheckpoint(range.PartitionId);
        return Math.Max(1, Math.Max(from, floor));
    }

    /// <summary>
    /// Replays each partition's entries after its range end, up to the WAL end, against the frontier exactly as
    /// the restore will, and fails when a materialization names an intent the frontier cannot expand. Entries not
    /// yet marked committed are checked too: a WAL marks an entry committed lazily, and an entry the store already
    /// applied must not escape the check because its mark is late.
    /// </summary>
    private static void VerifyExpandsTail(
        IWAL wal,
        IReadOnlyList<PartitionBackupRange> ranges,
        Dictionary<PreparedIntentIdentity, PreparedIntent> live,
        HashSet<PreparedIntentIdentity> settled,
        CancellationToken ct)
    {
        List<(int PartitionId, long LogIndex, PreparedIntentIdentity Identity)>? missing = null;

        foreach (PartitionBackupRange range in ranges)
        {
            int partitionId = range.PartitionId;
            long end = wal.GetMaxLog(partitionId);
            if (end <= range.ToIndex)
                continue;

            HashSet<PreparedIntentIdentity> preparedSince = [];
            HashSet<PreparedIntentIdentity> removedSince = [];
            HashSet<PreparedIntentIdentity> settledSince = [];

            bool IsLive(PreparedIntentIdentity identity) =>
                preparedSince.Contains(identity) || (live.ContainsKey(identity) && !removedSince.Contains(identity));

            void Missing(long logIndex, PreparedIntentIdentity identity) =>
                (missing ??= []).Add((partitionId, logIndex, identity));

            ForEachEntry(wal, partitionId, range.ToIndex + 1, end, log =>
            {
                if (log.LogData is null || log.LogData.Length == 0)
                    return;

                if (log.LogType == ReplicationTypes.KeyValues)
                {
                    KeyValueMessage message = ReplicationSerializer.UnserializeKeyValueMessage(log.LogData);
                    if (RestoreEngine.TryGetByReferenceIdentity(message, out PreparedIntentIdentity identity) && !IsLive(identity)
                        && !settledSince.Contains(identity) && !settled.Contains(identity))
                        Missing(log.Id, identity);
                    return;
                }

                if (log.LogType != ReplicationTypes.PreparedIntent)
                    return;

                foreach (PreparedIntentCommand command in PreparedIntentStore.DecodeDelta(log.LogData))
                {
                    switch (command)
                    {
                        case PrepareIntentCommand prepare:
                        {
                            PreparedIntentIdentity identity = IdentityOf(prepare.Intent);
                            preparedSince.Add(identity);
                            removedSince.Remove(identity);
                            break;
                        }

                        case ResolveIntentCommand { Commit: true, MaterializeOnResolve: true } resolve:
                        {
                            PreparedIntentIdentity identity = new(resolve.TransactionId, resolve.Epoch, resolve.Key);
                            if (!IsLive(identity) && !settledSince.Contains(identity) && !settled.Contains(identity))
                                Missing(log.Id, identity);
                            break;
                        }

                        case RemoveIntentCommand remove:
                        {
                            PreparedIntentIdentity identity = new(remove.TransactionId, remove.Epoch, remove.Key);
                            if (!IsLive(identity))
                                break;

                            preparedSince.Remove(identity);
                            removedSince.Add(identity);
                            settledSince.Add(identity);
                            break;
                        }
                    }
                }
            }, ct);
        }

        if (missing is null)
            return;

        string named = string.Join(", ", missing.Take(ReportedMissingLimit).Select(m =>
            $"partition {m.PartitionId} entry {m.LogIndex}: {m.Identity.TransactionId}/{m.Identity.Epoch} '{m.Identity.Key}'"));

        throw new BackupDriverException(
            $"{missing.Count} materialization(s) after the backup range name a prepared intent that was live at the range " +
            $"end but settled before this node's intents were walked, and whose prepare is outside the WAL the backup " +
            $"reads ({named}); a restore of a chain built on this backup could not expand them, so it was not published.")
        {
            CutUnverified = true
        };
    }

    /// <summary>Visits every entry of <paramref name="partitionId"/> in <c>[from, to]</c> that the log did not roll
    /// back, in index order.</summary>
    private static void ForEachEntry(IWAL wal, int partitionId, long from, long to, Action<RaftLog> visit, CancellationToken ct)
    {
        long cursor = from;
        while (cursor <= to)
        {
            ct.ThrowIfCancellationRequested();

            List<RaftLog> batch = wal.ReadLogsRange(partitionId, cursor, PageSize);
            if (batch.Count == 0)
                return;

            foreach (RaftLog log in batch)
            {
                if (log.Id < cursor || log.Id > to)
                    continue;

                if (log.Type is RaftLogType.RolledBack or RaftLogType.RolledBackCheckpoint)
                    continue;

                visit(log);
            }

            if (batch.Count < PageSize)
                return;

            cursor = batch[^1].Id + 1;
        }
    }

    internal static PreparedIntentIdentity IdentityOf(PreparedIntent intent) =>
        new(intent.TransactionId, intent.Epoch, intent.Key);

    internal byte[] Serialize() =>
        PreparedIntentStore.SerializeBackupFrontier(Live, Settled.Select(s => (s.TransactionId, s.Epoch, s.Key)));

    internal static IntentFrontier Deserialize(Stream payload)
    {
        (List<PreparedIntent> live, List<(Kommander.Time.HLCTimestamp TransactionId, long Epoch, string Key)> settled) =
            PreparedIntentStore.DeserializeBackupFrontier(payload);

        List<PreparedIntentIdentity> identities = new(settled.Count);
        foreach ((Kommander.Time.HLCTimestamp transactionId, long epoch, string key) in settled)
            identities.Add(new PreparedIntentIdentity(transactionId, epoch, key));

        return new IntentFrontier(live, identities);
    }
}
