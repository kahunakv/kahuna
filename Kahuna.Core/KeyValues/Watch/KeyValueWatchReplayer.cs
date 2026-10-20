using System.Diagnostics.Metrics;

using Kommander;
using Kommander.Data;
using Kommander.Time;
using Kommander.WAL;

using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.Persistence.Pitr;
using Kahuna.Server.Replication;
using Kahuna.Server.Replication.Protos;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues.Watch;

/// <summary>
/// Replays one partition's retained history into a watch that resumes from a cursor W, then hands the watch over to
/// the live stream with no gap.
///
/// <para><b>Where the replay starts.</b> With the commit admission window, every commit above W comes from an entry
/// whose time is above <c>W − MaxClockLeadMs</c>: a write outside a transaction is reported at the entry that admitted
/// it, and a durable transaction at its settle, which follows its prepare, whose time is above the commit timestamp
/// minus the bound. So the replay reads from the first committed entry at or after <c>W − MaxClockLeadMs</c>, and every
/// prepare a replayed event needs is inside the range.</para>
///
/// <para><b>Where it ends.</b> The watch registered before the replay started and holds back the partition's live
/// frames with their log index. The replay reads through <c>L1</c>, the highest entry whose apply had started when the
/// replay began: every entry above it applies after the registration and reaches the watch live; every entry at or
/// below it is in the log. The held-back frames at or below <c>L1</c> are dropped, and the rest follow the replay.</para>
///
/// <para><b>Same classification as live.</b> The replay reports what the live sources report: a write made outside a
/// transaction (not a range-move copy, not refused by the window), and a durable transaction's write when its pending
/// intent commits. It can report a repeat the live path's ledger would have suppressed; delivery is at-least-once and
/// the consumer drops a revision it already holds.</para>
///
/// <para><b>Never a silent gap.</b> The watch ends with <c>HistoryUnavailable</c> when the replay cannot prove it has
/// every entry it needs: the window is off, a snapshot install covers the start, the entry before the start was
/// compacted (entries the start needed may have gone with it), or an index between the start and <c>L1</c> is
/// missing.</para>
/// </summary>
internal sealed class KeyValueWatchReplayer
{
    private const int PageSize = 512;

    private static readonly Counter<long> Replays =
        KeyValueWatchMetrics.Meter.CreateCounter<long>(
            "kahuna.kv.watch.replays_total",
            description: "Watch resumes replayed per partition, by outcome (replayed, history_unavailable).");

    private readonly IRaft raft;

    private readonly Func<int, long> liveIndex;

    private readonly PartitionInstallBoundaries installBoundaries;

    private readonly bool windowOn;

    private readonly ILogger<IKahuna> logger;

    public KeyValueWatchReplayer(IRaft raft, Func<int, long> liveIndex, PartitionInstallBoundaries installBoundaries, bool windowOn, ILogger<IKahuna> logger)
    {
        this.raft = raft;
        this.liveIndex = liveIndex;
        this.installBoundaries = installBoundaries;
        this.windowOn = windowOn;
        this.logger = logger;
    }

    /// <summary>Starts the replay of <paramref name="partitionId"/> from <paramref name="from"/> into
    /// <paramref name="watch"/>, off the caller's thread. Call it after the watch is registered with the partition held
    /// back. The task ends when the replay hands over, fails, or the watch ends.</summary>
    public void Start(KeyValueWatch watch, int partitionId, HLCTimestamp from, bool reportsTransactionEnds) =>
        _ = Task.Run(() => RunAsync(watch, partitionId, from, reportsTransactionEnds));

    private async Task RunAsync(KeyValueWatch watch, int partitionId, HLCTimestamp from, bool reportsTransactionEnds)
    {
        try
        {
            if (!windowOn)
            {
                Unavailable(watch, partitionId, "the commit admission window is off");
                return;
            }

            // Read after the registration: see the type's remarks.
            long live = liveIndex(partitionId);

            IWAL wal = raft.WalAdapter;
            HLCTimestamp start = new(from.N, from.L - CommitAdmission.MaxClockLeadMs, from.C);
            long boundary = installBoundaries.Of(partitionId);

            long first = live <= 0 ? -1 : LogTimeIndex.FirstIndexAtOrAfter(wal, partitionId, start);

            if (first < 0 || first > live)
            {
                // No applied entry at or after the start: nothing above the cursor has committed here yet, provided the
                // log still reaches back past the start (the last applied entry is retained and older than it).
                if (live > 0 && (live <= boundary || ReadOne(wal, partitionId, live) is null))
                {
                    Unavailable(watch, partitionId, $"the entries before the start index were compacted (applied {live})");
                    return;
                }

                Replays.Add(1, new KeyValuePair<string, object?>("outcome", "replayed"));
                watch.CompleteReplay(partitionId, live);
                return;
            }

            // The search sees committed rows only, but an applied entry can still read Proposed (a new leader's
            // inherited entry before its re-commit is durable). Start before such rows: an entry older than the start
            // reports nothing above the cursor, so replaying it costs only the read.
            while (first > 1 && ReadOne(wal, partitionId, first - 1) is { Type: RaftLogType.Proposed } earlier && earlier.Time >= start)
                first--;

            if (first <= boundary)
            {
                Unavailable(watch, partitionId, $"a snapshot install covers the start index {first} (installed through {boundary})");
                return;
            }

            // The entry before the start must be retained: if it was compacted, entries the start needed may have
            // been compacted with it, and the binary search could only see what is left.
            if (first > 1 && ReadOne(wal, partitionId, first - 1) is null)
            {
                Unavailable(watch, partitionId, $"the entry before the start index {first} was compacted");
                return;
            }

            ReplayClassifier classifier = new(watch, partitionId, from, reportsTransactionEnds);

            long next = first;
            while (next <= live)
            {
                List<RaftLog> page = wal.ReadLogsRange(partitionId, next, PageSize);

                if (page.Count == 0)
                {
                    Unavailable(watch, partitionId, $"entry {next} is missing from the retained log");
                    return;
                }

                foreach (RaftLog log in page)
                {
                    if (log.Id > live)
                        break;

                    if (log.Id != next)
                    {
                        Unavailable(watch, partitionId, $"entry {next} is missing from the retained log (found {log.Id})");
                        return;
                    }

                    next++;

                    // An applied entry is committed. A row can still read Proposed when its re-commit is not durable yet
                    // (a new leader's inherited entry), so only a rolled-back row is skipped here.
                    if (log.Type is RaftLogType.RolledBack or RaftLogType.ProposedCheckpoint or RaftLogType.CommittedCheckpoint)
                        continue;

                    if (!await classifier.ReplayAsync(log).ConfigureAwait(false))
                        return;
                }
            }

            Replays.Add(1, new KeyValuePair<string, object?>("outcome", "replayed"));
            watch.CompleteReplay(partitionId, live);
        }
        catch (OperationCanceledException)
        {
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Watch replay of partition {PartitionId} failed", partitionId);
            Unavailable(watch, partitionId, "the replay failed");
        }
    }

    private static RaftLog? ReadOne(IWAL wal, int partitionId, long index)
    {
        List<RaftLog> rows = wal.ReadLogsRange(partitionId, index, 1);
        return rows.Count > 0 && rows[0].Id == index ? rows[0] : null;
    }

    private void Unavailable(KeyValueWatch watch, int partitionId, string reason)
    {
        Replays.Add(1, new KeyValuePair<string, object?>("outcome", "history_unavailable"));

        if (logger.IsEnabled(LogLevel.Information))
            logger.LogInformation("Watch resume on partition {PartitionId} answered HistoryUnavailable: {Reason}", partitionId, reason);

        watch.End(KeyValueWatchFrame.ForHistoryUnavailable(partitionId));
    }

    /// <summary>
    /// Turns replayed entries into the frames the live sources would have produced, tracking the intents the replay
    /// saw prepared. Every intent whose commit lies above the cursor was prepared inside the replayed range.
    /// </summary>
    private sealed class ReplayClassifier
    {
        private readonly KeyValueWatch watch;

        private readonly int partitionId;

        private readonly HLCTimestamp from;

        private readonly bool reportsTransactionEnds;

        // The intents prepared in the replayed range, by key.
        private readonly Dictionary<string, PreparedIntent> intents = new(StringComparer.Ordinal);

        // Pending intents per transaction attempt, and the attempts the watch received an event of.
        private readonly Dictionary<(HLCTimestamp TransactionId, long Epoch), int> pending = [];

        private readonly HashSet<(HLCTimestamp TransactionId, long Epoch)> reported = [];

        public ReplayClassifier(KeyValueWatch watch, int partitionId, HLCTimestamp from, bool reportsTransactionEnds)
        {
            this.watch = watch;
            this.partitionId = partitionId;
            this.from = from;
            this.reportsTransactionEnds = reportsTransactionEnds;
        }

        /// <summary>Replays one applied entry. False once the watch ended.</summary>
        public async ValueTask<bool> ReplayAsync(RaftLog log)
        {
            if (log.LogData is null || log.LogData.Length == 0)
                return true;

            if (log.LogType == ReplicationTypes.KeyValues)
                return await ReplayRecordAsync(log).ConfigureAwait(false);

            if (log.LogType == ReplicationTypes.PreparedIntent)
                return await ReplayDeltaAsync(log).ConfigureAwait(false);

            return true;
        }

        private async ValueTask<bool> ReplayRecordAsync(RaftLog log)
        {
            KeyValueMessage message = ReplicationSerializer.UnserializeKeyValueMessage(log.LogData);
            KeyValueRequestType type = KeyValueMessageDecoder.RecordType(message);

            if (KeyValueMessageDecoder.Classify(type) != KeyValueRecordKind.ValueMutation)
                return true;

            // A transaction's materialization repeats a decision the intent's settle reports; a range move's copy
            // repeats a mutation the source reported.
            if (message.TransactionIdNode != 0 || message.TransactionIdPhysical != 0 || message.TransactionIdCounter != 0 || message.StateTransferCopy)
                return true;

            HLCTimestamp lastModified = new(message.LastModifiedNode, message.LastModifiedPhysical, message.LastModifiedCounter);

            if (CommitAdmission.Judge(log.Time, lastModified, message.AdmissionLagMs) != CommitAdmissionVerdict.Admitted)
                return true;

            if (lastModified <= from || !watch.Scope.Matches(message.Key))
                return true;

            KeyValueWatchEventKind kind = type switch
            {
                KeyValueRequestType.TryDelete => KeyValueWatchEventKind.Deleted,
                KeyValueRequestType.TryExtend => KeyValueWatchEventKind.Extended,
                _ => KeyValueWatchEventKind.Set
            };

            byte[]? value = null;
            if (kind == KeyValueWatchEventKind.Set && !watch.KeysOnly)
                (_, value) = KeyValueMessageDecoder.Decode(message);

            KeyValueWatchEvent @event = new(
                message.Key,
                kind,
                value,
                message.Revision,
                new HLCTimestamp(message.ExpireNode, message.ExpirePhysical, message.ExpireCounter),
                lastModified,
                HLCTimestamp.Zero,
                partitionId);

            return await watch.DeliverReplayedAsync(KeyValueWatchFrame.ForEvent(@event), CancellationToken.None).ConfigureAwait(false);
        }

        private async ValueTask<bool> ReplayDeltaAsync(RaftLog log)
        {
            foreach (PreparedIntentCommand command in PreparedIntentStore.DecodeDelta(log.LogData!))
            {
                switch (command)
                {
                    case PrepareIntentCommand prepare:
                        Prepare(log, prepare);
                        break;

                    case ResolveIntentCommand resolve:
                        if (!await ResolveAsync(resolve).ConfigureAwait(false))
                            return false;
                        break;

                    case RemoveIntentCommand remove:
                        if (intents.TryGetValue(remove.Key, out PreparedIntent? removed)
                            && removed.TransactionId == remove.TransactionId && removed.Epoch == remove.Epoch)
                            intents.Remove(remove.Key);
                        break;
                }
            }

            return true;
        }

        private void Prepare(RaftLog log, PrepareIntentCommand prepare)
        {
            PreparedIntent intent = prepare.Intent;
            bool sameIdentity = intents.TryGetValue(intent.Key, out PreparedIntent? existing)
                && existing.TransactionId == intent.TransactionId && existing.Epoch == intent.Epoch;

            if (sameIdentity)
                return;

            // The live store installs nothing over another transaction's pending intent, nor a prepare the window
            // refused.
            if (existing is { IsPending: true })
                return;

            if (CommitAdmission.Judge(log.Time, intent.CommitTimestamp, prepare.AdmissionLagMs) != CommitAdmissionVerdict.Admitted)
                return;

            intents[intent.Key] = intent with { Resolution = PreparedIntentResolution.Pending };

            (HLCTimestamp, long) attempt = (intent.TransactionId, intent.Epoch);
            pending[attempt] = pending.GetValueOrDefault(attempt) + 1;
        }

        private async ValueTask<bool> ResolveAsync(ResolveIntentCommand resolve)
        {
            if (!intents.TryGetValue(resolve.Key, out PreparedIntent? intent)
                || intent.TransactionId != resolve.TransactionId || intent.Epoch != resolve.Epoch || !intent.IsPending)
                return true;

            intents[resolve.Key] = intent with { Resolution = resolve.Commit ? PreparedIntentResolution.Committed : PreparedIntentResolution.Aborted };

            (HLCTimestamp, long) attempt = (intent.TransactionId, intent.Epoch);
            int left = pending.GetValueOrDefault(attempt) - 1;
            if (left <= 0)
                pending.Remove(attempt);
            else
                pending[attempt] = left;

            if (!resolve.Commit)
                return true;

            if (intent.CommitTimestamp > from && watch.Scope.Matches(intent.Key))
            {
                KeyValueWatchEvent @event = new(
                    intent.Key,
                    intent.State == KeyValueState.Deleted ? KeyValueWatchEventKind.Deleted : KeyValueWatchEventKind.Set,
                    watch.KeysOnly || intent.State == KeyValueState.Deleted ? null : intent.Value,
                    intent.Revision,
                    intent.Expires,
                    intent.CommitTimestamp,
                    intent.TransactionId,
                    partitionId,
                    intent.Origin);

                if (!await watch.DeliverReplayedAsync(KeyValueWatchFrame.ForEvent(@event), CancellationToken.None).ConfigureAwait(false))
                    return false;

                reported.Add(attempt);
            }

            if (left <= 0 && reportsTransactionEnds && reported.Remove(attempt))
                return await watch.DeliverReplayedAsync(KeyValueWatchFrame.ForTransactionEnd(partitionId, intent.TransactionId), CancellationToken.None).ConfigureAwait(false);

            return true;
        }
    }
}
