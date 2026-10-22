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
/// minus the bound. So the replay reads from the first committed entry at or after <c>W − MaxClockLeadMs</c>
/// (<see cref="FirstReplayIndex"/>), and every prepare a replayed event needs is inside the range. An entry admitted
/// with no bound breaks that proof; the partition's unbounded commit ceiling (<see cref="PartitionHistoryBounds"/>)
/// says how high such commits reach, and a cursor below it is refused.</para>
///
/// <para><b>Where it ends.</b> The watch registered before the replay started and holds back the partition's live
/// items with their log index. The replay reads through <c>L1</c>, the highest entry whose apply had started when the
/// replay began: every entry above it applies after the registration and reaches the watch live; every entry at or
/// below it is in the log. The held-back items at or below <c>L1</c> are dropped, and the rest follow the replay.</para>
///
/// <para><b>Same classification as live.</b> A write made outside a transaction is reported as the replicator reports
/// it (not a transaction's materialization, not a range-move copy, not refused by the window), and held behind a key
/// whose intent is pending. Intent commands go through <see cref="PreparedIntentStateMachine"/>, the function the live
/// store applies, with the store's own checks before it (a re-driven prepare of a settled identity, the admission
/// verdict); a durable transaction's write is reported where the live store reports it, at the commit resolve that
/// takes a pending intent to committed, followed by its transaction end when it was the transaction's last pending
/// intent on the partition. The replay can report a repeat the live path's ledger would have suppressed; delivery is
/// at-least-once and the consumer drops a mutation it already holds.</para>
///
/// <para><b>Never a silent gap.</b> The watch ends with <c>HistoryUnavailable</c> when the replay cannot prove it has
/// every entry it needs: the window is off, the cursor is below the unbounded commit ceiling, a snapshot install
/// covers the start, the entry before the start was compacted (entries the start needed may have gone with it), or an
/// index between the start and <c>L1</c> is missing.</para>
///
/// <para><b>Lifetime.</b> A replay stops as soon as its watch ends (it checks between entries), and the node runs at
/// most its limit of replays at once: the service reserves a slot per resumed partition before it registers.</para>
/// </summary>
internal sealed class KeyValueWatchReplayer
{
    private const int PageSize = 512;

    private static readonly Counter<long> Replays =
        KeyValueWatchMetrics.Meter.CreateCounter<long>(
            "kahuna.kv.watch.replays_total",
            description: "Watch resumes replayed per partition, by outcome (replayed, history_unavailable, cancelled).");

    private readonly IRaft raft;

    private readonly Func<int, long> liveIndex;

    private readonly PartitionHistoryBounds historyBounds;

    private readonly bool windowOn;

    private readonly ILogger<IKahuna> logger;

    private readonly int maxConcurrentReplays;

    private int runningReplays;

    public KeyValueWatchReplayer(IRaft raft, Func<int, long> liveIndex, PartitionHistoryBounds historyBounds, bool windowOn, ILogger<IKahuna> logger, int maxConcurrentReplays = 0)
    {
        this.raft = raft;
        this.liveIndex = liveIndex;
        this.historyBounds = historyBounds;
        this.windowOn = windowOn;
        this.logger = logger;
        this.maxConcurrentReplays = maxConcurrentReplays;
    }

    /// <summary>Replays running on the node now.</summary>
    internal int RunningReplays => Volatile.Read(ref runningReplays);

    /// <summary>Reserves <paramref name="count"/> replay slots, or none when the node's limit would be passed. Each
    /// reserved slot is consumed by one <see cref="Start"/>, which frees it when its replay ends; a slot reserved and
    /// never started is handed back with <see cref="ReleaseSlots"/>.</summary>
    internal bool TryReserveSlots(int count)
    {
        int now = Interlocked.Add(ref runningReplays, count);
        if (maxConcurrentReplays <= 0 || now <= maxConcurrentReplays)
            return true;

        Interlocked.Add(ref runningReplays, -count);
        return false;
    }

    /// <summary>Hands back reserved slots that were never started.</summary>
    internal void ReleaseSlots(int count) => Interlocked.Add(ref runningReplays, -count);

    /// <summary>Starts the replay of <paramref name="partitionId"/> from <paramref name="from"/> into
    /// <paramref name="watch"/>, off the caller's thread, in a slot reserved with <see cref="TryReserveSlots"/>. Call it
    /// after the watch is registered with the partition held back. The task ends when the replay hands over, fails, or
    /// the watch ends.</summary>
    public Task Start(KeyValueWatch watch, int partitionId, HLCTimestamp from) =>
        Task.Run(() => RunAsync(watch, partitionId, from));

    /// <summary>
    /// The first index a replay from <paramref name="from"/> reads on <paramref name="partitionId"/>: the first
    /// committed entry at or after <c>from − MaxClockLeadMs</c>, moved back over the applied rows just before it that
    /// still read Proposed. Zero or less when no committed entry is that recent. The replay also reads the entry before
    /// this one, as proof that the search did not start after missing history, so a history pin keeps that one too.
    /// </summary>
    internal static long FirstReplayIndex(IWAL wal, int partitionId, HLCTimestamp from)
    {
        HLCTimestamp start = new(from.N, from.L - CommitAdmission.MaxClockLeadMs, from.C);

        long first = LogTimeIndex.FirstIndexAtOrAfter(wal, partitionId, start);
        if (first <= 0)
            return first;

        // The search sees committed rows only, but an applied entry can still read Proposed (a new leader's
        // inherited entry before its re-commit is durable). Start before such rows: an entry older than the start
        // reports nothing above the cursor, so replaying it costs only the read.
        while (first > 1 && ReadOne(wal, partitionId, first - 1) is { Type: RaftLogType.Proposed } earlier && earlier.Time >= start)
            first--;

        return first;
    }

    private async Task RunAsync(KeyValueWatch watch, int partitionId, HLCTimestamp from)
    {
        CancellationToken lifetime = watch.Lifetime;

        try
        {
            if (!windowOn)
            {
                Unavailable(watch, partitionId, "the commit admission window is off");
                return;
            }

            HLCTimestamp ceiling = historyBounds.UnboundedCeiling(partitionId);
            if (ceiling > from)
            {
                Unavailable(watch, partitionId, $"an entry admitted with no commit bound carries a commit at {ceiling}, above the cursor");
                return;
            }

            // Read after the registration: see the type's remarks.
            long live = liveIndex(partitionId);

            IWAL wal = raft.WalAdapter;
            long boundary = historyBounds.InstallBoundary(partitionId);

            long first = live <= 0 ? -1 : FirstReplayIndex(wal, partitionId, from);

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

            ReplayClassifier classifier = new(watch, partitionId, from, lifetime);

            long next = first;
            while (next <= live)
            {
                lifetime.ThrowIfCancellationRequested();

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

                    lifetime.ThrowIfCancellationRequested();

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
        catch (OperationCanceledException) when (lifetime.IsCancellationRequested)
        {
            Replays.Add(1, new KeyValuePair<string, object?>("outcome", "cancelled"));
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Watch replay of partition {PartitionId} failed", partitionId);
            Unavailable(watch, partitionId, "the replay failed");
        }
        finally
        {
            Interlocked.Decrement(ref runningReplays);
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
    /// Turns replayed entries into the frames the live sources would have produced, applying the intent commands of
    /// the replayed range to its own intent map with the live state machine. Every intent whose commit lies above the
    /// cursor was prepared inside the replayed range.
    /// </summary>
    private sealed class ReplayClassifier
    {
        private readonly KeyValueWatch watch;

        private readonly int partitionId;

        private readonly HLCTimestamp from;

        private readonly CancellationToken lifetime;

        // The intents of the replayed range, by key, as the live store would hold them.
        private readonly Dictionary<string, PreparedIntent> intents = new(StringComparer.Ordinal);

        // The identities whose resolved intent the replayed range removed: a later prepare of one is a re-driven
        // duplicate the live store rejects.
        private readonly HashSet<(HLCTimestamp TransactionId, long Epoch, string Key)> settled = [];

        // Pending intents per transaction attempt, kept as the live store's index keeps them.
        private readonly Dictionary<(HLCTimestamp TransactionId, long Epoch), int> pending = [];

        public ReplayClassifier(KeyValueWatch watch, int partitionId, HLCTimestamp from, CancellationToken lifetime)
        {
            this.watch = watch;
            this.partitionId = partitionId;
            this.from = from;
            this.lifetime = lifetime;
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

            // Held behind the key's pending intent, as the replicator holds it live.
            string? holdKey = intents.TryGetValue(message.Key, out PreparedIntent? intent) && intent.IsPending ? message.Key : null;

            return await watch.DeliverReplayedAsync(KeyValueWatchFrame.ForEvent(@event), holdKey, lifetime).ConfigureAwait(false);
        }

        private async ValueTask<bool> ReplayDeltaAsync(RaftLog log)
        {
            foreach (PreparedIntentCommand command in PreparedIntentStore.DecodeDelta(log.LogData!))
            {
                if (!await ApplyAsync(log, command).ConfigureAwait(false))
                    return false;
            }

            return true;
        }

        // The live store's apply, minus what only the live path needs (the ledger, the fence, persistence).
        private async ValueTask<bool> ApplyAsync(RaftLog log, PreparedIntentCommand command)
        {
            string key = command switch
            {
                PrepareIntentCommand prepare => prepare.Intent.Key,
                ResolveIntentCommand resolve => resolve.Key,
                RemoveIntentCommand remove => remove.Key,
                _ => ""
            };

            intents.TryGetValue(key, out PreparedIntent? existing);

            if (command is PrepareIntentCommand candidate)
            {
                if (settled.Contains((candidate.Intent.TransactionId, candidate.Intent.Epoch, key)))
                    return true;

                bool sameIdentity = existing is not null
                    && existing.TransactionId == candidate.Intent.TransactionId
                    && existing.Epoch == candidate.Intent.Epoch;

                if (candidate.AdmissionLagMs > 0
                    && !sameIdentity
                    && CommitAdmission.Judge(log.Time, candidate.Intent.CommitTimestamp, candidate.AdmissionLagMs) != CommitAdmissionVerdict.Admitted)
                    return true;
            }

            PreparedIntentApplyResult result = PreparedIntentStateMachine.Apply(existing, command);
            if (result.Outcome != TransactionApplyOutcome.Applied)
                return true;

            if (result.Intent is null)
                intents.Remove(key);
            else
                intents[key] = result.Intent;

            // The pending count of each attempt, moved as the live index moves it.
            bool drained = false;

            if (existing is { IsPending: true })
            {
                (HLCTimestamp, long) attempt = (existing.TransactionId, existing.Epoch);
                int left = pending.GetValueOrDefault(attempt) - 1;
                if (left <= 0)
                {
                    pending.Remove(attempt);
                    drained = true;
                }
                else
                {
                    pending[attempt] = left;
                }
            }

            if (result.Intent is { IsPending: true } installed)
            {
                (HLCTimestamp, long) attempt = (installed.TransactionId, installed.Epoch);
                pending[attempt] = pending.GetValueOrDefault(attempt) + 1;
            }

            if (command is RemoveIntentCommand && existing is { IsResolved: true })
                settled.Add((existing.TransactionId, existing.Epoch, key));

            if (existing is not { IsPending: true })
                return true;

            bool committedNow = command is ResolveIntentCommand { Commit: true };

            if (committedNow && existing.CommitTimestamp > from && watch.Scope.Matches(key))
            {
                KeyValueWatchEvent @event = new(
                    key,
                    existing.State == KeyValueState.Deleted ? KeyValueWatchEventKind.Deleted : KeyValueWatchEventKind.Set,
                    watch.KeysOnly || existing.State == KeyValueState.Deleted ? null : existing.Value,
                    existing.Revision,
                    existing.Expires,
                    existing.CommitTimestamp,
                    existing.TransactionId,
                    partitionId,
                    existing.Origin);

                if (!await watch.DeliverReplayedAsync(KeyValueWatchFrame.ForEvent(@event), null, lifetime).ConfigureAwait(false))
                    return false;
            }

            // The watch delivers an end only for a transaction it delivered an event of.
            if (committedNow && drained
                && !await watch.DeliverReplayedAsync(KeyValueWatchFrame.ForTransactionEnd(partitionId, existing.TransactionId), null, lifetime).ConfigureAwait(false))
                return false;

            // The intent left pending: what the replay held behind its key follows.
            watch.ReleaseReplayed(key);
            return true;
        }
    }
}
