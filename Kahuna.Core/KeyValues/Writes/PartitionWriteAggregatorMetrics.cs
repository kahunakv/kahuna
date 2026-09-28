using Kahuna.Server.Replication;
using System.Diagnostics.Metrics;

namespace Kahuna.Server.KeyValues.Writes;

/// <summary>
/// <see cref="System.Diagnostics.Metrics"/> instruments for the partition write aggregator, recorded at the
/// lane's single-threaded ownership point. All tags are low-cardinality (a bounded reason/outcome string) —
/// never a key, partition id, request id, or transaction id. The primary effectiveness signal is
/// <c>dispatched entries / dispatched batches</c>: under a coalescing burst it approaches the batch cap, not one.
/// </summary>
internal static class PartitionWriteAggregatorMetrics
{
    internal static readonly Meter Meter = new("Kahuna", "1.0");

    /// <summary>Submissions admitted, tagged by admission class (<c>ordinary</c> / <c>terminal</c>) so the
    /// share of terminal work (decisions, settles, materializations) in the offered load is visible.</summary>
    internal static readonly Counter<long> AdmittedItems =
        Meter.CreateCounter<long>("kahuna.kv.write.admitted", description: "Submissions admitted to the aggregator, tagged by admission class.");

    internal static readonly Counter<long> Rejections =
        Meter.CreateCounter<long>("kahuna.kv.write.rejections", description: "Admissions/dispatches rejected or released, tagged by reason.");

    internal static readonly Counter<long> DispatchedBatches =
        Meter.CreateCounter<long>("kahuna.kv.write.batches", description: "Raft batches dispatched by the aggregator.");

    /// <summary>Log entries dispatched, tagged by the admission class of the submission that carried them.
    /// Summing over the tag gives the total; the split shows how much of each proposal is terminal work
    /// riding with (or ahead of) ordinary writes.</summary>
    internal static readonly Counter<long> DispatchedEntries =
        Meter.CreateCounter<long>("kahuna.kv.write.entries", description: "Log entries dispatched across all aggregator batches, tagged by admission class.");

    /// <summary>Log entries dispatched, tagged by the stage that produced the submission carrying them
    /// (<c>one_phase</c>, <c>record_init</c>, <c>prepare</c>, <c>re_prepare</c>, <c>decision</c>,
    /// <c>materialize</c>, <c>settle</c>, <c>other</c>). Divided by committed transactions, it says what each
    /// commit's log entries are — the bundle, the materialization records, the settle — instead of only how many
    /// are ordinary or terminal. The tag set is fixed.</summary>
    internal static readonly Counter<long> DispatchedStageEntries =
        Meter.CreateCounter<long>("kahuna.kv.write.stage_entries", description: "Log entries dispatched across all aggregator batches, tagged by the stage that produced the submission.");

    internal static readonly Counter<long> BatchOutcomes =
        Meter.CreateCounter<long>("kahuna.kv.write.outcomes", description: "Batch outcomes tagged success/transient/permanent.");

    internal static readonly Histogram<int> BatchItemCount =
        Meter.CreateHistogram<int>("kahuna.kv.write.batch_items", unit: "{entries}", description: "Entries per dispatched batch.");

    internal static readonly Histogram<long> BatchBytes =
        Meter.CreateHistogram<long>("kahuna.kv.write.batch_bytes", unit: "By", description: "Serialized bytes per dispatched batch.");

    /// <summary>Age of the oldest item in a dispatched batch, tagged by that item's admission class. One
    /// sample per batch: this is the batch's head-of-line wait, not every submission's delay — see
    /// <see cref="SubmissionQueueDelayMs"/> for the per-submission distribution.</summary>
    internal static readonly Histogram<long> QueueAgeMs =
        Meter.CreateHistogram<long>("kahuna.kv.write.queue_age", unit: "ms", description: "Age of the oldest item in a dispatched batch, tagged by its admission class.");

    /// <summary>Per-submission time from admission to dispatch, tagged by admission class, by the log type of
    /// the submission's first entry (<c>kv</c>, <c>record</c>, <c>intent</c>, <c>other</c>), and by the stage
    /// that produced the submission (<c>one_phase</c>, <c>record_init</c>, <c>prepare</c>, <c>re_prepare</c>,
    /// <c>decision</c>, <c>materialize</c>, <c>settle</c>, <c>other</c>). One sample per submission, so a
    /// decision that waited behind a materialization window shows up on its own series even when it was not the
    /// oldest item of its batch. The stage tag stays off <see cref="RaftDurationMs"/> and
    /// <see cref="CompletionDelayMs"/> deliberately: those record once per batch, and a batch mixes stages.</summary>
    internal static readonly Histogram<long> SubmissionQueueDelayMs =
        Meter.CreateHistogram<long>("kahuna.kv.write.submission_queue_delay", unit: "ms", description: "Per-submission admission-to-dispatch delay, tagged by admission class, log type, and producing stage.");

    private static readonly KeyValuePair<string, object?> TypeKv = new("type", "kv");
    private static readonly KeyValuePair<string, object?> TypeRecord = new("type", "record");
    private static readonly KeyValuePair<string, object?> TypeIntent = new("type", "intent");
    private static readonly KeyValuePair<string, object?> TypeOther = new("type", "other");

    private static KeyValuePair<string, object?> TypeTag(string logType) =>
        logType == ReplicationTypes.KeyValues ? TypeKv
        : logType == ReplicationTypes.TransactionRecord ? TypeRecord
        : logType == ReplicationTypes.PreparedIntent ? TypeIntent
        : TypeOther;

    private static readonly KeyValuePair<string, object?> StageOnePhase = new("stage", "one_phase");
    private static readonly KeyValuePair<string, object?> StageRecordInit = new("stage", "record_init");
    private static readonly KeyValuePair<string, object?> StagePrepare = new("stage", "prepare");
    private static readonly KeyValuePair<string, object?> StageRePrepare = new("stage", "re_prepare");
    private static readonly KeyValuePair<string, object?> StageDecision = new("stage", "decision");
    private static readonly KeyValuePair<string, object?> StageMaterialize = new("stage", "materialize");
    private static readonly KeyValuePair<string, object?> StageSettle = new("stage", "settle");
    private static readonly KeyValuePair<string, object?> StageOther = new("stage", "other");

    private static KeyValuePair<string, object?> StageTag(WriteSubmissionStage stage) => stage switch
    {
        WriteSubmissionStage.OnePhase => StageOnePhase,
        WriteSubmissionStage.RecordInit => StageRecordInit,
        WriteSubmissionStage.Prepare => StagePrepare,
        WriteSubmissionStage.RePrepare => StageRePrepare,
        WriteSubmissionStage.Decision => StageDecision,
        WriteSubmissionStage.Materialize => StageMaterialize,
        WriteSubmissionStage.Settle => StageSettle,
        _ => StageOther
    };

    /// <summary>Duration of the detached Raft round trip only. It stops when the executor returns, before the
    /// completion message reaches the lane mailbox and the submissions are applied and completed — that tail
    /// is <see cref="CompletionDelayMs"/>.</summary>
    internal static readonly Histogram<double> RaftDurationMs =
        Meter.CreateHistogram<double>("kahuna.kv.write.raft_duration", unit: "ms", description: "Aggregator Raft-call duration.");

    /// <summary>Time from the Raft round trip returning to the end of the lane's completion turn: mailbox
    /// wait plus the ordered apply/complete of every submission in the batch. A large value with a small
    /// <see cref="RaftDurationMs"/> means the lane mailbox or the producers' completion adapters, not Raft,
    /// hold the batch's callers.</summary>
    internal static readonly Histogram<double> CompletionDelayMs =
        Meter.CreateHistogram<double>("kahuna.kv.write.completion_delay", unit: "ms", description: "Time from Raft return to the end of the lane's batch-completion turn.");

    /// <summary>Submissions per dispatched batch — the fill measured against the item cap
    /// (<c>MaxBatchItems</c> counts submissions, while <see cref="BatchItemCount"/> counts their entries).</summary>
    internal static readonly Histogram<int> BatchSubmissionCount =
        Meter.CreateHistogram<int>("kahuna.kv.write.batch_submissions", unit: "{submissions}", description: "Submissions per dispatched batch.");

    /// <summary>One partition's dispatch-to-dispatch cycle split into consecutive stages, tagged by
    /// <c>stage</c>: <c>raft</c>, <c>completion_mailbox</c>, <c>completion_turn</c>, <c>hold</c>,
    /// <c>arrival_wait</c>, <c>wake_late</c>, <c>wake_mailbox</c>, <c>dispatch</c> — whose per-cycle values sum to
    /// the <c>cycle</c> series recorded alongside. One sample per stage per closed cycle; a cycle whose batch
    /// overlapped a later dispatch, or whose partition went idle between the two, is not recorded. The tag set
    /// is fixed.</summary>
    internal static readonly Histogram<double> CycleStageMs =
        Meter.CreateHistogram<double>("kahuna.kv.write.cycle_stage", unit: "ms", description: "Aggregator dispatch-to-dispatch cycle per stage, tagged by stage; the stages sum to the cycle series.");

    /// <summary>Closed cycles by what started the dispatching turn: <c>completion</c> (re-dispatch inside the
    /// completion turn), <c>wake</c> (a timer), <c>submit</c> (an arrival).</summary>
    internal static readonly Counter<long> CycleTriggers =
        Meter.CreateCounter<long>("kahuna.kv.write.cycle_trigger", description: "Closed aggregator cycles, tagged by the kind of turn that dispatched the next batch.");

    /// <summary>How late each aggregator wake fired against the delay it asked for, tagged by <c>timer</c>
    /// (<c>delay</c> for the timer queue, <c>precise</c> for the spin-tailed wait) and by <c>deadline</c>
    /// (<c>flush</c> for a linger or post-completion-hold wake, <c>age</c> for a queue-age release wake). A
    /// wake with no delay left (its deadline already passed when armed) is not recorded.</summary>
    internal static readonly Histogram<double> WakeLatenessMs =
        Meter.CreateHistogram<double>("kahuna.kv.write.wake_lateness", unit: "ms", description: "Aggregator wake fire time minus its requested delay, tagged by timer kind.");

    private static readonly KeyValuePair<string, object?> StageRaft = new("stage", "raft");
    private static readonly KeyValuePair<string, object?> StageCompletionMailbox = new("stage", "completion_mailbox");
    private static readonly KeyValuePair<string, object?> StageCompletionTurn = new("stage", "completion_turn");
    private static readonly KeyValuePair<string, object?> StageHold = new("stage", "hold");
    private static readonly KeyValuePair<string, object?> StageArrivalWait = new("stage", "arrival_wait");
    private static readonly KeyValuePair<string, object?> StageWakeLate = new("stage", "wake_late");
    private static readonly KeyValuePair<string, object?> StageWakeMailbox = new("stage", "wake_mailbox");
    private static readonly KeyValuePair<string, object?> StageDispatch = new("stage", "dispatch");
    private static readonly KeyValuePair<string, object?> StageCycle = new("stage", "cycle");

    private static readonly KeyValuePair<string, object?> TriggerCompletion = new("trigger", "completion");
    private static readonly KeyValuePair<string, object?> TriggerWake = new("trigger", "wake");
    private static readonly KeyValuePair<string, object?> TriggerSubmit = new("trigger", "submit");

    private static readonly KeyValuePair<string, object?> TimerDelay = new("timer", "delay");
    private static readonly KeyValuePair<string, object?> TimerPrecise = new("timer", "precise");
    private static readonly KeyValuePair<string, object?> DeadlineFlush = new("deadline", "flush");
    private static readonly KeyValuePair<string, object?> DeadlineAge = new("deadline", "age");

    internal static void CycleClosed(in PartitionWriteCycleStages stages)
    {
        CycleStageMs.Record(stages.Raft, StageRaft);
        CycleStageMs.Record(stages.CompletionMailbox, StageCompletionMailbox);
        CycleStageMs.Record(stages.CompletionTurn, StageCompletionTurn);
        CycleStageMs.Record(stages.Hold, StageHold);
        CycleStageMs.Record(stages.ArrivalWait, StageArrivalWait);
        CycleStageMs.Record(stages.WakeLate, StageWakeLate);
        CycleStageMs.Record(stages.WakeMailbox, StageWakeMailbox);
        CycleStageMs.Record(stages.Dispatch, StageDispatch);
        CycleStageMs.Record(stages.Cycle, StageCycle);

        CycleTriggers.Add(1, stages.Trigger switch
        {
            PartitionWriteDispatchTrigger.Completion => TriggerCompletion,
            PartitionWriteDispatchTrigger.Wake => TriggerWake,
            _ => TriggerSubmit
        });
    }

    internal static void WakeFired(double latenessMs, bool precise, bool flush) =>
        WakeLatenessMs.Record(latenessMs, precise ? TimerPrecise : TimerDelay, flush ? DeadlineFlush : DeadlineAge);

    private static readonly KeyValuePair<string, object?> ClassOrdinary = new("class", "ordinary");
    private static readonly KeyValuePair<string, object?> ClassTerminal = new("class", "terminal");

    private static readonly KeyValuePair<string, object?> ReasonQueueFull = new("reason", "queue_full");
    private static readonly KeyValuePair<string, object?> ReasonOversized = new("reason", "oversized");
    private static readonly KeyValuePair<string, object?> ReasonInboxFull = new("reason", "inbox_full");
    private static readonly KeyValuePair<string, object?> ReasonStopping = new("reason", "stopping");
    private static readonly KeyValuePair<string, object?> ReasonFenceStale = new("reason", "fence_stale");
    private static readonly KeyValuePair<string, object?> ReasonQueueExpired = new("reason", "queue_expired");
    private static readonly KeyValuePair<string, object?> ReasonUnflushedBacklog = new("reason", "unflushed_backlog");
    private static readonly KeyValuePair<string, object?> OutcomeSuccess = new("outcome", "success");
    private static readonly KeyValuePair<string, object?> OutcomeTransient = new("outcome", "transient");
    private static readonly KeyValuePair<string, object?> OutcomePermanent = new("outcome", "permanent");

    internal static void RejectedQueueFull() => Rejections.Add(1, ReasonQueueFull);
    internal static void RejectedOversized() => Rejections.Add(1, ReasonOversized);
    internal static void RejectedInboxFull() => Rejections.Add(1, ReasonInboxFull);
    internal static void RejectedStopping() => Rejections.Add(1, ReasonStopping);
    internal static void RejectedUnflushedBacklog() => Rejections.Add(1, ReasonUnflushedBacklog);
    internal static void ReleasedFenceStale() => Rejections.Add(1, ReasonFenceStale);
    internal static void ReleasedQueueExpired() => Rejections.Add(1, ReasonQueueExpired);

    private static KeyValuePair<string, object?> ClassTag(WriteAdmissionClass cls) =>
        cls == WriteAdmissionClass.Terminal ? ClassTerminal : ClassOrdinary;

    internal static void Admitted(WriteAdmissionClass cls) => AdmittedItems.Add(1, ClassTag(cls));

    /// <summary>One submission selected into a batch: its admission-to-dispatch delay, by class, log type, and
    /// the stage the submission carried from its creation site.</summary>
    internal static void SubmissionDispatched(long queueDelayMs, WriteAdmissionClass cls, string firstEntryLogType, WriteSubmissionStage stage, int entries)
    {
        KeyValuePair<string, object?> stageTag = StageTag(stage);
        SubmissionQueueDelayMs.Record(queueDelayMs, ClassTag(cls), TypeTag(firstEntryLogType), stageTag);
        if (entries > 0)
            DispatchedStageEntries.Add(entries, stageTag);
    }

    internal static void BatchDispatched(int submissions, int ordinaryEntries, int terminalEntries, long bytes, long oldestAgeMs, WriteAdmissionClass oldestClass)
    {
        int entries = ordinaryEntries + terminalEntries;
        DispatchedBatches.Add(1);
        BatchSubmissionCount.Record(submissions);
        if (ordinaryEntries > 0)
            DispatchedEntries.Add(ordinaryEntries, ClassOrdinary);
        if (terminalEntries > 0)
            DispatchedEntries.Add(terminalEntries, ClassTerminal);
        BatchItemCount.Record(entries);
        BatchBytes.Record(bytes);
        QueueAgeMs.Record(oldestAgeMs, ClassTag(oldestClass));
    }

    internal static void BatchSettled(bool success, bool transient, double durationMs)
    {
        RaftDurationMs.Record(durationMs);
        BatchOutcomes.Add(1, success ? OutcomeSuccess : transient ? OutcomeTransient : OutcomePermanent);
    }

    /// <summary>Recorded once per batch at the end of the lane's completion turn.</summary>
    internal static void BatchCompleted(double completionDelayMs) => CompletionDelayMs.Record(completionDelayMs);

    /// <summary>
    /// Creates the observable gauges for a facade's admission state on a fresh, <b>instance-owned</b>
    /// <see cref="Meter"/> and returns it. The caller must dispose the returned meter when the aggregator
    /// stops — the gauge callbacks capture <paramref name="admission"/>, so a static registration would keep
    /// every disposed node's registry reachable and accumulate duplicate instruments across construction. This
    /// mirrors the instance-meter ownership of <c>SnapshotFloorStore</c>. Counters/histograms above stay on the
    /// shared static meter: they record by tag and capture no per-instance state, so they do not leak.
    /// </summary>
    internal static Meter RegisterGauges(PartitionAdmissionRegistry admission)
    {
        Meter gaugeMeter = new("Kahuna", "1.0");
        gaugeMeter.CreateObservableGauge("kahuna.kv.write.queued_items", admission.TotalReservedItems, description: "Items admitted but not yet completed, across partitions.");
        gaugeMeter.CreateObservableGauge("kahuna.kv.write.queued_bytes", admission.TotalReservedBytes, unit: "By", description: "Serialized bytes admitted but not yet completed.");
        gaugeMeter.CreateObservableGauge("kahuna.kv.write.in_flight_partitions", () => (long)admission.InFlightBatches, description: "Batches awaiting their Raft result across all partitions; equals partitions-with-a-batch-in-flight at the default one-batch-per-partition cap.");
        return gaugeMeter;
    }
}
