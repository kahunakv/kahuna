namespace Kahuna.Server.Replication;

/// <summary>What one reader of the log does with entries of one type.</summary>
internal enum LogTypeUse
{
    /// <summary>The reader has no decision for the type. A test fails for any type of <see cref="ReplicationTypes"/>
    /// that a reader answers with this.</summary>
    Unknown,

    /// <summary>The reader reads entries of the type.</summary>
    Read,

    /// <summary>The reader skips entries of the type on purpose: they carry nothing it follows.</summary>
    Skipped
}

/// <summary>
/// The decision of each reader of a partition log, other than the apply router, for each log type: whether it reads
/// entries of the type or skips them on purpose. A reader that does not know a new type skips its entries with no
/// error, and for a durable type that is a lost event, a restore that stops, or a completion that answers before the
/// apply. Each reader below has the switch of its own code, and a test walks every constant of
/// <see cref="ReplicationTypes"/> and fails for a type that a reader answers with <see cref="LogTypeUse.Unknown"/>, as
/// the test of <see cref="ReplicationLogRouter"/> does for the router. Adding a type means deciding it here for each
/// reader, and changing the reader where it reads the type.
/// </summary>
internal static class ReplicationLogReaders
{
    /// <summary>The names of the readers, for the test.</summary>
    internal static readonly IReadOnlyList<(string Name, Func<string, LogTypeUse> UseOf)> All =
    [
        ("ordered_apply_awaiter", OrderedApplyAwaiter),
        ("submission_keys", SubmissionKeys),
        ("watch_replayer", WatchReplayer),
        ("restore_engine", RestoreEngine),
        ("intent_frontier", IntentFrontier),
        ("backup_commit_ceiling", BackupCommitCeiling),
        ("write_metrics", WriteMetrics),
        ("apply_totals", ApplyTotals)
    ];

    /// <summary><c>DurableOrderedApplyAwaiter</c>: waits for the ordered apply of the record, intent and bundle
    /// entries of a committed durable submission.</summary>
    internal static LogTypeUse OrderedApplyAwaiter(string logType) => logType switch
    {
        ReplicationTypes.TransactionRecord or ReplicationTypes.PreparedIntent or ReplicationTypes.OnePhaseBundle => LogTypeUse.Read,
        ReplicationTypes.Locks or ReplicationTypes.KeyValues or ReplicationTypes.RangeMap or ReplicationTypes.SnapshotFloor
            or ReplicationTypes.CoordinatorDecision or ReplicationTypes.CompletionReceipt or ReplicationTypes.ProgressTick
            or ReplicationTypes.HistoryHold => LogTypeUse.Skipped,
        _ => LogTypeUse.Unknown
    };

    /// <summary><c>DurableProposalSubmission.KeysChangedBy</c>: names the keys of the intent and bundle entries. A
    /// record, a receipt and a progress tick name none; any other entry does not tell its keys, so every operation of
    /// the partition waits for its delivery.</summary>
    internal static LogTypeUse SubmissionKeys(string logType) => logType switch
    {
        ReplicationTypes.PreparedIntent or ReplicationTypes.OnePhaseBundle => LogTypeUse.Read,
        ReplicationTypes.TransactionRecord or ReplicationTypes.CompletionReceipt or ReplicationTypes.ProgressTick
            or ReplicationTypes.Locks or ReplicationTypes.KeyValues or ReplicationTypes.RangeMap or ReplicationTypes.SnapshotFloor
            or ReplicationTypes.CoordinatorDecision or ReplicationTypes.HistoryHold => LogTypeUse.Skipped,
        _ => LogTypeUse.Unknown
    };

    /// <summary><c>KeyValueWatchReplayer</c>: replays the key-value records and the prepared intents of an intent or
    /// bundle entry.</summary>
    internal static LogTypeUse WatchReplayer(string logType) => logType switch
    {
        ReplicationTypes.KeyValues or ReplicationTypes.PreparedIntent or ReplicationTypes.OnePhaseBundle => LogTypeUse.Read,
        ReplicationTypes.Locks or ReplicationTypes.RangeMap or ReplicationTypes.SnapshotFloor or ReplicationTypes.CoordinatorDecision
            or ReplicationTypes.CompletionReceipt or ReplicationTypes.TransactionRecord or ReplicationTypes.ProgressTick
            or ReplicationTypes.HistoryHold => LogTypeUse.Skipped,
        _ => LogTypeUse.Unknown
    };

    /// <summary><c>RestoreEngine</c>: restores the key-value records and follows the prepared intents of an intent or
    /// bundle entry, from which a materializing settle installs.</summary>
    internal static LogTypeUse RestoreEngine(string logType) => logType switch
    {
        ReplicationTypes.KeyValues or ReplicationTypes.PreparedIntent or ReplicationTypes.OnePhaseBundle => LogTypeUse.Read,
        ReplicationTypes.Locks or ReplicationTypes.RangeMap or ReplicationTypes.SnapshotFloor or ReplicationTypes.CoordinatorDecision
            or ReplicationTypes.CompletionReceipt or ReplicationTypes.TransactionRecord or ReplicationTypes.ProgressTick
            or ReplicationTypes.HistoryHold => LogTypeUse.Skipped,
        _ => LogTypeUse.Unknown
    };

    /// <summary><c>IntentFrontier</c>: folds the prepared intents of an intent or bundle entry, and checks the
    /// by-reference records of the tail.</summary>
    internal static LogTypeUse IntentFrontier(string logType) => logType switch
    {
        ReplicationTypes.KeyValues or ReplicationTypes.PreparedIntent or ReplicationTypes.OnePhaseBundle => LogTypeUse.Read,
        ReplicationTypes.Locks or ReplicationTypes.RangeMap or ReplicationTypes.SnapshotFloor or ReplicationTypes.CoordinatorDecision
            or ReplicationTypes.CompletionReceipt or ReplicationTypes.TransactionRecord or ReplicationTypes.ProgressTick
            or ReplicationTypes.HistoryHold => LogTypeUse.Skipped,
        _ => LogTypeUse.Unknown
    };

    /// <summary><c>BackupDriver.MaxCommittedKeyValueCommitHlc</c>: reads the commit timestamps of the key-value records
    /// and of the materializing settles. A bundle entry installs no row, so it carries no commit to count.</summary>
    internal static LogTypeUse BackupCommitCeiling(string logType) => logType switch
    {
        ReplicationTypes.KeyValues or ReplicationTypes.PreparedIntent => LogTypeUse.Read,
        ReplicationTypes.OnePhaseBundle or ReplicationTypes.Locks or ReplicationTypes.RangeMap or ReplicationTypes.SnapshotFloor
            or ReplicationTypes.CoordinatorDecision or ReplicationTypes.CompletionReceipt or ReplicationTypes.TransactionRecord
            or ReplicationTypes.ProgressTick or ReplicationTypes.HistoryHold => LogTypeUse.Skipped,
        _ => LogTypeUse.Unknown
    };

    /// <summary><c>PartitionWriteAggregatorMetrics</c>: the <c>type</c> tag of a submission. The key-value, record,
    /// intent and bundle entries have their own value; the others count as <c>other</c>.</summary>
    internal static LogTypeUse WriteMetrics(string logType) => logType switch
    {
        ReplicationTypes.KeyValues or ReplicationTypes.TransactionRecord or ReplicationTypes.PreparedIntent
            or ReplicationTypes.OnePhaseBundle => LogTypeUse.Read,
        ReplicationTypes.Locks or ReplicationTypes.RangeMap or ReplicationTypes.SnapshotFloor or ReplicationTypes.CoordinatorDecision
            or ReplicationTypes.CompletionReceipt or ReplicationTypes.ProgressTick or ReplicationTypes.HistoryHold => LogTypeUse.Skipped,
        _ => LogTypeUse.Unknown
    };

    /// <summary><c>KeyValueApplyTotals</c>: the <c>type</c> tag of an applied entry. The key-value, record, intent,
    /// bundle and receipt entries have their own value; the others count as <c>other</c>.</summary>
    internal static LogTypeUse ApplyTotals(string logType) => logType switch
    {
        ReplicationTypes.KeyValues or ReplicationTypes.TransactionRecord or ReplicationTypes.PreparedIntent
            or ReplicationTypes.OnePhaseBundle or ReplicationTypes.CompletionReceipt => LogTypeUse.Read,
        ReplicationTypes.Locks or ReplicationTypes.RangeMap or ReplicationTypes.SnapshotFloor or ReplicationTypes.CoordinatorDecision
            or ReplicationTypes.ProgressTick or ReplicationTypes.HistoryHold => LogTypeUse.Skipped,
        _ => LogTypeUse.Unknown
    };
}
