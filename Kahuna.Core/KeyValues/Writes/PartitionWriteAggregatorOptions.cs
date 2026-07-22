namespace Kahuna.Server.KeyValues.Writes;

/// <summary>
/// Tuning for the partition write aggregator. Defaults are conservative starting points; the hosting
/// surfaces and validation that expose these are added with the configuration task. Linger 0 means an idle
/// partition dispatches immediately and only batches work that accumulates behind an in-flight call.
/// </summary>
internal sealed record PartitionWriteAggregatorOptions
{
    public int LingerMs { get; init; } = 1;

    public int MaxBatchItems { get; init; } = 512;

    public int MaxBatchBytes { get; init; } = 4 * 1024 * 1024;

    public int MaxQueuedItemsPerPartition { get; init; } = 8192;

    public long MaxQueuedBytesPerPartition { get; init; } = 32L * 1024 * 1024;

    /// <summary>Extra per-partition item headroom above <see cref="MaxQueuedItemsPerPartition"/> that only a
    /// <see cref="WriteAdmissionClass.Terminal"/> submission may consume, so a partition saturated with ordinary
    /// writes still admits the decision/settle that finishes an already-prepared transaction on that partition.</summary>
    public int TerminalReserveItemsPerPartition { get; init; } = 256;

    /// <summary>Extra per-partition byte headroom above <see cref="MaxQueuedBytesPerPartition"/> reserved for
    /// <see cref="WriteAdmissionClass.Terminal"/> submissions.</summary>
    public long TerminalReserveBytesPerPartition { get; init; } = 4L * 1024 * 1024;

    /// <summary>Node-global item ceiling across all partitions for <see cref="WriteAdmissionClass.Ordinary"/>
    /// admissions, so a burst spread across many partitions cannot retain unbounded memory in aggregate. &lt;= 0
    /// disables the global item bound.</summary>
    public long MaxQueuedItemsGlobal { get; init; } = 131_072;

    /// <summary>Node-global serialized-byte ceiling across all partitions for ordinary admissions. &lt;= 0
    /// disables the global byte bound.</summary>
    public long MaxQueuedBytesGlobal { get; init; } = 512L * 1024 * 1024;

    /// <summary>Extra node-global item headroom above <see cref="MaxQueuedItemsGlobal"/> reserved for terminal
    /// submissions, so global ordinary saturation cannot reject settlement anywhere on the node.</summary>
    public long TerminalReserveItemsGlobal { get; init; } = 8192;

    /// <summary>Extra node-global byte headroom above <see cref="MaxQueuedBytesGlobal"/> reserved for terminal
    /// submissions.</summary>
    public long TerminalReserveBytesGlobal { get; init; } = 64L * 1024 * 1024;

    /// <summary>Hard ceiling on a single submission's serialized bytes. A submission larger than this is rejected
    /// at admission (retryable backpressure) rather than dispatched alone, so one pathological value cannot
    /// occupy a whole partition batch or blow the byte budgets. Must be &gt;= <see cref="MaxBatchBytes"/> so a
    /// legitimately large value below the ceiling still dispatches alone. &lt;= 0 disables the hard ceiling.</summary>
    public long MaxOperationBytes { get; init; } = 64L * 1024 * 1024;

    /// <summary>Maximum wall-clock time a dispatched batch's Raft round trip may take before the scheduler cancels
    /// it; the cancelled call settles as a retryable outcome and the lane frees its capacity. Bounds an in-flight
    /// batch so it cannot outlive queue age or hang shutdown. &lt;= 0 disables the deadline.</summary>
    public int BatchExecutionTimeoutMs { get; init; } = 30_000;

    /// <summary>Maximum time an admitted item may wait before dispatch; on expiry it is released as MustRetry.
    /// Must stay well below the proposal/replication-intent lease so a released item is never proposed late.</summary>
    public int MaxQueueDelayMs { get; init; } = 1000;

    public int LaneCount { get; init; } = 8;

    /// <summary>Ordinary-submission inbox bound per lane; control messages bypass it. &lt;= 0 disables.</summary>
    public int AggregatorInboxSize { get; init; } = 16_384;
}
