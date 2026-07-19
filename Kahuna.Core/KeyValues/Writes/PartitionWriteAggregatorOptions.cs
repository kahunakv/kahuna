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

    /// <summary>Maximum time an admitted item may wait before dispatch; on expiry it is released as MustRetry.
    /// Must stay well below the proposal/replication-intent lease so a released item is never proposed late.</summary>
    public int MaxQueueDelayMs { get; init; } = 1000;

    public int LaneCount { get; init; } = 8;

    /// <summary>Ordinary-submission inbox bound per lane; control messages bypass it. &lt;= 0 disables.</summary>
    public int AggregatorInboxSize { get; init; } = 16_384;
}
