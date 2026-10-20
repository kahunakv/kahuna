
using Google.Protobuf;
using Kommander;

using Kahuna.Server.Replication;
using Kahuna.Server.Replication.Protos;
using Kahuna.Utils;

namespace Kahuna.Server.KeyValues.Ranges;

/// <summary>
/// The whole-partition state transfer for the meta partition (id 0). Kommander invokes it to
/// repair a node that has fallen below the meta-partition WAL compaction floor: a lagging follower
/// whose needed log entries were already compacted, or a cold-restarted node whose local WAL was
/// trimmed past the tail it would replay.
///
/// <para>The meta partition hosts <b>three</b> independent state machines — the range-descriptor map
/// (<see cref="RangeMapStore"/>), the snapshot-floor hold registry (<see cref="SnapshotFloorStore"/>) and the history
/// hold registry (<see cref="HistoryHoldStore"/>). A repair must export and install <b>all of them</b> as one blob: installing only one would leave the
/// recovering node's other state machine stale or empty. This is required because
/// <see cref="SnapshotFloorStore"/> replicates <b>deltas</b> — once old deltas are compacted, the
/// hold set can only be reconstructed from a whole-state snapshot, not from surviving log entries.
/// (The range map still replicates full snapshots, but it shares the partition's compaction floor,
/// so it rides the same transfer for free.)</para>
///
/// <para><b>Atomicity.</b> <see cref="ImportPartitionState"/> parses and validates both sub-states
/// before mutating either, so a corrupt or truncated blob throws without leaving the node
/// half-updated; Kommander seeds the receiver's checkpoint only after the import returns, so a
/// failed import is retried cleanly on the next delivery.</para>
///
/// <para><b>Export index.</b> Kommander exports at the last checkpoint index and resumes the
/// follower at <c>checkpoint + 1</c>, so the tail of log entries above the checkpoint is re-applied
/// on top of the installed snapshot. Both state machines tolerate this: range-map entries are full
/// snapshots (idempotent) and snapshot-floor deltas are keyed upserts/removes (idempotent), so
/// re-applying the tail converges regardless of the exact index the exported in-memory state
/// reflects.</para>
/// </summary>
internal sealed class MetaSystemStateTransfer : IRaftSystemStateTransfer
{
    private readonly RangeMapStore rangeMapStore;

    private readonly SnapshotFloorStore snapshotFloorStore;

    private readonly HistoryHoldStore historyHoldStore;

    public MetaSystemStateTransfer(RangeMapStore rangeMapStore, SnapshotFloorStore snapshotFloorStore, HistoryHoldStore historyHoldStore)
    {
        this.rangeMapStore = rangeMapStore;
        this.snapshotFloorStore = snapshotFloorStore;
        this.historyHoldStore = historyHoldStore;
    }

    public Task<Stream> ExportPartitionState(int partitionId, long upToIndex, CancellationToken ct)
    {
        MetaSystemStateMessage message = new()
        {
            RangeMap = UnsafeByteOperations.UnsafeWrap(rangeMapStore.SerializeState()),
            SnapshotFloor = UnsafeByteOperations.UnsafeWrap(snapshotFloorStore.SerializeState()),
            HistoryHolds = UnsafeByteOperations.UnsafeWrap(historyHoldStore.SerializeState()),
        };

        // The message is serialised straight into pooled 64 KB segments: no exact-size array for the whole
        // blob and no doubling buffer, the same shape as the user-partition export.
        SegmentedBufferStream stream = new();

        try
        {
            message.WriteTo(stream);
        }
        catch
        {
            stream.Dispose();
            throw;
        }

        return Task.FromResult<Stream>(stream);
    }

    public async Task ImportPartitionState(int partitionId, Stream snapshot, CancellationToken ct)
    {
        // Buffer the delivery in pooled segments and parse from them directly: no doubling buffer and no
        // exact-size copy of the blob before decoding.
        MetaSystemStateMessage message;
        using (SegmentedBufferStream buffer = new())
        {
            await snapshot.CopyToAsync(buffer, ct).ConfigureAwait(false);
            message = ReplicationSerializer.UnserializeMetaSystemStateMessage(buffer);
        }

        // Parse and validate every sub-state before installing any: a decode/validation failure here throws before any
        // swap, leaving the node's prior state intact so Kommander retries. A blob from an exporter that predates the
        // history hold registry carries no field 3 and installs an empty registry.
        RangeMap rangeMap = rangeMapStore.ParseState(message.RangeMap.Span);
        Dictionary<string, SnapshotHold> holds = snapshotFloorStore.ParseState(message.SnapshotFloor.Span);
        Dictionary<string, HistoryHold> historyHolds = historyHoldStore.ParseState(message.HistoryHolds.Span);

        rangeMapStore.CommitState(rangeMap);
        snapshotFloorStore.CommitState(holds);
        historyHoldStore.CommitState(historyHolds);
    }
}
