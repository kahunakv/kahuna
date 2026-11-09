
namespace Kahuna.Server.Persistence;

public enum BackgroundWriteType
{
    QueueStoreLock,
    QueueStoreKeyValue,
    Flush,
    FlushAndNotify,
    /// <summary>
    /// The flush a backup capture waits for: the same full drain and forced snapshot capture as
    /// <see cref="FlushAndNotify"/>, without the targeted revision cleanup, so the writer's turn is short
    /// just before the capture loads the disk. The next periodic flush runs the cleanup.
    /// </summary>
    FlushBeforeCapture,
    /// <summary>
    /// The flush a whole-partition state transfer waits for, before the exporter's scan and before the
    /// installer's purge: the same full drain as <see cref="FlushAndNotify"/>, the durability floors
    /// persisted on the periodic capture cadence, and nothing that could wait on the cluster. The
    /// transfer needs every queued write landed and nothing else; the targeted revision cleanup that
    /// <see cref="FlushAndNotify"/> runs begins with a meta-partition catch-up confirmation, which a
    /// replica installing a snapshot while it is not a roster member cannot get, and the install's drain
    /// parked on it for 174 s of a 205-s install. No forced store-snapshot rewrite either: the installer
    /// replaces its stores next and the exporter reads them directly.
    /// </summary>
    FlushForStateTransfer,
    ForgetUnhostedPartitions,
    /// <summary>
    /// One more pass of the backend-wide revision sweep, sent by the writer to itself when a pass
    /// deleted rows, stopped on its budget, and nothing is queued for the flush. A flush cycle runs
    /// one pass, so a sweep with millions of rows to delete moved at one pass per flush period on a
    /// writer that had nothing else to do. The pass is skipped when writes are queued: the next flush
    /// cycle runs the flush first and then the sweep, as before.
    /// </summary>
    RevisionSweepCatchUp
}