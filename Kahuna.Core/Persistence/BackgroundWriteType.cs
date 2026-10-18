
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
    ForgetUnhostedPartitions
}