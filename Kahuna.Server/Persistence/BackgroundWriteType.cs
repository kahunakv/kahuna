
namespace Kahuna.Persistence;

public enum BackgroundWriteType
{
    QueueStoreLock,
    QueueStoreKeyValue,
    Flush
}