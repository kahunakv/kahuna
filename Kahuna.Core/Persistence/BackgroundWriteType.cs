
namespace Kahuna.Server.Persistence;

public enum BackgroundWriteType
{
    QueueStoreLock,
    QueueStoreKeyValue,
    Flush
}