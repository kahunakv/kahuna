
namespace Kahuna.Server.Persistence;

internal static class PersistenceResponseStatic
{
    internal static readonly PersistenceResponse FailedResponse = new (PersistenceResponseType.Failed);
    
    internal static readonly PersistenceResponse SuccessResponse = new (PersistenceResponseType.Success);
}