
using Kahuna.Shared.Locks;

namespace Kahuna.Client;

public sealed class KahunaException : Exception
{
    public LockResponseType ErrorCode { get; }
    
    public KahunaException(string message, LockResponseType errorCode) : base(message)
    {
        ErrorCode = errorCode;
    }
}