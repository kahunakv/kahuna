
using Kahuna.Shared.Communication.Grpc;
using Kahuna.Shared.Locks;

namespace Kahuna.Communication.External.Grpc;

/// <summary>
/// Typed "no definitive answer was produced; retry to resolve it" responses for the lock gRPC
/// surface, one factory per response message.
/// </summary>
internal static class LockMustRetry
{
    private const GrpcLockResponseType Type = (GrpcLockResponseType)LockResponseType.MustRetry;

    public static GrpcTryLockResponse TryLock() => new() { Type = Type };

    public static GrpcExtendLockResponse ExtendLock() => new() { Type = Type };

    public static GrpcUnlockResponse Unlock() => new() { Type = Type };

    public static GrpcGetLockResponse GetLock() => new() { Type = Type };
}
