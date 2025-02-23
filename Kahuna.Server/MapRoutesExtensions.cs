namespace Kahuna;

public static class MapRoutesExtensions
{
    public static void MapKahunaRoutes(this WebApplication app)
    {
        app.MapPost("/v1/kahuna/lock", async (ExternLockRequest request, IKahuna locks) =>
        {
            if (string.IsNullOrEmpty(request.LockName))
                return new() { Type = LockResponseType.Errored };

            if (string.IsNullOrEmpty(request.LockId))
                return new() { Type = LockResponseType.Errored };
            
            // Console.WriteLine("LOCK {0} {1} {2}", request.LockName, request.LockId, request.ExpiresMs);

            (LockResponseType response, long fencingToken) = await locks.TryLock(request.LockName, request.LockId, request.ExpiresMs);

            return new ExternLockResponse { Type = response, FencingToken = fencingToken };
        });

        app.MapPost("/v1/kahuna/extend-lock", async (ExternLockRequest request, IKahuna locks) =>
        {
            if (string.IsNullOrEmpty(request.LockName))
                return new() { Type = LockResponseType.Errored };

            if (string.IsNullOrEmpty(request.LockId))
                return new() { Type = LockResponseType.Errored };
            
            // Console.WriteLine("EXTEND-LOCK {0} {1} {2}", request.LockName, request.LockId, request.ExpiresMs);

            LockResponseType response = await locks.TryExtendLock(request.LockName, request.LockId, request.ExpiresMs);

            return new ExternLockResponse { Type = response };
        });

        app.MapPost("/v1/kahuna/unlock", async (ExternLockRequest request, IKahuna locks) =>
        {
            if (string.IsNullOrEmpty(request.LockName))
                return new() { Type = LockResponseType.Errored };

            if (string.IsNullOrEmpty(request.LockId))
                return new() { Type = LockResponseType.Errored };
            
            // Console.WriteLine("UNLOCK {0} {1} {2}", request.LockName, request.LockId, request.ExpiresMs);

            LockResponseType response = await locks.TryUnlock(request.LockName, request.LockId);

            return new ExternLockResponse { Type = response };
        });

        app.MapPost("/v1/kahuna/get-lock", async (ExternGetLockRequest request, IKahuna locks) =>
        {
            if (string.IsNullOrEmpty(request.LockName))
                return new() { Type = LockResponseType.Errored };
            
            // Console.WriteLine("UNLOCK {0} {1} {2}", request.LockName, request.LockId, request.ExpiresMs);

            (LockResponseType response, ReadOnlyLockContext? context) = await locks.GetLock(request.LockName);

            if (context is not null)
                return new() { Type = response, Owner = context.Owner, Expires = context.Expires, FencingToken = context.FencingToken };
            
            return new ExternGetLockResponse { Type = response };
        });
    }
}