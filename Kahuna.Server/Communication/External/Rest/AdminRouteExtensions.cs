using Kahuna.Server.Communication;

namespace Kahuna.Communication.External.Rest;

/// <summary>
/// Marks a REST route as an admin operation: the <see cref="AdminGate"/> must admit the caller before the handler
/// runs. A refused call gets 401 and the handler never runs.
/// </summary>
internal static class AdminRouteExtensions
{
    public static RouteHandlerBuilder RequireAdminKey(this RouteHandlerBuilder builder) =>
        builder.AddEndpointFilter(static async (context, next) =>
        {
            // A host that never registered the gate has no admin keys, which is the not-enforced gate.
            AdminGate gate = context.HttpContext.RequestServices.GetService<AdminGate>() ?? AdminGate.Disabled;

            if (!gate.Admits(context.HttpContext))
                return Results.Unauthorized();

            return await next(context);
        });
}
