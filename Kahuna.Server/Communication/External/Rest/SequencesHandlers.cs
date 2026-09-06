using Kahuna.Server.Routing;
using Kahuna.Shared.Communication.Rest;
using Kahuna.Shared.Routing;
using Kahuna.Shared.Sequences;

namespace Kahuna.Communication.External.Rest;

public static class SequencesHandlers
{
    public static void MapSequenceRoutes(WebApplication app)
    {
        app.MapPost("/v1/sequences/create", async (KahunaSequenceCreateRequest request, IKahuna kahuna, CancellationToken cancellationToken) =>
        {
            if (string.IsNullOrWhiteSpace(request.Name))
                return new KahunaSequenceResponse { Type = SequenceResponseType.InvalidInput };

            using RouteCaptureScope.Scope routeScope = RouteCaptureScope.Begin(out RouteCapture? capture);

            (SequenceResponseType response, long revision) = await kahuna.LocateAndCreateSequence(
                request.Name,
                request.InitialValue,
                request.Increment,
                request.MaxValue,
                request.Durability,
                cancellationToken
            );

            return new KahunaSequenceResponse { Type = response, Revision = revision, Route = SequenceRoute(capture, request.Name) };
        });

        app.MapPost("/v1/sequences/get", async (KahunaSequenceNameRequest request, IKahuna kahuna, CancellationToken cancellationToken) =>
        {
            if (string.IsNullOrWhiteSpace(request.Name))
                return new KahunaSequenceResponse { Type = SequenceResponseType.InvalidInput };

            using RouteCaptureScope.Scope routeScope = RouteCaptureScope.Begin(out RouteCapture? capture);

            (SequenceResponseType response, ReadOnlySequenceEntry? sequence) = await kahuna.LocateAndGetSequence(
                request.Name,
                request.Durability,
                cancellationToken
            );

            return new KahunaSequenceResponse { Type = response, Sequence = sequence, Revision = sequence?.Revision ?? -1, Route = SequenceRoute(capture, request.Name) };
        });

        app.MapPost("/v1/sequences/next", async (KahunaSequenceNextRequest request, IKahuna kahuna, CancellationToken cancellationToken) =>
        {
            if (string.IsNullOrWhiteSpace(request.Name))
                return new KahunaSequenceResponse { Type = SequenceResponseType.InvalidInput };

            using RouteCaptureScope.Scope routeScope = RouteCaptureScope.Begin(out RouteCapture? capture);

            (SequenceResponseType response, SequenceAllocation allocation) = await kahuna.LocateAndNextSequenceValue(
                request.Name,
                request.IdempotencyKey,
                request.Durability,
                cancellationToken
            );

            return new KahunaSequenceResponse { Type = response, Allocation = allocation, Revision = allocation.Revision, Route = SequenceRoute(capture, request.Name) };
        });

        app.MapPost("/v1/sequences/reserve", async (KahunaSequenceReserveRequest request, IKahuna kahuna, CancellationToken cancellationToken) =>
        {
            if (string.IsNullOrWhiteSpace(request.Name) || request.Count <= 0)
                return new KahunaSequenceResponse { Type = SequenceResponseType.InvalidInput };

            using RouteCaptureScope.Scope routeScope = RouteCaptureScope.Begin(out RouteCapture? capture);

            (SequenceResponseType response, SequenceAllocation allocation) = await kahuna.LocateAndReserveSequenceRange(
                request.Name,
                request.Count,
                request.IdempotencyKey,
                request.Durability,
                cancellationToken
            );

            return new KahunaSequenceResponse { Type = response, Allocation = allocation, Revision = allocation.Revision, Route = SequenceRoute(capture, request.Name) };
        });

        app.MapPost("/v1/sequences/delete", async (KahunaSequenceNameRequest request, IKahuna kahuna, CancellationToken cancellationToken) =>
        {
            if (string.IsNullOrWhiteSpace(request.Name))
                return new KahunaSequenceResponse { Type = SequenceResponseType.InvalidInput };

            using RouteCaptureScope.Scope routeScope = RouteCaptureScope.Begin(out RouteCapture? capture);

            SequenceResponseType response = await kahuna.LocateAndDeleteSequence(
                request.Name,
                request.Durability,
                cancellationToken
            );

            return new KahunaSequenceResponse { Type = response, Route = SequenceRoute(capture, request.Name) };
        });
    }

    /// <summary>
    /// The route the request resolved, if any. The sequence subsystem trims a name before routing it,
    /// so the hint is looked up under the trimmed form — the untrimmed string never reaches the router
    /// and would find nothing.
    /// </summary>
    private static KahunaRouteHint? SequenceRoute(RouteCapture? capture, string? name) =>
        string.IsNullOrEmpty(name) ? null : RouteHintWriter.Rest(capture, KahunaRoutingDomain.Sequence, name.Trim());
}
