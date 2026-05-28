using Kahuna.Shared.Communication.Rest;
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

            (SequenceResponseType response, long revision) = await kahuna.LocateAndCreateSequence(
                request.Name,
                request.InitialValue,
                request.Increment,
                request.MaxValue,
                request.Durability,
                cancellationToken
            );

            return new KahunaSequenceResponse { Type = response, Revision = revision };
        });

        app.MapPost("/v1/sequences/get", async (KahunaSequenceNameRequest request, IKahuna kahuna, CancellationToken cancellationToken) =>
        {
            if (string.IsNullOrWhiteSpace(request.Name))
                return new KahunaSequenceResponse { Type = SequenceResponseType.InvalidInput };

            (SequenceResponseType response, ReadOnlySequenceEntry? sequence) = await kahuna.LocateAndGetSequence(
                request.Name,
                request.Durability,
                cancellationToken
            );

            return new KahunaSequenceResponse { Type = response, Sequence = sequence, Revision = sequence?.Revision ?? -1 };
        });

        app.MapPost("/v1/sequences/next", async (KahunaSequenceNextRequest request, IKahuna kahuna, CancellationToken cancellationToken) =>
        {
            if (string.IsNullOrWhiteSpace(request.Name))
                return new KahunaSequenceResponse { Type = SequenceResponseType.InvalidInput };

            (SequenceResponseType response, SequenceAllocation allocation) = await kahuna.LocateAndNextSequenceValue(
                request.Name,
                request.IdempotencyKey,
                request.Durability,
                cancellationToken
            );

            return new KahunaSequenceResponse { Type = response, Allocation = allocation, Revision = allocation.Revision };
        });

        app.MapPost("/v1/sequences/reserve", async (KahunaSequenceReserveRequest request, IKahuna kahuna, CancellationToken cancellationToken) =>
        {
            if (string.IsNullOrWhiteSpace(request.Name) || request.Count <= 0)
                return new KahunaSequenceResponse { Type = SequenceResponseType.InvalidInput };

            (SequenceResponseType response, SequenceAllocation allocation) = await kahuna.LocateAndReserveSequenceRange(
                request.Name,
                request.Count,
                request.IdempotencyKey,
                request.Durability,
                cancellationToken
            );

            return new KahunaSequenceResponse { Type = response, Allocation = allocation, Revision = allocation.Revision };
        });

        app.MapPost("/v1/sequences/delete", async (KahunaSequenceNameRequest request, IKahuna kahuna, CancellationToken cancellationToken) =>
        {
            if (string.IsNullOrWhiteSpace(request.Name))
                return new KahunaSequenceResponse { Type = SequenceResponseType.InvalidInput };

            SequenceResponseType response = await kahuna.LocateAndDeleteSequence(
                request.Name,
                request.Durability,
                cancellationToken
            );

            return new KahunaSequenceResponse { Type = response };
        });
    }
}
