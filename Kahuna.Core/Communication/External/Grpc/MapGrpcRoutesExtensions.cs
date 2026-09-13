
using Kahuna.Server.Communication;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;

namespace Kahuna.Communication.External.Grpc;

/// <summary>
/// Provides extension methods for mapping gRPC routes within a <see cref="WebApplication"/> instance.
/// </summary>
/// <remarks>
/// This class includes functionality for mapping specific gRPC services to routes in a .NET application,
/// leveraging services such as <see cref="LocksService"/>, <see cref="KeyValuesService"/>, and <see cref="SequencesService"/>.
/// These mappings enable the integration of gRPC endpoints into the application's request pipeline.
/// </remarks>
public static class MapGrpcRoutesExtensions
{
    public static void MapGrpcKahunaRoutes(this WebApplication app)
    {
        // Fail at startup, not at the first call: the services cannot be activated without the gate.
        if (app.Services.GetService<NodeTransportGate>() is null)
            throw new InvalidOperationException(
                "The Kahuna gRPC services require a NodeTransportGate. Call services.AddNodeTransportGate() before building the application.");

        app.MapGrpcService<LocksService>();
        app.MapGrpcService<KeyValuesService>();
        app.MapGrpcService<SequencesService>();
        app.MapGrpcService<ClusterService>();
        app.MapGrpcService<BackupsService>();
    }
}
