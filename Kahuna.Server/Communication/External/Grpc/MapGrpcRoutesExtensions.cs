
namespace Kahuna.Communication.External.Grpc;

/// <summary>
/// Provides extension methods for mapping gRPC routes within a <see cref="WebApplication"/> instance.
/// </summary>
/// <remarks>
/// This class includes functionality for mapping specific gRPC services to routes in a .NET application,
/// leveraging services such as <see cref="LocksService"/> and <see cref="KeyValuesService"/>.
/// These mappings enable the integration of gRPC endpoints into the application's request pipeline.
/// </remarks>
public static class MapGrpcRoutesExtensions
{
    public static void MapGrpcKahunaRoutes(this WebApplication app)
    {
        app.MapGrpcService<LocksService>();
        app.MapGrpcService<KeyValuesService>();
    }
}