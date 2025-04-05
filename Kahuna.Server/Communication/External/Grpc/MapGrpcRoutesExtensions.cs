
namespace Kahuna.Communication.External.Grpc;

public static class MapGrpcRoutesExtensions
{
    public static void MapGrpcKahunaRoutes(this WebApplication app)
    {
        app.MapGrpcService<LocksService>();
        app.MapGrpcService<KeyValuesService>();
    }
}