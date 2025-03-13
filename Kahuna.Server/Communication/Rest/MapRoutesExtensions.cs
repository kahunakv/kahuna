
namespace Kahuna.Communication.Rest;

public static class MapRoutesExtensions
{
    public static void MapRestKahunaRoutes(this WebApplication app)
    {
        app.MapGet("/", () => "Kahuna.Server");
        
        LocksHandlers.MapLocksRoutes(app);
        KeyValuesHandlers.MapKeyValueRoutes(app);
    }
}