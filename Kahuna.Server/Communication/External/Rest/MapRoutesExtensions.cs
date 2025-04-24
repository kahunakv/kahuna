
namespace Kahuna.Communication.External.Rest;

/// <summary>
/// Provides extension methods for mapping REST API routes in a web application.
/// </summary>
/// <remarks>
/// This class consolidates route-mapping logic for various API endpoints, including
/// locks and key-value operations, enhancing modularity and maintaining clean separation of concerns.
/// </remarks>
public static class MapRoutesExtensions
{
    public static void MapRestKahunaRoutes(this WebApplication app)
    {
        app.MapGet("/", () => "Kahuna.Server");
        
        LocksHandlers.MapLocksRoutes(app);
        KeyValuesHandlers.MapKeyValueRoutes(app);
    }
}