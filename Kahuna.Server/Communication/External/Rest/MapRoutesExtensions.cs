
using Kahuna.Server;

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
    public static void MapRestKahunaRoutes(this WebApplication app, KahunaCommandLineOptions opts)
    {
        // Owns the HTTP root: the dashboard page when it is enabled, and the plain text response the
        // root has always given when it is not.
        DashboardHandlers.MapDashboardRoutes(app, opts);

        LocksHandlers.MapLocksRoutes(app);
        KeyValuesHandlers.MapKeyValueRoutes(app);
        SequencesHandlers.MapSequenceRoutes(app);
        ClusterHandlers.MapClusterRoutes(app);
        RangesHandlers.MapRangesRoutes(app);
        BackupsHandlers.MapBackupsRoutes(app);
    }
}
