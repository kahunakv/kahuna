namespace Kahuna.Shared.Communication;

/// <summary>
/// Where an admin key travels on each transport. A node that has admin keys configured refuses its admin
/// operations (node removal, replication factor, range-map DDL, backups and restore) without one of them.
/// </summary>
public static class KahunaAdminCredential
{
    /// <summary>The HTTP header that carries the admin key on REST calls.</summary>
    public const string HeaderName = "X-Kahuna-Admin-Key";

    /// <summary>The metadata entry that carries the admin key on gRPC calls. gRPC metadata names are lower case.</summary>
    public const string MetadataName = "x-kahuna-admin-key";
}
