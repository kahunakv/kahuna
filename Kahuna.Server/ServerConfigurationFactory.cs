using Kahuna.Server.Configuration;
using Kahuna.Server.Persistence.Providers;

namespace Kahuna.Server;

/// <summary>
/// Builds the <see cref="KahunaConfiguration"/> the server host registers and a clustered node runs on.
/// <para>
/// It starts from the same mapping as the embedded node (<see cref="EmbeddedOptionsFactory"/>, then
/// <see cref="EmbeddedKahunaNode.BuildKahunaConfiguration"/>) and adds only the settings a server has and an
/// embedded node does not. The clustered server used to keep a second, hand-written copy of that mapping, and
/// every knob added to one copy and not the other reached one node shape only: the backup settings, the
/// durable-record retention budget and the maintenance interval were all parsed and then ignored on a cluster.
/// </para>
/// </summary>
public static class ServerConfigurationFactory
{
    /// <param name="opts">Parsed command line, with the storage and WAL paths and the storage revision already resolved.</param>
    /// <param name="functions">The user-defined script functions loaded from the extension assemblies.</param>
    public static KahunaConfiguration Create(
        KahunaCommandLineOptions opts,
        Kahuna.Extensibility.KahunaFunctionRegistry? functions = null,
        StorageProviders? storageProviders = null)
    {
        // The validator replaces a non-positive retention with a built-in value, which suits an embedding host
        // that leaves the field unset. A server flag that is set to 0 is a mistake, so refuse it here.
        if (opts.RevisionsToKeepCached < 1)
            throw new KahunaServerException(
                $"--revisions-to-cache must be at least 1, got {opts.RevisionsToKeepCached}.");

        KahunaConfiguration configuration = EmbeddedKahunaNode.BuildKahunaConfiguration(
            EmbeddedOptionsFactory.CreateEmbeddedOptions(opts, transportSecurity: null, functions, storageProviders),
            singleProcessRaftGroup: false);

        // Server-only: the listener certificate, the inter-node transport, and what this node tells clients in
        // routing hints. Set before validation, which loads and checks the certificate.
        configuration.HttpsCertificate = opts.HttpsCertificate;
        configuration.HttpsCertificatePassword = opts.HttpsCertificatePassword;
        configuration.InterNodeGrpcScheme = opts.RaftGrpcScheme;
        configuration.AdvertisedClientEndpoint = opts.AdvertisedClientEndpoint;
        configuration.AdvertisedClientScheme = opts.AdvertisedClientScheme;
        configuration.AdvertisePeerEndpoints = !opts.DisablePeerEndpointAdvertisement;
        configuration.RoutingHintsEnabled = !opts.DisableRoutingHints;

        return ConfigurationValidator.Validate(configuration, opts.WalPath);
    }
}
