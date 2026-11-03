/**
 * This file is part of Kahuna
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Kahuna.Client;
using Microsoft.Extensions.Logging;

namespace Microsoft.Extensions.DependencyInjection;

/// <summary>
/// Registers a <see cref="KahunaClient"/> in a service container.
/// <para>
/// The client is a singleton: it owns pooled gRPC channels and a routing cache, and both are meant to
/// live as long as the application. Its logger comes from the container when one is registered.
/// </para>
/// </summary>
public static class KahunaClientServiceCollectionExtensions
{
    /// <summary>Registers a <see cref="KahunaClient"/> for one node or one load-balanced address.</summary>
    public static IServiceCollection AddKahunaClient(
        this IServiceCollection services,
        string url,
        Action<KahunaOptions>? configureOptions = null
    )
    {
        ArgumentException.ThrowIfNullOrEmpty(url);

        return AddKahunaClient(services, [url], configureOptions);
    }

    /// <summary>Registers a <see cref="KahunaClient"/> for the given cluster endpoints.</summary>
    public static IServiceCollection AddKahunaClient(
        this IServiceCollection services,
        IReadOnlyList<string> urls,
        Action<KahunaOptions>? configureOptions = null
    )
    {
        ArgumentNullException.ThrowIfNull(services);

        string[] endpoints = ValidateEndpoints(urls);

        services.AddSingleton(sp => Create(sp, endpoints, configureOptions));

        return services;
    }

    /// <summary>
    /// Registers a <see cref="KahunaClient"/> under <paramref name="serviceKey"/>, for an application
    /// that talks to more than one Kahuna cluster.
    /// </summary>
    public static IServiceCollection AddKeyedKahunaClient(
        this IServiceCollection services,
        object? serviceKey,
        IReadOnlyList<string> urls,
        Action<KahunaOptions>? configureOptions = null
    )
    {
        ArgumentNullException.ThrowIfNull(services);

        string[] endpoints = ValidateEndpoints(urls);

        services.AddKeyedSingleton(serviceKey, (sp, _) => Create(sp, endpoints, configureOptions));

        return services;
    }

    private static KahunaClient Create(IServiceProvider services, string[] endpoints, Action<KahunaOptions>? configureOptions)
    {
        KahunaOptions options = new();
        configureOptions?.Invoke(options);

        return new(endpoints, services.GetService<ILogger<KahunaClient>>(), options: options);
    }

    // Copied, so a caller that changes its list after the call cannot change the endpoints of a client
    // that the container has not built yet.
    private static string[] ValidateEndpoints(IReadOnlyList<string> urls)
    {
        ArgumentNullException.ThrowIfNull(urls);

        if (urls.Count == 0)
            throw new ArgumentException("At least one Kahuna endpoint is required.", nameof(urls));

        string[] endpoints = new string[urls.Count];

        for (int i = 0; i < endpoints.Length; i++)
        {
            string url = urls[i];

            if (string.IsNullOrWhiteSpace(url))
                throw new ArgumentException($"Kahuna endpoint at index {i} is empty.", nameof(urls));

            endpoints[i] = url;
        }

        return endpoints;
    }
}
