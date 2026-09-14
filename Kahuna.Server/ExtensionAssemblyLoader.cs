
using System.Reflection;
using System.Runtime.Loader;
using System.Security.Cryptography;

using Microsoft.Extensions.Logging;

using Kahuna.Extensibility;

namespace Kahuna;

/// <summary>
/// Loads user-defined script functions from extension assemblies named on the command line.
///
/// <para>An operator who runs the shipped binary cannot edit <c>Program.cs</c>, so this is the only
/// way for them to add a function. Nothing is loaded unless <c>--extension-assembly</c> is given:
/// there is no plugin directory, no probing and no discovery, so a node that was not asked to load
/// an extension runs none of this code.</para>
///
/// <para>Every failure stops startup. A node that came up missing one function would answer scripts
/// that call it with <c>Errored</c> while its peers answered normally — the exact intermittent,
/// node-dependent failure this feature works hardest to make impossible. Refusing to start turns
/// that into one clear message at the moment the operator can still fix it.</para>
///
/// <para>An extension assembly is fully trusted code. It runs in this process with the node's
/// privileges, and nothing sandboxes it. Each loaded file is logged with its path and the SHA-256 of
/// its bytes, so an operator can confirm every node loaded the same artifact.</para>
/// </summary>
public static partial class ExtensionAssemblyLoader
{
    [LoggerMessage(
        Level = LogLevel.Information,
        Message = "Loaded extension assembly {Path} (sha256 {Hash}): {Providers} provider(s), {Functions} function(s). Every node of the cluster must load the same artifact")]
    private static partial void LogExtensionAssemblyLoaded(this ILogger logger, string path, string hash, int providers, int functions);

    /// <summary>
    /// Builds the node's function registry from the given assembly paths.
    /// </summary>
    /// <param name="paths">
    /// Assembly paths from <c>--extension-assembly</c>. Null or empty returns an empty registry
    /// without loading anything.
    /// </param>
    /// <param name="logger">Receives one line per loaded assembly.</param>
    /// <exception cref="KahunaServerException">
    /// A path is unreadable, an assembly cannot be loaded, an assembly publishes no provider, a
    /// provider cannot be built, or a registration is rejected.
    /// </exception>
    public static KahunaFunctionRegistry Load(IEnumerable<string>? paths, ILogger logger)
    {
        KahunaFunctionRegistry registry = new();

        if (paths is null)
            return registry;

        foreach (string path in paths)
        {
            if (string.IsNullOrWhiteSpace(path))
                throw new KahunaServerException("--extension-assembly was given an empty path");

            LoadOne(path, registry, logger);
        }

        return registry;
    }

    private static void LoadOne(string path, KahunaFunctionRegistry registry, ILogger logger)
    {
        string fullPath = Path.GetFullPath(path);

        if (!File.Exists(fullPath))
            throw new KahunaServerException($"--extension-assembly '{path}' does not exist (resolved to '{fullPath}')");

        string hash = HashFile(fullPath);

        Assembly assembly;

        try
        {
            // One context per assembly, and not collectible: an extension lives for the life of the
            // process, and a collectible context would only add unload machinery for something that is
            // never unloaded. Its own context lets an extension carry its own dependency versions
            // without colliding with the ones the server already loaded.
            AssemblyLoadContext context = new(name: "kahuna-extension:" + Path.GetFileNameWithoutExtension(fullPath), isCollectible: false);

            assembly = context.LoadFromAssemblyPath(fullPath);
        }
        catch (Exception ex)
        {
            throw new KahunaServerException($"--extension-assembly '{fullPath}' could not be loaded: {ex.GetType().Name}: {ex.Message}");
        }

        Type[] providerTypes = FindProviderTypes(assembly, fullPath);

        if (providerTypes.Length == 0)
            throw new KahunaServerException(
                $"--extension-assembly '{fullPath}' publishes no functions: it contains no public, non-abstract {nameof(IKahunaFunctionProvider)} implementation with a parameterless constructor");

        int before = registry.Count;

        foreach (Type providerType in providerTypes)
        {
            IKahunaFunctionProvider provider;

            try
            {
                provider = (IKahunaFunctionProvider)Activator.CreateInstance(providerType)!;
            }
            catch (Exception ex)
            {
                Exception cause = ex is TargetInvocationException { InnerException: { } inner } ? inner : ex;

                throw new KahunaServerException(
                    $"--extension-assembly '{fullPath}': the constructor of provider '{providerType.FullName}' threw {cause.GetType().Name}: {cause.Message}");
            }

            try
            {
                provider.Register(registry);
            }
            catch (ArgumentException ex)
            {
                // A rejected name: a duplicate, a built-in, or a shape no script could call. The
                // registry's own message already says which, and it is the actionable half.
                throw new KahunaServerException(
                    $"--extension-assembly '{fullPath}': provider '{providerType.FullName}' registered an unacceptable function: {ex.Message}");
            }
            catch (Exception ex)
            {
                throw new KahunaServerException(
                    $"--extension-assembly '{fullPath}': provider '{providerType.FullName}' threw {ex.GetType().Name} while registering: {ex.Message}");
            }
        }

        logger.LogExtensionAssemblyLoaded(fullPath, hash, providerTypes.Length, registry.Count - before);
    }

    /// <summary>
    /// Finds the provider types an assembly publishes: public, concrete, and buildable with no
    /// arguments. A type that implements the interface but cannot be built is skipped rather than
    /// reported, because the "no provider" message below is what an operator can act on.
    /// </summary>
    private static Type[] FindProviderTypes(Assembly assembly, string fullPath)
    {
        Type[] types;

        try
        {
            types = assembly.GetTypes();
        }
        catch (ReflectionTypeLoadException ex)
        {
            // A partly-loadable assembly usually means a missing dependency next to the file. Name the
            // first real cause: the aggregate message on its own says nothing useful.
            Exception? cause = Array.Find(ex.LoaderExceptions, candidate => candidate is not null);

            throw new KahunaServerException(
                $"--extension-assembly '{fullPath}': its types could not be loaded, which usually means a dependency is missing next to it. First cause: {cause?.Message ?? "unknown"}");
        }

        List<Type> providers = [];

        foreach (Type type in types)
        {
            if (!type.IsPublic || type.IsAbstract || type.IsInterface)
                continue;

            if (!typeof(IKahunaFunctionProvider).IsAssignableFrom(type))
                continue;

            if (type.GetConstructor(Type.EmptyTypes) is null)
                continue;

            providers.Add(type);
        }

        // Reflection order is not defined. Sorting makes two nodes that load the same file register in
        // the same order, so a registration failure reproduces instead of depending on the run.
        providers.Sort(static (left, right) => string.CompareOrdinal(left.FullName, right.FullName));

        return providers.ToArray();
    }

    private static string HashFile(string fullPath)
    {
        using FileStream stream = File.OpenRead(fullPath);

        return Convert.ToHexStringLower(SHA256.HashData(stream));
    }
}
