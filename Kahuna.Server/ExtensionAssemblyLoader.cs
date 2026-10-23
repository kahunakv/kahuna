using System.Reflection;
using System.Runtime.Loader;
using System.Security.Cryptography;

using Microsoft.Extensions.Logging;

using Kahuna.Extensibility;
using Kahuna.Server.Persistence.Providers;

namespace Kahuna;

/// <summary>
/// Loads extension assemblies named on the command line: user-defined script functions
/// (<see cref="IKahunaFunctionProvider"/>) and storage engines (<see cref="IPersistenceBackendProvider"/>
/// for <c>--storage</c>, <see cref="IWalProvider"/> for <c>--wal-storage</c>). One assembly may publish
/// any of them.
///
/// <para>An operator who runs the shipped binary cannot edit <c>Program.cs</c>, so this is the only
/// way for them to add a function or a storage engine. Nothing is loaded unless
/// <c>--extension-assembly</c> is given: there is no plugin directory, no probing and no discovery,
/// so a node that was not asked to load an extension runs none of this code.</para>
///
/// <para>Every failure stops startup. A node that came up missing one function would answer scripts
/// that call it with <c>Errored</c> while its peers answered normally — the exact intermittent,
/// node-dependent failure this feature works hardest to make impossible. Refusing to start turns
/// that into one clear message at the moment the operator can still fix it. A storage engine may
/// not take a type name that is already registered, so an extension never replaces a built-in
/// engine behind the operator's back.</para>
///
/// <para>An extension assembly is fully trusted code. It runs in this process with the node's
/// privileges, and nothing sandboxes it. Each loaded file is logged with its path and the SHA-256 of
/// its bytes, so an operator can confirm every node loaded the same artifact.</para>
/// </summary>
public static partial class ExtensionAssemblyLoader
{
    [LoggerMessage(
        Level = LogLevel.Information,
        Message = "Loaded extension assembly {Path} (sha256 {Hash}): {Providers} provider(s), {Functions} function(s), storage engines [{StorageEngines}]. Every node of the cluster must load the same artifact")]
    private static partial void LogExtensionAssemblyLoaded(this ILogger logger, string path, string hash, int providers, int functions, string storageEngines);

    /// <summary>
    /// Builds the node's function registry from the given assembly paths, and registers the storage
    /// engines they publish in <paramref name="storageProviders"/>.
    /// </summary>
    /// <param name="paths">
    /// Assembly paths from <c>--extension-assembly</c>. Null or empty returns an empty registry
    /// without loading anything.
    /// </param>
    /// <param name="logger">Receives one line per loaded assembly.</param>
    /// <param name="storageProviders">
    /// The node's storage engines. Null for a caller that accepts functions only: an assembly that
    /// publishes a storage engine then fails.
    /// </param>
    /// <exception cref="KahunaServerException">
    /// A path is unreadable, an assembly cannot be loaded, an assembly publishes no provider, a
    /// provider cannot be built, or a registration is rejected.
    /// </exception>
    public static KahunaFunctionRegistry Load(IEnumerable<string>? paths, ILogger logger, StorageProviders? storageProviders = null)
    {
        KahunaFunctionRegistry registry = new();

        if (paths is null)
            return registry;

        foreach (string path in paths)
        {
            if (string.IsNullOrWhiteSpace(path))
                throw new KahunaServerException("--extension-assembly was given an empty path");

            LoadOne(path, registry, storageProviders, logger);
        }

        return registry;
    }

    private static void LoadOne(string path, KahunaFunctionRegistry registry, StorageProviders? storageProviders, ILogger logger)
    {
        string fullPath = Path.GetFullPath(path);

        if (!File.Exists(fullPath))
            throw new KahunaServerException($"--extension-assembly '{path}' does not exist (resolved to '{fullPath}')");

        string hash = HashFile(fullPath);

        Assembly assembly;

        try
        {
            assembly = new ExtensionLoadContext(fullPath).LoadFromAssemblyPath(fullPath);
        }
        catch (Exception ex)
        {
            throw new KahunaServerException($"--extension-assembly '{fullPath}' could not be loaded: {ex.GetType().Name}: {ex.Message}");
        }

        Type[] types = LoadTypes(assembly, fullPath);
        Type[] functionProviderTypes = FindProviderTypes<IKahunaFunctionProvider>(types);
        Type[] backendProviderTypes = FindProviderTypes<IPersistenceBackendProvider>(types);
        Type[] walProviderTypes = FindProviderTypes<IWalProvider>(types);

        if (functionProviderTypes.Length == 0 && backendProviderTypes.Length == 0 && walProviderTypes.Length == 0)
            throw new KahunaServerException(
                $"--extension-assembly '{fullPath}' publishes nothing: it contains no public, non-abstract {nameof(IKahunaFunctionProvider)}, " +
                $"{nameof(IPersistenceBackendProvider)} or {nameof(IWalProvider)} implementation with a parameterless constructor");

        if (storageProviders is null && (backendProviderTypes.Length > 0 || walProviderTypes.Length > 0))
            throw new KahunaServerException(
                $"--extension-assembly '{fullPath}' publishes a storage engine, and this host does not take storage engines from extensions");

        int before = registry.Count;

        foreach (Type providerType in functionProviderTypes)
        {
            IKahunaFunctionProvider provider = Create<IKahunaFunctionProvider>(providerType, fullPath);

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

        List<string> storageEngines = [];

        foreach (Type providerType in backendProviderTypes)
        {
            IPersistenceBackendProvider provider = Create<IPersistenceBackendProvider>(providerType, fullPath);
            string storageType = StorageTypeOf(provider.StorageType, providerType, fullPath);

            if (storageProviders!.FindBackend(storageType) is not null)
                throw new KahunaServerException(
                    $"--extension-assembly '{fullPath}': provider '{providerType.FullName}' registers storage type '{storageType}', which is already registered");

            storageProviders.Register(provider);
            storageEngines.Add("storage:" + storageType);
        }

        foreach (Type providerType in walProviderTypes)
        {
            IWalProvider provider = Create<IWalProvider>(providerType, fullPath);
            string storageType = StorageTypeOf(provider.StorageType, providerType, fullPath);

            if (storageProviders!.FindWal(storageType) is not null)
                throw new KahunaServerException(
                    $"--extension-assembly '{fullPath}': provider '{providerType.FullName}' registers WAL storage type '{storageType}', which is already registered");

            storageProviders.Register(provider);
            storageEngines.Add("wal:" + storageType);
        }

        logger.LogExtensionAssemblyLoaded(fullPath, hash, functionProviderTypes.Length, registry.Count - before, string.Join(", ", storageEngines));
    }

    private static T Create<T>(Type providerType, string fullPath)
    {
        try
        {
            return (T)Activator.CreateInstance(providerType)!;
        }
        catch (Exception ex)
        {
            Exception cause = ex is TargetInvocationException { InnerException: { } inner } ? inner : ex;

            throw new KahunaServerException(
                $"--extension-assembly '{fullPath}': the constructor of provider '{providerType.FullName}' threw {cause.GetType().Name}: {cause.Message}");
        }
    }

    private static string StorageTypeOf(string? storageType, Type providerType, string fullPath)
    {
        if (string.IsNullOrWhiteSpace(storageType))
            throw new KahunaServerException(
                $"--extension-assembly '{fullPath}': provider '{providerType.FullName}' has no storage type name");

        return storageType;
    }

    private static Type[] LoadTypes(Assembly assembly, string fullPath)
    {
        try
        {
            return assembly.GetTypes();
        }
        catch (ReflectionTypeLoadException ex)
        {
            // A partly-loadable assembly usually means a missing dependency next to the file. Name the
            // first real cause: the aggregate message on its own says nothing useful.
            Exception? cause = Array.Find(ex.LoaderExceptions, candidate => candidate is not null);

            throw new KahunaServerException(
                $"--extension-assembly '{fullPath}': its types could not be loaded, which usually means a dependency is missing next to it. First cause: {cause?.Message ?? "unknown"}");
        }
    }

    /// <summary>
    /// Finds the provider types of one kind that an assembly publishes: public, concrete, and buildable with no
    /// arguments. A type that implements the interface but cannot be built is skipped rather than
    /// reported, because the "publishes nothing" message is what an operator can act on.
    /// </summary>
    private static Type[] FindProviderTypes<T>(Type[] types)
    {
        List<Type> providers = [];

        foreach (Type type in types)
        {
            if (!type.IsPublic || type.IsAbstract || type.IsInterface)
                continue;

            if (!typeof(T).IsAssignableFrom(type))
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

    /// <summary>
    /// The load context of one extension assembly, for the life of the process: not collectible, since an
    /// extension is never unloaded. An assembly the server has (Kahuna, Kommander, the runtime, their
    /// dependencies) is the server's own copy, so the extension's types implement the server's interfaces;
    /// any other dependency comes from the extension's directory, as its <c>.deps.json</c> lists it.
    /// </summary>
    private sealed class ExtensionLoadContext(string assemblyPath)
        : AssemblyLoadContext(name: "kahuna-extension:" + Path.GetFileNameWithoutExtension(assemblyPath), isCollectible: false)
    {
        private static readonly HashSet<string> HostAssemblies = ReadHostAssemblies();

        private readonly AssemblyDependencyResolver resolver = new(assemblyPath);

        protected override Assembly? Load(AssemblyName assemblyName)
        {
            if (assemblyName.Name is null || HostAssemblies.Contains(assemblyName.Name))
                return null;

            string? path = resolver.ResolveAssemblyToPath(assemblyName);
            return path is null ? null : LoadFromAssemblyPath(path);
        }

        protected override IntPtr LoadUnmanagedDll(string unmanagedDllName)
        {
            string? path = resolver.ResolveUnmanagedDllToPath(unmanagedDllName);
            return path is null ? IntPtr.Zero : LoadUnmanagedDllFromPath(path);
        }

        // The assemblies the host was started with: the runtime's trusted platform assemblies, which for an
        // application include its own dependencies.
        private static HashSet<string> ReadHostAssemblies()
        {
            HashSet<string> names = new(StringComparer.OrdinalIgnoreCase);

            if (AppContext.GetData("TRUSTED_PLATFORM_ASSEMBLIES") is string list)
                foreach (string file in list.Split(Path.PathSeparator, StringSplitOptions.RemoveEmptyEntries))
                    names.Add(Path.GetFileNameWithoutExtension(file));

            return names;
        }
    }
}
