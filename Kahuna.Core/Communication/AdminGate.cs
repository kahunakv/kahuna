using System.Collections.Frozen;
using System.Security.Cryptography;
using System.Text;
using Google.Protobuf.Reflection;
using ProtoServiceDescriptor = Google.Protobuf.Reflection.ServiceDescriptor;
using Grpc.AspNetCore.Server;
using Grpc.Core;
using Grpc.Core.Interceptors;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Kahuna.Server.Configuration;
using Kahuna.Shared.Communication;

namespace Kahuna.Server.Communication;

/// <summary>
/// Admits a caller to an admin operation (node removal, replication factor, range-map DDL, backups and restore)
/// only when it presents one of the configured admin keys. Data operations never consult it.
/// <para>
/// The keys come from <see cref="KahunaConfiguration.AdminApiKeyFile"/>, one per line, so a rotation can accept
/// the old and the new key at the same time. With no key file the gate admits every caller: that keeps a node
/// that has not turned the credential on working as before, and the host warns about it at startup.
/// </para>
/// <para>
/// REST callers send the key in <see cref="HeaderName"/>; gRPC callers in the <see cref="MetadataName"/> metadata
/// entry. Keys are compared as SHA-256 digests with a fixed-time comparison against every configured key, so
/// neither the time nor the length of a wrong key tells which keys exist. A key is never logged.
/// </para>
/// </summary>
public sealed partial class AdminGate
{
    /// <summary>The HTTP header that carries the admin key.</summary>
    public const string HeaderName = KahunaAdminCredential.HeaderName;

    /// <summary>The gRPC metadata entry that carries the admin key.</summary>
    public const string MetadataName = KahunaAdminCredential.MetadataName;

    /// <summary>The shortest key a key file may hold.</summary>
    public const int MinKeyLength = 32;

    /// <summary>A gate that admits every caller.</summary>
    public static readonly AdminGate Disabled = new([], NullLogger<AdminGate>.Instance);

    /// <summary>
    /// The full names of the gRPC admin methods, built from the generated service descriptors so that a renamed
    /// or removed method fails at startup instead of silently leaving the gate. Node-to-node methods
    /// (<c>Backups.CaptureBackupPart</c>, the batch streams) are not here: the peer-certificate gate covers them.
    /// </summary>
    public static readonly FrozenSet<string> GrpcAdminMethods = BuildGrpcAdminMethods();

    private readonly byte[][] keyDigests;

    private readonly ILogger<AdminGate> logger;

    public AdminGate(IReadOnlyList<string> keys, ILogger<AdminGate> logger)
    {
        keyDigests = new byte[keys.Count][];
        for (int i = 0; i < keys.Count; i++)
            keyDigests[i] = SHA256.HashData(Encoding.UTF8.GetBytes(keys[i]));

        this.logger = logger;
    }

    /// <summary>True when callers of admin operations must present a configured key.</summary>
    public bool IsEnforced => keyDigests.Length > 0;

    /// <summary>
    /// Whether <paramref name="presented"/> is one of the configured keys. Always true when the gate is not
    /// enforced. Every configured key is compared, also after a match, so the time does not depend on which one
    /// matched.
    /// </summary>
    public bool IsAuthorized(string? presented)
    {
        if (keyDigests.Length == 0)
            return true;

        if (string.IsNullOrEmpty(presented))
            return false;

        Span<byte> digest = stackalloc byte[SHA256.HashSizeInBytes];
        SHA256.HashData(Encoding.UTF8.GetBytes(presented), digest);

        bool matched = false;
        foreach (byte[] key in keyDigests)
            matched |= CryptographicOperations.FixedTimeEquals(digest, key);

        return matched;
    }

    /// <summary>
    /// Refuses a gRPC admin call with <see cref="StatusCode.Unauthenticated"/> unless it carries a configured key.
    /// </summary>
    public void RequireAdmin(ServerCallContext context)
    {
        if (IsAuthorized(context.RequestHeaders.GetValue(MetadataName)))
            return;

        LogRejected(logger, context.Method, context.Peer);

        throw new RpcException(new(StatusCode.Unauthenticated, "A valid admin key is required."));
    }

    /// <summary>
    /// Whether a REST admin request carries a configured key; logs the refusal when it does not.
    /// </summary>
    public bool Admits(HttpContext httpContext)
    {
        if (IsAuthorized(httpContext.Request.Headers[HeaderName].ToString()))
            return true;

        LogRejected(logger, httpContext.Request.Path.Value ?? "", httpContext.Connection.RemoteIpAddress?.ToString() ?? "unknown");
        return false;
    }

    /// <summary>
    /// Reads the admin keys from <paramref name="path"/>: one key per line, with blank lines and lines that start
    /// with <c>#</c> ignored. An empty path means no keys (the gate is not enforced). A file that cannot be read,
    /// that holds no key, or that holds a key shorter than <see cref="MinKeyLength"/> stops the node, so a typo
    /// cannot leave the admin operations open while the operator believes they are closed.
    /// </summary>
    public static IReadOnlyList<string> LoadKeys(string? path)
    {
        if (string.IsNullOrEmpty(path))
            return [];

        string[] lines;
        try
        {
            lines = File.ReadAllLines(path);
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            throw new KahunaServerException($"AdminApiKeyFile / --admin-api-key-file '{path}' cannot be read: {ex.Message}");
        }

        List<string> keys = new(lines.Length);
        for (int i = 0; i < lines.Length; i++)
        {
            string line = lines[i].Trim();
            if (line.Length == 0 || line[0] == '#')
                continue;

            if (line.Length < MinKeyLength)
                throw new KahunaServerException(
                    $"AdminApiKeyFile / --admin-api-key-file '{path}' line {i + 1} holds a key shorter than {MinKeyLength} characters. " +
                    "Generate one with: openssl rand -base64 32");

            keys.Add(line);
        }

        if (keys.Count == 0)
            throw new KahunaServerException($"AdminApiKeyFile / --admin-api-key-file '{path}' holds no key.");

        return keys;
    }

    private static FrozenSet<string> BuildGrpcAdminMethods()
    {
        (ProtoServiceDescriptor Service, string[] Methods)[] admin =
        [
            (Cluster.Descriptor, ["Leave", "SetReplicationFactor", "SplitRange", "MergeRanges"]),
            (Backups.Descriptor, ["TakeFullBackup", "TakeIncrementalBackup", "TakeCoordinatedBackup", "ValidateChain", "Restore", "RunBackupGarbageCollection"]),
            (KeyValuer.Descriptor, ["RegisterKeyRange", "RemoveKeyRange"])
        ];

        HashSet<string> names = new(StringComparer.Ordinal);
        foreach ((ProtoServiceDescriptor service, string[] methods) in admin)
        {
            foreach (string method in methods)
            {
                MethodDescriptor descriptor = service.FindMethodByName(method)
                    ?? throw new InvalidOperationException($"Admin gRPC method {service.FullName}.{method} does not exist.");

                names.Add($"/{service.FullName}/{descriptor.Name}");
            }
        }

        return names.ToFrozenSet(StringComparer.Ordinal);
    }

    [LoggerMessage(Level = LogLevel.Warning, Message = "Admin operation {Operation} refused from {Peer}: missing or invalid admin key")]
    private static partial void LogRejected(ILogger logger, string operation, string peer);
}

/// <summary>
/// Applies the <see cref="AdminGate"/> to the gRPC admin methods (<see cref="AdminGate.GrpcAdminMethods"/>) and
/// passes every other call through untouched. One interceptor covers all services, so a new admin method is
/// gated by adding its name, not by remembering a check in its handler.
/// </summary>
public sealed class AdminGateInterceptor : Interceptor
{
    private readonly AdminGate gate;

    public AdminGateInterceptor(AdminGate gate)
    {
        this.gate = gate;
    }

    private void Check(ServerCallContext context)
    {
        if (gate.IsEnforced && AdminGate.GrpcAdminMethods.Contains(context.Method))
            gate.RequireAdmin(context);
    }

    public override Task<TResponse> UnaryServerHandler<TRequest, TResponse>(
        TRequest request, ServerCallContext context, UnaryServerMethod<TRequest, TResponse> continuation)
    {
        Check(context);
        return continuation(request, context);
    }

    public override Task<TResponse> ClientStreamingServerHandler<TRequest, TResponse>(
        IAsyncStreamReader<TRequest> requestStream, ServerCallContext context, ClientStreamingServerMethod<TRequest, TResponse> continuation)
    {
        Check(context);
        return continuation(requestStream, context);
    }

    public override Task ServerStreamingServerHandler<TRequest, TResponse>(
        TRequest request, IServerStreamWriter<TResponse> responseStream, ServerCallContext context, ServerStreamingServerMethod<TRequest, TResponse> continuation)
    {
        Check(context);
        return continuation(request, responseStream, context);
    }

    public override Task DuplexStreamingServerHandler<TRequest, TResponse>(
        IAsyncStreamReader<TRequest> requestStream, IServerStreamWriter<TResponse> responseStream, ServerCallContext context, DuplexStreamingServerMethod<TRequest, TResponse> continuation)
    {
        Check(context);
        return continuation(requestStream, responseStream, context);
    }
}

public static class AdminGateServiceCollectionExtensions
{
    /// <summary>
    /// Registers the <see cref="AdminGate"/> for <paramref name="keys"/> (no keys: not enforced) and adds its
    /// interceptor to every gRPC service of the host. The REST admin routes find the gate through
    /// <c>RequireAdminKey</c>.
    /// </summary>
    public static IServiceCollection AddAdminGate(this IServiceCollection services, IReadOnlyList<string> keys)
    {
        services.AddSingleton(provider => new AdminGate(keys, provider.GetRequiredService<ILogger<AdminGate>>()));
        services.AddSingleton<AdminGateInterceptor>();
        services.Configure<GrpcServiceOptions>(static options => options.Interceptors.Add<AdminGateInterceptor>());
        return services;
    }
}
