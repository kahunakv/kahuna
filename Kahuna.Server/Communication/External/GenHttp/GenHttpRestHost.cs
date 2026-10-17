
using System.Net;

using GenHTTP.Api.Infrastructure;

using Microsoft.Extensions.Options;

namespace Kahuna.Server.Communication.External.GenHttp;

/// <summary>
/// Runs the GenHTTP listener beside Kestrel. Kestrel keeps every listener it already has, gRPC and the
/// Raft transport included: the GenHTTP internal engine speaks HTTP/1.1 only and writes no trailers,
/// so it cannot carry gRPC.
/// </summary>
internal static class GenHttpRestHost
{
    /// <summary>
    /// Starts the listener when <c>--genhttp-port</c> is set. Call it after the node's services are
    /// started, because the handler resolves <see cref="IKahuna"/> at once.
    /// </summary>
    public static async Task<IServerHost?> StartAsync(WebApplication app, KahunaCommandLineOptions opts)
    {
        if (opts.GenHttpPort <= 0)
            return null;

        if (opts.GenHttpPort > ushort.MaxValue)
            throw new KahunaServerException($"--genhttp-port {opts.GenHttpPort} is not a valid port");

        ILoggerFactory loggerFactory = app.Services.GetRequiredService<ILoggerFactory>();

        // The minimal API's options, resolver chain included, so both listeners write identical JSON.
        Microsoft.AspNetCore.Http.Json.JsonOptions jsonOptions =
            app.Services.GetRequiredService<IOptions<Microsoft.AspNetCore.Http.Json.JsonOptions>>().Value;

        GenHttpRestHandler handler = new(
            app.Services.GetRequiredService<IKahuna>(),
            jsonOptions.SerializerOptions,
            loggerFactory.CreateLogger("Kahuna.GenHttp"));

        IServerHost host = opts.GenHttpEngine switch
        {
            "internal" => GenHTTP.Engine.Internal.Host.Create(),
            "ioxide" => GenHTTP.Engine.Ioxide.Host.Create(),
            _ => throw new KahunaServerException($"--genhttp-engine '{opts.GenHttpEngine}' is not internal or ioxide")
        };

        // Per-request logging is off for the same reason Program.cs filters Kestrel's request logs.
        host.Bind(IPAddress.Any, (ushort)opts.GenHttpPort)
            .Handler(handler)
            .Logging(loggerFactory, logRequests: false);

        await host.StartAsync();

        if (app.Logger.IsEnabled(LogLevel.Information))
            app.Logger.LogInformation("GenHTTP listener (experimental, HTTP/1.1, {Engine} engine) bound on port {Port}", opts.GenHttpEngine, opts.GenHttpPort);

        return host;
    }
}
