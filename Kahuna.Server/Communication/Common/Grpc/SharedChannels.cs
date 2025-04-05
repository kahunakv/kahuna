
using Grpc.Net.Client;
using System.Collections.Concurrent;
using System.Net.Security;
using Kahuna.Server.Configuration;

namespace Kahuna.Communication.Common.Grpc;

internal static class SharedChannels
{
    private static readonly ConcurrentDictionary<string, GrpcChannel> channels = new();
    
    private static SocketsHttpHandler? httpHandler;
    
    public static GrpcChannel GetChannel(string leader, KahunaConfiguration configuration)
    {
        if (!channels.TryGetValue(leader, out GrpcChannel? channel))
        {
            channel = GrpcChannel.ForAddress($"https://{leader}", new()
            {
                HttpHandler = GetHandler(configuration),
            });
            
            channels.TryAdd(leader, channel);
        }
        
        return channel;
    }
    
    private static SocketsHttpHandler GetHandler(KahunaConfiguration configuration)
    {
        if (httpHandler is not null)
            return httpHandler;
        
        SslClientAuthenticationOptions sslOptions = new()
        {
            RemoteCertificateValidationCallback = delegate { return true; }
        };
        
        SocketsHttpHandler handler = new()
        {
            SslOptions = sslOptions,
            ConnectTimeout = TimeSpan.FromSeconds(10),
            PooledConnectionIdleTimeout = Timeout.InfiniteTimeSpan,
            KeepAlivePingDelay = TimeSpan.FromSeconds(60),
            KeepAlivePingTimeout = TimeSpan.FromSeconds(30),
            EnableMultipleHttp2Connections = true
        };

        httpHandler = handler;

        return handler;
    }
}