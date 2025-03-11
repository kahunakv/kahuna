
using Grpc.Net.Client;
using System.Collections.Concurrent;
using System.Net.Security;
using Kahuna.Configuration;

namespace Kahuna.Communication.Grpc;

public class SharedChannels
{
    private static readonly ConcurrentDictionary<string, GrpcChannel> channels = new();
    
    private static HttpClientHandler? httpHandler;
    
    public static GrpcChannel GetChannel(string leader, KahunaConfiguration configuration)
    {
        if (!channels.TryGetValue(leader, out GrpcChannel? channel))
        {
            channel = GrpcChannel.ForAddress($"https://{leader}", new() { HttpHandler = GetHandler(configuration) });
            channels.TryAdd(leader, channel);
        }
        
        return channel;
    }
    
    private static HttpClientHandler GetHandler(KahunaConfiguration configuration)
    {
        if (httpHandler is not null)
            return httpHandler;
        
        HttpClientHandler handler = new();

        if (string.IsNullOrEmpty(configuration.HttpsCertificate))
            return handler;
        
        handler.ServerCertificateCustomValidationCallback = (httpRequestMessage, cert, cetChain, policyErrors) =>
        {
            // Optionally, check for other policyErrors
            if (policyErrors == SslPolicyErrors.None)
                return true;

            // Compare the certificate's thumbprint to our trusted thumbprint.
            return cert is not null && cert.Thumbprint.Equals(configuration.HttpsTrustedThumbprint, StringComparison.OrdinalIgnoreCase);
          
            //if (cert is not null)
            //    Console.WriteLine("{0} {1}", cert.Thumbprint, configuration.HttpsTrustedThumbprint);
            //return true;
        };

        httpHandler = handler;
        
        return handler;
    }
}