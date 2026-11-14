
using Kommander;
using Kahuna.Server.KeyValues.Data;
using Kommander.Time;
using Kommander.Communication.Grpc;
using Microsoft.Extensions.Logging;

using Google.Protobuf;
using Grpc.Core;
using Grpc.Net.Client;
using System.Collections.Concurrent;
using Google.Protobuf.Collections;

using Kahuna.Shared.Locks;
using Kahuna.Shared.Sequences;
using Kahuna.Shared.KeyValue;
using Kahuna.Shared.Communication.Grpc;
using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues;
using Kahuna.Server.Locks;
using Kahuna.Server.Communication.Internode.Grpc;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.KeyValues.Writes;
using Kahuna.Server.Locks.Data;
using Kahuna.Server.Persistence.Pitr;

namespace Kahuna.Server.Communication.Internode;

/// <summary>
/// Provides gRPC-based inter-node communication functionalities within the Kahuna Server.
/// This implementation facilitates distributed locking, key-value operations, and other
/// inter-node coordination mechanisms.
/// </summary>
public partial class GrpcInterNodeCommunication : IInterNodeCommunication
{
    private readonly ConcurrentDictionary<string, Lazy<GrpcServerBatcher>> batchers = new();

    private readonly ConcurrentDictionary<string, TransportFailureLogGate> transportFailureLogGates = new();

    private readonly KahunaConfiguration configuration;

    private readonly RaftTransportSecurityOptions transportSecurity;

    private readonly ILogger<GrpcInterNodeCommunication> logger;

    /// <param name="configuration">Kahuna configuration.</param>
    /// <param name="transportSecurity">
    /// The effective options of this node's <c>RaftConfiguration</c>. Kahuna dials the same URLs Kommander
    /// does, and <see cref="SharedChannels"/> builds each URL's pool once from the first caller's options,
    /// so any other options would strip the client certificate from Raft traffic to that peer as well.
    /// </param>
    /// <param name="logger">Logger.</param>
    public GrpcInterNodeCommunication(KahunaConfiguration configuration, RaftTransportSecurityOptions transportSecurity, ILogger<GrpcInterNodeCommunication> logger)
    {
        this.configuration = configuration;
        this.transportSecurity = transportSecurity;
        this.logger = logger;
    }

    /// <summary>The options every channel this transport opens is built with.</summary>
    internal RaftTransportSecurityOptions TransportSecurity => transportSecurity;

    // Qualified: the Kahuna.Server.Sequencer namespace would otherwise shadow the generated client.
    private global::Sequencer.SequencerClient GetSequencerClient(string node)
    {
        return new(GetSequencerChannel(node));
    }

    internal GrpcChannel GetSequencerChannel(string node) =>
        SharedChannels.GetChannel(ResolveNodeUrl(node), transportSecurity);

    /// <summary>
    /// Qualifies a bare peer endpoint (host:port) with the configured inter-node gRPC scheme.
    /// Without this, the shared channel pool defaults bare endpoints to https, which pins
    /// Kahuna's inter-node traffic to TLS even when the cluster dials its Raft channels over
    /// cleartext HTTP/2. An endpoint that already carries a scheme passes through unchanged.
    /// </summary>
    private string ResolveNodeUrl(string node) =>
        node.StartsWith("https://", StringComparison.Ordinal) || node.StartsWith("http://", StringComparison.Ordinal)
            ? node
            : configuration.InterNodeGrpcScheme + node;

    internal GrpcServerBatcher GetSharedBatcher(string url)
    {
        Lazy<GrpcServerBatcher> lazyBatcher = batchers.GetOrAdd(ResolveNodeUrl(url), static (u, self) => self.GetSharedBatchers(u), this);
        return lazyBatcher.Value;
    }

    private Lazy<GrpcServerBatcher> GetSharedBatchers(string url)
    {
        return new(() => new(url, transportSecurity, logger));
    }

    /// <summary>
    /// True for transport failures that mean the remote node was unreachable or stopped answering —
    /// killed, partitioned away, or too slow — where surfacing MustRetry lets the caller re-route.
    /// Other status codes (e.g. an application error on the remote) still propagate as exceptions.
    /// </summary>
    private static bool IsRetryableTransportFailure(RpcException ex) =>
        InterNodeTransportFailure.IsRetryable(ex);

    /// <summary>
    /// The single exit every batched forward awaits through. The batcher settles a pending request with the
    /// raw transport exception when its stream to the peer cannot be opened, is torn down mid-flight, or
    /// stays quiet past the request deadline; here that exception becomes an empty answer so the calling
    /// forward can return the operation's own typed <c>MustRetry</c> instead of leaking the transport into
    /// the embedding application. A refused connection sends nothing, so the operation demonstrably did not
    /// run; a stream that died after the write is indeterminate, which is exactly what <c>MustRetry</c>
    /// means to every caller (the retried operation re-resolves the leader and is idempotent or consults
    /// the durable decision). Any other failure keeps propagating unchanged.
    /// </summary>
    private async Task<GrpcServerBatcherResponse> ForwardAsync(Task<GrpcServerBatcherResponse> pending, string operation, string node)
    {
        try
        {
            return await pending.ConfigureAwait(false);
        }
        catch (RpcException ex) when (IsRetryableTransportFailure(ex))
        {
            LogForwardingTransportFailure(operation, node, ex.StatusCode);

            return default;
        }
    }

    /// <summary>
    /// Logs a transport failure at most once per <see cref="TransportFailureLogQuietMs"/> per peer, carrying
    /// the number of forwards to that peer that failed silently since the previous line. Under load a dead
    /// leader fails thousands of forwards per second for the seconds the placement takes to move; a line
    /// per forward was itself an operational problem, and the batcher already logs each stream eviction.
    /// </summary>
    private void LogForwardingTransportFailure(string operation, string node, StatusCode statusCode)
    {
        TransportFailureLogGate gate = transportFailureLogGates.GetOrAdd(node, static _ => new());

        long now = Environment.TickCount64;
        long last = Volatile.Read(ref gate.LastLogTicks);

        if (now - last < TransportFailureLogQuietMs || Interlocked.CompareExchange(ref gate.LastLogTicks, now, last) != last)
        {
            Interlocked.Increment(ref gate.Suppressed);
            return;
        }

        long suppressed = Interlocked.Exchange(ref gate.Suppressed, 0);

        LogForwardingTransportFailure(logger, operation, node, statusCode, suppressed);
    }

    /// <summary>Minimum spacing between two transport-failure log lines for the same peer.</summary>
    internal const int TransportFailureLogQuietMs = 1000;

    private sealed class TransportFailureLogGate
    {
        // Far enough in the past that the first failure always logs, without overflowing the subtraction.
        public long LastLogTicks = long.MinValue / 2;

        public long Suppressed;
    }

    [LoggerMessage(Level = LogLevel.Warning, Message = "{Operation} forwarding to {Node} failed with transport status {StatusCode}, returning MustRetry ({Suppressed} more forwards to this node failed the same way since the previous line)")]
    private static partial void LogForwardingTransportFailure(ILogger<GrpcInterNodeCommunication> logger, string operation, string node, StatusCode statusCode, long suppressed);
}
