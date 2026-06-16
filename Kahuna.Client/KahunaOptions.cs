
/**
 * This file is part of Kahuna
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace Kahuna.Client;

/// <summary>
/// Represents configuration options for the Kahuna client.
/// </summary>
public class KahunaOptions
{
    public bool UpgradeUrls { get; set; }

    public int MinConnections { get; set; } = 1;

    public int MaxConnections { get; set; } = 1;

    /// <summary>
    /// Maximum time to wait for a batched operation to complete when the caller supplies no
    /// <see cref="CancellationToken"/>.  If the server does not respond within this window the
    /// operation is cancelled and a <see cref="OperationCanceledException"/> is surfaced to the
    /// caller.  Set to <see cref="System.Threading.Timeout.InfiniteTimeSpan"/> to disable the
    /// default deadline (restores the pre-CT3 hang-forever behaviour).
    /// </summary>
    public TimeSpan DefaultOperationTimeout { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Number of gRPC channels (and matching streaming pairs) created per server URL.
    /// A higher value increases parallelism at the cost of additional connections.
    /// Must be at least 1; defaults to 2.
    /// </summary>
    public int GrpcChannelPoolSize { get; set; } = 2;

    /// <summary>
    /// When a batch has fewer items than this threshold, the batcher waits
    /// <see cref="BatchCoalescingDelayMs"/> milliseconds before dispatching to allow more requests
    /// to accumulate (higher throughput, slightly higher latency).  Set to 0 or 1 to disable
    /// coalescing entirely.  Defaults to 10.
    /// </summary>
    public int BatchCoalescingThreshold { get; set; } = 10;

    /// <summary>
    /// Maximum coalescing delay in milliseconds applied when a batch is smaller than
    /// <see cref="BatchCoalescingThreshold"/>.  The actual delay is a random value in
    /// [1, <c>BatchCoalescingDelayMs</c>].  Set to 0 to disable the delay while keeping
    /// the threshold check.  Defaults to 2.
    /// </summary>
    public int BatchCoalescingDelayMs { get; set; } = 2;

    /// <summary>
    /// When <see langword="true"/>, TLS server certificate validation is skipped entirely.
    /// Intended for development and local testing only — leaves connections open to MITM attacks.
    /// </summary>
    public bool AllowInsecureCertificateValidation { get; set; }

    /// <summary>
    /// When non-empty, only server certificates whose SHA-256 thumbprint (hex, case-insensitive)
    /// matches one of these values are accepted.  An empty list falls back to standard OS chain
    /// and hostname validation.  Ignored when <see cref="AllowInsecureCertificateValidation"/> is
    /// <see langword="true"/>.
    /// </summary>
    public IReadOnlyList<string> TrustedServerCertificateThumbprints { get; set; } = [];
}