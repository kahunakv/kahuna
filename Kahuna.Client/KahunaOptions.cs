
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
}