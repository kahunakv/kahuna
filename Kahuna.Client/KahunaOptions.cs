
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
}