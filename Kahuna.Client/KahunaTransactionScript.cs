
/**
 * This file is part of Kahuna
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text;

namespace Kahuna.Client;

/// <summary>
/// Represents a transaction script that can be executed on a KahunaClient instance.
/// </summary>
public class KahunaTransactionScript
{
    private readonly KahunaClient kahunaClient;

    private readonly byte[] script;

    private readonly string hash;
    
    /// <summary>
    /// Constructor
    /// </summary>
    /// <param name="kahunaClient"></param>
    /// <param name="script"></param>
    public KahunaTransactionScript(KahunaClient kahunaClient, string script)
    {
        this.kahunaClient = kahunaClient;
        this.script = Encoding.UTF8.GetBytes(script);
        this.hash = Blake3.Hasher.Hash(this.script).ToString();
    }
    
    /// <summary>
    /// Executes the loaded script
    /// </summary>
    /// <returns></returns>
    public async Task<KahunaKeyValueTransactionResult> Run()
    {
        return await kahunaClient.ExecuteKeyValueTransactionScript(script, hash).ConfigureAwait(false);
    }
}