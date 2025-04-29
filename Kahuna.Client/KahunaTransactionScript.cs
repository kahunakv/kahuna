
/**
 * This file is part of Kahuna
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Client;

/// <summary>
/// Represents a transaction script that can be executed on a KahunaClient instance.
/// </summary>
public class KahunaTransactionScript
{
    /// <summary>
    /// Represents an instance of the Kahuna client used to interact with the Kahuna
    /// service for performing various operations such as executing transaction scripts,
    /// handling locks, and managing resources.
    /// </summary>
    private readonly KahunaClient kahunaClient;

    /// <summary>
    /// Represents the script content for a Kahuna transaction script, stored as a byte array.
    /// </summary>
    private readonly byte[] script;

    /// <summary>
    /// Represents the hash value of the transaction script generated using Blake3 hashing algorithm.
    /// This hash is used for identifying the script and ensuring its integrity during execution.
    /// </summary>
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
    /// Executes the Kahuna transaction script.
    /// </summary>
    /// <param name="parameters">A list of key-value parameters for the transaction script. This parameter is optional.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>A task representing the asynchronous operation. The task result contains the result of the Kahuna key-value transaction.</returns>
    public async Task<KahunaKeyValueTransactionResult> Run(List<KeyValueParameter>? parameters = null, CancellationToken cancellationToken = default)
    {
        return await kahunaClient.ExecuteKeyValueTransactionScript(script, hash, parameters, cancellationToken).ConfigureAwait(false);
    }
}