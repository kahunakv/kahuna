
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
    /// <remarks>
    /// The client's task is handed back directly rather than awaited: nothing happens here after the
    /// call, so an <c>async</c> wrapper would add a second task and state machine for no work. No
    /// guard is needed either, because the method being called reports a synchronous failure through
    /// its returned task already.
    /// </remarks>
    public Task<KahunaKeyValueTransactionResult> Run(List<KeyValueParameter>? parameters = null, CancellationToken cancellationToken = default)
    {
        return kahunaClient.ExecuteKeyValueTransactionScript(script, hash, parameters, cancellationToken);
    }

    /// <summary>
    /// Executes the Kahuna transaction script at the given admission priority, which decides how the server
    /// orders this transaction against others when it is at its concurrency ceiling.
    /// </summary>
    /// <param name="priority">Relative importance of this execution. Has no effect below the server's ceiling.</param>
    /// <param name="parameters">A list of key-value parameters for the transaction script. This parameter is optional.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>A task representing the asynchronous operation. The task result contains the result of the Kahuna key-value transaction.</returns>
    public Task<KahunaKeyValueTransactionResult> Run(
        TransactionPriority priority,
        List<KeyValueParameter>? parameters = null,
        CancellationToken cancellationToken = default
    )
    {
        return kahunaClient.ExecuteKeyValueTransactionScript(script, hash, parameters, priority, cancellationToken);
    }
}