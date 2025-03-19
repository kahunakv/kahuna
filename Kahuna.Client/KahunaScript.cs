
using System.Text;

namespace Kahuna.Client;

public class KahunaScript
{
    private readonly KahunaClient kahunaClient;

    private readonly byte[] script;

    private readonly string hash;
    
    /// <summary>
    /// Constructor
    /// </summary>
    /// <param name="kahunaClient"></param>
    /// <param name="script"></param>
    public KahunaScript(KahunaClient kahunaClient, string script)
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
        return await kahunaClient.ExecuteKeyValueTransaction(script, hash).ConfigureAwait(false);
    }
}