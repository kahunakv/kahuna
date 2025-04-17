
using System.Text;
using Kahuna.Server.Configuration;
using Microsoft.Extensions.ObjectPool;

namespace Kahuna.Server.ScriptParser;

/// <summary>
/// Parses Kahuna Scripts
///
/// A pool of script parsers is created to avoid the overhead of creating a new parser for each script.
/// </summary>
internal sealed class ScriptParserProcessor
{
    private static readonly DefaultObjectPoolProvider ScriptPoolProvider = new() { MaximumRetained = 32 };

    private readonly ObjectPool<scriptParser> scriptParserPool;
    
    public ScriptParserProcessor(KahunaConfiguration configuration, ILogger<IKahuna> logger)
    {
         scriptParserPool = ScriptPoolProvider.Create(new ScriptParserObjectPolicy(configuration, logger));
    }   
    
    /// <summary>
    /// Parses a byte[] script and returns its AST
    /// </summary>
    /// <param name="script"></param>
    /// <param name="hash"></param>    
    /// <returns></returns>
    public NodeAst Parse(ReadOnlySpan<byte> script, string? hash)
    {
        scriptParser scriptParser = scriptParserPool.Get();

        try
        {
            return scriptParser.Parse(script, hash);
        }
        finally
        {
            scriptParserPool.Return(scriptParser);
        }                
    }
    
    /// <summary>
    /// Parses a string script and returns its AST
    /// </summary>
    /// <param name="script"></param>    
    /// <returns></returns>
    public NodeAst Parse(string script)
    {
        scriptParser scriptParser = scriptParserPool.Get();

        try
        {        
            Span<byte> scriptBytes = stackalloc byte[Encoding.UTF8.GetByteCount(script)];
            Encoding.UTF8.GetBytes(script.AsSpan(), scriptBytes);
            
            return scriptParser.Parse(scriptBytes, null);
        }
        finally
        {
            scriptParserPool.Return(scriptParser);
        } 
    }
}