
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
    private static readonly DefaultObjectPoolProvider ScriptPoolProvider = new();

    private readonly ObjectPool<scriptParser> scriptParserPool;

    /// <summary>
    /// Processes script parsing operations using a pool of reusable script parsers.
    /// </summary>
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
    /// Parses a string script and returns its AST.
    ///
    /// <para>The text is handed to the scanner as it stands. This used to encode the script to UTF-8 bytes so
    /// it could call the byte overload, which decoded those same bytes straight back into a string, so every
    /// in-process parse paid for an encode, a decode and a string it threw away. Nothing else was gained,
    /// because this overload does not use the cache the byte overload keys by hash.</para>
    /// </summary>
    public NodeAst Parse(string script)
    {
        scriptParser scriptParser = scriptParserPool.Get();

        try
        {
            return scriptParser.Parse(script);
        }
        finally
        {
            scriptParserPool.Return(scriptParser);
        }
    }
}