
using System.Text;
using Kahuna.Server.Configuration;

namespace Kahuna.Server.ScriptParser;

/// <summary>
/// Creates a new parser instance 
/// </summary>
public static class ScriptParserProcessor
{
    /// <summary>
    /// Parses a byte[] script and returns its AST
    /// </summary>
    /// <param name="script"></param>
    /// <param name="hash"></param>
    /// <param name="configuration"></param>
    /// <param name="logger"></param>
    /// <returns></returns>
    public static NodeAst Parse(ReadOnlySpan<byte> script, string? hash, KahunaConfiguration configuration, ILogger<IKahuna> logger)
    {
        scriptParser scriptParser = new(configuration, logger);
        return scriptParser.Parse(script, hash);
    }
    
    /// <summary>
    /// Parses a string script and returns its AST
    /// </summary>
    /// <param name="script"></param>
    /// <param name="configuration"></param>
    /// <param name="logger"></param>
    /// <returns></returns>
    public static NodeAst Parse(string script, KahunaConfiguration configuration, ILogger<IKahuna> logger)
    {
        scriptParser scriptParser = new(configuration, logger);
        
        Span<byte> scriptBytes = stackalloc byte[Encoding.UTF8.GetByteCount(script)];
        Encoding.UTF8.GetBytes(script.AsSpan(), scriptBytes);
        
        return scriptParser.Parse(scriptBytes, null);
    }
}