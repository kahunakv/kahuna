
using System.Text;

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
    /// <param name="logger"></param>
    /// <returns></returns>
    public static NodeAst Parse(byte[] script, string? hash, ILogger<IKahuna> logger)
    {
        scriptParser scriptParser = new(logger);
        return scriptParser.Parse(script, hash);
    }
    
    /// <summary>
    /// Parses a string script and returns its AST
    /// </summary>
    /// <param name="script"></param>
    /// <param name="logger"></param>
    /// <returns></returns>
    public static NodeAst Parse(string script, ILogger<IKahuna> logger)
    {
        scriptParser scriptParser = new(logger);
        return scriptParser.Parse(Encoding.UTF8.GetBytes(script), null);
    }
}