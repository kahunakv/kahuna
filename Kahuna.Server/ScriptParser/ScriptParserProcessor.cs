
using System.Text;

namespace Kahuna.Server.ScriptParser;

/// <summary>
/// Creates a new parser instance 
/// </summary>
public static class ScriptParserProcessor
{
    public static NodeAst Parse(byte[] script, string? hash)
    {
        scriptParser scriptParser = new();
        return scriptParser.Parse(script, hash);
    }
    
    public static NodeAst Parse(string script)
    {
        scriptParser scriptParser = new();
        return scriptParser.Parse(Encoding.UTF8.GetBytes(script), null);
    }
}