
namespace Kahuna.Server.ScriptParser;

/// <summary>
/// Creates a new parser instance 
/// </summary>
public static class ScriptParserProcessor
{
    public static NodeAst Parse(string sql)
    {
        scriptParser scriptParser = new();
        return scriptParser.Parse(sql);
    }
}