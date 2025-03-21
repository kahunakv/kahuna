
namespace Kahuna.Server.ScriptParser;

public sealed class KahunaScriptException: Exception
{
    public int Line { get; }
    
    public KahunaScriptException(string message, int line) : base(message)
    {
        Line = line;
    }
}