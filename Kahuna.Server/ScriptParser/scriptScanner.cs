
namespace Kahuna.Server.ScriptParser;

/// <summary>
/// Scanner for the Script Parser
/// </summary>
internal partial class scriptScanner
{
    public string? YYError { get; set; }    

    /// <summary>
    /// Intercepts the yyerror method
    /// </summary>
    /// <param name="format"></param>
    /// <param name="args"></param>
    public override void yyerror(string format, params object[] args)
    {
        base.yyerror(format, args);

        YYError = string.Format(format, args);		
    }
}