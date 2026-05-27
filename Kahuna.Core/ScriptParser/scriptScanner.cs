namespace Kahuna.Server.ScriptParser;

/// <summary>
/// Scanner for the Script Parser
/// </summary>
internal partial class scriptScanner
{
    private int lastTokenLine;

    private int lastTokenColumn;

    private string? lastTokenText;

    public string? YYError { get; set; }

    public int YYErrorLine { get; private set; }

    public int YYErrorColumn { get; private set; }

    public string? YYErrorToken { get; private set; }

    /// <summary>
    /// Records the current token location for parser error reporting.
    /// </summary>
    /// <param name="line">One-based line number from the generated scanner.</param>
    /// <param name="column">Zero-based column number from the generated scanner.</param>
    /// <param name="length">Token length.</param>
    internal void SetTokenLocation(int line, int column, int length)
    {
        lastTokenLine = line;
        lastTokenColumn = column + 1;
        lastTokenText = yytext;

        int tokenEndColumn = Math.Max(lastTokenColumn, lastTokenColumn + length);
        yylloc = new(lastTokenLine, lastTokenColumn, lastTokenLine, tokenEndColumn);
    }

    /// <summary>
    /// Intercepts the yyerror method
    /// </summary>
    /// <param name="format"></param>
    /// <param name="args"></param>
    public override void yyerror(string format, params object[] args)
    {
        base.yyerror(format, args);

        YYError = string.Format(format, args);
        YYErrorLine = lastTokenLine;
        YYErrorColumn = lastTokenColumn;
        YYErrorToken = lastTokenText;
    }
}
