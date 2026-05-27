
namespace Kahuna.Server.ScriptParser;

/// <summary>
/// Represents an exception that occurs during the execution or parsing of a Kahuna script.
/// </summary>
public sealed class KahunaScriptException: Exception
{
    /// <summary>
    /// Gets the line number in the Kahuna script where the exception occurred.
    /// </summary>
    public int Line { get; }

    /// <summary>
    /// Gets the column number in the Kahuna script where the exception occurred.
    /// </summary>
    public int Column { get; }

    /// <summary>
    /// Represents an exception that occurs during the execution or parsing of a Kahuna script.
    /// </summary>
    public KahunaScriptException(string message, int line) : base(message)
    {
        Line = line;
    }

    /// <summary>
    /// Represents an exception that occurs during the execution or parsing of a Kahuna script.
    /// </summary>
    public KahunaScriptException(string message, int line, int column) : base(message)
    {
        Line = line;
        Column = column;
    }
}
