using Kahuna.Server.ScriptParser;

namespace Kahuna.Server.KeyValues.Transactions.Data;

/// <summary>
/// Reads the string an inspection function was given.
///
/// <para>A string argument is not converted from another type. A number or a boolean passed where a
/// string belongs is a mistake in the script, and converting it would let the mistake reach a stored
/// value instead of the author. This is the rule <c>concat</c>, <c>upper</c> and <c>length</c>
/// already follow.</para>
/// </summary>
internal static class ScriptStringArgument
{
    /// <summary>
    /// Returns the characters of one argument, or raises a script error that names the line.
    ///
    /// <para>A string result whose value is null reads as the empty string, the same as
    /// <c>concat</c> reads it. Null is how an unset value arrives, and every function here answers
    /// the same question about it that it answers about "".</para>
    /// </summary>
    internal static string Require(NodeAst ast, KeyValueExpressionResult argument, string function)
    {
        if (argument.Type != KeyValueExpressionType.StringType)
            throw new KahunaScriptException($"Cannot use '{function}' function on argument {argument.Type}", ast.yyline);

        return argument.StrValue ?? "";
    }
}
