using Kahuna.Server.ScriptParser;

namespace Kahuna.Server.KeyValues.Transactions.Data;

/// <summary>
/// Converts an evaluated expression into the whole number a position or a length has to be.
///
/// <para>Array indexing and the string functions accept the same three shapes: a long, a double that
/// holds a whole number, and a string that holds one. Accepting all three is what lets a subscript or
/// a start position be computed by arithmetic, or read out of a stored value, without the author
/// converting it first.</para>
///
/// <para>The conversion lives in one place so that the two callers cannot drift. A second copy would
/// be the natural home of a rule that holds on one side only, and the caller reading the other copy
/// would have no way to see it.</para>
/// </summary>
internal static class ScriptIntegerArgument
{
    /// <summary>
    /// Converts one argument, or raises a script error that names the line.
    /// </summary>
    /// <param name="ast">The node the error reports, for its line number.</param>
    /// <param name="argument">The evaluated argument.</param>
    /// <param name="name">
    /// What the number is, capitalized, for the message: "Index", "Start index" or "Length". The
    /// message says what the author wrote in their own terms, which a generic word cannot.
    /// </param>
    internal static long Require(NodeAst ast, KeyValueExpressionResult argument, string name)
    {
        switch (argument.Type)
        {
            case KeyValueExpressionType.LongType:
                return argument.LongValue;

            case KeyValueExpressionType.DoubleType:
                // A fractional value is refused rather than truncated. Silently reading element 1 for
                // arr[1.9] hides an arithmetic mistake the author would otherwise see immediately.
                double value = argument.DoubleValue;

                if (double.IsNaN(value) || double.IsInfinity(value) || Math.Floor(value) != value)
                    throw new KahunaScriptException(name + " must be a whole number. Found: " + value, ast.yyline);

                if (value < long.MinValue || value > long.MaxValue)
                    throw new KahunaScriptException(name + " is out of range: " + value, ast.yyline);

                return (long)value;

            case KeyValueExpressionType.StringType:
                // An empty string is refused rather than read as zero. It is far more likely to be an unset
                // variable than a deliberate request for the first character.
                if (long.TryParse(argument.StrValue, out long converted))
                    return converted;

                throw new KahunaScriptException(name + " must be an integer: " + argument.Type + " '" + argument.StrValue + "'", ast.yyline);

            default:
                throw new KahunaScriptException(name + " must be an integer. Found: " + argument.Type, ast.yyline);
        }
    }
}
