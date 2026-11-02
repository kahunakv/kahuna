using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.ScriptParser;

namespace Kahuna.Server.KeyValues.Transactions.Functions;

/// <summary>
/// The numeric limits of the script language: 'max_int', 'min_int', 'max_double' and 'min_double'.
///
/// <para>An arithmetic result outside these limits is a script error, so a script that wants another
/// outcome (a cap, a reset, its own error) checks the value against a limit before the operation. Two of
/// the limits have no practical literal: the smallest integer has none, because a minus sign is an
/// operator and its magnitude is one above the largest integer, and the largest double needs all 309
/// digits, because a float literal has no exponent form.</para>
///
/// <para>'min_double' is the most negative double, as <see cref="double.MinValue"/> is in .NET. It is not
/// the smallest positive double, which is what the same name means in C and Java.</para>
///
/// <para>Each result is one shared instance. A result carries no revision or expiry and is immutable, so
/// sharing it is safe, and a call allocates nothing.</para>
/// </summary>
internal static class NumericLimitFunctions
{
    private static readonly KeyValueExpressionResult MaxInt = new(long.MaxValue);

    private static readonly KeyValueExpressionResult MinInt = new(long.MinValue);

    private static readonly KeyValueExpressionResult MaxDouble = new(double.MaxValue);

    private static readonly KeyValueExpressionResult MinDouble = new(double.MinValue);

    internal static KeyValueExpressionResult ExecuteMaxInt(NodeAst ast, List<KeyValueExpressionResult> arguments) =>
        NoArguments(ast, arguments, "max_int", MaxInt);

    internal static KeyValueExpressionResult ExecuteMinInt(NodeAst ast, List<KeyValueExpressionResult> arguments) =>
        NoArguments(ast, arguments, "min_int", MinInt);

    internal static KeyValueExpressionResult ExecuteMaxDouble(NodeAst ast, List<KeyValueExpressionResult> arguments) =>
        NoArguments(ast, arguments, "max_double", MaxDouble);

    internal static KeyValueExpressionResult ExecuteMinDouble(NodeAst ast, List<KeyValueExpressionResult> arguments) =>
        NoArguments(ast, arguments, "min_double", MinDouble);

    private static KeyValueExpressionResult NoArguments(
        NodeAst ast,
        List<KeyValueExpressionResult> arguments,
        string name,
        KeyValueExpressionResult value
    )
    {
        if (arguments.Count != 0)
            throw new KahunaScriptException($"Invalid number of arguments for '{name}' function", ast.yyline);

        return value;
    }
}
