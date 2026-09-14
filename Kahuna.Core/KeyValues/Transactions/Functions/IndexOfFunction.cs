using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.ScriptParser;

namespace Kahuna.Server.KeyValues.Transactions.Functions;

/// <summary>
/// The position of the first occurrence of one string inside another, counted from zero, or -1 when
/// it does not occur. Ordinal, for the reason stated on <see cref="StartsWithFunction"/>.
///
/// <para>A missing needle returns -1 rather than raising an error, because "is it there, and where"
/// is one question and a script asks it to branch. An empty needle occurs at position 0, which is the
/// answer that keeps <c>index_of(s, x) &gt;= 0</c> equivalent to "s contains x" for every x.</para>
/// </summary>
internal static class IndexOfFunction
{
    internal static KeyValueExpressionResult Execute(NodeAst ast, List<KeyValueExpressionResult> arguments)
    {
        if (arguments.Count != 2)
            throw new KahunaScriptException("Invalid number of arguments for 'index_of' function", ast.yyline);

        string source = ScriptStringArgument.Require(ast, arguments[0], "index_of");
        string needle = ScriptStringArgument.Require(ast, arguments[1], "index_of");

        return new((long)source.AsSpan().IndexOf(needle.AsSpan(), StringComparison.Ordinal));
    }
}
