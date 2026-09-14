using Kommander.Time;

using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.ScriptParser;

namespace Kahuna.Server.KeyValues.Transactions.Functions;

/// <summary>
/// The logical counter of the same clock reading <see cref="HlcFunction"/> returns.
///
/// <para>The counter separates events that fall in one millisecond. It resets to zero whenever the
/// physical component advances, so it orders events only together with that component, never on its
/// own.</para>
/// </summary>
internal static class HlcCounterFunction
{
    internal static KeyValueExpressionResult Execute(ScriptTransactionContext context, NodeAst ast, List<KeyValueExpressionResult> arguments)
    {
        if (arguments.Count != 0)
            throw new KahunaScriptException("Invalid number of arguments for 'hlc_counter' function", ast.yyline);

        HLCTimestamp reading = context.GetClockReading(ast);

        return new((long)reading.C);
    }
}
