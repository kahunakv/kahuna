using Kommander.Time;

using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.ScriptParser;

namespace Kahuna.Server.KeyValues.Transactions.Functions;

/// <summary>
/// The physical component of the cluster clock, in milliseconds since the unix epoch.
///
/// <para>This is the reading a script compares when the comparison decides an order, an expiry or a
/// deadline. <c>current_time()</c> cannot answer those: it is the wall clock of whichever node ran
/// the script, so two nodes can disagree, and a leader change silently moves the clock the comparison
/// reads.</para>
///
/// <para>The reading is taken once per script execution, so two calls in one script return the same
/// value and <c>hlc_counter()</c> describes the same instant this does.</para>
/// </summary>
internal static class HlcFunction
{
    internal static KeyValueExpressionResult Execute(ScriptTransactionContext context, NodeAst ast, List<KeyValueExpressionResult> arguments)
    {
        if (arguments.Count != 0)
            throw new KahunaScriptException("Invalid number of arguments for 'hlc' function", ast.yyline);

        HLCTimestamp reading = context.GetClockReading(ast);

        return new(reading.L);
    }
}
