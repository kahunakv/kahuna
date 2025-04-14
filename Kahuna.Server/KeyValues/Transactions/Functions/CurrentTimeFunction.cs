
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.ScriptParser;

namespace Kahuna.Server.KeyValues.Transactions.Functions;

internal static class CurrentTimeFunction
{
    internal static KeyValueExpressionResult Execute(NodeAst ast, List<KeyValueExpressionResult> arguments)
    {
        if (arguments.Count != 0)
            throw new KahunaScriptException("Invalid number of arguments for 'current_time' function", ast.yyline);

        return new(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
    }
}