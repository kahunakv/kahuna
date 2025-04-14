
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.ScriptParser;

namespace Kahuna.Server.KeyValues.Transactions.Functions;

internal static class IsDoubleFunction
{
    internal static KeyValueExpressionResult Execute(NodeAst ast, List<KeyValueExpressionResult> arguments)
    {
        if (arguments.Count != 1)
            throw new KahunaScriptException("Invalid number of arguments for 'is_double' function", ast.yyline);
        
        KeyValueExpressionResult argument = arguments[0];

        return new(argument.Type == KeyValueExpressionType.DoubleType);
    }
}