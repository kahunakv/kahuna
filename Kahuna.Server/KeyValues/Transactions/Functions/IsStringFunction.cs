
using Kahuna.Server.ScriptParser;
using Kahuna.Server.KeyValues.Transactions.Data;

namespace Kahuna.Server.KeyValues.Transactions.Functions;

internal static class IsStringFunction
{
    internal static KeyValueExpressionResult Execute(NodeAst ast, List<KeyValueExpressionResult> arguments)
    {
        if (arguments.Count != 1)
            throw new KahunaScriptException("Invalid number of arguments for 'is_string' function", ast.yyline);
        
        KeyValueExpressionResult argument = arguments[0];

        return new(argument.Type == KeyValueExpressionType.StringType);
    }
}