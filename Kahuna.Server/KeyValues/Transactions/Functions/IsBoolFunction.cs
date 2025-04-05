
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.ScriptParser;

namespace Kahuna.Server.KeyValues.Transactions.Functions;

internal static class IsBoolFunction
{
    internal static KeyValueExpressionResult Execute(NodeAst ast, List<KeyValueExpressionResult> arguments)
    {
        if (arguments.Count != 1)
            throw new KahunaScriptException("Invalid number of arguments for 'is_bool' function", ast.yyline);
        
        KeyValueExpressionResult argument = arguments[0];

        return new() { Type = KeyValueExpressionType.BoolType, BoolValue = argument.Type == KeyValueExpressionType.BoolType };
    }
}