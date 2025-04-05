using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.ScriptParser;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues.Transactions.Operators;

internal static class NotSetOperator
{
    public static KeyValueExpressionResult Eval(KeyValueTransactionContext context, NodeAst ast)
    {
        if (context.Result is null)
            throw new KahunaScriptException("Invalid NOT SET expression", ast.yyline);
        
        return new() { Type = KeyValueExpressionType.BoolType, BoolValue = context.Result.Type != KeyValueResponseType.Set };
    }
}