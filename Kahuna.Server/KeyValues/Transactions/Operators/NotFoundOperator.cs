using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.ScriptParser;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues.Transactions.Operators;

internal static class NotFoundOperator
{
    public static KeyValueExpressionResult Eval(KeyValueTransactionContext context, NodeAst ast)
    {
        if (context.Result is null)
            throw new KahunaScriptException("Invalid NOT FOUND expression", ast.yyline);
        
        return new(context.Result.Type != KeyValueResponseType.Get);
    }
}