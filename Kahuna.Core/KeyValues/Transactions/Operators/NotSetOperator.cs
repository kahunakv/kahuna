using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.ScriptParser;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues.Transactions.Operators;

/// <summary>
/// NOT SET: true when the last write statement did not take effect — a conditional SET whose condition failed,
/// or a DELETE or EXTEND of a key that does not exist. A SET, DELETE or EXTEND that changed its key is a write
/// that took effect.
/// </summary>
internal static class NotSetOperator
{
    public static KeyValueExpressionResult Eval(ScriptTransactionContext context, NodeAst ast)
    {
        if (context.ModifiedResult is null)
            throw new KahunaScriptException("Invalid NOT SET expression", ast.yyline);

        return KeyValueExpressionResult.FromBool(context.ModifiedResult.Type is not (KeyValueResponseType.Set or KeyValueResponseType.Deleted or KeyValueResponseType.Extended));
    }
}
