using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.ScriptParser;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues.Transactions.Operators;

/// <summary>
/// NOT DELETED: true when the last DELETE or EDELETE statement deleted nothing — the key did not exist. Only a
/// delete decides it, so a SET, an EXTEND or a read between the delete and the guard does not change the answer.
/// </summary>
internal static class NotDeletedOperator
{
    public static KeyValueExpressionResult Eval(ScriptTransactionContext context, NodeAst ast)
    {
        if (context.LastDeleteType is not { } type)
            throw new KahunaScriptException("Invalid NOT DELETED expression", ast.yyline);

        return KeyValueExpressionResult.FromBool(type != KeyValueResponseType.Deleted);
    }
}
