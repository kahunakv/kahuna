using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.ScriptParser;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues.Transactions.Operators;

/// <summary>
/// NOT EXTENDED: true when the last EXTEND or EEXTEND statement extended nothing — the key did not exist. Only an
/// extend decides it, so a SET, a DELETE or a read between the extend and the guard does not change the answer.
/// </summary>
internal static class NotExtendedOperator
{
    public static KeyValueExpressionResult Eval(ScriptTransactionContext context, NodeAst ast)
    {
        if (context.LastExtendType is not { } type)
            throw new KahunaScriptException("Invalid NOT EXTENDED expression", ast.yyline);

        return KeyValueExpressionResult.FromBool(type != KeyValueResponseType.Extended);
    }
}
