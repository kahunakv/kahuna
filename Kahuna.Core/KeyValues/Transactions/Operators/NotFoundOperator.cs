using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.ScriptParser;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues.Transactions.Operators;

/// <summary>
/// NOT FOUND: true when the last read statement did not find what it read. Only reads decide it, so a LET,
/// a write or an expression between the read and the guard does not change the answer. A GET that finds its
/// key answers Get and an EXISTS that finds its key answers Exists; both are found.
/// </summary>
internal static class NotFoundOperator
{
    public static KeyValueExpressionResult Eval(ScriptTransactionContext context, NodeAst ast)
    {
        if (context.LastReadType is not { } type)
            throw new KahunaScriptException("Invalid NOT FOUND expression", ast.yyline);

        return KeyValueExpressionResult.FromBool(type is not (KeyValueResponseType.Get or KeyValueResponseType.Exists));
    }
}
