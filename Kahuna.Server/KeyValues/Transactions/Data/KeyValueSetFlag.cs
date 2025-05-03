
using Kahuna.Server.ScriptParser;

namespace Kahuna.Server.KeyValues.Transactions.Data;

public sealed class KeyValueSetFlag
{
    public NodeType NodeType { get; }
    
    public NodeAst? ExprAst { get; }
    
    public KeyValueSetFlag(NodeType nodeType, NodeAst? exprAst)
    {
        NodeType = nodeType;
        ExprAst = exprAst;
    }
}