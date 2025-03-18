
namespace Kahuna.Server.ScriptParser;

public enum NodeType
{
    StmtList,
    Integer,
    String,
    Identifier,
    Set,
    Get,
    Eset,
    Eget,
    If,
    Equals,
    NotEquals,
    LessThan,
    GreaterThan,
    LessThanEquals,
    GreaterThanEquals,
    And,
    Or,
    Not,
    SetNotExists,
    SetExists
}