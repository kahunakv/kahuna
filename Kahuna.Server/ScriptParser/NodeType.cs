
namespace Kahuna.Server.ScriptParser;

public enum NodeType
{
    Integer,
    String,
    Float,
    Boolean,
    Identifier,
    StmtList,
    Set,
    Get,
    Delete,
    Extend,
    Eset,
    Eget,
    Edelete,
    Eextend,
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
    Add,
    Subtract,
    Mult,
    Div,
    FuncCall,
    ArgumentList,
    SetNotExists,
    SetExists,
    Begin,
    Rollback,
    Commit,
    Return
}