
using Kahuna.Server.ScriptParser;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.KeyValues.Transactions.Commands;

namespace Kahuna.Server.KeyValues;

/// <summary>
/// Provides functionality to assist with acquiring the necessary locks for key-value transactions.
/// Traverses the AST looking for operations that require locks.
/// </summary>
internal sealed class KeyValueLockHelper : BaseCommand
{
    /// <summary>
    /// Obtains that must be acquired in advance to start the transaction
    /// </summary>
    /// <param name="context"></param>
    /// <param name="ast"></param>
    /// <param name="ephemeralLocks"></param>
    /// <param name="persistentLocks"></param>
    /// <exception cref="KahunaScriptException"></exception>
    /// <exception cref="ArgumentOutOfRangeException"></exception>
    internal static void GetLocksToAcquire(
        KeyValueTransactionContext context, 
        NodeAst ast, 
        HashSet<string> ephemeralLocks, 
        HashSet<string> persistentLocks,
        HashSet<string> ephemeralPrefixLocksToAcquire,
        HashSet<string> persistentPrefixLocksToAcquire
    )
    {
        while (true)
        {
            //Console.WriteLine("AST={0}", ast.nodeType);
            
            switch (ast.nodeType)
            {
                case NodeType.StmtList:
                {
                    if (ast.leftAst is not null) 
                        GetLocksToAcquire(context, ast.leftAst, ephemeralLocks, persistentLocks, ephemeralPrefixLocksToAcquire, persistentPrefixLocksToAcquire);

                    if (ast.rightAst is not null)
                    {
                        ast = ast.rightAst!;
                        continue;
                    }

                    break;
                }
                
                case NodeType.Begin:
                    if (ast.leftAst is not null) 
                        GetLocksToAcquire(context, ast.leftAst, ephemeralLocks, persistentLocks, ephemeralPrefixLocksToAcquire, persistentPrefixLocksToAcquire);
                    break;
                
                case NodeType.If:
                {
                    if (ast.rightAst is not null) 
                        GetLocksToAcquire(context, ast.rightAst, ephemeralLocks, persistentLocks, ephemeralPrefixLocksToAcquire, persistentPrefixLocksToAcquire);
                    
                    if (ast.extendedOne is not null) 
                        GetLocksToAcquire(context, ast.extendedOne, ephemeralLocks, persistentLocks, ephemeralPrefixLocksToAcquire, persistentPrefixLocksToAcquire);

                    break;
                }
                
                case NodeType.For:
                    if (ast.extendedOne is not null) 
                        GetLocksToAcquire(context, ast.extendedOne, ephemeralLocks, persistentLocks, ephemeralPrefixLocksToAcquire, persistentPrefixLocksToAcquire);
                    break;
                
                case NodeType.Set:
                    if (ast.leftAst is null)
                        throw new KahunaScriptException("Invalid SET expression", ast.yyline);
                    
                    persistentLocks.Add(GetKeyName(context, ast.leftAst));
                    break;
                    
                case NodeType.Eset:
                    if (ast.leftAst is null)
                        throw new KahunaScriptException("Invalid SET expression", ast.yyline);
                    
                    ephemeralLocks.Add(GetKeyName(context, ast.leftAst));
                    break;
                
                case NodeType.Get:
                    if (ast.leftAst is null)
                        throw new KahunaScriptException("Invalid SET expression", ast.yyline);
                    
                    if (ast.extendedOne is null) // make sure if isn't querying a revision
                        persistentLocks.Add(GetKeyName(context, ast.leftAst));
                    break;
                    
                case NodeType.Eget:
                    if (ast.leftAst is null)
                        throw new KahunaScriptException("Invalid GET expression", ast.yyline);
                    
                    if (ast.extendedOne is null) // make sure if isn't querying a revision
                        ephemeralLocks.Add(GetKeyName(context, ast.leftAst));
                    break;
                
                case NodeType.Extend:
                    if (ast.leftAst is null)
                        throw new KahunaScriptException("Invalid EXTEND expression", ast.yyline);
                    
                    persistentLocks.Add(GetKeyName(context, ast.leftAst));
                    break;
                    
                case NodeType.Eextend:
                    if (ast.leftAst is null)
                        throw new KahunaScriptException("Invalid EXTEND expression", ast.yyline);
                    
                    ephemeralLocks.Add(GetKeyName(context, ast.leftAst));
                    break;
                
                case NodeType.Delete:
                    if (ast.leftAst is null)
                        throw new KahunaScriptException("Invalid DELETE expression", ast.yyline);
                    
                    persistentLocks.Add(GetKeyName(context, ast.leftAst));
                    break;
                
                case NodeType.Edelete:
                    if (ast.leftAst is null)
                        throw new KahunaScriptException("Invalid DELETE expression", ast.yyline);
                    
                    ephemeralLocks.Add(GetKeyName(context, ast.leftAst));
                    break;                               
                
                case NodeType.GetByPrefix:
                    if (ast.leftAst is null)
                        throw new KahunaScriptException("Invalid GET BY PREFIX expression", ast.yyline);
                    
                    persistentPrefixLocksToAcquire.Add(GetKeyName(context, ast.leftAst));
                    break;
                
                case NodeType.EgetByPrefix:
                    if (ast.leftAst is null)
                        throw new KahunaScriptException("Invalid GET BY PREFIX expression", ast.yyline);
                    
                    ephemeralPrefixLocksToAcquire.Add(GetKeyName(context, ast.leftAst));
                    break;
                
                case NodeType.IntegerType:
                case NodeType.StringType:
                case NodeType.FloatType:
                case NodeType.BooleanType:
                case NodeType.Identifier:
                case NodeType.Let:
                case NodeType.Equals:
                case NodeType.NotEquals:
                case NodeType.LessThan:
                case NodeType.GreaterThan:
                case NodeType.LessThanEquals:
                case NodeType.GreaterThanEquals:
                case NodeType.And:
                case NodeType.Or:
                case NodeType.Not:
                case NodeType.Add:
                case NodeType.Subtract:
                case NodeType.Mult:
                case NodeType.Div:
                case NodeType.ArrayIndex:
                case NodeType.FuncCall:
                case NodeType.ArgumentList:
                case NodeType.NotFound:
                case NodeType.NotSet:
                case NodeType.SetNotExists:
                case NodeType.SetExists:
                case NodeType.SetCmp:
                case NodeType.SetCmpRev:
                case NodeType.Rollback:
                case NodeType.Commit:
                case NodeType.Return:
                case NodeType.Sleep:
                case NodeType.Throw:
                case NodeType.Placeholder:
                case NodeType.Exists:
                case NodeType.Eexists:
                case NodeType.BeginOptionList:
                case NodeType.BeginOption:
                case NodeType.NullType:                
                case NodeType.ScanByPrefix:
                case NodeType.EscanByPrefix:
                case NodeType.Range:
                    break;
                
                default:
                    throw new NotImplementedException();
            }

            break;
        }
    }
}