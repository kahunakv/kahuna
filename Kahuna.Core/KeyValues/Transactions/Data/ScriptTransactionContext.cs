
using Kommander.Time;

using Kahuna.Server.ScriptParser;
using Kahuna.Server.KeyValues.Transactions.Functions;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues.Transactions.Data;

/// <summary>
/// Extends <see cref="TransactionContext"/> with script-execution state: parameters, local
/// variables, execution flow control, and AST-backed accessors. Only the script executor and
/// its command/expression helpers depend on this type.
/// </summary>
internal sealed class ScriptTransactionContext : TransactionContext
{
    /// <summary>
    /// Controls whether the script executor continues executing statements or stops.
    /// </summary>
    public KeyValueExecutionStatus Status { get; set; } = KeyValueExecutionStatus.Continue;

    /// <summary>
    /// The first statement whose response stopped this script, or null when the script ran to its end or ended
    /// by its own control flow (COMMIT, ROLLBACK, RETURN). The executor reports it as the script's outcome.
    /// </summary>
    internal ScriptStatementFailure? StatementFailure { get; private set; }

    /// <summary>
    /// Stops the script because a statement's response cannot be built on: a retryable refusal, a conflict, or
    /// a malformed request. Marks the transaction for abort so nothing staged so far is committed, and records
    /// the statement so the script reports that response, not a generic abort, as its outcome. The first
    /// failure wins: once the script is stopped no later statement runs, so a second call is a no-op.
    /// </summary>
    internal void StopOnStatementFailure(string statement, string key, KeyValueDurability durability, KeyValueResponseType type)
    {
        Action = KeyValueTransactionAction.Abort;
        Status = KeyValueExecutionStatus.Stop;
        StatementFailure ??= new ScriptStatementFailure(statement, key, durability, type);
    }

    /// <summary>
    /// Script parameters (placeholders) passed into the script at execution time.
    /// </summary>
    public List<KeyValueParameter>? Parameters { get; init; }

    /// <summary>
    /// The node's frozen table of callable functions, built-in and user-defined.
    ///
    /// <para>It rides on the context because a function call is evaluated deep inside the expression
    /// walk, which has no other route back to the node that is running the script. Every path that
    /// builds a context must set it: a call with no table raises a clear internal error rather than
    /// reporting a registered function as undefined.</para>
    /// </summary>
    internal ScriptFunctionTable? FunctionTable { get; init; }

    /// <summary>
    /// The hybrid logical clock of the node that runs this script, and the node id its readings carry.
    ///
    /// <para>The clock rides on the context because a function call is evaluated deep inside the
    /// expression walk, which has no other route back to the node. Only the clock itself is carried,
    /// not the raft handle that owns it: a script reads the time, and nothing more.</para>
    /// </summary>
    internal HybridLogicalClock? HybridLogicalClock { get; init; }

    /// <summary>The id this node stamps on the timestamps it mints.</summary>
    internal int LocalNodeId { get; init; }

    /// <summary>
    /// The one clock reading this script execution observes, minted on the first call that asks for it.
    /// </summary>
    private HLCTimestamp clockReading;

    /// <summary>
    /// The clock reading for this script execution.
    ///
    /// <para>One execution observes one reading. That is what makes the physical component and the
    /// counter describe the same instant, so a script that reads both cannot pair the milliseconds of
    /// one timestamp with the counter of another. It also matches how the rest of a transaction
    /// behaves: reads come from one snapshot, and the clock is no different.</para>
    ///
    /// <para>The reading is minted with <c>TrySendOrLocalEvent</c>, so it is installed in the clock
    /// rather than only observed. A timestamp a node hands out but does not record could be minted
    /// again by the next event, and two events with one timestamp cannot be ordered.</para>
    /// </summary>
    internal HLCTimestamp GetClockReading(NodeAst ast)
    {
        // A minted reading never has a zero physical component: it is milliseconds since the unix epoch,
        // and the clock refuses to pack a non-positive value. So zero is an unambiguous "not yet read".
        if (clockReading.L != 0)
            return clockReading;

        if (HybridLogicalClock is null)
            throw new KahunaScriptException("Internal error: no clock is attached to this transaction context", ast.yyline);

        clockReading = HybridLogicalClock.TrySendOrLocalEvent(LocalNodeId);

        return clockReading;
    }

    /// <summary>
    /// The statement-list subtree the current descent should execute as one batched
    /// set-many/delete-many. Resolved by a single probe at the top of a statement-list spine
    /// (instead of re-scanning the prefix at every recursion level) and consumed — nulled —
    /// when the descent reaches it.
    /// </summary>
    internal NodeAst? BatchBoundary { get; set; }

    /// <summary>True when <see cref="BatchBoundary"/> batches as set-many; false for delete-many.</summary>
    internal bool BatchBoundaryIsSetMany { get; set; }

    /// <summary>The statement list the last probe examined, and what it concluded.</summary>
    private NodeAst? probedList;

    private NodeAst? probedBoundary;

    private bool probedIsSetMany;

    /// <summary>
    /// Restores a statement list's batch boundary from the last probe, when that probe examined this same
    /// list. Returns false when it did not, and the caller must probe.
    ///
    /// <para>A loop body is entered once per iteration and the boundary is consumed on the way in, so the
    /// probe ran again on every iteration. Its answer cannot change between them: the probe resolves each
    /// statement's key through the key-name rule, which reads the syntax tree and the script's parameters,
    /// and neither moves during an execution. A script variable never names a key, so the loop variable
    /// cannot change the answer either.</para>
    ///
    /// <para>One list is remembered, not a map. A loop body is one list, which is the shape this exists
    /// for; nested loops that alternate between two bodies simply fall back to probing.</para>
    /// </summary>
    internal bool TryRestoreBatchProbe(NodeAst list)
    {
        if (!ReferenceEquals(probedList, list))
            return false;

        BatchBoundary = probedBoundary;
        BatchBoundaryIsSetMany = probedIsSetMany;

        return true;
    }

    /// <summary>Remembers what a probe of <paramref name="list"/> concluded.</summary>
    internal void RecordBatchProbe(NodeAst list)
    {
        probedList = list;
        probedBoundary = BatchBoundary;
        probedIsSetMany = BatchBoundaryIsSetMany;
    }

    /// <summary>
    /// Local-scope variables allocated during script execution; released when the script finishes.
    /// </summary>
    private Dictionary<string, KeyValueExpressionResult>? Variables { get; set; }

    /// <summary>
    /// Returns the value of a named local variable. Throws a script error when undefined.
    /// </summary>
    public KeyValueExpressionResult GetVariable(NodeAst ast, string varName)
    {
        if (Variables is null || !Variables.TryGetValue(varName, out KeyValueExpressionResult? value))
            throw new KahunaScriptException("Undefined variable: " + varName, ast.yyline);

        return value;
    }

    /// <summary>
    /// Sets or overwrites a local variable.
    /// </summary>
    public void SetVariable(NodeAst ast, string varName, KeyValueExpressionResult value)
    {
        Variables ??= new();
        Variables[varName] = value;
    }

    /// <summary>
    /// Placeholder name to value, built only for a script that resolves placeholders often enough to pay
    /// for it. Null until then, and the walk over <see cref="Parameters"/> answers instead.
    /// </summary>
    private Dictionary<string, string?>? parameterIndex;

    /// <summary>How many placeholders this execution has resolved so far.</summary>
    private int parameterLookups;

    /// <summary>
    /// The number of lookups after which the index is worth its allocation. A script that names one or
    /// two placeholders once is answered by the walk; a script inside a loop, or one with a long
    /// parameter list, crosses this within the first statements and then resolves in one step.
    /// </summary>
    private const int ParameterIndexThreshold = 4;

    /// <summary>
    /// Returns the string value of a parameter placeholder. Throws a script error when missing.
    ///
    /// <para>The same placeholder is resolved more than once per statement — by the expression walk, by
    /// the lock helper, and by the batch probe — and again per loop iteration, so a list walk is repeated
    /// work whose answer is fixed for the whole execution. The index below removes the repetition without
    /// charging a script that only ever resolves a placeholder once.</para>
    /// </summary>
    public string GetParameter(NodeAst ast)
    {
        if (Parameters is null)
            throw new KahunaScriptException("Undefined parameter: " + ast.yytext!, ast.yyline);

        if (parameterIndex is null && Parameters.Count > 1 && ++parameterLookups >= ParameterIndexThreshold)
        {
            parameterIndex = new(Parameters.Count, StringComparer.Ordinal);

            // Added rather than assigned, so a name that appears twice resolves to its first value —
            // which is what the walk returned, because it stopped at the first match.
            foreach (KeyValueParameter parameter in Parameters)
            {
                if (parameter.Key is not null)
                    parameterIndex.TryAdd(parameter.Key, parameter.Value);
            }
        }

        if (parameterIndex is not null)
        {
            if (parameterIndex.TryGetValue(ast.yytext!, out string? indexed))
                return indexed!;

            throw new KahunaScriptException("Undefined parameter: " + ast.yytext!, ast.yyline);
        }

        foreach (KeyValueParameter variable in Parameters)
        {
            if (variable.Key == ast.yytext!)
                return variable.Value!;
        }

        throw new KahunaScriptException("Undefined parameter: " + ast.yytext!, ast.yyline);
    }
}
