
using System.Buffers;
using System.Diagnostics;
using System.Runtime.CompilerServices;

using Kahuna.Server.KeyValues.Logging;

using Kahuna.Extensibility;
using Kahuna.Server.ScriptParser;
using Kahuna.Server.KeyValues.Transactions.Data;

namespace Kahuna.Server.KeyValues.Transactions.Functions;

/// <summary>
/// Represents a static class that provides functionality to evaluate function calls
/// within a key-value transaction context using abstract syntax trees (ASTs).
/// </summary>
internal static class CallFunction
{        
    private static readonly Dictionary<string, Func<NodeAst, List<KeyValueExpressionResult>, KeyValueExpressionResult>> FunctionMap = new()
    {
        { "abs", AbsFunction.Execute },
        { "pow", PowFunction.Execute },
        { "round", RoundFunction.Execute },
        { "ceil", CeilFunction.Execute },
        { "floor", FloorFunction.Execute },
        { "min", MinFunction.Execute },
        { "max", MaxFunction.Execute },
        { "nearly_equals", NearlyEqualsFunction.Execute },
        { "to_int", CastToLongFunction.Execute },
        { "to_integer", CastToLongFunction.Execute },
        { "to_long", CastToLongFunction.Execute },
        { "to_number", CastToLongFunction.Execute },
        { "is_int", IsLongFunction.Execute },
        { "is_integer", IsLongFunction.Execute },
        { "is_long", IsLongFunction.Execute },
        { "to_float", CastToDoubleFunction.Execute },
        { "to_double", CastToDoubleFunction.Execute },
        { "is_float", IsDoubleFunction.Execute },
        { "is_double", IsDoubleFunction.Execute },
        { "to_str", CastToStrFunction.Execute },
        { "to_string", CastToStrFunction.Execute },        
        { "is_str", IsStringFunction.Execute },
        { "is_string", IsStringFunction.Execute },
        { "to_bool", CastToBoolFunction.Execute },
        { "to_boolean", CastToBoolFunction.Execute },
        { "is_bool", IsBoolFunction.Execute },
        { "is_boolean", IsBoolFunction.Execute },
        { "is_null", IsNullFunction.Execute },
        { "is_array", IsArrayFunction.Execute },
        { "to_json", ToJsonFunction.Execute },
        { "concat", ConcatFunction.Execute },
        { "upper", UpperFunction.Execute },
        { "lower", LowerFunction.Execute },
        { "count", CountFunction.Execute },        
        { "revision", GetRevisionFunction.Execute },
        { "rev", GetRevisionFunction.Execute },
        { "expires", GetExpiresFunction.Execute },
        { "len", GetLengthFunction.Execute },
        { "length", GetLengthFunction.Execute },
        { "current_time", CurrentTimeFunction.Execute },
        { "substring", SubstringFunction.Execute },
        { "starts_with", StartsWithFunction.Execute },
        { "ends_with", EndsWithFunction.Execute },
        { "index_of", IndexOfFunction.Execute },
        { "split", SplitFunction.Execute },
        { "trim", TrimFunction.Execute }
    };

    /// <summary>
    /// Built-ins that need the running script's context, and cannot be expressed by the signature above.
    ///
    /// <para>They live in a second map rather than behind a widened signature because widening it would
    /// hand every built-in a reference to transaction state that all but two of them have no business
    /// reading. They are still ordinary built-ins in every other respect: they are merged into the same
    /// table, so a deployment cannot register a function that shadows one.</para>
    /// </summary>
    private static readonly Dictionary<string, ScriptBuiltInWithContext> ContextualFunctionMap = new()
    {
        { "hlc", HlcFunction.Execute },
        { "hlc_counter", HlcCounterFunction.Execute }
    };

    /// <summary>
    /// Whether a name belongs to a built-in, counting every alias a built-in answers to.
    ///
    /// <para>The extension registry asks this instead of keeping its own list of reserved names. A
    /// copied list would drift the moment a built-in is added, and the drift would let a deployment
    /// register a custom function that shadows the new built-in and changes what its scripts mean.</para>
    /// </summary>
    internal static bool IsBuiltIn(string name) => FunctionMap.ContainsKey(name) || ContextualFunctionMap.ContainsKey(name);

    /// <summary>How many built-in names and aliases exist, across both maps.</summary>
    internal static int BuiltInCount => FunctionMap.Count + ContextualFunctionMap.Count;

    /// <summary>Every built-in name and alias. Ordinal, case-sensitive, in no defined order.</summary>
    internal static IEnumerable<string> BuiltInNames
    {
        get
        {
            foreach (string name in FunctionMap.Keys)
                yield return name;

            foreach (string name in ContextualFunctionMap.Keys)
                yield return name;
        }
    }

    /// <summary>Every built-in, for the per-node table that merges them with the registered ones.</summary>
    internal static IReadOnlyDictionary<string, Func<NodeAst, List<KeyValueExpressionResult>, KeyValueExpressionResult>> BuiltInFunctions => FunctionMap;

    /// <summary>Every context-taking built-in, for the same table.</summary>
    internal static IReadOnlyDictionary<string, ScriptBuiltInWithContext> ContextualBuiltInFunctions => ContextualFunctionMap;

    /// <summary>
    /// How many arguments fit in the inline buffer a call uses. Eight covers every built-in and any
    /// reasonable user-defined function, so the common call converts its arguments without touching
    /// the pool. A call with more arguments rents instead; it does not fail.
    /// </summary>
    private const int InlineArgumentCapacity = 8;

    /// <summary>
    /// A fixed-size argument buffer held in the calling frame.
    ///
    /// <para><c>stackalloc</c> cannot be used here: <see cref="KahunaValue"/> holds an object
    /// reference for its string, bytes and array cases, which makes it a managed type. An inline
    /// array is the equivalent that the garbage collector can track.</para>
    /// </summary>
    [InlineArray(InlineArgumentCapacity)]
    private struct InlineArgumentBuffer
    {
#pragma warning disable IDE0051, CS0169
        private KahunaValue element0;
#pragma warning restore IDE0051, CS0169
    }

    /// <summary>
    /// Evaluates a function call represented by an abstract syntax tree (AST) node within a given transactional context.
    /// </summary>
    /// <param name="context">The transaction context in which the function evaluation is executed.</param>
    /// <param name="ast">The representation of the abstract syntax tree for the function being evaluated.</param>
    /// <returns>The result of evaluating the function as a <see cref="KeyValueExpressionResult"/>.</returns>
    /// <exception cref="KahunaScriptException">
    /// Thrown when the AST node for the function is invalid, the function name is missing,
    /// or the function is not defined.
    /// </exception>
    public static KeyValueExpressionResult Eval(ScriptTransactionContext context, NodeAst ast)
    {
        if (ast.leftAst is null)
            throw new KahunaScriptException("Invalid function expression", ast.yyline);

        if (string.IsNullOrEmpty(ast.leftAst.yytext))
            throw new KahunaScriptException("Invalid function name", ast.yyline);

        string name = ast.leftAst.yytext;

        // The table replaces the closed built-in map, but the order of work around it is unchanged:
        // arguments are still evaluated before the name is resolved, so a script whose argument is
        // itself broken reports that argument's error exactly as it did before.
        List<KeyValueExpressionResult> arguments = [];

        if (ast.rightAst is not null)
            GetFuncCallArguments(context, ast.rightAst, arguments);

        ScriptFunctionTable table = context.FunctionTable
            ?? throw new KahunaScriptException("Internal error: no script function table is attached to this transaction context", ast.yyline);

        if (!table.TryGet(name, out ScriptFunctionEntry entry))
            throw new KahunaScriptException(table.DescribeUndefined(name), ast.yyline);

        if (entry.BuiltIn is not null)
            return entry.BuiltIn(ast, arguments);

        if (entry.BuiltInWithContext is not null)
            return entry.BuiltInWithContext(context, ast, arguments);

        return InvokeCustom(context, table, entry, name, ast, arguments);
    }

    /// <summary>
    /// Invokes a user-defined function and contains whatever it does.
    ///
    /// <para>Three things happen here that do not happen for a built-in. The argument count is
    /// checked against the bounds the host registered. The arguments and the result are translated
    /// between the evaluator's value type and the published one. Every exception is converted into a
    /// script error, so arbitrary third-party code cannot decide a transaction's outcome.</para>
    ///
    /// <para>The outcome of a failure is always <c>Errored</c>. It is never <c>MustRetry</c>, which
    /// would spin a client on a failure that will repeat, and never <c>Aborted</c>, which tells a
    /// client its transaction lost a genuine conflict. A function that throws produced bad output for
    /// this input, and that is what <c>Errored</c> means.</para>
    /// </summary>
    private static KeyValueExpressionResult InvokeCustom(
        ScriptTransactionContext context,
        ScriptFunctionTable table,
        ScriptFunctionEntry entry,
        string name,
        NodeAst ast,
        List<KeyValueExpressionResult> arguments
    )
    {
        int count = arguments.Count;

        if (count < entry.MinArgs || (entry.MaxArgs >= 0 && count > entry.MaxArgs))
            throw new KahunaScriptException($"Invalid number of arguments for '{name}' function", ast.yyline);

        KahunaFunctionContext functionContext = new(
            name,
            ast.yyline,
            context.TransactionId,
            context.ReadTimestamp,
            table.NodeName,
            table.Logger
        );

        InlineArgumentBuffer inline = default;
        scoped Span<KahunaValue> inlineSpan = inline;
        KahunaValue[]? rented = null;

        try
        {
            scoped Span<KahunaValue> args;

            if (count <= InlineArgumentCapacity)
            {
                args = inlineSpan[..count];
            }
            else
            {
                rented = ArrayPool<KahunaValue>.Shared.Rent(count);
                args = rented.AsSpan(0, count);
            }

            for (int i = 0; i < count; i++)
                args[i] = ScriptFunctionMarshal.ToKahunaValue(arguments[i]);

            // Reading the clock is the single most expensive thing this method does that the function
            // itself does not: two GetTimestamp calls cost more than the whole rest of the dispatch.
            // So it is paid only when the node is configured to report it. With the timer off the call
            // is still counted — one Interlocked increment — but not timed, and a call then costs
            // what a built-in of the same arity costs.
            bool timed = table.SlowWarnTicks > 0;

            long started = timed ? Stopwatch.GetTimestamp() : 0;

            KahunaValue result = Invoke(entry, functionContext, args, table, name, ast);

            if (timed)
            {
                long elapsed = Stopwatch.GetTimestamp() - started;

                entry.Stats?.Record(elapsed);

                // A slow function is not aborted. .NET cannot preempt one, and the transaction timeout
                // is already the backstop that reclaims the session. The warning exists because the
                // cost is otherwise invisible: the call blocks a request path while the transaction
                // holds its locks, so one slow function shows up as latency on unrelated keys.
                if (elapsed > table.SlowWarnTicks)
                    table.Logger.LogUserFunctionSlow(name, elapsed * 1000d / Stopwatch.Frequency, context.TransactionId, table.SlowWarnMs);
            }
            else
            {
                entry.Stats?.RecordCallOnly();
            }

            return ScriptFunctionMarshal.ToExpressionResult(result);
        }
        finally
        {
            // Cleared on return: a parked buffer would otherwise keep every string, byte array and
            // nested array of the last call reachable for as long as the pool holds the array.
            if (rented is not null)
                ArrayPool<KahunaValue>.Shared.Return(rented, clearArray: true);
        }
    }

    /// <summary>
    /// Calls the function and maps whatever comes out of it to a script error.
    ///
    /// <para><see cref="OperationCanceledException"/> is deliberately not special-cased. A function
    /// has no cancellation contract, so a raw cancellation out of one is a failure like any other and
    /// must not be read as the transaction timing out.</para>
    ///
    /// <para><see cref="StackOverflowException"/> and <see cref="OutOfMemoryException"/> are not
    /// caught here because they cannot be. A function that overflows the stack takes the process
    /// down. A registered function has the full trust of the host process, which is stated in the
    /// documentation for anyone who installs one.</para>
    /// </summary>
    private static KahunaValue Invoke(
        ScriptFunctionEntry entry,
        in KahunaFunctionContext functionContext,
        ReadOnlySpan<KahunaValue> args,
        ScriptFunctionTable table,
        string name,
        NodeAst ast
    )
    {
        try
        {
            return entry.Custom!(in functionContext, args);
        }
        catch (KahunaScriptException)
        {
            throw;
        }
        catch (KahunaFunctionException ex)
        {
            // The function reported this failure on purpose, so it is not logged as unexpected.
            throw new KahunaScriptException($"Function '{name}' failed: {ex.Message}", ast.yyline);
        }
        catch (Exception ex)
        {
            table.Logger.LogUserFunctionThrew(ex, name, functionContext.TransactionId);

            throw new KahunaScriptException($"Function '{name}' threw {ex.GetType().Name}: {ex.Message}", ast.yyline);
        }
    }

    /// <summary>
    /// Recursively extracts and evaluates function call arguments from the provided abstract syntax tree (AST) node
    /// and appends the results to the specified list of arguments within a given transactional context.
    /// </summary>
    /// <param name="context">The transactional context in which the evaluation occurs.</param>
    /// <param name="ast">The abstract syntax tree (AST) node representing the function call arguments.</param>
    /// <param name="arguments">The list to which the evaluated function call arguments are appended.</param>
    private static void GetFuncCallArguments(ScriptTransactionContext context, NodeAst ast, List<KeyValueExpressionResult> arguments)
    {
        while (true)
        {
            switch (ast.nodeType)
            {
                case NodeType.ArgumentList:
                {
                    if (ast.leftAst is not null)
                        GetFuncCallArguments(context, ast.leftAst, arguments);

                    if (ast.rightAst is not null)
                    {
                        ast = ast.rightAst!;
                        continue;
                    }

                    break;
                }
                
                default:
                    arguments.Add(KeyValueTransactionExpression.Eval(context, ast));
                    break;
            }

            break;
        }
    }
}