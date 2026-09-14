
using BenchmarkDotNet.Attributes;

using Microsoft.Extensions.Logging.Abstractions;

using Kahuna.Extensibility;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.KeyValues.Transactions.Functions;
using Kahuna.Server.ScriptParser;

namespace Kahuna.Microbenchmarks;

/// <summary>
/// Compares the cost of calling a user-defined function against the cost of calling a built-in of
/// the same arity, through the real production dispatcher.
///
/// <para>The claim the design makes is that a registered function costs what a built-in costs: one
/// frozen-dictionary probe, then the call. Two things could break that. The merged table could make
/// the lookup slower than the old closed dictionary did. The argument marshalling could allocate per
/// call, which on a request path that holds locks is worse than the time it costs.</para>
///
/// <para>So this measures <c>CallFunction.Eval</c> itself, on a real table and a real transaction
/// context, rather than a copy of the dispatch logic. <c>MemoryDiagnoser</c> is what the allocation
/// claim rests on: a scalar two-argument custom call should allocate no more than the built-in does,
/// and both allocate only the argument list the walk already built.</para>
/// </summary>
[MemoryDiagnoser]
public class ScriptFunctionDispatchBenchmark
{
    private ScriptTransactionContext context = null!;

    private NodeAst customCall = null!;

    private NodeAst builtInCall = null!;

    private NodeAst customCallSixArgs = null!;

    /// <summary>A second context whose table has the slow-call timer on, so the clock cost is visible.</summary>
    private ScriptTransactionContext timedContext = null!;

    [GlobalSetup]
    public void Setup()
    {
        KahunaFunctionRegistry registry = new();

        // A function that does nothing but return: what is measured is the dispatch around it, not
        // the body.
        registry.Register("bench_min", static (in KahunaFunctionContext _, ReadOnlySpan<KahunaValue> args) => args[0], 1, -1);

        ScriptFunctionTable table = new(registry, "bench-node", slowWarnMs: 0, NullLogger<IKahuna>.Instance);

        context = new()
        {
            TransactionId = default,
            FunctionTable = table
        };

        // A registry can only be frozen once, so the timed table gets its own.
        KahunaFunctionRegistry timedRegistry = new();

        timedRegistry.Register("bench_min", static (in KahunaFunctionContext _, ReadOnlySpan<KahunaValue> args) => args[0], 1, -1);

        timedContext = new()
        {
            TransactionId = default,
            FunctionTable = new(timedRegistry, "bench-node", slowWarnMs: 50, NullLogger<IKahuna>.Instance)
        };

        // min(a, b) is the built-in baseline: two arguments, numeric, no allocation of its own.
        builtInCall = Call("min", Literal(3), Literal(7));
        customCall = Call("bench_min", Literal(3), Literal(7));

        // Six arguments still fit the inline buffer, so this shows the per-argument marshalling cost
        // without the pool rental that a larger call would add.
        // There is no variadic built-in to compare this against: every built-in takes a fixed one or
        // two arguments. So this row is not a ratio against a built-in — it is the per-argument slope,
        // and most of what it adds over the two-argument row is the argument walk both paths share.
        customCallSixArgs = Call("bench_min", Literal(1), Literal(2), Literal(3), Literal(4), Literal(5), Literal(6));
    }

    [Benchmark(Baseline = true)]
    public KeyValueExpressionResult BuiltInTwoArgs() => CallFunction.Eval(context, builtInCall);

    [Benchmark]
    public KeyValueExpressionResult CustomTwoArgs() => CallFunction.Eval(context, customCall);

    [Benchmark]
    public KeyValueExpressionResult CustomTwoArgsTimed() => CallFunction.Eval(timedContext, customCall);

    [Benchmark]
    public KeyValueExpressionResult CustomSixArgs() => CallFunction.Eval(context, customCallSixArgs);

    private static NodeAst Literal(int value) => new(NodeType.IntegerType, null, null, null, null, null, null, value.ToString(), 1);

    /// <summary>
    /// Builds the tree shape the parser builds for a call: the name on the left, and a right-leaning
    /// argument list on the right.
    /// </summary>
    private static NodeAst Call(string name, params NodeAst[] arguments)
    {
        NodeAst? list = null;

        for (int i = arguments.Length - 1; i >= 0; i--)
            list = list is null ? arguments[i] : new(NodeType.ArgumentList, arguments[i], list, null, null, null, null, null, 1);

        return new(NodeType.FuncCall, new(NodeType.Identifier, null, null, null, null, null, null, name, 1), list, null, null, null, null, null, 1);
    }
}
