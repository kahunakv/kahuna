using System.Text;

using Microsoft.Extensions.Logging;

using Kahuna.Extensibility;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.Tests;

/// <summary>
/// Drives user-defined functions through the real script entry point on an embedded node. Nothing
/// here reaches into the evaluator: every assertion goes through <c>TryExecuteTransactionScript</c>,
/// because a test that called the table directly would pass even if the call site were never wired.
/// </summary>
public sealed class TestUserFunctionEvaluation
{
    private readonly ILoggerFactory loggerFactory;

    public TestUserFunctionEvaluation(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper, quietKommander: true);
    }

    private async Task<EmbeddedKahunaNode> StartNodeAsync(Action<EmbeddedKahunaOptions> configure, CancellationToken ct)
    {
        EmbeddedKahunaOptions options = new()
        {
            TimerInitialDelay = TimeSpan.FromMilliseconds(50),
            ReadIOThreads = 1,
            WriteIOThreads = 1,
            PartitionExecutorPoolSize = 1,
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 4
        };

        configure(options);

        EmbeddedKahunaNode node = new(options, loggerFactory);

        await node.StartAsync(ct);

        return node;
    }

    private static string RandomKey() => Guid.NewGuid().ToString("N")[..10];

    private static async Task<KeyValueTransactionResult> RunAsync(EmbeddedKahunaNode node, string script, List<KeyValueParameter>? parameters = null)
    {
        return await node.Kahuna.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, parameters);
    }

    private static string TextOf(KeyValueTransactionResult result) => Encoding.UTF8.GetString(result.Value ?? []);

    [Fact]
    public async Task TestFunctionInALetAndAReturn()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        await using EmbeddedKahunaNode node = await StartNodeAsync(o =>
            o.Functions.Register("acme_double", static (in KahunaFunctionContext _, ReadOnlySpan<KahunaValue> args) => KahunaValue.From(args[0].AsLong() * 2), 1, 1), ct);

        KeyValueTransactionResult result = await RunAsync(node, """
        LET x = acme_double(21)
        RETURN x
        """);

        Assert.Equal(KeyValueResponseType.Get, result.Type);
        Assert.Equal("42", TextOf(result));
    }

    [Fact]
    public async Task TestFunctionAsASingleCommandOutsideATransaction()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        await using EmbeddedKahunaNode node = await StartNodeAsync(o =>
            o.Functions.Register("acme_tag", static (in KahunaFunctionContext _, ReadOnlySpan<KahunaValue> args) => KahunaValue.From("tag:" + args[0].AsString()), 1, 1), ct);

        string key = RandomKey();

        await node.WaitForLeaderForKeyAsync(key, ct);

        // A bare SET is the single-command path, which builds its context through a different helper
        // than a BEGIN block. If that helper misses the function table, this is the test that fails.
        KeyValueTransactionResult set = await RunAsync(node, $"SET `{key}` acme_tag('one')");

        Assert.Equal(KeyValueResponseType.Set, set.Type);

        KeyValueTransactionResult get = await RunAsync(node, $"GET `{key}`");

        Assert.Equal("tag:one", TextOf(get));
    }

    [Fact]
    public async Task TestFunctionInASetValueInsideATransaction()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        await using EmbeddedKahunaNode node = await StartNodeAsync(o =>
            o.Functions.Register("acme_upper_tag", static (in KahunaFunctionContext _, ReadOnlySpan<KahunaValue> args) => KahunaValue.From(args[0].AsString().ToUpperInvariant()), 1, 1), ct);

        string key = RandomKey();

        await node.WaitForLeaderForKeyAsync(key, ct);

        KeyValueTransactionResult set = await RunAsync(node, $"BEGIN SET `{key}` acme_upper_tag('hello') COMMIT END");

        Assert.Equal(KeyValueResponseType.Set, set.Type);
        Assert.Equal("HELLO", TextOf(await RunAsync(node, $"GET `{key}`")));
    }

    [Fact]
    public async Task TestFunctionInAnIfCondition()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        await using EmbeddedKahunaNode node = await StartNodeAsync(o =>
            o.Functions.Register("acme_is_even", static (in KahunaFunctionContext _, ReadOnlySpan<KahunaValue> args) => KahunaValue.From(args[0].AsLong() % 2 == 0), 1, 1), ct);

        KeyValueTransactionResult even = await RunAsync(node, """
        IF acme_is_even(4) THEN
            RETURN 'even'
        ELSE
            RETURN 'odd'
        END
        """);

        Assert.Equal("even", TextOf(even));

        KeyValueTransactionResult odd = await RunAsync(node, """
        IF acme_is_even(5) THEN
            RETURN 'even'
        ELSE
            RETURN 'odd'
        END
        """);

        Assert.Equal("odd", TextOf(odd));
    }

    [Fact]
    public async Task TestFunctionNestedInsideABuiltIn()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        await using EmbeddedKahunaNode node = await StartNodeAsync(o =>
            o.Functions.Register("acme_tag", static (in KahunaFunctionContext _, ReadOnlySpan<KahunaValue> args) => KahunaValue.From("tag:" + args[0].AsString()), 1, 1), ct);

        Assert.Equal("TAG:ONE", TextOf(await RunAsync(node, "RETURN upper(acme_tag('one'))")));
    }

    [Fact]
    public async Task TestBuiltInNestedInsideAFunction()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        await using EmbeddedKahunaNode node = await StartNodeAsync(o =>
            o.Functions.Register("acme_thrice", static (in KahunaFunctionContext _, ReadOnlySpan<KahunaValue> args) => KahunaValue.From(args[0].AsLong() * 3), 1, 1), ct);

        Assert.Equal("15", TextOf(await RunAsync(node, "RETURN acme_thrice(abs(-5))")));
    }

    [Fact]
    public async Task TestEveryArgumentKindArrivesWithTheRightKind()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        List<KahunaValueKind> seen = [];

        await using EmbeddedKahunaNode node = await StartNodeAsync(o =>
            o.Functions.Register("acme_kinds", (in KahunaFunctionContext _, ReadOnlySpan<KahunaValue> args) =>
            {
                lock (seen)
                {
                    seen.Clear();

                    foreach (KahunaValue value in args)
                        seen.Add(value.Kind);
                }

                return KahunaValue.From((long)args.Length);
            }), ct);

        KeyValueTransactionResult result = await RunAsync(node, "RETURN acme_kinds(null, true, 7, 1.5, 'text')");

        Assert.Equal("5", TextOf(result));

        Assert.Equal(
            [KahunaValueKind.Null, KahunaValueKind.Bool, KahunaValueKind.Long, KahunaValueKind.Double, KahunaValueKind.String],
            seen);
    }

    [Fact]
    public async Task TestValueReadFromAKeyReachesTheFunction()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        KahunaValueKind observed = KahunaValueKind.Null;
        string payload = string.Empty;

        await using EmbeddedKahunaNode node = await StartNodeAsync(o =>
            o.Functions.Register("acme_echo", (in KahunaFunctionContext _, ReadOnlySpan<KahunaValue> args) =>
            {
                observed = args[0].Kind;
                payload = args[0].AsString();

                return KahunaValue.From((long)payload.Length);
            }, 1, 1), ct);

        string key = RandomKey();

        await node.WaitForLeaderForKeyAsync(key, ct);

        Assert.Equal(KeyValueResponseType.Set, (await RunAsync(node, $"SET `{key}` 'stored'")).Type);

        KeyValueTransactionResult result = await RunAsync(node, $"""
        LET v = GET `{key}`
        RETURN acme_echo(v)
        """);

        // A text value stored in a key reaches a function as a String, not as a byte buffer. The
        // evaluator decodes it on the way out of the read, so a function sees the same kind an
        // expression sees. Bytes is the kind of a value a function itself produced.
        Assert.True(result.Type == KeyValueResponseType.Get, $"{result.Type}: {result.Reason}");
        Assert.Equal(KahunaValueKind.String, observed);
        Assert.Equal("stored", payload);
        Assert.Equal("6", TextOf(result));
    }

    [Fact]
    public async Task TestByteBufferArgumentReachesTheFunctionAsBytes()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        KahunaValueKind observed = KahunaValueKind.Null;
        string payload = string.Empty;

        await using EmbeddedKahunaNode node = await StartNodeAsync(o =>
        {
            o.Functions.Register("acme_raw", static (in KahunaFunctionContext _, ReadOnlySpan<KahunaValue> _) => KahunaValue.From(Encoding.UTF8.GetBytes("raw bytes")));

            o.Functions.Register("acme_take", (in KahunaFunctionContext _, ReadOnlySpan<KahunaValue> args) =>
            {
                observed = args[0].Kind;
                payload = Encoding.UTF8.GetString(args[0].AsBytes().Span);

                return KahunaValue.From((long)args[0].AsBytes().Length);
            }, 1, 1);
        }, ct);

        KeyValueTransactionResult result = await RunAsync(node, "RETURN acme_take(acme_raw())");

        Assert.True(result.Type == KeyValueResponseType.Get, $"{result.Type}: {result.Reason}");
        Assert.Equal(KahunaValueKind.Bytes, observed);
        Assert.Equal("raw bytes", payload);
        Assert.Equal("9", TextOf(result));
    }

    [Fact]
    public async Task TestPlaceholderParameterReachesTheFunction()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        await using EmbeddedKahunaNode node = await StartNodeAsync(o =>
            o.Functions.Register("acme_tag", static (in KahunaFunctionContext _, ReadOnlySpan<KahunaValue> args) => KahunaValue.From("tag:" + args[0].AsString()), 1, 1), ct);

        List<KeyValueParameter> parameters = [new() { Key = "@name", Value = "boris" }];

        Assert.Equal("tag:boris", TextOf(await RunAsync(node, "RETURN acme_tag(@name)", parameters)));
    }

    [Fact]
    public async Task TestFunctionReceivesItsContext()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        string functionName = string.Empty;
        string nodeName = string.Empty;
        int line = 0;
        bool sawTransactionId = false;

        await using EmbeddedKahunaNode node = await StartNodeAsync(o =>
        {
            o.NodeName = "function-context-node";

            o.Functions.Register("acme_context", (in KahunaFunctionContext context, ReadOnlySpan<KahunaValue> _) =>
            {
                functionName = context.FunctionName;
                nodeName = context.NodeName;
                line = context.Line;
                sawTransactionId = context.TransactionId.L > 0;

                return KahunaValue.From(true);
            });
        }, ct);

        Assert.Equal("true", TextOf(await RunAsync(node, """
        LET ignored = 1
        RETURN acme_context()
        """)));

        Assert.Equal("acme_context", functionName);
        Assert.Equal("function-context-node", nodeName);
        Assert.Equal(2, line);
        Assert.True(sawTransactionId);
    }

    [Fact]
    public async Task TestVariadicFunctionAcceptsAnyCount()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        await using EmbeddedKahunaNode node = await StartNodeAsync(o =>
            o.Functions.Register("acme_sum", static (in KahunaFunctionContext _, ReadOnlySpan<KahunaValue> args) =>
            {
                long total = 0;

                foreach (KahunaValue value in args)
                    total += value.AsLong();

                return KahunaValue.From(total);
            }), ct);

        Assert.Equal("0", TextOf(await RunAsync(node, "RETURN acme_sum()")));
        Assert.Equal("6", TextOf(await RunAsync(node, "RETURN acme_sum(1, 2, 3)")));

        // Past the inline buffer, so the argument span comes from the pool instead of the frame.
        Assert.Equal("78", TextOf(await RunAsync(node, "RETURN acme_sum(1,2,3,4,5,6,7,8,9,10,11,12)")));
    }

    [Fact]
    public async Task TestTooFewOrTooManyArgumentsIsErrored()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        await using EmbeddedKahunaNode node = await StartNodeAsync(o =>
            o.Functions.Register("acme_pair", static (in KahunaFunctionContext _, ReadOnlySpan<KahunaValue> args) => KahunaValue.From(args[0].AsLong() + args[1].AsLong()), 2, 2), ct);

        KeyValueTransactionResult few = await RunAsync(node, "RETURN acme_pair(1)");

        Assert.Equal(KeyValueResponseType.Errored, few.Type);
        Assert.Contains("acme_pair", few.Reason ?? "", StringComparison.Ordinal);

        KeyValueTransactionResult many = await RunAsync(node, "RETURN acme_pair(1, 2, 3)");

        Assert.Equal(KeyValueResponseType.Errored, many.Type);
        Assert.Contains("acme_pair", many.Reason ?? "", StringComparison.Ordinal);
    }

    [Fact]
    public async Task TestArityFailureLeavesNoWriteAndNoHeldLock()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        await using EmbeddedKahunaNode node = await StartNodeAsync(o =>
            o.Functions.Register("acme_pair", static (in KahunaFunctionContext _, ReadOnlySpan<KahunaValue> args) => KahunaValue.From(args[0].AsLong() + args[1].AsLong()), 2, 2), ct);

        string key = RandomKey();

        await node.WaitForLeaderForKeyAsync(key, ct);

        KeyValueTransactionResult failed = await RunAsync(node, $"""
        BEGIN
            SET `{key}` 'written'
            SET `{key}` acme_pair(1)
        COMMIT
        END
        """);

        Assert.Equal(KeyValueResponseType.Errored, failed.Type);

        // The first SET must have rolled back with the transaction.
        Assert.Equal(KeyValueResponseType.DoesNotExist, (await RunAsync(node, $"GET `{key}`")).Type);

        // And the lock must be gone: a following transaction on the same key commits immediately.
        KeyValueTransactionResult after = await RunAsync(node, $"BEGIN SET `{key}` 'after' COMMIT END");

        Assert.Equal(KeyValueResponseType.Set, after.Type);
        Assert.Equal("after", TextOf(await RunAsync(node, $"GET `{key}`")));
    }

    [Fact]
    public async Task TestReportedFailureIsErroredWithItsReason()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        await using EmbeddedKahunaNode node = await StartNodeAsync(o =>
            o.Functions.Register("acme_strict", static (in KahunaFunctionContext context, ReadOnlySpan<KahunaValue> args) =>
            {
                if (args[0].AsLong() < 0)
                    context.Fail("the value must not be negative");

                return KahunaValue.From(args[0].AsLong());
            }, 1, 1), ct);

        KeyValueTransactionResult result = await RunAsync(node, "RETURN acme_strict(-1)");

        Assert.Equal(KeyValueResponseType.Errored, result.Type);
        Assert.Contains("acme_strict", result.Reason ?? "", StringComparison.Ordinal);
        Assert.Contains("must not be negative", result.Reason ?? "", StringComparison.Ordinal);
    }

    [Fact]
    public async Task TestUnexpectedExceptionIsErroredAndTheNodeKeepsServing()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        await using EmbeddedKahunaNode node = await StartNodeAsync(o =>
        {
            o.Functions.Register("acme_boom", static (in KahunaFunctionContext _, ReadOnlySpan<KahunaValue> _) => throw new InvalidOperationException("kaboom"));
            o.Functions.Register("acme_fine", static (in KahunaFunctionContext _, ReadOnlySpan<KahunaValue> _) => KahunaValue.From(1L));
        }, ct);

        KeyValueTransactionResult result = await RunAsync(node, "RETURN acme_boom()");

        Assert.Equal(KeyValueResponseType.Errored, result.Type);
        Assert.Contains("acme_boom", result.Reason ?? "", StringComparison.Ordinal);
        Assert.Contains("InvalidOperationException", result.Reason ?? "", StringComparison.Ordinal);

        // The node must still be healthy: an exception out of third-party code cannot be allowed to
        // poison the script engine for every later request.
        string key = RandomKey();

        await node.WaitForLeaderForKeyAsync(key, ct);

        Assert.Equal("1", TextOf(await RunAsync(node, "RETURN acme_fine()")));
        Assert.Equal(KeyValueResponseType.Set, (await RunAsync(node, $"BEGIN SET `{key}` 'ok' COMMIT END")).Type);
    }

    [Fact]
    public async Task TestCancellationOutOfAFunctionIsErroredNotAborted()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        await using EmbeddedKahunaNode node = await StartNodeAsync(o =>
            o.Functions.Register("acme_cancel", static (in KahunaFunctionContext _, ReadOnlySpan<KahunaValue> _) => throw new OperationCanceledException("not a timeout")), ct);

        KeyValueTransactionResult result = await RunAsync(node, "RETURN acme_cancel()");

        // A function has no cancellation contract, so a raw cancellation out of one is a failure like
        // any other. Reading it as the transaction timing out would report Aborted and invite a retry
        // of something that cannot succeed.
        Assert.Equal(KeyValueResponseType.Errored, result.Type);
        Assert.Contains("acme_cancel", result.Reason ?? "", StringComparison.Ordinal);
    }

    [Fact]
    public async Task TestUnknownFunctionNamesTheNodeAndItsFingerprint()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        await using EmbeddedKahunaNode node = await StartNodeAsync(o =>
        {
            o.NodeName = "lonely-node";
            o.Functions.Register("acme_known", static (in KahunaFunctionContext _, ReadOnlySpan<KahunaValue> _) => KahunaValue.Null);
        }, ct);

        KeyValueTransactionResult result = await RunAsync(node, "RETURN acme_missing(1)");

        Assert.Equal(KeyValueResponseType.Errored, result.Type);
        Assert.Contains("acme_missing", result.Reason ?? "", StringComparison.Ordinal);
        Assert.Contains("lonely-node", result.Reason ?? "", StringComparison.Ordinal);
        Assert.Contains("functions ", result.Reason ?? "", StringComparison.Ordinal);
    }

    [Fact]
    public async Task TestReturnedBytesAreCopiedNotAliased()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        byte[] reused = Encoding.UTF8.GetBytes("first!");

        await using EmbeddedKahunaNode node = await StartNodeAsync(o =>
            o.Functions.Register("acme_buffer", (in KahunaFunctionContext _, ReadOnlySpan<KahunaValue> _) => KahunaValue.From(reused)), ct);

        string key = RandomKey();

        await node.WaitForLeaderForKeyAsync(key, ct);

        Assert.Equal(KeyValueResponseType.Set, (await RunAsync(node, $"BEGIN SET `{key}` acme_buffer() COMMIT END")).Type);

        // A function is allowed to reuse its own buffer between calls. The stored value must not move
        // with it, so the engine copies on the way out.
        Encoding.UTF8.GetBytes("second").CopyTo(reused, 0);

        Assert.Equal("first!", TextOf(await RunAsync(node, $"GET `{key}`")));
    }

    [Fact]
    public async Task TestEveryReturnKindStores()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        await using EmbeddedKahunaNode node = await StartNodeAsync(o =>
        {
            o.Functions.Register("acme_null", static (in KahunaFunctionContext _, ReadOnlySpan<KahunaValue> _) => KahunaValue.Null);
            o.Functions.Register("acme_bool", static (in KahunaFunctionContext _, ReadOnlySpan<KahunaValue> _) => KahunaValue.From(true));
            o.Functions.Register("acme_long", static (in KahunaFunctionContext _, ReadOnlySpan<KahunaValue> _) => KahunaValue.From(9007199254740993L));
            o.Functions.Register("acme_double", static (in KahunaFunctionContext _, ReadOnlySpan<KahunaValue> _) => KahunaValue.From(1.5d));
            o.Functions.Register("acme_string", static (in KahunaFunctionContext _, ReadOnlySpan<KahunaValue> _) => KahunaValue.From("text"));
            o.Functions.Register("acme_bytes", static (in KahunaFunctionContext _, ReadOnlySpan<KahunaValue> _) => KahunaValue.From(Encoding.UTF8.GetBytes("raw")));
        }, ct);

        Assert.Equal("true", TextOf(await RunAsync(node, "RETURN acme_bool()")));

        // A 64-bit value a double cannot hold. It proves the scalar slot is not shared with the
        // floating-point case.
        Assert.Equal("9007199254740993", TextOf(await RunAsync(node, "RETURN acme_long()")));
        Assert.Equal("1.5", TextOf(await RunAsync(node, "RETURN acme_double()")));
        Assert.Equal("text", TextOf(await RunAsync(node, "RETURN acme_string()")));
        Assert.Equal("raw", TextOf(await RunAsync(node, "RETURN acme_bytes()")));
        Assert.Equal("true", TextOf(await RunAsync(node, "RETURN is_null(acme_null())")));
    }

    [Fact]
    public async Task TestArrayArgumentAndArrayResult()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        await using EmbeddedKahunaNode node = await StartNodeAsync(o =>
        {
            o.Functions.Register("acme_wrap", static (in KahunaFunctionContext _, ReadOnlySpan<KahunaValue> args) =>
                KahunaValue.FromArray([args[0], KahunaValue.From("end")]), 1, 1);

            o.Functions.Register("acme_count", static (in KahunaFunctionContext _, ReadOnlySpan<KahunaValue> args) =>
                KahunaValue.From((long)args[0].AsArray().Count), 1, 1);
        }, ct);

        Assert.Equal("2", TextOf(await RunAsync(node, "RETURN acme_count(acme_wrap('start'))")));
    }

    [Fact]
    public async Task TestFunctionInARolledBackTransactionLeavesNoTrace()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        int calls = 0;

        await using EmbeddedKahunaNode node = await StartNodeAsync(o =>
            o.Functions.Register("acme_mark", (in KahunaFunctionContext _, ReadOnlySpan<KahunaValue> _) =>
            {
                Interlocked.Increment(ref calls);

                return KahunaValue.From("marked");
            }), ct);

        string key = RandomKey();

        await node.WaitForLeaderForKeyAsync(key, ct);

        KeyValueTransactionResult result = await RunAsync(node, $"""
        BEGIN
            SET `{key}` acme_mark()
        ROLLBACK
        END
        """);

        Assert.NotEqual(KeyValueResponseType.Errored, result.Type);
        Assert.Equal(1, Volatile.Read(ref calls));
        Assert.Equal(KeyValueResponseType.DoesNotExist, (await RunAsync(node, $"GET `{key}`")).Type);
    }

    [Fact]
    public async Task TestConcurrentTransactionsShareOneRegistrationWithoutCrossTalk()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        const int Callers = 32;

        int calls = 0;

        await using EmbeddedKahunaNode node = await StartNodeAsync(o =>
            o.Functions.Register("acme_identity", (in KahunaFunctionContext _, ReadOnlySpan<KahunaValue> args) =>
            {
                Interlocked.Increment(ref calls);

                // A stateless function under real concurrency: every caller must get its own argument
                // back, never another caller's.
                return KahunaValue.From(args[0].AsLong());
            }, 1, 1), ct);

        Task<KeyValueTransactionResult>[] running = new Task<KeyValueTransactionResult>[Callers];

        for (int i = 0; i < Callers; i++)
        {
            int value = i;

            running[i] = Task.Run(() => RunAsync(node, $"RETURN acme_identity({value})"), ct);
        }

        KeyValueTransactionResult[] results = await Task.WhenAll(running);

        for (int i = 0; i < Callers; i++)
        {
            Assert.Equal(KeyValueResponseType.Get, results[i].Type);
            Assert.Equal(i.ToString(), TextOf(results[i]));
        }

        Assert.True(Volatile.Read(ref calls) >= Callers);
    }

    [Fact]
    public async Task TestNodeCountsAndTimesEveryCall()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        await using EmbeddedKahunaNode node = await StartNodeAsync(o =>
            o.Functions.Register("acme_counted", static (in KahunaFunctionContext _, ReadOnlySpan<KahunaValue> _) => KahunaValue.From(1L)), ct);

        for (int i = 0; i < 5; i++)
            Assert.Equal("1", TextOf(await RunAsync(node, "RETURN acme_counted()")));

        KahunaManager manager = (KahunaManager)node.Kahuna;

        Assert.Equal(5, manager.GetUserFunctionStats().Single(stat => stat.Name == "acme_counted").Calls);
    }
}
