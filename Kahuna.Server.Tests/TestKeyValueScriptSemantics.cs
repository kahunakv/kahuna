
using System.Globalization;
using System.Text;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.KeyValues.Transactions.Functions;
using Kahuna.Server.ScriptParser;
using Kahuna.Shared.KeyValue;
using Kommander;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// Execution-level tests for the script language: range bounds, hexadecimal literals, unary minus, the one
/// truthiness model shared by IF and the logical operators, exact numeric equality, short-circuit evaluation,
/// division by zero, and the BEGIN option list.
/// </summary>
public class TestKeyValueScriptSemantics : BaseCluster
{
    private readonly ILogger<IRaft> raftLogger;

    private readonly ILogger<IKahuna> kahunaLogger;

    public TestKeyValueScriptSemantics(ITestOutputHelper outputHelper)
    {
        ILoggerFactory loggerFactory = TestLogFactory.Create(outputHelper, quietKommander: true);

        raftLogger = loggerFactory.CreateLogger<IRaft>();
        kahunaLogger = loggerFactory.CreateLogger<IKahuna>();
    }

    private static string GetRandomKey()
    {
        return Guid.NewGuid().ToString("N")[..10];
    }

    private static async Task AssertReturns(IKahuna kahuna, string script, string expected)
    {
        KeyValueTransactionResult resp = await kahuna.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);

        Assert.Equal(KeyValueResponseType.Get, resp.Type);
        Assert.Equal(expected, Encoding.UTF8.GetString(resp.Value ?? []));
    }

    private static async Task<string> AssertErrored(IKahuna kahuna, string script)
    {
        KeyValueTransactionResult resp = await kahuna.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);

        Assert.Equal(KeyValueResponseType.Errored, resp.Type);
        Assert.False(string.IsNullOrEmpty(resp.Reason), "an errored script must carry a reason");

        // A framework exception name in the reason means the failure escaped the script error path and the
        // caller has no line to look at.
        Assert.DoesNotContain("NotImplementedException", resp.Reason, StringComparison.Ordinal);
        Assert.DoesNotContain("FormatException", resp.Reason, StringComparison.Ordinal);
        Assert.DoesNotContain("DivideByZeroException", resp.Reason, StringComparison.Ordinal);
        Assert.DoesNotContain("ArgumentException", resp.Reason, StringComparison.Ordinal);
        Assert.DoesNotContain("ArgumentOutOfRangeException", resp.Reason, StringComparison.Ordinal);
        Assert.DoesNotContain("OverflowException", resp.Reason, StringComparison.Ordinal);

        return resp.Reason!;
    }

    /// <summary>
    /// Asserts that the script failed with a script error: one that names a line and carries the given text.
    /// A framework exception message such as "Value was either too large or too small for an Int32." names
    /// no exception type, so the line is the only sign that the failure went through the script error path.
    /// </summary>
    private static async Task AssertScriptError(IKahuna kahuna, string script, string expected)
    {
        string reason = await AssertErrored(kahuna, script);

        Assert.Contains(" at line ", reason, StringComparison.Ordinal);
        Assert.Contains(expected, reason, StringComparison.Ordinal);
    }

    /// <summary>
    /// A double literal with the given number of integer digits: a one followed by zeros.
    /// </summary>
    private static string LargeFloatLiteral(int digits) => "1" + new string('0', digits - 1) + ".0";

    /// <summary>
    /// The largest finite double written out in full, so that the scanner reads it as a float literal.
    /// </summary>
    private static readonly string MaxDoubleLiteral = "17976931348623157" + new string('0', 292) + ".0";

    /// <summary>
    /// The range operator passed its right operand to a count-taking helper, so "10..15" produced fifteen
    /// values starting at ten. The only range in the suite started at one, where a count and an end bound
    /// happen to agree, which is why the bug survived.
    /// </summary>
    [Theory, CombinatorialData]
    public async Task TestRangeBounds([CombinatorialValues("memory")] string storage, [CombinatorialValues(1)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna _, IKahuna _) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            await AssertReturns(kahuna1, "RETURN count(10..15)", "6");
            await AssertReturns(kahuna1, "RETURN count(1..10)", "10");
            await AssertReturns(kahuna1, "RETURN count(0..0)", "1");
            await AssertReturns(kahuna1, "RETURN (10..15)[0]", "10");
            await AssertReturns(kahuna1, "RETURN (10..15)[5]", "15");

            // A start above the end is an empty range, so "0..n-1" runs no iterations when n is zero rather
            // than aborting the transaction.
            await AssertReturns(kahuna1, "RETURN count(5..1)", "0");
            await AssertReturns(kahuna1, "RETURN count(0..0-1)", "0");

            string script = """
            LET total = 0
            FOR i IN 10..15 DO
                LET total = total + i
            END
            RETURN total
            """;

            await AssertReturns(kahuna1, script, "75");

            // A range too large to materialize is a script error, not a multi-gigabyte allocation.
            string reason = await AssertErrored(kahuna1, "RETURN count(0..2000000000)");
            Assert.Contains("exceeds the limit", reason, StringComparison.Ordinal);
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }

    /// <summary>
    /// The scanner accepts a hexadecimal literal and a signed expression; evaluation used to reject the first
    /// with a raw framework exception, and the grammar could not express the second at all.
    /// </summary>
    [Theory, CombinatorialData]
    public async Task TestLiteralsAndUnaryMinus([CombinatorialValues("memory")] string storage, [CombinatorialValues(1)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna _, IKahuna _) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            await AssertReturns(kahuna1, "RETURN 0x1A", "26");
            await AssertReturns(kahuna1, "RETURN 0xff", "255");
            await AssertReturns(kahuna1, "RETURN 0x0", "0");
            await AssertReturns(kahuna1, "RETURN is_long(0x1A)", "true");

            // A sign is an operator now, so a negative hexadecimal reads the same way a negative decimal does.
            await AssertReturns(kahuna1, "RETURN -0x1A", "-26");

            await AssertReturns(kahuna1, "RETURN -5", "-5");
            await AssertReturns(kahuna1, "RETURN is_long(-5)", "true");
            await AssertReturns(kahuna1, "RETURN -5.5", "-5.5");
            await AssertReturns(kahuna1, "RETURN 5-3", "2");
            await AssertReturns(kahuna1, "RETURN 5 - 3", "2");
            await AssertReturns(kahuna1, "RETURN -2 * 3", "-6");
            await AssertReturns(kahuna1, "LET x = 7 RETURN -x", "-7");
            await AssertReturns(kahuna1, "RETURN abs(-9)", "9");

            // An integer literal too large for the type is a script error rather than an overflow exception.
            string reason = await AssertErrored(kahuna1, "RETURN 99999999999999999999");
            Assert.Contains("out of range", reason, StringComparison.Ordinal);

            // A literal is read when it is evaluated, and a branch that is not taken never evaluates one.
            // So the same out-of-range literal in dead code is not an error. This is what keeps the literal
            // cache lazy: reading every literal up front would report this script as broken.
            await AssertReturns(kahuna1, "IF false THEN RETURN 99999999999999999999 END RETURN 'ok'", "ok");
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }

    /// <summary>
    /// IF accepted only a boolean and silently took the ELSE branch for anything else, while the logical
    /// operators treated a non-zero number as true. The two now share one rule: a condition must be a boolean.
    /// </summary>
    [Theory, CombinatorialData]
    public async Task TestConditionsRequireBoolean([CombinatorialValues("memory")] string storage, [CombinatorialValues(1)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna _, IKahuna _) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            await AssertReturns(kahuna1, "IF true THEN RETURN 'yes' ELSE RETURN 'no' END", "yes");
            await AssertReturns(kahuna1, "IF false THEN RETURN 'yes' ELSE RETURN 'no' END", "no");
            await AssertReturns(kahuna1, "IF 1 == 1 && 2 == 2 THEN RETURN 'yes' ELSE RETURN 'no' END", "yes");

            // "IF 1" used to run the ELSE branch without a word while "IF 1 && 1" ran the THEN branch.
            foreach (string condition in new[] { "1", "0", "1 && 1", "1 || 0", "!1", "'text'", "null", "1.5" })
            {
                string reason = await AssertErrored(kahuna1, $"IF {condition} THEN RETURN 'yes' ELSE RETURN 'no' END");
                Assert.Contains("expected a boolean", reason, StringComparison.Ordinal);
            }

            await AssertReturns(kahuna1, "RETURN !true", "false");
            await AssertReturns(kahuna1, "RETURN !false", "true");
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }

    /// <summary>
    /// Equality on doubles compared within a fixed tolerance of 0.001, so two clearly different values read as
    /// equal and a script had no way to ask for an exact answer.
    /// </summary>
    [Theory, CombinatorialData]
    public async Task TestNumericEqualityIsExact([CombinatorialValues("memory")] string storage, [CombinatorialValues(1)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna _, IKahuna _) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            await AssertReturns(kahuna1, "RETURN 1 == 1.0009", "false");
            await AssertReturns(kahuna1, "RETURN 1 != 1.0009", "true");
            await AssertReturns(kahuna1, "RETURN 1 == 1.0", "true");
            await AssertReturns(kahuna1, "RETURN 1.5 == 1.5", "true");
            await AssertReturns(kahuna1, "RETURN 1.5 == 1.5005", "false");
            await AssertReturns(kahuna1, "RETURN 2.0 == 2", "true");
            await AssertReturns(kahuna1, "RETURN '1.0009' == 1", "false");

            // A script that wants a tolerance now states its own.
            await AssertReturns(kahuna1, "RETURN nearly_equals(1, 1.0009, 0.001)", "true");
            await AssertReturns(kahuna1, "RETURN nearly_equals(1, 1.0009, 0.0001)", "false");
            await AssertReturns(kahuna1, "RETURN nearly_equals(1, 1, 0)", "true");

            string reason = await AssertErrored(kahuna1, "RETURN nearly_equals(1, 1.0009, -1)");
            Assert.Contains("must not be negative", reason, StringComparison.Ordinal);
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }

    /// <summary>
    /// The logical operators evaluated both operands, so the ordinary divide guard errored the whole
    /// transaction; and division by zero raised a framework exception for integers while returning infinity
    /// for doubles.
    /// </summary>
    [Theory, CombinatorialData]
    public async Task TestShortCircuitAndDivisionByZero([CombinatorialValues("memory")] string storage, [CombinatorialValues(1)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna _, IKahuna _) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            // The guard idiom: with a zero divisor the right operand must never be evaluated.
            string guard = """
            LET d = 0
            LET n = 10
            IF d != 0 && n / d > 1 THEN
                RETURN 'divided'
            ELSE
                RETURN 'guarded'
            END
            """;

            await AssertReturns(kahuna1, guard, "guarded");

            // The same shape for OR: a true left operand settles the result, so the right one is not run.
            string orGuard = """
            LET d = 0
            LET n = 10
            IF d == 0 || n / d > 1 THEN
                RETURN 'guarded'
            ELSE
                RETURN 'divided'
            END
            """;

            await AssertReturns(kahuna1, orGuard, "guarded");

            // A short circuit also skips the type check on the operand it never evaluates.
            await AssertReturns(kahuna1, "RETURN false && 1", "false");
            await AssertReturns(kahuna1, "RETURN true || 1", "true");

            // Both numeric types answer alike, and neither leaks a framework exception.
            foreach (string division in new[] { "1 / 0", "1.0 / 0.0", "1 / 0.0", "1.0 / 0", "1 / '0'" })
            {
                string reason = await AssertErrored(kahuna1, $"RETURN {division}");
                Assert.Contains("Division by zero", reason, StringComparison.Ordinal);
            }

            await AssertReturns(kahuna1, "RETURN 10 / 2", "5");
            await AssertReturns(kahuna1, "RETURN 10.0 / 4", "2.5");
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }

    /// <summary>
    /// Every option of a multi-option BEGIN was discarded, so the transaction ran on defaults. The single
    /// multi-option BEGIN in the suite is an end-to-end test whose two branches were therefore identical.
    /// </summary>
    [Theory, CombinatorialData]
    public async Task TestBeginOptionsAreHonored([CombinatorialValues("memory")] string storage, [CombinatorialValues(1)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna _, IKahuna _) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            string key = GetRandomKey();

            // A timeout in second position must still be applied: the sleep runs far past it.
            string script = $"""
            BEGIN (locking=pessimistic, timeout=1)
             SLEEP 300
             SET '{key}' 'value'
             COMMIT
            END
            """;

            KeyValueTransactionResult resp = await kahuna1.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Aborted, resp.Type);
            Assert.Equal("Transaction aborted by timeout", resp.Reason);

            // The same transaction with a generous timeout in the same position commits.
            script = $"""
            BEGIN (locking=pessimistic, timeout=20000)
             SET '{key}' 'value'
             COMMIT
            END
            """;

            resp = await RetryOnMustRetry(kahuna1, Encoding.UTF8.GetBytes(script), null, null);
            Assert.Equal(KeyValueResponseType.Set, resp.Type);

            // A repeated option is rejected rather than letting one of the two values quietly win.
            script = $"""
            BEGIN (timeout=1000, timeout=2000)
             SET '{key}' 'value'
             COMMIT
            END
            """;

            string reason = await AssertErrored(kahuna1, script);
            Assert.Contains("Duplicated BEGIN option: timeout", reason, StringComparison.Ordinal);

            // An unreadable option value in second position is rejected, which it could not be while the
            // option list was being discarded. The wording of that message is not asserted here.
            script = $"""
            BEGIN (locking=pessimistic, timeout=abc)
             SET '{key}' 'value'
             COMMIT
            END
            """;

            reason = await AssertErrored(kahuna1, script);
            Assert.Contains("timeout", reason, StringComparison.Ordinal);
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }

    /// <summary>
    /// A prefix scan inside a transaction reached the unimplemented branch of the dispatch switch and surfaced
    /// a framework exception name with no line. It now states the limitation and names the alternative.
    /// </summary>
    [Theory, CombinatorialData]
    public async Task TestScanByPrefixInsideTransactionIsRefused([CombinatorialValues("memory")] string storage, [CombinatorialValues(1)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna _, IKahuna _) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            string prefix = GetRandomKey();

            foreach (string statement in new[] { $"SCAN BY PREFIX '{prefix}'", $"ESCAN BY PREFIX '{prefix}'" })
            {
                string reason = await AssertErrored(kahuna1, $"BEGIN {statement} END");
                Assert.Contains("SCAN BY PREFIX is not supported inside transactions", reason, StringComparison.Ordinal);
                Assert.Contains("GET BY BUCKET", reason, StringComparison.Ordinal);
            }

            // The same statement outside a transaction still works.
            KeyValueTransactionResult resp = await kahuna1.TryExecuteTransactionScript(
                Encoding.UTF8.GetBytes($"SCAN BY PREFIX '{prefix}'"), null, null);

            Assert.Equal(KeyValueResponseType.DoesNotExist, resp.Type);
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }

    /// <summary>
    /// The lexer patterns always recognised backslash escapes, but the actions passed them through as
    /// literal two-character text. A line break can be written either as an escape or as a raw line break
    /// inside the quotes; both must reach the stored value unchanged.
    /// </summary>
    [Theory, CombinatorialData]
    public async Task TestStringEscapes([CombinatorialValues("memory")] string storage, [CombinatorialValues(1)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna _, IKahuna _) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            await AssertReturns(kahuna1, "RETURN \"a\\nb\"", "a\nb");
            await AssertReturns(kahuna1, "RETURN 'a\\nb'", "a\nb");
            await AssertReturns(kahuna1, "RETURN \"a\\tb\"", "a\tb");
            await AssertReturns(kahuna1, "RETURN \"a\\\\b\"", "a\\b");
            await AssertReturns(kahuna1, "RETURN \"a\\\"b\"", "a\"b");
            await AssertReturns(kahuna1, "RETURN 'a\\'b'", "a'b");
            await AssertReturns(kahuna1, "RETURN \"\\u0041\"", "A");
            await AssertReturns(kahuna1, "RETURN \"\\x41\"", "A");

            // The patterns accept an octal escape, so the decoder honours it: one to three digits.
            await AssertReturns(kahuna1, "RETURN \"\\101\"", "A");
            await AssertReturns(kahuna1, "RETURN \"a\\101b\"", "aAb");

            // A literal with no backslash is untouched.
            await AssertReturns(kahuna1, "RETURN \"plain text\"", "plain text");

            // An escape nobody defined is refused, rather than guessed at by dropping or keeping the
            // backslash.
            string reason = await AssertErrored(kahuna1, "RETURN \"a\\qb\"");
            Assert.Contains("Unknown escape sequence", reason, StringComparison.Ordinal);

            // A value written with an escape round-trips through the store.
            string key = GetRandomKey();

            KeyValueTransactionResult write = await kahuna1.TryExecuteTransactionScript(
                Encoding.UTF8.GetBytes($"SET '{key}' \"line1\\nline2\""), null, null);

            Assert.Equal(KeyValueResponseType.Set, write.Type);

            KeyValueTransactionResult read = await kahuna1.TryExecuteTransactionScript(
                Encoding.UTF8.GetBytes($"GET '{key}'"), null, null);

            Assert.Equal(KeyValueResponseType.Get, read.Type);
            Assert.Equal("line1\nline2", Encoding.UTF8.GetString(read.Value ?? []));

            // A raw line break inside the quotes is kept as it is.
            await AssertReturns(kahuna1, "RETURN 'a\nb'", "a\nb");

            string rawKey = GetRandomKey();

            KeyValueTransactionResult rawWrite = await kahuna1.TryExecuteTransactionScript(
                Encoding.UTF8.GetBytes($"BEGIN\n  SET '{rawKey}' 'line1\nline2'\n  COMMIT\nEND"), null, null);

            Assert.Equal(KeyValueResponseType.Set, rawWrite.Type);

            KeyValueTransactionResult rawRead = await kahuna1.TryExecuteTransactionScript(
                Encoding.UTF8.GetBytes($"GET '{rawKey}'"), null, null);

            Assert.Equal(KeyValueResponseType.Get, rawRead.Type);
            Assert.Equal("line1\nline2", Encoding.UTF8.GetString(rawRead.Value ?? []));
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }

    /// <summary>
    /// The index expression is converted before it is checked. A helper already accepted a double and a
    /// numeric string, but the bounds check inspected the original expression instead of the converted
    /// index, and the field it read is zero for both of those types, so every non-integer subscript was
    /// reported as out of range by a message saying the index had to be an integer.
    /// </summary>
    [Theory, CombinatorialData]
    public async Task TestArrayIndexTypes([CombinatorialValues("memory")] string storage, [CombinatorialValues(1)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna _, IKahuna _) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            await AssertReturns(kahuna1, "RETURN (10..15)[1]", "11");
            await AssertReturns(kahuna1, "RETURN (10..15)[1.0]", "11");
            await AssertReturns(kahuna1, "RETURN (10..15)[\"1\"]", "11");
            await AssertReturns(kahuna1, "RETURN (10..15)[0-0+1]", "11");

            // Out of range stays a script error naming the line, for every accepted index type.
            foreach (string index in new[] { "6", "6.0", "\"6\"", "0-1" })
            {
                string reason = await AssertErrored(kahuna1, $"RETURN (10..15)[{index}]");
                Assert.Contains("Index must be positive and less than size of array", reason, StringComparison.Ordinal);
                Assert.Contains("at line 1", reason, StringComparison.Ordinal);
            }

            // A fractional subscript is refused rather than truncated, and an empty string is refused
            // rather than read as element zero. Both are far likelier to be a mistake than a request.
            string fractional = await AssertErrored(kahuna1, "RETURN (10..15)[1.5]");
            Assert.Contains("Index must be a whole number", fractional, StringComparison.Ordinal);

            string empty = await AssertErrored(kahuna1, "RETURN (10..15)[\"\"]");
            Assert.Contains("Index must be an integer", empty, StringComparison.Ordinal);

            string wrongType = await AssertErrored(kahuna1, "RETURN (10..15)[true]");
            Assert.Contains("Index must be an integer", wrongType, StringComparison.Ordinal);
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }

    /// <summary>
    /// Two pessimistic transactions over the same keys, written in opposite order. The keys were collected
    /// into hash sets and acquired in enumeration order, so the two could each take part of the overlap and
    /// both abort with nothing done. Acquisition is now ordered, so on a shared leader one of the two always
    /// gets through.
    /// </summary>
    [Theory, CombinatorialData]
    public async Task TestOverlappingTransactionsDoNotBothAbort([CombinatorialValues("memory")] string storage, [CombinatorialValues(1)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna _, IKahuna _) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            for (int attempt = 0; attempt < 20; attempt++)
            {
                string prefix = GetRandomKey();
                string first = $"{prefix}/aaa";
                string second = $"{prefix}/zzz";

                string forward = $"BEGIN (timeout=5000) SET '{first}' 'x' SET '{second}' 'x' COMMIT END";
                string backward = $"BEGIN (timeout=5000) SET '{second}' 'y' SET '{first}' 'y' COMMIT END";

                DateTime startedAt = DateTime.UtcNow;

                Task<KeyValueTransactionResult> left = kahuna1.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(forward), null, null);
                Task<KeyValueTransactionResult> right = kahuna1.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(backward), null, null);

                KeyValueTransactionResult[] results = await Task.WhenAll(left, right);

                TimeSpan elapsed = DateTime.UtcNow - startedAt;

                bool anySucceeded = results.Any(r => r.Type is KeyValueResponseType.Set or KeyValueResponseType.Get);

                Assert.True(anySucceeded, $"both transactions failed: {results[0].Type} {results[0].Reason} / {results[1].Type} {results[1].Reason}");

                // A live holder answers immediately, so neither side may sit out the transaction deadline.
                Assert.True(elapsed < TimeSpan.FromSeconds(5), $"the pair took {elapsed.TotalMilliseconds:F0} ms, which is the transaction deadline");
            }
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }

    /// <summary>
    /// A mutation statement records its outcome twice: once on the context, where the prepare path reads
    /// the highest modification timestamp and the batched forms read the whole thing, and once as the
    /// result answered to the caller. The two are not interchangeable.
    ///
    /// <para>A <c>SET</c>'s recorded outcome carries the value it wrote, because the durable path needs
    /// it; the answered result deliberately does not. A <c>DELETE</c> and an <c>EXTEND</c> carry no value
    /// in either, which is why those two may share one list and a <c>SET</c> may not. Sharing the list in
    /// a <c>SET</c> would start returning the written bytes on every set response, so the shapes are
    /// asserted here rather than left to a reviewer to notice.</para>
    /// </summary>
    [Theory, CombinatorialData]
    public async Task TestMutationResultsCarryNoValue([CombinatorialValues("memory")] string storage, [CombinatorialValues(1)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna _, IKahuna _) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            string key = GetRandomKey();

            KeyValueTransactionResult set = await Run(kahuna1, $"BEGIN SET '{key}' 'payload' COMMIT END");

            Assert.Equal(KeyValueResponseType.Set, set.Type);
            Assert.Null(set.Value);
            Assert.NotNull(set.Values);
            Assert.Equal(key, set.Values![0].Key);

            // A read of the same key does carry the value, so the assertion above is about the set
            // response and not about an empty store.
            KeyValueTransactionResult get = await Run(kahuna1, $"BEGIN GET '{key}' COMMIT END");

            Assert.Equal("payload", Encoding.UTF8.GetString(get.Value ?? []));

            KeyValueTransactionResult extend = await Run(kahuna1, $"BEGIN EXTEND '{key}' 60000 COMMIT END");

            Assert.Null(extend.Value);

            KeyValueTransactionResult delete = await Run(kahuna1, $"BEGIN DELETE '{key}' COMMIT END");

            Assert.Null(delete.Value);
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }

    /// <summary>
    /// Long arithmetic wrapped around: long.MaxValue + 1 gave long.MinValue, and a counter at the top of the
    /// range was stored as that value. Two other overflows reached the caller as a raw framework exception.
    /// </summary>
    [Theory, CombinatorialData]
    public async Task TestIntegerOverflowIsAScriptError([CombinatorialValues("memory")] string storage, [CombinatorialValues(1)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna _, IKahuna _) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            string[] overflows =
            [
                "9223372036854775807 + 1",
                "9223372036854775807 - -1",
                "-9223372036854775807 - 2",
                "9223372036854775807 * 2",
                "-3037000500 * 3037000500",
                "(-9223372036854775807 - 1) / -1",
                "abs(-9223372036854775807 - 1)",
                // A numeric string operand follows the same rule as a long one.
                "'9223372036854775807' + 1",
                "9223372036854775807 + '1'",
                "'-9223372036854775807' - 2",
                "4611686018427387904 * '2'",
                "'-9223372036854775808' / -1",
                "(-9223372036854775807 - 1) / '-1'"
            ];

            foreach (string overflow in overflows)
                await AssertScriptError(kahuna1, $"RETURN {overflow}", "Integer overflow");

            await AssertScriptError(kahuna1, "RETURN -'-9223372036854775808'", "Cannot negate -9223372036854775808");

            // The values at the edge of the range are not overflows.
            await AssertReturns(kahuna1, "RETURN 9223372036854775806 + 1", "9223372036854775807");
            await AssertReturns(kahuna1, "RETURN -9223372036854775807 - 1", "-9223372036854775808");
            await AssertReturns(kahuna1, "RETURN 4611686018427387903 * 2", "9223372036854775806");
            await AssertReturns(kahuna1, "RETURN -3037000499 * 3037000499", "-9223372030926249001");
            await AssertReturns(kahuna1, "RETURN (-9223372036854775807 - 1) / 1", "-9223372036854775808");
            await AssertReturns(kahuna1, "RETURN abs(-9223372036854775807)", "9223372036854775807");
            await AssertReturns(kahuna1, "RETURN -'-9223372036854775807'", "9223372036854775807");
            await AssertReturns(kahuna1, "RETURN '9223372036854775806' + 1", "9223372036854775807");

            // A counter at the top of the range: the update fails, and the stored value does not change. The
            // same update runs once as a single-key script and once inside an explicit transaction.
            string key = GetRandomKey();

            Assert.Equal(KeyValueResponseType.Set, (await Run(kahuna1, $"SET `{key}` 9223372036854775807")).Type);

            await AssertScriptError(kahuna1, $"LET v = GET `{key}`\nSET `{key}` (v + 1)", "Integer overflow");
            await AssertScriptError(kahuna1, $"BEGIN LET v = GET `{key}`\nSET `{key}` (v + 1) COMMIT END", "Integer overflow");
            await AssertReturns(kahuna1, $"GET `{key}`", "9223372036854775807");
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }

    /// <summary>
    /// A double that is not finite reached a stored key: a float literal that overflowed, an operator result,
    /// and pow() all gave an infinity or NaN with no error. A NaN then made every comparison false.
    /// </summary>
    [Theory, CombinatorialData]
    public async Task TestNonFiniteDoublesAreRefused([CombinatorialValues("memory")] string storage, [CombinatorialValues(1)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna _, IKahuna _) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            string big = LargeFloatLiteral(310);
            string tiny = "0." + new string('0', 400) + "1";

            await AssertScriptError(kahuna1, $"RETURN {big}", "Float out of range");
            await AssertScriptError(kahuna1, $"RETURN -{big}", "Float out of range");
            await AssertScriptError(kahuna1, $"RETURN {tiny}", "Float out of range");

            string[] operators =
            [
                $"{MaxDoubleLiteral} * 2",
                $"{MaxDoubleLiteral} + {MaxDoubleLiteral}",
                $"-{MaxDoubleLiteral} - {MaxDoubleLiteral}",
                $"{MaxDoubleLiteral} / 0.5",
                $"{MaxDoubleLiteral} * '2'",
                $"9223372036854775807 * {MaxDoubleLiteral}"
            ];

            foreach (string op in operators)
                await AssertScriptError(kahuna1, $"RETURN {op}", "is not a finite number");

            await AssertScriptError(kahuna1, "RETURN pow(10, 400)", "is not a finite number");
            await AssertScriptError(kahuna1, "RETURN pow(-1, 0.5)", "is not a finite number");
            await AssertScriptError(kahuna1, "RETURN pow(0, -1)", "is not a finite number");

            // A string that spells a value that is not finite is not a numeric string.
            await AssertScriptError(kahuna1, "RETURN 'Infinity' + 1.0", "Invalid operands");
            await AssertScriptError(kahuna1, "RETURN 'NaN' * 1", "Invalid operands");
            await AssertScriptError(kahuna1, "RETURN 1 + '1e400'", "Invalid operands");
            await AssertScriptError(kahuna1, "RETURN -'NaN'", "Invalid operand to negate");
            await AssertScriptError(kahuna1, "RETURN 'NaN' > 1", "Invalid operands");

            // A refused value is never stored, and the key keeps its earlier value.
            string key = GetRandomKey();

            Assert.Equal(KeyValueResponseType.Set, (await Run(kahuna1, $"SET `{key}` 1.5")).Type);

            await AssertScriptError(kahuna1, $"SET `{key}` pow(10, 400)", "is not a finite number");
            await AssertScriptError(kahuna1, $"BEGIN SET `{key}` {MaxDoubleLiteral} * 10 COMMIT END", "is not a finite number");
            await AssertReturns(kahuna1, $"GET `{key}`", "1.5");

            // Finite values at the edge of the range still work, and an operator result that underflows is
            // an ordinary zero rather than an error.
            await AssertReturns(kahuna1, $"RETURN {MaxDoubleLiteral}", double.MaxValue.ToString(CultureInfo.InvariantCulture));
            await AssertReturns(kahuna1, $"RETURN -{MaxDoubleLiteral}", double.MinValue.ToString(CultureInfo.InvariantCulture));
            await AssertReturns(kahuna1, "RETURN pow(2, 1023)", Math.Pow(2, 1023).ToString(CultureInfo.InvariantCulture));
            await AssertReturns(kahuna1, "RETURN 0.000001", 0.000001.ToString(CultureInfo.InvariantCulture));
            await AssertReturns(kahuna1, "RETURN 0.0", "0");
            await AssertReturns(kahuna1, "RETURN 0.000", "0");

            string small = "0." + new string('0', 199) + "1";

            await AssertReturns(kahuna1, $"RETURN {small} * {small}", "0");
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }

    /// <summary>
    /// to_int saturated a double outside the long range to long.MaxValue, and to_double accepted 'NaN' and
    /// '1e400' while it refused 'Infinity'.
    /// </summary>
    [Theory, CombinatorialData]
    public async Task TestNumericCastsRefuseNonFiniteAndOutOfRange([CombinatorialValues("memory")] string storage, [CombinatorialValues(1)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna _, IKahuna _) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            // A finite double truncates toward zero.
            await AssertReturns(kahuna1, "RETURN to_int(2.5)", "2");
            await AssertReturns(kahuna1, "RETURN to_int(-1.5)", "-1");
            await AssertReturns(kahuna1, "RETURN to_int(-9223372036854775808.0)", "-9223372036854775808");
            await AssertReturns(kahuna1, "RETURN to_int(pow(2, 62))", "4611686018427387904");
            await AssertReturns(kahuna1, "RETURN to_int(round(2.7, 0))", "3");

            // 2^63 is one above long.MaxValue, so it is out of range, although a cast to long saturates it.
            await AssertScriptError(kahuna1, "RETURN to_int(pow(2, 63))", "out of range");
            await AssertScriptError(kahuna1, "RETURN to_int(9223372036854775808.0)", "out of range");
            await AssertScriptError(kahuna1, "RETURN to_int(-pow(10, 300))", "out of range");

            await AssertScriptError(kahuna1, "RETURN to_double('NaN')", "Cannot cast string value 'NaN' to double");
            await AssertScriptError(kahuna1, "RETURN to_double('Infinity')", "Cannot cast string value 'Infinity' to double");
            await AssertScriptError(kahuna1, "RETURN to_double('-Infinity')", "Cannot cast string value '-Infinity' to double");
            await AssertScriptError(kahuna1, "RETURN to_double('1e400')", "Cannot cast string value '1e400' to double");

            await AssertReturns(kahuna1, "RETURN to_double('1.5')", "1.5");
            await AssertReturns(kahuna1, "RETURN to_double('-2.5e3')", "-2500");
            await AssertReturns(kahuna1, "RETURN to_int('-42')", "-42");

            // A stored NaN stays readable as text, but a script cannot turn it into a number.
            string key = GetRandomKey();

            Assert.Equal(KeyValueResponseType.Set, (await Run(kahuna1, $"SET `{key}` 'NaN'")).Type);

            await AssertReturns(kahuna1, $"GET `{key}`", "NaN");
            await AssertScriptError(kahuna1, $"LET v = GET `{key}`\nRETURN to_int(to_double(v))", "Cannot cast string value 'NaN' to double");
            await AssertScriptError(kahuna1, $"LET v = GET `{key}`\nRETURN v + 1", "Invalid operands");
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }

    /// <summary>
    /// to_double parsed with the culture of the server, so on a server whose culture uses a comma for decimals
    /// '1.5' read as fifteen. The function is called directly here because the culture belongs to the calling
    /// thread, and the script path runs on actor threads that the test cannot set.
    /// </summary>
    [Fact]
    public void TestNumericCastsIgnoreTheServerCulture()
    {
        CultureInfo saved = CultureInfo.CurrentCulture;

        try
        {
            CultureInfo.CurrentCulture = CultureInfo.GetCultureInfo("de-DE");

            NodeAst ast = new(NodeType.FuncCall, null, null, null, null, null, null, "to_double", 1);

            KeyValueExpressionResult asDouble = CastToDoubleFunction.Execute(ast, [new KeyValueExpressionResult("1.5")]);

            Assert.Equal(KeyValueExpressionType.DoubleType, asDouble.Type);
            Assert.Equal(1.5, asDouble.DoubleValue);

            KeyValueExpressionResult asLong = CastToLongFunction.Execute(ast, [new KeyValueExpressionResult("-42")]);

            Assert.Equal(KeyValueExpressionType.LongType, asLong.Type);
            Assert.Equal(-42, asLong.LongValue);

            // The thousands separator of the culture is not accepted either.
            Assert.Throws<KahunaScriptException>(() => CastToLongFunction.Execute(ast, [new KeyValueExpressionResult("1.000")]));
        }
        finally
        {
            CultureInfo.CurrentCulture = saved;
        }
    }

    /// <summary>
    /// A long converted to a double before the comparison, so 2^53 + 1 compared equal to 2^53, and
    /// long.MaxValue compared equal to 2^63.
    /// </summary>
    [Theory, CombinatorialData]
    public async Task TestLongAndDoubleCompareExactly([CombinatorialValues("memory")] string storage, [CombinatorialValues(1)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna _, IKahuna _) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            (string Expression, string Expected)[] cases =
            [
                ("9007199254740993 == 9007199254740992.0", "false"),
                ("9007199254740993 != 9007199254740992.0", "true"),
                ("9007199254740993 > 9007199254740992.0", "true"),
                ("9007199254740993 >= 9007199254740992.0", "true"),
                ("9007199254740993 < 9007199254740992.0", "false"),
                ("9007199254740993 <= 9007199254740992.0", "false"),
                ("9007199254740992.0 < 9007199254740993", "true"),
                ("9007199254740992.0 == 9007199254740993", "false"),
                ("9223372036854775807 == 9223372036854775808.0", "false"),
                ("9223372036854775807 < 9223372036854775808.0", "true"),
                ("(-9223372036854775807 - 1) == -9223372036854775808.0", "true"),
                ("(-9223372036854775807 - 1) > -9223372036854775808.0", "false"),
                ("-9223372036854775807 > -9223372036854775808.0", "true"),
                ("'9007199254740993' == 9007199254740992.0", "false"),
                ("9007199254740993 == '9007199254740992.0'", "false"),
                ("'9007199254740993' > 9007199254740992.0", "true"),
                ("1 == 1.0", "true"),
                ("1 == 1.0009", "false"),
                ("2 > 1.5", "true"),
                ("1 < 1.5", "true"),
                ("-1 < -0.5", "true"),
                ("-1 > -1.5", "true"),
                ("0 == -0.0", "true")
            ];

            foreach ((string expression, string expected) in cases)
                await AssertReturns(kahuna1, $"RETURN {expression}", expected);

            // A CASE matches by the rule of ==.
            string switchScript = """
            SWITCH 9007199254740993
              CASE 9007199254740992.0 THEN RETURN 'rounded'
              CASE 9007199254740993.0 THEN RETURN 'rounded too'
            ELSE
              RETURN 'exact'
            END
            """;

            await AssertReturns(kahuna1, switchScript, "exact");

            // min and max choose by the exact value and return the chosen argument with its own type.
            await AssertReturns(kahuna1, "RETURN max(9007199254740993, 9007199254740992.0)", "9007199254740993");
            await AssertReturns(kahuna1, "RETURN is_long(max(9007199254740993, 9007199254740992.0))", "true");
            await AssertReturns(kahuna1, "RETURN is_double(min(9007199254740993, 9007199254740992.0))", "true");
            await AssertReturns(kahuna1, "RETURN is_long(max(3, 2.5))", "true");
            await AssertReturns(kahuna1, "RETURN is_double(min(3, 2.5))", "true");
            await AssertReturns(kahuna1, "RETURN max(1, 2.5)", "2.5");
            await AssertReturns(kahuna1, "RETURN is_long(max(2, 2.0))", "true");
            await AssertReturns(kahuna1, "RETURN is_double(max(2.0, 2))", "true");
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }

    /// <summary>
    /// Several statement options narrowed a script value with no check: SET EX cast a long to an int, so a
    /// large expiry became one millisecond; AS OF and CMPREV truncated a fraction and saturated a large
    /// double; the integer literal of EXTEND and AT went through int.Parse, which refused a hexadecimal literal
    /// and a large one with a framework message. round() passed its digits to Math.Round unchecked.
    /// </summary>
    [Theory, CombinatorialData]
    public async Task TestStatementOptionsRefuseNarrowedValues([CombinatorialValues("memory")] string storage, [CombinatorialValues(1)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna _, IKahuna _) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            // SET EX: an expiry outside the int range is refused, and nothing is stored.
            foreach (string ex in new[] { "4294967297", "2147483648", "-1" })
            {
                string expiring = GetRandomKey();

                await AssertScriptError(kahuna1, $"SET `{expiring}` 'v' EX {ex}", "SET EX");
                Assert.Equal(KeyValueResponseType.DoesNotExist, (await Run(kahuna1, $"GET `{expiring}`")).Type);
            }

            string kept = GetRandomKey();

            Assert.Equal(KeyValueResponseType.Set, (await Run(kahuna1, $"SET `{kept}` 'v' EX 2147483647")).Type);
            await AssertReturns(kahuna1, $"GET `{kept}`", "v");

            // Zero means no expiry.
            string noExpiry = GetRandomKey();

            Assert.Equal(KeyValueResponseType.Set, (await Run(kahuna1, $"SET `{noExpiry}` 'v' EX 0")).Type);
            await Task.Delay(50, TestContext.Current.CancellationToken);
            await AssertReturns(kahuna1, $"GET `{noExpiry}`", "v");

            // round() accepts the digits Math.Round accepts, and refuses the others with a script error.
            await AssertReturns(kahuna1, "RETURN round(1.25, 15)", "1.25");
            await AssertReturns(kahuna1, "RETURN round(1.5, 0)", "2");

            // A value exactly halfway rounds to the even neighbor.
            await AssertReturns(kahuna1, "RETURN round(2.5, 0)", "2");
            await AssertReturns(kahuna1, "RETURN round(3.5, 0)", "4");

            foreach (string digits in new[] { "16", "20", "-1" })
                await AssertScriptError(kahuna1, $"RETURN round(1.5, {digits})", "digits");

            string key = GetRandomKey();

            Assert.Equal(KeyValueResponseType.Set, (await Run(kahuna1, $"SET `{key}` 'v'")).Type);

            // AS OF and CMPREV take a whole number in the long range.
            string huge = LargeFloatLiteral(31);

            foreach (string value in new[] { "1.5", "'abc'", "'99999999999999999999'", huge })
            {
                await AssertScriptError(kahuna1, $"GET `{key}` AS OF {value}", "AS OF");
                await AssertScriptError(kahuna1, $"SET `{key}` 'w' CMPREV {value}", "CMPREV");
            }

            await AssertScriptError(kahuna1, $"GET `{key}` AS OF -1", "AS OF");
            await AssertReturns(kahuna1, $"GET `{key}`", "v");

            // The integer literal of EXTEND and AT reads like every other integer literal: a hexadecimal one
            // works, and one outside the accepted range is a script error.
            await AssertScriptError(kahuna1, $"EXTEND `{key}` 99999999999", "EXTEND");
            await AssertScriptError(kahuna1, $"EXTEND `{key}` 2147483648", "EXTEND");
            Assert.Equal(KeyValueResponseType.Extended, (await Run(kahuna1, $"EXTEND `{key}` 0xEA60")).Type);

            await AssertScriptError(kahuna1, $"GET `{key}` AT 99999999999999999999", "out of range");
            await AssertScriptError(kahuna1, $"EXISTS `{key}` AT 99999999999999999999", "out of range");
            Assert.NotEqual(KeyValueResponseType.Errored, (await Run(kahuna1, $"GET `{key}` AT 99999999999")).Type);
            Assert.NotEqual(KeyValueResponseType.Errored, (await Run(kahuna1, $"EXISTS `{key}` AT 0x0")).Type);

            // An index of 2^63 names the value the author wrote, not the saturated one.
            await AssertScriptError(kahuna1, "RETURN substring('abc', 9223372036854775808.0)", "out of range");
            await AssertScriptError(kahuna1, "RETURN [1, 2][9223372036854775808.0]", "out of range");
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }

    /// <summary>
    /// Boundary rules that already held when overflow became a script error. They are kept here so that a
    /// change to the numeric code cannot break them without a failure.
    /// </summary>
    [Theory, CombinatorialData]
    public async Task TestNumericBoundariesThatAlreadyHeld([CombinatorialValues("memory")] string storage, [CombinatorialValues(1)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna _, IKahuna _) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            await AssertScriptError(kahuna1, "RETURN 9223372036854775808", "Integer out of range");
            await AssertScriptError(kahuna1, "RETURN -(-9223372036854775807 - 1)", "Cannot negate -9223372036854775808");
            await AssertScriptError(kahuna1, "RETURN to_int('9223372036854775808')", "Cannot cast string value '9223372036854775808' to int");
            await AssertScriptError(kahuna1, "RETURN 1 / '0.0'", "Division by zero");

            await AssertReturns(kahuna1, "RETURN 0.0 == -0.0", "true");
            await AssertReturns(kahuna1, "RETURN 0x8000000000000000", "-9223372036854775808");
            await AssertReturns(kahuna1, "RETURN 0x7FFFFFFFFFFFFFFF", "9223372036854775807");
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }

    /// <summary>
    /// The numeric limits are functions because two of them have no practical literal: the smallest
    /// integer has none at all, and the largest double needs all 309 digits. A script needs them to check a
    /// value before an operation that would overflow.
    /// </summary>
    [Theory, CombinatorialData]
    public async Task TestNumericLimitFunctions([CombinatorialValues("memory")] string storage, [CombinatorialValues(1)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna _, IKahuna _) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            await AssertReturns(kahuna1, "RETURN max_int()", "9223372036854775807");
            await AssertReturns(kahuna1, "RETURN min_int()", "-9223372036854775808");
            await AssertReturns(kahuna1, "RETURN max_double()", double.MaxValue.ToString(CultureInfo.InvariantCulture));
            await AssertReturns(kahuna1, "RETURN min_double()", double.MinValue.ToString(CultureInfo.InvariantCulture));

            await AssertReturns(kahuna1, "RETURN is_long(max_int()) && is_long(min_int())", "true");
            await AssertReturns(kahuna1, "RETURN is_double(max_double()) && is_double(min_double())", "true");
            await AssertReturns(kahuna1, "RETURN min_int() == -9223372036854775807 - 1", "true");
            await AssertReturns(kahuna1, $"RETURN max_double() == {MaxDoubleLiteral}", "true");
            await AssertReturns(kahuna1, "RETURN min_double() == -max_double()", "true");

            // The limits are the edge of the range, not past it.
            await AssertScriptError(kahuna1, "RETURN max_int() + 1", "Integer overflow");
            await AssertScriptError(kahuna1, "RETURN min_int() - 1", "Integer overflow");
            await AssertScriptError(kahuna1, "RETURN max_double() * 2", "is not a finite number");

            foreach (string name in new[] { "max_int", "min_int", "max_double", "min_double" })
                await AssertScriptError(kahuna1, $"RETURN {name}(1)", $"Invalid number of arguments for '{name}' function");

            // The guard idiom: check the headroom before the add, and choose the outcome.
            string key = GetRandomKey();

            Assert.Equal(KeyValueResponseType.Set, (await Run(kahuna1, $"SET `{key}` 9223372036854775800")).Type);

            string capped =
                $"LET v = GET `{key}`\n" +
                "LET n = 10\n" +
                "IF to_int(v) > max_int() - n THEN\n" +
                $"  SET `{key}` max_int()\n" +
                "  RETURN 'capped'\n" +
                "END\n" +
                $"SET `{key}` (v + n)\n" +
                "RETURN 'added'";

            await AssertReturns(kahuna1, capped, "capped");
            await AssertReturns(kahuna1, $"GET `{key}`", "9223372036854775807");

            // A start value for a minimum search: any finite value replaces it.
            string minimum =
                "LET lowest = max_double()\n" +
                "FOR x IN [3.5, -2, 7] DO\n" +
                "  IF x < lowest THEN\n" +
                "    LET lowest = x\n" +
                "  END\n" +
                "END\n" +
                "RETURN lowest";

            await AssertReturns(kahuna1, minimum, "-2");
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }

    private static Task<KeyValueTransactionResult> Run(IKahuna kahuna, string script)
    {
        return kahuna.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);
    }
}
