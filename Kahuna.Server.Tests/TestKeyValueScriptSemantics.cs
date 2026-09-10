
using System.Text;
using Kahuna.Server.KeyValues.Transactions.Data;
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

        return resp.Reason!;
    }

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
    /// literal two-character text. A control character is also excluded from an unescaped literal, so until
    /// now there was no way at all to put a line break into a value.
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
}
