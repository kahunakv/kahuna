using System.Text;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// Execution-level tests for the string inspection functions: substring, starts_with, ends_with,
/// index_of, split and trim.
///
/// <para>Every one of them compares characters ordinally, so a test that only used ASCII letters
/// would pass under a culture-aware comparison too. The cases below therefore include the pairs a
/// locale treats as equal or as one character.</para>
/// </summary>
public class TestKeyValueScriptStringFunctions : BaseCluster
{
    private readonly ILogger<IRaft> raftLogger;

    private readonly ILogger<IKahuna> kahunaLogger;

    public TestKeyValueScriptStringFunctions(ITestOutputHelper outputHelper)
    {
        ILoggerFactory loggerFactory = TestLogFactory.Create(outputHelper, quietKommander: true);

        raftLogger = loggerFactory.CreateLogger<IRaft>();
        kahunaLogger = loggerFactory.CreateLogger<IKahuna>();
    }

    private static async Task AssertReturns(IKahuna kahuna, string script, string expected)
    {
        KeyValueTransactionResult resp = await kahuna.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);

        Assert.True(resp.Type == KeyValueResponseType.Get, $"expected Get for \"{script}\", got {resp.Type}: {resp.Reason}");
        Assert.Equal(expected, Encoding.UTF8.GetString(resp.Value ?? []));
    }

    private static async Task<string> AssertErrored(IKahuna kahuna, string script)
    {
        KeyValueTransactionResult resp = await kahuna.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);

        Assert.Equal(KeyValueResponseType.Errored, resp.Type);
        Assert.False(string.IsNullOrEmpty(resp.Reason), "an errored script must carry a reason");

        // A framework exception name in the reason means the failure escaped the script error path and the
        // caller has no line to look at.
        Assert.DoesNotContain("ArgumentOutOfRangeException", resp.Reason, StringComparison.Ordinal);
        Assert.DoesNotContain("IndexOutOfRangeException", resp.Reason, StringComparison.Ordinal);
        Assert.DoesNotContain("NullReferenceException", resp.Reason, StringComparison.Ordinal);
        Assert.DoesNotContain("ArgumentException", resp.Reason, StringComparison.Ordinal);

        return resp.Reason!;
    }

    [Theory, CombinatorialData]
    public async Task TestSubstring([CombinatorialValues("memory")] string storage, [CombinatorialValues(1)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna _, IKahuna _) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            await AssertReturns(kahuna1, "RETURN substring('hello', 0)", "hello");
            await AssertReturns(kahuna1, "RETURN substring('hello', 1)", "ello");

            // The last position, then the position just past it. A start equal to the length is the empty
            // string, which is what makes substring(s, index_of(s, x) + 1) work for a trailing separator.
            await AssertReturns(kahuna1, "RETURN substring('hello', 4)", "o");
            await AssertReturns(kahuna1, "RETURN substring('hello', 5)", "");

            // A single character, and the empty string, at both of their boundaries.
            await AssertReturns(kahuna1, "RETURN substring('a', 0)", "a");
            await AssertReturns(kahuna1, "RETURN substring('a', 1)", "");
            await AssertReturns(kahuna1, "RETURN substring('', 0)", "");

            await AssertReturns(kahuna1, "RETURN substring('hello', 1, 3)", "ell");
            await AssertReturns(kahuna1, "RETURN substring('hello', 0, 5)", "hello");
            await AssertReturns(kahuna1, "RETURN substring('hello', 1, 0)", "");
            await AssertReturns(kahuna1, "RETURN substring('hello', 5, 0)", "");

            // A start computed by arithmetic, which is the shape that reads one field out of a value.
            await AssertReturns(kahuna1, "RETURN substring('42:ok', index_of('42:ok', ':') + 1)", "ok");
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }

    /// <summary>
    /// A position that does not describe the string is refused. Clamping it to the nearest valid one
    /// would answer with a part of the string the arithmetic never asked for, and the author would see
    /// a wrong value instead of the mistake that produced it.
    /// </summary>
    [Theory, CombinatorialData]
    public async Task TestSubstringRefusesPositionsOutsideTheString([CombinatorialValues("memory")] string storage, [CombinatorialValues(1)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna _, IKahuna _) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            string past = await AssertErrored(kahuna1, "RETURN substring('hello', 6)");
            Assert.Contains("Start index must be between 0 and 5", past, StringComparison.Ordinal);
            Assert.Contains("at line 1", past, StringComparison.Ordinal);

            string emptyPast = await AssertErrored(kahuna1, "RETURN substring('', 1)");
            Assert.Contains("Start index must be between 0 and 0", emptyPast, StringComparison.Ordinal);

            string negative = await AssertErrored(kahuna1, "RETURN substring('hello', 0-1)");
            Assert.Contains("Start index must be between 0 and 5", negative, StringComparison.Ordinal);

            // A fractional position is refused exactly as a fractional subscript is.
            string fractional = await AssertErrored(kahuna1, "RETURN substring('hello', 1.5)");
            Assert.Contains("Start index must be a whole number", fractional, StringComparison.Ordinal);

            string fractionalLength = await AssertErrored(kahuna1, "RETURN substring('hello', 0, 1.5)");
            Assert.Contains("Length must be a whole number", fractionalLength, StringComparison.Ordinal);

            string negativeLength = await AssertErrored(kahuna1, "RETURN substring('hello', 1, 0-1)");
            Assert.Contains("Length must not be negative", negativeLength, StringComparison.Ordinal);

            string longLength = await AssertErrored(kahuna1, "RETURN substring('hello', 2, 4)");
            Assert.Contains("Length must not exceed the 3 characters after index 2", longLength, StringComparison.Ordinal);

            string notAString = await AssertErrored(kahuna1, "RETURN substring(5, 0)");
            Assert.Contains("Cannot use 'substring' function on argument", notAString, StringComparison.Ordinal);

            string oneArgument = await AssertErrored(kahuna1, "RETURN substring('hello')");
            Assert.Contains("Invalid number of arguments for 'substring' function", oneArgument, StringComparison.Ordinal);

            string fourArguments = await AssertErrored(kahuna1, "RETURN substring('hello', 0, 1, 2)");
            Assert.Contains("Invalid number of arguments for 'substring' function", fourArguments, StringComparison.Ordinal);
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }

    [Theory, CombinatorialData]
    public async Task TestStartsWithAndEndsWith([CombinatorialValues("memory")] string storage, [CombinatorialValues(1)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna _, IKahuna _) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            await AssertReturns(kahuna1, "RETURN starts_with('hello', 'he')", "true");
            await AssertReturns(kahuna1, "RETURN starts_with('hello', 'hello')", "true");
            await AssertReturns(kahuna1, "RETURN starts_with('hello', 'hello!')", "false");
            await AssertReturns(kahuna1, "RETURN starts_with('hello', 'el')", "false");

            await AssertReturns(kahuna1, "RETURN ends_with('hello', 'lo')", "true");
            await AssertReturns(kahuna1, "RETURN ends_with('hello', 'hello')", "true");
            await AssertReturns(kahuna1, "RETURN ends_with('hello', 'll')", "false");

            // The comparison is ordinal: case is a difference, and so is an accent a locale folds away.
            await AssertReturns(kahuna1, "RETURN starts_with('hello', 'He')", "false");
            await AssertReturns(kahuna1, "RETURN ends_with('HELLO', 'lo')", "false");
            await AssertReturns(kahuna1, "RETURN starts_with('resume', 'résumé')", "false");

            // Empty operands, at both ends of both functions.
            await AssertReturns(kahuna1, "RETURN starts_with('hello', '')", "true");
            await AssertReturns(kahuna1, "RETURN ends_with('hello', '')", "true");
            await AssertReturns(kahuna1, "RETURN starts_with('', '')", "true");
            await AssertReturns(kahuna1, "RETURN ends_with('', '')", "true");
            await AssertReturns(kahuna1, "RETURN starts_with('', 'a')", "false");
            await AssertReturns(kahuna1, "RETURN ends_with('', 'a')", "false");

            // A single character on both sides.
            await AssertReturns(kahuna1, "RETURN starts_with('a', 'a')", "true");
            await AssertReturns(kahuna1, "RETURN ends_with('a', 'a')", "true");

            string notAString = await AssertErrored(kahuna1, "RETURN starts_with('hello', 5)");
            Assert.Contains("Cannot use 'starts_with' function on argument", notAString, StringComparison.Ordinal);

            string arity = await AssertErrored(kahuna1, "RETURN ends_with('hello')");
            Assert.Contains("Invalid number of arguments for 'ends_with' function", arity, StringComparison.Ordinal);
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }

    [Theory, CombinatorialData]
    public async Task TestIndexOf([CombinatorialValues("memory")] string storage, [CombinatorialValues(1)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna _, IKahuna _) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            await AssertReturns(kahuna1, "RETURN index_of('a:b', ':')", "1");
            await AssertReturns(kahuna1, "RETURN index_of(':ab', ':')", "0");
            await AssertReturns(kahuna1, "RETURN index_of('ab:', ':')", "2");

            // The first occurrence, not the last.
            await AssertReturns(kahuna1, "RETURN index_of('a:b:c', ':')", "1");

            // A needle of more than one character, and one that is not there.
            await AssertReturns(kahuna1, "RETURN index_of('a::b', '::')", "1");
            await AssertReturns(kahuna1, "RETURN index_of('abc', 'z')", "-1");
            await AssertReturns(kahuna1, "RETURN index_of('', 'a')", "-1");

            // Ordinal: a case difference is a difference.
            await AssertReturns(kahuna1, "RETURN index_of('abc', 'B')", "-1");

            // An empty needle occurs at the start, so "index_of(s, x) >= 0" reads as "s contains x" for
            // every x, including the empty string.
            await AssertReturns(kahuna1, "RETURN index_of('abc', '')", "0");
            await AssertReturns(kahuna1, "RETURN index_of('', '')", "0");

            // A single character in a single-character string.
            await AssertReturns(kahuna1, "RETURN index_of('a', 'a')", "0");

            string arity = await AssertErrored(kahuna1, "RETURN index_of('abc')");
            Assert.Contains("Invalid number of arguments for 'index_of' function", arity, StringComparison.Ordinal);
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }

    [Theory, CombinatorialData]
    public async Task TestTrim([CombinatorialValues("memory")] string storage, [CombinatorialValues(1)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna _, IKahuna _) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            await AssertReturns(kahuna1, "RETURN trim('  hello  ')", "hello");
            await AssertReturns(kahuna1, "RETURN trim('hello  ')", "hello");
            await AssertReturns(kahuna1, "RETURN trim('  hello')", "hello");
            await AssertReturns(kahuna1, "RETURN trim('hello')", "hello");

            // Whitespace between the characters is not touched.
            await AssertReturns(kahuna1, "RETURN trim('  a b  ')", "a b");

            // A tab and a newline are whitespace too.
            await AssertReturns(kahuna1, "RETURN trim('\\t hello \\n')", "hello");

            await AssertReturns(kahuna1, "RETURN trim('')", "");
            await AssertReturns(kahuna1, "RETURN trim('   ')", "");
            await AssertReturns(kahuna1, "RETURN trim('a')", "a");
            await AssertReturns(kahuna1, "RETURN trim(' a ')", "a");

            string arity = await AssertErrored(kahuna1, "RETURN trim('a', 'b')");
            Assert.Contains("Invalid number of arguments for 'trim' function", arity, StringComparison.Ordinal);

            string notAString = await AssertErrored(kahuna1, "RETURN trim(5)");
            Assert.Contains("Cannot use 'trim' function on argument", notAString, StringComparison.Ordinal);
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }

    /// <summary>
    /// The parts, their count, and the empty parts an author has to be able to rely on: a value packed
    /// as "a:b:c" must read field two as field two whether or not field one was empty.
    /// </summary>
    [Theory, CombinatorialData]
    public async Task TestSplit([CombinatorialValues("memory")] string storage, [CombinatorialValues(1)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna _, IKahuna _) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            await AssertReturns(kahuna1, "RETURN count(split('a,b,c', ','))", "3");
            await AssertReturns(kahuna1, "RETURN split('a,b,c', ',')[0]", "a");
            await AssertReturns(kahuna1, "RETURN split('a,b,c', ',')[1]", "b");
            await AssertReturns(kahuna1, "RETURN split('a,b,c', ',')[2]", "c");

            // A string with no separator in it is one part, not an empty array.
            await AssertReturns(kahuna1, "RETURN count(split('abc', ','))", "1");
            await AssertReturns(kahuna1, "RETURN split('abc', ',')[0]", "abc");

            // One separator, then a leading one, a trailing one, and two adjacent ones. Every empty part
            // is kept, so the position of a field does not depend on whether an earlier field was empty.
            await AssertReturns(kahuna1, "RETURN count(split('a,b', ','))", "2");

            await AssertReturns(kahuna1, "RETURN count(split(',a', ','))", "2");
            await AssertReturns(kahuna1, "RETURN split(',a', ',')[0]", "");
            await AssertReturns(kahuna1, "RETURN split(',a', ',')[1]", "a");

            await AssertReturns(kahuna1, "RETURN count(split('a,', ','))", "2");
            await AssertReturns(kahuna1, "RETURN split('a,', ',')[1]", "");

            await AssertReturns(kahuna1, "RETURN count(split('a,,b', ','))", "3");
            await AssertReturns(kahuna1, "RETURN split('a,,b', ',')[1]", "");

            // A split of the empty string is one empty part.
            await AssertReturns(kahuna1, "RETURN count(split('', ','))", "1");
            await AssertReturns(kahuna1, "RETURN split('', ',')[0]", "");

            // A separator of more than one character is consumed whole, so the single ':' between the
            // two '::' pairs stays inside a part.
            await AssertReturns(kahuna1, "RETURN count(split('a::b:c::d', '::'))", "3");
            await AssertReturns(kahuna1, "RETURN split('a::b:c::d', '::')[1]", "b:c");

            // Ordinal: a separator that differs only in case does not cut.
            await AssertReturns(kahuna1, "RETURN count(split('aXb', 'x'))", "1");

            // A separator equal to the whole string leaves an empty part on each side.
            await AssertReturns(kahuna1, "RETURN count(split(',', ','))", "2");
            await AssertReturns(kahuna1, "RETURN split(',', ',')[0]", "");
            await AssertReturns(kahuna1, "RETURN split(',', ',')[1]", "");
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }

    /// <summary>
    /// The result is the array type the language already had, so the two things a script does with an
    /// array both work on it with no further change.
    /// </summary>
    [Theory, CombinatorialData]
    public async Task TestSplitResultIsAnOrdinaryArray([CombinatorialValues("memory")] string storage, [CombinatorialValues(1)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna _, IKahuna _) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            string script = """
            LET parts = split('a,b,c', ',')
            LET joined = ''
            FOR p IN parts DO
                LET joined = concat(joined, p)
            END
            RETURN joined
            """;

            await AssertReturns(kahuna1, script, "abc");

            // count() feeds the loop bound, which is the other half of the same claim.
            string indexed = """
            LET parts = split('10,20,30', ',')
            LET total = 0
            FOR i IN 0..count(parts)-1 DO
                LET total = total + to_int(parts[i])
            END
            RETURN total
            """;

            await AssertReturns(kahuna1, indexed, "60");
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }

    [Theory, CombinatorialData]
    public async Task TestSplitRefusesAnEmptySeparator([CombinatorialValues("memory")] string storage, [CombinatorialValues(1)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna _, IKahuna _) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            // One part per character and no cut at all are both plausible readings of an empty separator,
            // so it is refused rather than guessed.
            string reason = await AssertErrored(kahuna1, "RETURN count(split('abc', ''))");
            Assert.Contains("Separator for 'split' function must not be empty", reason, StringComparison.Ordinal);
            Assert.Contains("at line 1", reason, StringComparison.Ordinal);

            string emptyBoth = await AssertErrored(kahuna1, "RETURN count(split('', ''))");
            Assert.Contains("Separator for 'split' function must not be empty", emptyBoth, StringComparison.Ordinal);

            string arity = await AssertErrored(kahuna1, "RETURN split('abc')");
            Assert.Contains("Invalid number of arguments for 'split' function", arity, StringComparison.Ordinal);

            string notAString = await AssertErrored(kahuna1, "RETURN split('abc', 5)");
            Assert.Contains("Cannot use 'split' function on argument", notAString, StringComparison.Ordinal);
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }

    /// <summary>
    /// One call cannot materialize an unbounded list. The parts are counted before any of them is built,
    /// so a string past the limit is refused without allocating the list it asked for.
    ///
    /// <para>The input arrives as a parameter rather than as a literal, because a literal long enough to
    /// reach the limit would be refused by the script length limit first, and the cap would go
    /// untested.</para>
    /// </summary>
    [Theory, CombinatorialData]
    public async Task TestSplitRefusesMoreElementsThanTheLimit([CombinatorialValues("memory")] string storage, [CombinatorialValues(1)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna _, IKahuna _) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            const string script = "RETURN count(split(@packed, ','))";

            // Separators, so the part count is one above the separator count. This one lands exactly on
            // the limit and is allowed.
            string atTheLimit = new(',', 99_999);

            KeyValueTransactionResult allowed = await kahuna1.TryExecuteTransactionScript(
                Encoding.UTF8.GetBytes(script), null, [new() { Key = "@packed", Value = atTheLimit }]);

            Assert.True(allowed.Type == KeyValueResponseType.Get, $"expected Get, got {allowed.Type}: {allowed.Reason}");
            Assert.Equal("100000", Encoding.UTF8.GetString(allowed.Value ?? []));

            // One separator more is one part more, and it is refused.
            string pastTheLimit = new(',', 100_000);

            KeyValueTransactionResult refused = await kahuna1.TryExecuteTransactionScript(
                Encoding.UTF8.GetBytes(script), null, [new() { Key = "@packed", Value = pastTheLimit }]);

            Assert.Equal(KeyValueResponseType.Errored, refused.Type);
            Assert.Contains("exceeds the limit", refused.Reason ?? "", StringComparison.Ordinal);
            Assert.Contains("at line 1", refused.Reason ?? "", StringComparison.Ordinal);
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }

    /// <summary>
    /// The functions read what the store holds, which is the case the whole group exists for: one key
    /// carries several fields, and a script reads one of them.
    /// </summary>
    [Theory, CombinatorialData]
    public async Task TestStringFunctionsOverStoredValues([CombinatorialValues("memory")] string storage, [CombinatorialValues(1)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna _, IKahuna _) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            string key = "k" + Guid.NewGuid().ToString("N")[..10];

            string script = $"""
            SET {key} '17:failure:node-b'
            LET raw = GET {key}
            LET parts = split(raw, ':')
            LET epoch = to_int(parts[0])
            LET outcome = parts[1]
            LET origin = parts[2]
            IF epoch = 17 && outcome = 'failure' && starts_with(origin, 'node-') THEN
                RETURN substring(origin, 5)
            END
            RETURN 'no'
            """;

            await AssertReturns(kahuna1, script, "b");
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }
}
