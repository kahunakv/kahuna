using System.Text;
using BenchmarkDotNet.Attributes;
using Kahuna;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Microbenchmarks;

/// <summary>
/// Whole-script execution against a real embedded node. Every other script benchmark in this project
/// measures a fragment — scanner input decoding, expression-to-bytes conversion, function dispatch,
/// one operator — so none of them can show what a change to the executor, the expression walk, or a
/// command costs end to end. This one drives <c>TryExecuteTransactionScript</c>, the same entry point
/// the gRPC and REST surfaces call.
///
/// <para>The script shapes are chosen to separate the costs that move independently:</para>
/// <list type="bullet">
///   <item><b>Guard</b> — a guarded expression with no store access. Isolates the per-node expression
///     wrappers and the literal parse, with no write path to drown them.</item>
///   <item><b>GuardLoop</b> — the same guard inside a loop. Anything the executor rebuilds per
///     statement multiplies here, which is what makes a loop the shape where per-statement garbage
///     actually shows.</item>
///   <item><b>Parameters</b> — placeholder resolution, which walks the parameter list once per
///     reference rather than once per execution.</item>
///   <item><b>SetLoop</b> — a guarded write inside a loop, optimistic and pessimistic. The write path
///     dominates the absolute number, so read the two locking modes against each other rather than
///     against the read-only shapes.</item>
/// </list>
///
/// <para>Report allocated bytes, not only time. Most of the findings this benchmark exists to track
/// are allocation counts that a wall-clock number hides behind Raft and disk latency.</para>
/// </summary>
[MemoryDiagnoser]
public class ScriptExecutionBenchmark
{
    private EmbeddedKahunaNode node = null!;
    private string walPath = "";

    private byte[] guard = [];
    private byte[] guardLoop = [];
    private byte[] parameterized = [];
    private byte[] setLoopOptimistic = [];
    private byte[] setLoopPessimistic = [];

    private List<KeyValueParameter> parameters = [];

    [GlobalSetup]
    public async Task Setup()
    {
        walPath = Path.Combine(Path.GetTempPath(), "kahuna-script-bench-" + Guid.NewGuid().ToString("N"));

        node = new EmbeddedKahunaNode(new EmbeddedKahunaOptions
        {
            Storage = "memory",
            WalStorage = "memory",
            WalPath = walPath,
            InitialPartitions = 1
        });

        await node.StartAsync();
        await node.WaitForLeaderForKeyAsync("bench/script/k0");

        // A guard over local variables only: no key is touched, so the measurement is the expression
        // walk and nothing else.
        guard = Encoding.UTF8.GetBytes(
            """
            BEGIN
              LET x = 7
              LET y = 3
              IF x > 0 && y < 10 THEN
                LET z = x + y
              END
              COMMIT
            END
            """);

        // The same guard, 64 times. A per-statement or per-iteration rebuild is 64x larger here than
        // in the shape above, which is how the two numbers separate fixed cost from per-statement cost.
        guardLoop = Encoding.UTF8.GetBytes(
            """
            BEGIN
              LET x = 7
              LET y = 3
              LET total = 0
              FOR i IN 0..64 DO
                IF x > 0 && y < 10 THEN
                  LET total = total + i
                END
              END
              COMMIT
            END
            """);

        // One placeholder referenced from several statements, so the cost of resolving it is paid more
        // than once per execution.
        parameterized = Encoding.UTF8.GetBytes(
            """
            BEGIN
              LET a = @amount
              LET b = @amount
              IF @amount > 0 THEN
                LET c = @amount
              END
              COMMIT
            END
            """);

        parameters =
        [
            new KeyValueParameter { Key = "@amount", Value = "42" },
            new KeyValueParameter { Key = "@unused1", Value = "1" },
            new KeyValueParameter { Key = "@unused2", Value = "2" },
            new KeyValueParameter { Key = "@unused3", Value = "3" }
        ];

        // A guarded write inside a loop, once per locking mode. One key is written repeatedly on
        // purpose: the loop measures the per-statement path, and a fresh key per iteration would add
        // range-routing work that has nothing to do with it.
        setLoopOptimistic = Encoding.UTF8.GetBytes(
            """
            BEGIN (locking=optimistic)
              LET limit = 8
              FOR i IN 0..8 DO
                IF i < limit THEN
                  SET `bench/script/opt` 'v' EX 60000
                END
              END
              COMMIT
            END
            """);

        setLoopPessimistic = Encoding.UTF8.GetBytes(
            """
            BEGIN (locking=pessimistic)
              LET limit = 8
              FOR i IN 0..8 DO
                IF i < limit THEN
                  SET `bench/script/pes` 'v' EX 60000
                END
              END
              COMMIT
            END
            """);

        // Warm the parse cache and every JIT path the measured loop touches. A cold parse is a
        // different measurement, covered by ScriptParserBenchmark.
        for (int i = 0; i < 20; i++)
        {
            await Run(guard);
            await Run(guardLoop);
            await RunWithParameters(parameterized);
            await Run(setLoopOptimistic);
            await Run(setLoopPessimistic);
        }
    }

    [GlobalCleanup]
    public async Task Cleanup()
    {
        await node.DisposeAsync();

        try
        {
            if (Directory.Exists(walPath))
                Directory.Delete(walPath, recursive: true);
        }
        catch (IOException)
        {
            // A leftover temporary directory is not worth failing a benchmark run over.
        }
    }

    [Benchmark(Baseline = true)]
    public Task<KeyValueTransactionResult> Guard() => Run(guard);

    [Benchmark]
    public Task<KeyValueTransactionResult> GuardLoop() => Run(guardLoop);

    [Benchmark]
    public Task<KeyValueTransactionResult> Parameters() => RunWithParameters(parameterized);

    [Benchmark]
    public Task<KeyValueTransactionResult> SetLoopOptimistic() => Run(setLoopOptimistic);

    [Benchmark]
    public Task<KeyValueTransactionResult> SetLoopPessimistic() => Run(setLoopPessimistic);

    private Task<KeyValueTransactionResult> Run(byte[] script)
    {
        return node.Kahuna.TryExecuteTransactionScript(script, null, null);
    }

    private Task<KeyValueTransactionResult> RunWithParameters(byte[] script)
    {
        return node.Kahuna.TryExecuteTransactionScript(script, null, parameters);
    }
}
