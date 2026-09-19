using System.Diagnostics;
using System.Text;
using Kahuna;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander.Time;
using Microsoft.Extensions.Logging;

// Smoke check for the thread-free build of Kahuna.Core. It boots a single-node embedded Kahuna with
// in-memory storage and WAL, waits for the node to elect itself, runs a key-value transaction (write,
// commit, read back), runs the same through a transaction script, and disposes the node. It prints
// one line per phase, so a failure names the phase that failed.
//
// Exit codes: 0 pass, 1 a check failed, 2 a phase did not finish in time (a deadlock or a stall).
//
// Nothing here may block the thread. The single-threaded runtime has no other thread to release a
// blocked wait, so every wait is an await, and the deadline is a Task.WhenAny race.

TimeSpan phaseTimeout = TimeSpan.FromSeconds(30);

Stopwatch total = Stopwatch.StartNew();
ILoggerFactory loggerFactory = new ConsoleLoggerFactory();

Console.WriteLine($"[smoke] runtime: browser={OperatingSystem.IsBrowser()} framework={Environment.Version}");

try
{
    // The thread-free build has only in-memory storage. A native backend must be refused at
    // construction with the option named, not fail later inside RocksDB or SQLite.
    try
    {
        await using EmbeddedKahunaNode refused = new(new() { Storage = "rocksdb", StoragePath = "/tmp/kahuna" }, loggerFactory);
        Check(false, "the constructor refuses rocksdb storage in the thread-free build");
    }
    catch (ArgumentException ex) when (ex.Message.Contains("thread-free"))
    {
        Check(true, "the constructor refuses rocksdb storage in the thread-free build");
    }

    EmbeddedKahunaNode node = new(new()
    {
        Storage = "memory",
        WalStorage = "memory",
        InitialPartitions = 1,
        HeartbeatInterval = TimeSpan.FromMilliseconds(50),
        RecentHeartbeat = TimeSpan.FromMilliseconds(25),
        VotingTimeout = TimeSpan.FromMilliseconds(250),
        CheckLeaderInterval = TimeSpan.FromMilliseconds(25),
        UpdateNodesInterval = TimeSpan.FromMilliseconds(100),
        TimerInitialDelay = TimeSpan.FromMilliseconds(25),
        StartElectionTimeout = 100,
        EndElectionTimeout = 250,
    }, loggerFactory);

    Console.WriteLine($"[smoke] constructed at {total.ElapsedMilliseconds} ms");

    using CancellationTokenSource cts = new(TimeSpan.FromSeconds(120));

    await Phase("start", node.StartAsync(cts.Token));

    const string key = "smoke/interactive";
    await Phase("elect", node.WaitForLeaderForKeyAsync(key, cts.Token));

    // An interactive transaction: start, write, commit.
    (KeyValueResponseType started, TransactionHandle handle) = await PhaseOf("begin",
        node.Kahuna.LocateAndStartTransaction(new KeyValueTransactionOptions
        {
            Locking = KeyValueTransactionLocking.Pessimistic,
            Timeout = 5000
        }, cts.Token));
    Check(started == KeyValueResponseType.Set, $"the transaction started (status {started})");

    (KeyValueResponseType set, _, _) = await PhaseOf("write",
        node.Kahuna.LocateAndTrySetKeyValue(
            handle.TransactionId, key, "interactive-value"u8.ToArray(), null, -1, KeyValueFlags.Set, 0,
            KeyValueDurability.Persistent, cts.Token, 0, handle.CoordinatorKey, TransactionOperationId.NewRandom()));
    Check(set == KeyValueResponseType.Set, $"the write inside the transaction succeeded (status {set})");

    (KeyValueResponseType committed, string? reason) = await PhaseOf("commit",
        node.Kahuna.LocateAndCommitTransaction(handle, cts.Token));
    Check(committed == KeyValueResponseType.Committed, $"the transaction committed (status {committed}, reason {reason ?? "none"})");

    (KeyValueResponseType read, ReadOnlyKeyValueEntry? entry) = await PhaseOf("read",
        node.Kahuna.LocateAndTryGetValue(HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, cts.Token));
    Check(read == KeyValueResponseType.Get && entry?.Value is not null && Encoding.UTF8.GetString(entry.Value) == "interactive-value",
        $"the committed value reads back (status {read})");

    // A script transaction. No hash is passed, so the parse cache (which needs Blake3) is not used.
    KeyValueTransactionResult script = await PhaseOf("script",
        node.Kahuna.TryExecuteTransactionScript(
            Encoding.UTF8.GetBytes("BEGIN SET `smoke/script` 'script-value' COMMIT END"), null, null));
    Check(script.Type is not (KeyValueResponseType.Errored or KeyValueResponseType.Aborted or KeyValueResponseType.MustRetry or KeyValueResponseType.InvalidInput),
        $"the script transaction committed (status {script.Type}, reason {script.Reason ?? "none"})");

    (read, entry) = await PhaseOf("script read",
        node.Kahuna.LocateAndTryGetValue(HLCTimestamp.Zero, "smoke/script", -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, cts.Token));
    Check(read == KeyValueResponseType.Get && entry?.Value is not null && Encoding.UTF8.GetString(entry.Value) == "script-value",
        $"the script's value reads back (status {read})");

    await Phase("dispose", node.DisposeAsync().AsTask());

    Console.WriteLine($"[smoke] PASS in {total.ElapsedMilliseconds} ms");
    return 0;
}
catch (SmokeCheckFailed ex)
{
    Console.WriteLine($"[smoke] FAIL: {ex.Message}");
    return 1;
}
catch (SmokePhaseTimedOut ex)
{
    Console.WriteLine($"[smoke] FAIL: {ex.Message}");
    return 2;
}
catch (Exception ex)
{
    Console.WriteLine($"[smoke] FAIL: unexpected {ex}");
    return 1;
}

async Task Phase(string name, Task task)
{
    Stopwatch watch = Stopwatch.StartNew();

    if (await Task.WhenAny(task, Task.Delay(phaseTimeout)) != task)
        throw new SmokePhaseTimedOut($"phase '{name}' did not finish in {phaseTimeout.TotalSeconds:F0} s");

    await task;
    Console.WriteLine($"[smoke] {name}: ok in {watch.ElapsedMilliseconds} ms (at {total.ElapsedMilliseconds} ms)");
}

async Task<T> PhaseOf<T>(string name, Task<T> task)
{
    await Phase(name, (Task)task);
    return await task;
}

static void Check(bool condition, string what)
{
    if (!condition)
        throw new SmokeCheckFailed(what);

    Console.WriteLine($"[smoke] check: {what}");
}

sealed class SmokeCheckFailed(string message) : Exception(message);

sealed class SmokePhaseTimedOut(string message) : Exception(message);

/// <summary>
/// Writes warnings and errors to the console. Microsoft.Extensions.Logging.Console is not used: its
/// processor writes from a dedicated thread, which the single-threaded runtime cannot start.
/// </summary>
sealed class ConsoleLoggerFactory : ILoggerFactory
{
    public ILogger CreateLogger(string categoryName) => new ConsoleLogger(categoryName);

    public void AddProvider(ILoggerProvider provider) { }

    public void Dispose() { }

    private sealed class ConsoleLogger(string category) : ILogger
    {
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => logLevel >= LogLevel.Warning;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            if (!IsEnabled(logLevel))
                return;

            Console.WriteLine($"[{category}:{logLevel}] {formatter(state, exception)}{(exception is null ? "" : " " + exception)}");
        }
    }
}
