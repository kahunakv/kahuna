using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Text;
using Kahuna;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander.Time;
using Microsoft.Extensions.Logging;

// Smoke check for the thread-free build of Kahuna.Core. It runs in two parts and prints one line per
// phase, so a failure names the phase that failed.
//
// 1. A single-node embedded Kahuna with in-memory storage and WAL: it elects itself, runs a key-value
//    transaction (write, commit, read back), runs the same through a transaction script, and is
//    disposed.
// 2. A 3-node in-memory cluster on the same event loop: every partition gets a leader, a transaction
//    commits through each node, leadership stays stable with no fault (idle, then under a light write
//    load), the leader of the meta partition is stopped and another node takes over, and the stopped
//    node restarts empty, catches up and reads the last write. A network partition then leaves the
//    isolated node unable to commit while the majority elects a leader and commits, and the healed
//    node reads that write.
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

    await RunClusterAsync();

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

async Task RunClusterAsync()
{
    // The thread-free build accepts only the in-memory transports for a cluster member. Kommander's
    // REST client is in the browser build, so it is a real transport that must be refused.
    try
    {
        await using EmbeddedKahunaNode refused = new(new(), new Kahuna.Server.Communication.Internode.MemoryInterNodeCommmunication(),
            new Kommander.Communication.Rest.RestCommunication(), new Kommander.Discovery.StaticDiscovery([]), loggerFactory);
        Check(false, "the cluster constructor refuses a non-memory Raft transport in the thread-free build");
    }
    catch (ArgumentException ex) when (ex.Message.Contains("thread-free"))
    {
        Check(true, "the cluster constructor refuses a non-memory Raft transport in the thread-free build");
    }

    using CancellationTokenSource cts = new(TimeSpan.FromMinutes(5));

    // The embedded default timings: 100 ms heartbeat, 500 to 1500 ms election timeout.
    EmbeddedKahunaCluster cluster = await PhaseOf("cluster start", EmbeddedKahunaCluster.CreateInMemoryAsync(3, new()
    {
        NodeName = "smoke",
        Storage = "memory",
        WalStorage = "memory",
        InitialPartitions = 2
    }, loggerFactory, cts.Token));

    for (int i = 0; i < cluster.NodeCount; i++)
    {
        string key = $"cluster/through-{i}";
        await Phase($"cluster write through node {i}", CommitWriteAsync(cluster.GetNode(i), key, $"value-{i}", cts.Token));

        for (int reader = 0; reader < cluster.NodeCount; reader++)
        {
            string value = await PhaseOf($"cluster read of node {i}'s write through node {reader}", ReadAsync(cluster.GetNode(reader), key, cts.Token));
            Check(value == $"value-{i}", $"node {reader} reads the value written through node {i}");
        }
    }

    // Leadership must stay stable with no fault. Kommander counts every election start (a term
    // change) in one process-wide counter; this process runs only this cluster now.
    // The heartbeat counter proves that the listener sees Kommander's measurements at all, so that
    // zero elections is a measurement and not a silent listener.
    long elections = 0;
    long heartbeats = 0;
    double maxHeartbeatDelayMs = 0;

    using MeterListener listener = new();
    listener.InstrumentPublished = (instrument, meterListener) =>
    {
        if (instrument.Meter.Name == "Kommander" && instrument.Name is "raft.elections_started_total" or "raft.heartbeats_sent_total" or "raft.heartbeat_delay_ms")
            meterListener.EnableMeasurementEvents(instrument);
    };
    listener.SetMeasurementEventCallback<long>((instrument, value, _, _) =>
    {
        if (instrument.Name == "raft.elections_started_total")
            elections += value;
        else
            heartbeats += value;
    });
    listener.SetMeasurementEventCallback<double>((_, value, _, _) => maxHeartbeatDelayMs = Math.Max(maxHeartbeatDelayMs, value));
    listener.Start();

    TimeSpan window = TimeSpan.FromSeconds(30);
    TimeSpan windowPhaseTimeout = window + TimeSpan.FromSeconds(30);

    await Phase("cluster idle for 30 s", Task.Delay(window, cts.Token), windowPhaseTimeout);
    Console.WriteLine($"[smoke] idle: elections={elections} heartbeats={heartbeats} max heartbeat gap={maxHeartbeatDelayMs:F0} ms");
    Check(heartbeats > 0, $"the listener sees Kommander's heartbeats ({heartbeats} counted)");
    Check(elections == 0, $"no election while idle ({elections} started)");

    heartbeats = 0;
    maxHeartbeatDelayMs = 0;
    await Phase("cluster light load for 30 s", LightLoadAsync(cluster, window, cts.Token), windowPhaseTimeout);
    Console.WriteLine($"[smoke] light load: elections={elections} heartbeats={heartbeats} max heartbeat gap={maxHeartbeatDelayMs:F0} ms");
    Check(elections == 0, $"no election under a light write load ({elections} started)");

    listener.Dispose();

    int stopped = await PhaseOf("find the meta leader", cluster.GetLeaderIndexAsync(0, cts.Token));
    await Phase($"stop node {stopped}", cluster.StopNodeAsync(stopped));

    int newLeader = await PhaseOf("failover of the meta partition", cluster.GetLeaderIndexAsync(0, cts.Token));
    Check(newLeader != stopped, $"node {newLeader} took over the meta partition from node {stopped}");

    int writer = (stopped + 1) % cluster.NodeCount;
    await Phase("write after failover", CommitWriteAsync(cluster.GetNode(writer), "cluster/after-failover", "after", cts.Token));

    await Phase($"restart node {stopped}", cluster.RestartNodeAsync(stopped, cts.Token));

    string caughtUp = await PhaseOf("read through the restarted node", ReadAsync(cluster.GetNode(stopped), "cluster/after-failover", cts.Token));
    Check(caughtUp == "after", "the restarted node reads the write made while it was stopped");

    // A network partition: the isolated node keeps running and keeps believing that it leads, but it
    // reaches no quorum, so it cannot commit. The majority elects a leader of its own.
    const string partitionKey = "partition/key";
    int partitionId = 1 + Kahuna.Shared.Routing.HashPlacement.BucketOfKey(partitionKey, 2);
    int isolated = await PhaseOf("find the leader of the key's partition", cluster.GetLeaderIndexAsync(partitionId, cts.Token));

    cluster.IsolateNode(isolated);
    Check(cluster.IsLinkBlocked(isolated, (isolated + 1) % cluster.NodeCount), $"node {isolated} is cut off from the others");

    using (CancellationTokenSource minority = CancellationTokenSource.CreateLinkedTokenSource(cts.Token))
    {
        minority.CancelAfter(TimeSpan.FromSeconds(15));

        bool committed = true;
        try
        {
            await Phase("write from the minority side", CommitWriteAsync(cluster.GetNode(isolated), partitionKey, "from-minority", minority.Token), TimeSpan.FromSeconds(45));
        }
        catch (OperationCanceledException)
        {
            committed = false;
        }

        Check(!committed, "the isolated node cannot commit");
    }

    int majorityLeader = await PhaseOf("the majority elects a leader", LeaderAmongAsync(cluster, partitionId, isolated, cts.Token));
    await Phase("write from the majority side", CommitWriteAsync(cluster.GetNode(majorityLeader), partitionKey, "from-majority", cts.Token));

    cluster.UnblockAllLinks();

    string healed = await PhaseOf("read through the healed node", ReadAsync(cluster.GetNode(isolated), partitionKey, cts.Token));
    Check(healed == "from-majority", "the healed node reads the write the majority committed");

    await Phase("cluster dispose", cluster.DisposeAsync().AsTask());
}

// A leader of the partition among the nodes other than the excluded one. A node cut from the majority
// keeps believing that it leads, so the cluster-wide lookup can still name it.
static async Task<int> LeaderAmongAsync(EmbeddedKahunaCluster cluster, int partitionId, int excluded, CancellationToken ct)
{
    while (true)
    {
        ct.ThrowIfCancellationRequested();

        for (int i = 0; i < cluster.NodeCount; i++)
        {
            if (i == excluded)
                continue;

            try
            {
                if (await cluster.GetNode(i).Raft.AmILeader(partitionId, ct))
                    return i;
            }
            catch (Exception ex) when (ex is Kommander.RaftException or Kommander.PartitionNotHostedException)
            {
            }
        }

        await Task.Delay(50, ct);
    }
}

async Task LightLoadAsync(EmbeddedKahunaCluster cluster, TimeSpan duration, CancellationToken ct)
{
    Stopwatch watch = Stopwatch.StartNew();
    int writes = 0;

    while (watch.Elapsed < duration)
    {
        await CommitWriteAsync(cluster.GetNode(writes % cluster.NodeCount), $"cluster/load-{writes % 16}", $"v{writes}", ct);
        writes++;
        await Task.Delay(250, ct);
    }

    Console.WriteLine($"[smoke] light load: {writes} transactions");
}

// One interactive transaction (begin, write, commit) through the node. MustRetry, and a
// KahunaServerException from a forward to a node that just stopped, both mean "try again".
static async Task CommitWriteAsync(EmbeddedKahunaNode node, string key, string value, CancellationToken ct)
{
    while (true)
    {
        ct.ThrowIfCancellationRequested();

        try
        {
            (KeyValueResponseType started, TransactionHandle handle) = await node.Kahuna.LocateAndStartTransaction(
                new KeyValueTransactionOptions { Locking = KeyValueTransactionLocking.Pessimistic, Timeout = 5000 }, ct);

            if (started == KeyValueResponseType.Set)
            {
                (KeyValueResponseType set, _, _) = await node.Kahuna.LocateAndTrySetKeyValue(
                    handle.TransactionId, key, Encoding.UTF8.GetBytes(value), null, -1, KeyValueFlags.Set, 0,
                    KeyValueDurability.Persistent, ct, 0, handle.CoordinatorKey, TransactionOperationId.NewRandom());

                if (set == KeyValueResponseType.Set)
                {
                    (KeyValueResponseType committed, _) = await node.Kahuna.LocateAndCommitTransaction(handle, ct);
                    if (committed == KeyValueResponseType.Committed)
                        return;
                }
                else
                {
                    await node.Kahuna.LocateAndRollbackTransaction(handle, ct);
                }
            }
        }
        catch (KahunaServerException)
        {
        }

        await Task.Delay(50, ct);
    }
}

static async Task<string> ReadAsync(EmbeddedKahunaNode node, string key, CancellationToken ct)
{
    while (true)
    {
        try
        {
            (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await node.Kahuna.LocateAndTryGetValue(
                HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);

            if (type == KeyValueResponseType.Get && entry?.Value is not null)
                return Encoding.UTF8.GetString(entry.Value);
        }
        catch (KahunaServerException)
        {
        }

        await Task.Delay(50, ct);
    }
}

async Task Phase(string name, Task task, TimeSpan? timeout = null)
{
    Stopwatch watch = Stopwatch.StartNew();
    TimeSpan deadline = timeout ?? phaseTimeout;

    if (await Task.WhenAny(task, Task.Delay(deadline)) != task)
        throw new SmokePhaseTimedOut($"phase '{name}' did not finish in {deadline.TotalSeconds:F0} s");

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
