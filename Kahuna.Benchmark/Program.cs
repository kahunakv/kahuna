// See https://aka.ms/new-console-template for more information

using System.Diagnostics;
using Kahuna.Client;
using Kahuna.Shared.Locks;

Console.WriteLine("Kahuna Benchmark");

const int numberOfLocks = 500;

KahunaClient locks = new("https://localhost:8082", null);

List<Task> tasks = new(numberOfLocks);

Stopwatch stopwatch = Stopwatch.StartNew();

for (int i = 0; i < numberOfLocks; i++)
    tasks.Add(AdquireLockConcurrently(locks));

await Task.WhenAll(tasks);

Console.WriteLine("[1] Total time: " + stopwatch.Elapsed);

stopwatch.Restart();

for (int i = 0; i < numberOfLocks; i++)
    tasks.Add(AdquireLockConcurrently(locks));

await Task.WhenAll(tasks);

Console.WriteLine("[2] Total time: " + stopwatch.Elapsed);

stopwatch.Restart();

for (int i = 0; i < numberOfLocks; i++)
    tasks.Add(AdquireLockConcurrently(locks));

await Task.WhenAll(tasks);

Console.WriteLine("[3] Total time: " + stopwatch.Elapsed);

async Task AdquireLockConcurrently(KahunaClient locksx)
{
    string lockName = GetRandomLockName();

    await using KahunaLock redLock = await locksx.GetOrCreateLock(
        lockName, 
        expiry: TimeSpan.FromSeconds(5),
        consistency: LockConsistency.Consistent
    );

    if (!redLock.IsAcquired)
        throw new KahunaException("Not acquired " + lockName, LockResponseType.Errored);
}

static string GetRandomLockName()
{
    return Guid.NewGuid().ToString("N")[..16];
}