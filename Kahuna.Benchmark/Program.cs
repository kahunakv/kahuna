// See https://aka.ms/new-console-template for more information

using System.Diagnostics;
using Kahuna.Client;

Console.WriteLine("Hello, World!");

KahunaClient locks = new("http://localhost:2070");

List<Task> tasks = new(1000);

Stopwatch stopwatch = Stopwatch.StartNew();

for (int i = 0; i < 1000; i++)
    tasks.Add(AdquireLockConcurrently(locks));

await Task.WhenAll(tasks);

Console.WriteLine("Total time: " + stopwatch.Elapsed);

async Task AdquireLockConcurrently(KahunaClient locksx)
{
    string lockName = GetRandomLockName();

    await using KahunaLock redLock = await locksx.GetOrCreateLock(
        lockName, 
        expiry: TimeSpan.FromSeconds(5)
    );

    if (!redLock.IsAcquired)
        throw new Exception("Not adquired " + lockName);
}

static string GetRandomLockName()
{
    return Guid.NewGuid().ToString("N")[..10];
}