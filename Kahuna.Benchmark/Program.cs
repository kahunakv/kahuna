// See https://aka.ms/new-console-template for more information

using System.Diagnostics;
using DotNext;
using Kahuna.Client;
using Kahuna.Shared.Locks;

Console.WriteLine("Kahuna Benchmark");

const int numberOfLocks = 750;
const int MaxTokens = 1_000_000;

List<string> tokens = new(MaxTokens);

for (int k = 0; k < MaxTokens; k++)
    tokens.Add(GetRandomLockName());

KahunaClient locks = new("https://localhost:8082", null);

List<Task> tasks = new(numberOfLocks);

Stopwatch stopwatch = Stopwatch.StartNew();

for (int j = 0; j < 25; j++)
{
    tasks.Clear();
    
    for (int i = 0; i < numberOfLocks; i++)
        tasks.Add(AdquireLockConcurrently(locks));

    await Task.WhenAll(tasks);

    Console.WriteLine($"[{j + 1}] Total time: " + stopwatch.Elapsed);

    stopwatch.Restart();
}

return;

async Task AdquireLockConcurrently(KahunaClient locksx)
{
    try
    {
        string lockName = GetRandomLockNameFromList(tokens);

        await using KahunaLock kahunaLock = await locksx.GetOrCreateLock(
            lockName,
            expiry: TimeSpan.FromSeconds(5),
            consistency: LockConsistency.Consistent
        );

        if (!kahunaLock.IsAcquired)
            throw new KahunaException("Not acquired " + lockName, LockResponseType.Busy);
        
        if (kahunaLock.FencingToken > 1)
            Console.WriteLine("Got repeated token " + kahunaLock.FencingToken);
    }
    catch (KahunaException ex)
    {
        Console.WriteLine("KahunaException {0} {1}", ex.Message, ex.ErrorCode);
    }
    catch (Exception ex)
    {
        Console.WriteLine("Exception {0}", ex.Message);
    }
}

static string GetRandomLockName()
{
    return NanoidDotNet.Nanoid.Generate();
}

static string GetRandomLockNameFromList(List<string> tokens)
{
    return tokens[Random.Shared.Next(0, MaxTokens)];
}