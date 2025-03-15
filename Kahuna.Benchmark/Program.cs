// See https://aka.ms/new-console-template for more information

using System.Diagnostics;
using DotNext;
using Kahuna.Client;
using Kahuna.Shared.KeyValue;
using Kahuna.Shared.Locks;

Console.WriteLine("Kahuna Benchmark");

const int numberOfLocks = 5000;
const int MaxTokens = 100_000;

List<string> tokens = new(MaxTokens);

for (int k = 0; k < MaxTokens; k++)
    tokens.Add(GetRandomLockName());

KahunaClient locks = new(["https://localhost:8082", "https://localhost:8084", "https://localhost:8086"], null);

List<Task> tasks = new(numberOfLocks * 2);

Stopwatch stopwatch = Stopwatch.StartNew();

for (int j = 0; j < 25; j++)
{
    tasks.Clear();

    for (int i = 0; i < numberOfLocks; i++)
    {
        tasks.Add(SetKeyConcurrently(locks));
        tasks.Add(GetKeyConcurrently(locks));
    }

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
            consistency: LockConsistency.Linearizable
        );

        if (!kahunaLock.IsAcquired)
            throw new KahunaException("Not acquired " + lockName, LockResponseType.Busy);
        
        if (kahunaLock.FencingToken > 1)
            Console.WriteLine("Got repeated token " + kahunaLock.FencingToken);
    }
    catch (KahunaException ex)
    {
        Console.WriteLine("KahunaException {0} {1}", ex.Message, ex.LockErrorCode);
    }
    catch (Exception ex)
    {
        Console.WriteLine("Exception {0}", ex.Message);
    }
}

async Task SetKeyConcurrently(KahunaClient keyValues)
{
    try
    {
        string key = GetRandomLockNameFromList(tokens);
        string value = GetRandomLockNameFromList(tokens);

        (bool success, long revision) = await keyValues.SetKeyValue(key, value, TimeSpan.FromHours(5), KeyValueFlags.Set, KeyValueConsistency.Ephemeral);

        if (!success)
            throw new KahunaException("Not set " + key, LockResponseType.Busy);
        
        //if (revision > 1)
        //    Console.WriteLine("Got repeated revision " + revision);
    }
    catch (KahunaException ex)
    {
        Console.WriteLine("KahunaException {0} {1}", ex.Message, ex.KeyValueErrorCode);
    }
    catch (Exception ex)
    {
        Console.WriteLine("Exception {0}", ex.Message);
    }
}

async Task GetKeyConcurrently(KahunaClient keyValues)
{
    try
    {
        string key = GetRandomLockNameFromList(tokens);

        (byte[]? _, long revision) = await keyValues.GetKeyValue(key, KeyValueConsistency.Ephemeral);
        
        //if (revision > 1)
        //   Console.WriteLine("Got repeated revision " + revision);
    }
    catch (KahunaException ex)
    {
        Console.WriteLine("KahunaException {0} {1}", ex.Message, ex.KeyValueErrorCode);
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
