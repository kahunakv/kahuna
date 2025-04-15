// See https://aka.ms/new-console-template for more information

using System.Diagnostics;
using DotNext;
using Kahuna.Client;
using Kahuna.Shared.KeyValue;
using Kahuna.Shared.Locks;

Console.WriteLine("Kahuna Benchmark");

const int numberOfTasks = 350;
const int MaxTokens = 10_000;

List<string> tokens = new(MaxTokens);

for (int k = 0; k < MaxTokens; k++)
    tokens.Add(GetRandomLockName());

KahunaClient locks = new(["https://localhost:8082", "https://localhost:8084", "https://localhost:8086"], null);

const string myScript = """
BEGIN
 SET ppa 1000 NX
 LET x = GET ppa
 LET xn = to_int(x) + 1
 SET ppa xn
 COMMIT
END
""";

//KahunaScript kahunaScript = locks.LoadScript(myScript);

List<Task> tasks = new(numberOfTasks);

Stopwatch stopwatch = Stopwatch.StartNew();

for (int j = 0; j < 3; j++)
{
    tasks.Clear();

    for (int i = 0; i < numberOfTasks; i++)
    {
        if (i % 2 == 0)
            tasks.Add(SetKeyConcurrently(locks));
        else
            tasks.Add(GetKeyConcurrently(locks));
        //tasks.Add(ExecuteTxConcurrently(kahunaScript));
    }

    await Task.WhenAll(tasks);

    if (j <= 1)
        Console.WriteLine($"Warm Up: " + stopwatch.Elapsed);
    else
        Console.WriteLine($"[{(j - 2) + 1}] Total time: " + stopwatch.Elapsed);

    stopwatch.Restart();
}

return;

async Task AdquireLockConcurrently(KahunaClient locksx)
{
    try
    {
        using CancellationTokenSource cts = new();
        cts.CancelAfter(TimeSpan.FromSeconds(10));

        string lockName = GetRandomLockNameFromList(tokens);

        await using KahunaLock kahunaLock = await locksx.GetOrCreateLock(
            lockName,
            expiry: TimeSpan.FromSeconds(5),
            durability: LockDurability.Persistent,
            cancellationToken: cts.Token
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
        using CancellationTokenSource cts = new();
        cts.CancelAfter(TimeSpan.FromSeconds(10));

        string key = GetRandomLockNameFromList(tokens);
        string value = GetRandomLockNameFromList(tokens);

        KahunaKeyValue result = await keyValues.SetKeyValue(
            key, value,
            TimeSpan.FromHours(5),
            KeyValueFlags.Set,
            KeyValueDurability.Persistent,
            cancellationToken: cts.Token
        );

        if (!result.Success)
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
        using CancellationTokenSource src = new();
        src.CancelAfter(TimeSpan.FromSeconds(10));

        string key = GetRandomLockNameFromList(tokens);

        KahunaKeyValue result = await keyValues.GetKeyValue(key, KeyValueDurability.Persistent, src.Token);

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

async Task ExecuteTxConcurrently(KahunaScript ks)
{
    for (int i = 0; i < 10; i++)
    {
        try
        {
            await ks.Run();
            return;
        }
        catch (KahunaException ex)
        {
            if (ex.KeyValueErrorCode == KeyValueResponseType.Aborted)
            {
                await Task.Delay(50);
                continue;
            }

            throw;
        }
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
