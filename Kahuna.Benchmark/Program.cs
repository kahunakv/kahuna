
/**
 * This file is part of Kahuna
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Diagnostics;
using DotNext;
using Kahuna.Client;
using Kahuna.Shared.KeyValue;
using Kahuna.Shared.Locks;

Console.WriteLine("Kahuna Benchmark");

const int numberOfTasks = 550;
const int MaxTokens = 5_000;

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

for (int j = 0; j < 5; j++)
{
    tasks.Clear();

    for (int i = 0; i < numberOfTasks; i++)
    {
        int remainder = i % 4;
        
        switch (remainder)
        {
            case 3:
                tasks.Add(ExtendKeyConcurrently(locks));
                break;
            
            case 2:
                tasks.Add(SetKeyConcurrently(locks));
                break;
            
            case 1:
                tasks.Add(GetKeyConcurrently(locks));
                break;
            
            case 0:
                tasks.Add(DeleteKeyConcurrently(locks));
                break;                
        }               
    }

    await Task.WhenAll(tasks);

    if (j <= 1)
        Console.WriteLine($"Warm Up: {stopwatch.Elapsed}");
    else
        Console.WriteLine($"[{(j - 2) + 1}] Total time: {stopwatch.Elapsed}");

    stopwatch.Restart();
}

return;

async Task AdquireLockConcurrently(KahunaClient locksx)
{
    try
    {
        using CancellationTokenSource cts = new();
        cts.CancelAfter(TimeSpan.FromSeconds(5));

        string lockName = GetRandomLockNameFromList(tokens);

        await using KahunaLock kahunaLock = await locksx.GetOrCreateLock(
            lockName,
            expiry: TimeSpan.FromSeconds(30),
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
        cts.CancelAfter(TimeSpan.FromSeconds(5));

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

async Task DeleteKeyConcurrently(KahunaClient keyValues)
{
    try
    {
        using CancellationTokenSource cts = new();
        cts.CancelAfter(TimeSpan.FromSeconds(5));

        string key = GetRandomLockNameFromList(tokens);        

        KahunaKeyValue result = await keyValues.DeleteKeyValue(
            key, 
            KeyValueDurability.Persistent,
            cancellationToken: cts.Token
        );

        //if (!result.Success)
        //    throw new KahunaException("Not deleted " + key, LockResponseType.Busy);

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

async Task ExtendKeyConcurrently(KahunaClient keyValues)
{
    try
    {
        using CancellationTokenSource cts = new();
        cts.CancelAfter(TimeSpan.FromSeconds(5));

        string key = GetRandomLockNameFromList(tokens);        

        KahunaKeyValue result = await keyValues.ExtendKeyValue(
            key, 
            TimeSpan.FromSeconds(120),
            KeyValueDurability.Persistent,
            cancellationToken: cts.Token
        );

        //if (!result.Success)
        //    throw new KahunaException("Not deleted " + key, LockResponseType.Busy);

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
