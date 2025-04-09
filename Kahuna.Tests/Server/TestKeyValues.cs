
using Kommander;
using Kommander.Time;
using System.Text;
using Kahuna.Shared.KeyValue;
using Kahuna.Shared.Locks;
using Microsoft.Extensions.Logging;

namespace Kahuna.Tests.Server;

public class TestKeyValues : BaseCluster
{
    private readonly ILogger<IRaft> raftLogger;

    private readonly ILogger<IKahuna> kahunaLogger;

    public TestKeyValues(ITestOutputHelper outputHelper)
    {
        ILoggerFactory loggerFactory = LoggerFactory.Create(builder =>
        {
            builder
                .AddXUnit(outputHelper)
                .SetMinimumLevel(LogLevel.Debug);
        });

        raftLogger = loggerFactory.CreateLogger<IRaft>();
        kahunaLogger = loggerFactory.CreateLogger<IKahuna>();
    }

    private static string GetRandomLockName()
    {
        return Guid.NewGuid().ToString("N")[..10];
    }
    
    [Theory, CombinatorialData]
    public async Task TestSetAndGet(
        [CombinatorialValues("memory")] string storage,
        [CombinatorialValues(8, 16)] int partitions,
        [CombinatorialValues(KeyValueDurability.Ephemeral, KeyValueDurability.Persistent)] KeyValueDurability durability
        //[CombinatorialValues("memory")] string storage,
        //[CombinatorialValues(16)] int partitions,
        //[CombinatorialValues(LockDurability.Ephemeral)] LockDurability durability
    )
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) = await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        string keyName = GetRandomLockName();

        byte[] valueA = Encoding.UTF8.GetBytes(GetRandomLockName());
        byte[] ownerB = Encoding.UTF8.GetBytes(GetRandomLockName());

        (KeyValueResponseType response, long revision, _) = await kahuna1.LocateAndTrySetKeyValue(HLCTimestamp.Zero, keyName, valueA, null, -1, KeyValueFlags.Set, 0, durability, TestContext.Current.CancellationToken);
        Assert.Equal(KeyValueResponseType.Set, response);
        Assert.Equal(0, revision);

        await node1.LeaveCluster(true);
        await node2.LeaveCluster(true);
        await node3.LeaveCluster(true);
    }
}