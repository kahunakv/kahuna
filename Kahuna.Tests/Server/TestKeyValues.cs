
using Kommander;
using Kommander.Time;
using System.Text;
using Kahuna.Server.KeyValues;
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

    private static string GetRandomValue()
    {
        return Guid.NewGuid().ToString("N")[..10];
    }
    
    [Theory, CombinatorialData]
    public async Task TestSetAndGet(
        [CombinatorialValues("memory")] string storage,
        [CombinatorialValues(8, 16)] int partitions,
        [CombinatorialValues(KeyValueDurability.Ephemeral, KeyValueDurability.Persistent)] KeyValueDurability durability
    )
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) = await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        string keyName = GetRandomValue();
        byte[] valueA = Encoding.UTF8.GetBytes(GetRandomValue());

        (KeyValueResponseType response, long revision, _) = await kahuna1.LocateAndTrySetKeyValue(HLCTimestamp.Zero, keyName, valueA, null, -1, KeyValueFlags.Set, 0, durability, TestContext.Current.CancellationToken);
        Assert.Equal(KeyValueResponseType.Set, response);
        Assert.Equal(0, revision);
        
        (response, ReadOnlyKeyValueContext? readOnlyKeyValueContext) = await kahuna2.LocateAndTryGetValue(HLCTimestamp.Zero, keyName, -1, durability, TestContext.Current.CancellationToken);
        Assert.Equal(KeyValueResponseType.Get, response);
        Assert.NotNull(readOnlyKeyValueContext);
        Assert.Equal(0, readOnlyKeyValueContext.Revision);
        
        (response, readOnlyKeyValueContext) = await kahuna3.LocateAndTryGetValue(HLCTimestamp.Zero, keyName, -1, durability, TestContext.Current.CancellationToken);
        Assert.Equal(KeyValueResponseType.Get, response);
        Assert.NotNull(readOnlyKeyValueContext);
        Assert.Equal(0, readOnlyKeyValueContext.Revision);
        
        (response, revision, _) = await kahuna1.LocateAndTrySetKeyValue(HLCTimestamp.Zero, keyName, valueA, null, -1, KeyValueFlags.Set, 0, durability, TestContext.Current.CancellationToken);
        Assert.Equal(KeyValueResponseType.Set, response);
        Assert.Equal(1, revision);

        await LeaveCluster(node1, node2, node3);
    }
    
    [Theory, CombinatorialData]
    public async Task TestGetByBucket(
        [CombinatorialValues("memory")] string storage,
        [CombinatorialValues(8, 16)] int partitions,
        [CombinatorialValues(KeyValueDurability.Ephemeral, KeyValueDurability.Persistent)] KeyValueDurability durability
    )
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) = await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);
        
        string prefix = GetRandomValue();

        string keyName = prefix + "/" + GetRandomValue();
        byte[] value = Encoding.UTF8.GetBytes(GetRandomValue());

        (KeyValueResponseType response, long revision, _) = await kahuna1.LocateAndTrySetKeyValue(HLCTimestamp.Zero, keyName, value, null, -1, KeyValueFlags.Set, 0, durability, TestContext.Current.CancellationToken);
        Assert.Equal(KeyValueResponseType.Set, response);
        Assert.Equal(0, revision);
        
        keyName = prefix + "/" + GetRandomValue();
        value = Encoding.UTF8.GetBytes(GetRandomValue());
        
        (response, revision, _) = await kahuna2.LocateAndTrySetKeyValue(HLCTimestamp.Zero, keyName, value, null, -1, KeyValueFlags.Set, 0, durability, TestContext.Current.CancellationToken);
        Assert.Equal(KeyValueResponseType.Set, response);
        Assert.Equal(0, revision);
        
        keyName = prefix + "/" + GetRandomValue();
        value = Encoding.UTF8.GetBytes(GetRandomValue());
        
        (response, revision, _) = await kahuna3.LocateAndTrySetKeyValue(HLCTimestamp.Zero, keyName, value, null, -1, KeyValueFlags.Set, 0, durability, TestContext.Current.CancellationToken);
        Assert.Equal(KeyValueResponseType.Set, response);
        Assert.Equal(0, revision);
        
        KeyValueGetByBucketResult result = await kahuna1.LocateAndGetByBucket(HLCTimestamp.Zero, prefix, durability, TestContext.Current.CancellationToken);
        Assert.Equal(3, result.Items.Count);
        
        keyName = prefix + "/" + GetRandomValue();
        value = Encoding.UTF8.GetBytes(GetRandomValue());
        
        (response, revision, _) = await kahuna2.LocateAndTrySetKeyValue(HLCTimestamp.Zero, keyName, value, null, -1, KeyValueFlags.Set, 0, durability, TestContext.Current.CancellationToken);
        Assert.Equal(KeyValueResponseType.Set, response);
        Assert.Equal(0, revision);
        
        result = await kahuna1.LocateAndGetByBucket(HLCTimestamp.Zero, prefix, durability, TestContext.Current.CancellationToken);
        Assert.Equal(4, result.Items.Count);

        ValidateGetByBucketItems(prefix, result.Items);
        
        keyName = prefix + "/" + GetRandomValue();
        value = Encoding.UTF8.GetBytes(GetRandomValue());
        
        (response, revision, _) = await kahuna2.LocateAndTrySetKeyValue(HLCTimestamp.Zero, keyName, value, null, -1, KeyValueFlags.Set, 0, durability, TestContext.Current.CancellationToken);
        Assert.Equal(KeyValueResponseType.Set, response);
        Assert.Equal(0, revision);
        
        result = await kahuna1.LocateAndGetByBucket(HLCTimestamp.Zero, prefix, durability, TestContext.Current.CancellationToken);
        Assert.Equal(5, result.Items.Count);
        
        ValidateGetByBucketItems(prefix, result.Items);
        
        (response, revision, _) = await kahuna2.LocateAndTryDeleteKeyValue(HLCTimestamp.Zero, keyName, durability, TestContext.Current.CancellationToken);
        Assert.Equal(KeyValueResponseType.Deleted, response);
        Assert.Equal(0, revision);
        
        result = await kahuna1.LocateAndGetByBucket(HLCTimestamp.Zero, prefix, durability, TestContext.Current.CancellationToken);
        Assert.Equal(4, result.Items.Count);
        
        ValidateGetByBucketItems(prefix, result.Items);

        await LeaveCluster(node1, node2, node3);
    }
    
    [Theory, CombinatorialData]
    public async Task TestScanByPrefix(
        [CombinatorialValues("memory")] string storage,
        [CombinatorialValues(8, 16)] int partitions,
        [CombinatorialValues(KeyValueDurability.Ephemeral, KeyValueDurability.Persistent)] KeyValueDurability durability
    )
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) = await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);
        
        string prefix = GetRandomValue();

        string keyName = prefix + GetRandomValue();
        byte[] value = Encoding.UTF8.GetBytes(GetRandomValue());

        (KeyValueResponseType response, long revision, _) = await kahuna1.LocateAndTrySetKeyValue(HLCTimestamp.Zero, keyName, value, null, -1, KeyValueFlags.Set, 0, durability, TestContext.Current.CancellationToken);
        Assert.Equal(KeyValueResponseType.Set, response);
        Assert.Equal(0, revision);
        
        keyName = prefix + GetRandomValue();
        value = Encoding.UTF8.GetBytes(GetRandomValue());
        
        (response, revision, _) = await kahuna2.LocateAndTrySetKeyValue(HLCTimestamp.Zero, keyName, value, null, -1, KeyValueFlags.Set, 0, durability, TestContext.Current.CancellationToken);
        Assert.Equal(KeyValueResponseType.Set, response);
        Assert.Equal(0, revision);
        
        keyName = prefix + GetRandomValue();
        value = Encoding.UTF8.GetBytes(GetRandomValue());
        
        (response, revision, _) = await kahuna3.LocateAndTrySetKeyValue(HLCTimestamp.Zero, keyName, value, null, -1, KeyValueFlags.Set, 0, durability, TestContext.Current.CancellationToken);
        Assert.Equal(KeyValueResponseType.Set, response);
        Assert.Equal(0, revision);
        
        KeyValueGetByBucketResult result = await kahuna1.ScanAllByPrefix(prefix, durability, TestContext.Current.CancellationToken);
        Assert.Equal(3, result.Items.Count);
        
        keyName = prefix + GetRandomValue();
        value = Encoding.UTF8.GetBytes(GetRandomValue());
        
        (response, revision, _) = await kahuna2.LocateAndTrySetKeyValue(HLCTimestamp.Zero, keyName, value, null, -1, KeyValueFlags.Set, 0, durability, TestContext.Current.CancellationToken);
        Assert.Equal(KeyValueResponseType.Set, response);
        Assert.Equal(0, revision);
        
        result = await kahuna1.ScanAllByPrefix(prefix, durability, TestContext.Current.CancellationToken);
        Assert.Equal(4, result.Items.Count);

        ValidateGetByBucketItems(prefix, result.Items);
        
        keyName = prefix + GetRandomValue();
        value = Encoding.UTF8.GetBytes(GetRandomValue());
        
        (response, revision, _) = await kahuna3.LocateAndTrySetKeyValue(HLCTimestamp.Zero, keyName, value, null, -1, KeyValueFlags.Set, 0, durability, TestContext.Current.CancellationToken);
        Assert.Equal(KeyValueResponseType.Set, response);
        Assert.Equal(0, revision);
        
        result = await kahuna2.ScanAllByPrefix(prefix, durability, TestContext.Current.CancellationToken);
        Assert.Equal(5, result.Items.Count);
        
        ValidateGetByBucketItems(prefix, result.Items);
        
        (response, revision, _) = await kahuna2.LocateAndTryDeleteKeyValue(HLCTimestamp.Zero, keyName, durability, TestContext.Current.CancellationToken);
        Assert.Equal(KeyValueResponseType.Deleted, response);
        Assert.Equal(0, revision);
        
        result = await kahuna3.ScanAllByPrefix(prefix, durability, TestContext.Current.CancellationToken);
        Assert.Equal(4, result.Items.Count);
        
        ValidateGetByBucketItems(prefix, result.Items);

        await LeaveCluster(node1, node2, node3);
    }

    private static void ValidateGetByBucketItems(string prefix, List<(string, ReadOnlyKeyValueContext)> resultItems)
    {
        foreach ((string key, ReadOnlyKeyValueContext value) in resultItems)
        {
            Assert.StartsWith(prefix, key);
            Assert.Equal(0, value.Revision);
            Assert.NotNull(value.Value);
            Assert.Equal(10, value.Value.Length);
        }
    }
}