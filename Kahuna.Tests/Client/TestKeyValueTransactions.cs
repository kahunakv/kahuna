
using Kahuna.Client;
using Kahuna.Client.Communication;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Tests.Client;

public class TestKeyValueTransactions
{
    private const string url = "https://localhost:8082";

    private readonly string[] urls = ["https://localhost:8082", "https://localhost:8084", "https://localhost:8086"];

    [Theory, CombinatorialData]
    public async Task TestBasicSet(
        [CombinatorialValues(KahunaCommunicationType.Grpc, KahunaCommunicationType.Rest)] KahunaCommunicationType communicationType,
        [CombinatorialValues(KahunaClientType.SingleEndpoint, KahunaClientType.PoolOfEndpoints)] KahunaClientType clientType
    )
    {
        KahunaClient client = GetClientByType(communicationType, clientType);

        KahunaKeyValueTransactionResult response = await client.ExecuteKeyValueTransactionScript(
            "SET `" + GetRandomKeyName() + "` 'some value'",
            cancellationToken: TestContext.Current.CancellationToken
        );
        
        Assert.Equal(KeyValueResponseType.Set, response.Type);
        Assert.Equal(0, response.FirstRevision);
    }
    
    [Theory, CombinatorialData]
    public async Task TestSetPlaceholder(
        [CombinatorialValues(KahunaCommunicationType.Grpc, KahunaCommunicationType.Rest)] KahunaCommunicationType communicationType,
        [CombinatorialValues(KahunaClientType.SingleEndpoint, KahunaClientType.PoolOfEndpoints)] KahunaClientType clientType
    )
    {
        KahunaClient client = GetClientByType(communicationType, clientType);

        KahunaKeyValueTransactionResult response = await client.ExecuteKeyValueTransactionScript(
            "SET `" + GetRandomKeyName() + "` @value", 
            parameters: [new() { Key = "@value", Value = "some value" }],
            cancellationToken: TestContext.Current.CancellationToken
        );
        
        Assert.Equal(KeyValueResponseType.Set, response.Type);
        Assert.Equal(0, response.FirstRevision);
    }
    
    [Theory, CombinatorialData]
    public async Task TestBasicGet(
        [CombinatorialValues(KahunaCommunicationType.Grpc, KahunaCommunicationType.Rest)] KahunaCommunicationType communicationType,
        [CombinatorialValues(KahunaClientType.SingleEndpoint, KahunaClientType.PoolOfEndpoints)] KahunaClientType clientType
    )
    {
        KahunaClient client = GetClientByType(communicationType, clientType);

        string keyName = GetRandomKeyName();
        
        KahunaKeyValueTransactionResult response = await client.ExecuteKeyValueTransactionScript("SET `" + keyName + "` 'some value'", cancellationToken: TestContext.Current.CancellationToken);
        
        Assert.Equal(KeyValueResponseType.Set, response.Type);
        Assert.Equal(0, response.FirstRevision);

        response = await client.ExecuteKeyValueTransactionScript("GET `" + keyName + "`", cancellationToken: TestContext.Current.CancellationToken);
        
        Assert.Equal(KeyValueResponseType.Get, response.Type);
        Assert.Equal(0, response.FirstRevision);
    }
    
    [Theory, CombinatorialData]
    public async Task TestBasicExtend(
        [CombinatorialValues(KahunaCommunicationType.Grpc, KahunaCommunicationType.Rest)] KahunaCommunicationType communicationType,
        [CombinatorialValues(KahunaClientType.SingleEndpoint, KahunaClientType.PoolOfEndpoints)] KahunaClientType clientType
    )
    {
        KahunaClient client = GetClientByType(communicationType, clientType);

        string keyName = GetRandomKeyName();
        
        KahunaKeyValueTransactionResult response = await client.ExecuteKeyValueTransactionScript("SET `" + keyName + "` 'some value'", cancellationToken: TestContext.Current.CancellationToken);
        
        Assert.Equal(KeyValueResponseType.Set, response.Type);
        Assert.Equal(0, response.FirstRevision);

        response = await client.ExecuteKeyValueTransactionScript("EXTEND `" + keyName + "` 1000", cancellationToken: TestContext.Current.CancellationToken);
        
        Assert.Equal(KeyValueResponseType.Extended, response.Type);
        Assert.Equal(0, response.FirstRevision);
    }
    
    [Theory, CombinatorialData]
    public async Task TestBasicExists(
        [CombinatorialValues(KahunaCommunicationType.Grpc, KahunaCommunicationType.Rest)] KahunaCommunicationType communicationType,
        [CombinatorialValues(KahunaClientType.SingleEndpoint, KahunaClientType.PoolOfEndpoints)] KahunaClientType clientType
    )
    {
        KahunaClient client = GetClientByType(communicationType, clientType);

        string keyName = GetRandomKeyName();
        
        KahunaKeyValueTransactionResult response = await client.ExecuteKeyValueTransactionScript(
            "SET `" + keyName + "` 'some value'",
            cancellationToken: TestContext.Current.CancellationToken
        );
        
        Assert.Equal(KeyValueResponseType.Set, response.Type);
        Assert.Equal(0, response.FirstRevision);

        response = await client.ExecuteKeyValueTransactionScript(
            "EXISTS `" + keyName + "`",
            cancellationToken: TestContext.Current.CancellationToken
        );
        
        Assert.Equal(KeyValueResponseType.Exists, response.Type);
        Assert.Equal(0, response.FirstRevision);
    }
    
    [Theory, CombinatorialData]
    public async Task TestBasicDelete(
        [CombinatorialValues(KahunaCommunicationType.Grpc, KahunaCommunicationType.Rest)] KahunaCommunicationType communicationType,
        [CombinatorialValues(KahunaClientType.SingleEndpoint, KahunaClientType.PoolOfEndpoints)] KahunaClientType clientType
    )
    {
        KahunaClient client = GetClientByType(communicationType, clientType);

        string keyName = GetRandomKeyName();
        
        KahunaKeyValueTransactionResult response = await client.ExecuteKeyValueTransactionScript(
            "SET `" + keyName + "` 'some value'",
            cancellationToken: TestContext.Current.CancellationToken
        );
        
        Assert.Equal(KeyValueResponseType.Set, response.Type);
        Assert.Equal(0, response.FirstRevision);

        response = await client.ExecuteKeyValueTransactionScript(
            "DELETE `" + keyName + "`",
            cancellationToken: TestContext.Current.CancellationToken
        );
        
        Assert.Equal(KeyValueResponseType.Deleted, response.Type);
        Assert.Equal(0, response.FirstRevision);
    }
    
    [Theory, CombinatorialData]
    public async Task TestSnapshotIsolationConflict(
        [CombinatorialValues(KahunaCommunicationType.Grpc)] KahunaCommunicationType communicationType,
        [CombinatorialValues(KahunaClientType.SingleEndpoint)] KahunaClientType clientType
    )
    {
        KahunaClient client = GetClientByType(communicationType, clientType);
        
        string keyNameA = GetRandomKeyName();
        string keyNameB = GetRandomKeyName();
        
        KahunaKeyValue result = await client.SetKeyValue(keyNameA, "10", cancellationToken: TestContext.Current.CancellationToken);
        Assert.True(result.Success);
        
        result = await client.SetKeyValue(keyNameB, "10", cancellationToken: TestContext.Current.CancellationToken);
        Assert.True(result.Success);

        const string script1 = """
        BEGIN (locking="optimistic")
         LET av = GET @keyA
         LET bv = GET @keyB
         IF to_int(av) + to_int(bv) = 20 THEN
          SET @keyA 0
         END
         COMMIT
        END 
        """;
        
        const string script2 = """
        BEGIN (locking="optimistic")
         LET av = GET @keyA
         LET bv = GET @keyB
         IF to_int(av) + to_int(bv) = 20 THEN
          SET @keyB 0
         END
         COMMIT
        END 
        """;

        try
        {
            await Task.WhenAll(
                client.ExecuteKeyValueTransactionScript(
                    script1,
                    null, 
                    [new() { Key = "@keyA", Value = keyNameA }, new() { Key = "@keyB", Value = keyNameB }],
                    cancellationToken: TestContext.Current.CancellationToken
                ),
                client.ExecuteKeyValueTransactionScript(
                    script2,
                    null, 
                    [new() { Key = "@keyA", Value = keyNameA }, new() { Key = "@keyB", Value = keyNameB }],
                    cancellationToken: TestContext.Current.CancellationToken
                )
            );
            
            Assert.False(true);
        }
        catch (KahunaException e)
        {
            Assert.Equal(KeyValueResponseType.Aborted, e.KeyValueErrorCode);
        }
        
        KahunaKeyValue resultA = await client.GetKeyValue(keyNameA, cancellationToken: TestContext.Current.CancellationToken);
        KahunaKeyValue resultB = await client.GetKeyValue(keyNameB, cancellationToken: TestContext.Current.CancellationToken);
        
        Assert.True(
            ("10" == resultA.ValueAsString() && "0" == resultB.ValueAsString()) || 
            ("0" == resultA.ValueAsString() && "10" == resultB.ValueAsString())
        );
    }
    
    [Theory, CombinatorialData]
    public async Task TestSerializableConflict(
        [CombinatorialValues(KahunaCommunicationType.Grpc)] KahunaCommunicationType communicationType,
        [CombinatorialValues(KahunaClientType.SingleEndpoint)] KahunaClientType clientType
    )
    {
        KahunaClient client = GetClientByType(communicationType, clientType);

        int oneFailed = 0;
        
        string keyNameA = GetRandomKeyName();

        const string script1 = """
        BEGIN (locking="pessimistic")
         LET av = GET @keyA
         SLEEP 500
         SET @keyA 30
         COMMIT
        END 
        """;
        
        const string script2 = """
        BEGIN (locking="pessimistic")
         SET @keyA 20
         COMMIT
        END 
        """;

        try
        {
            await Task.WhenAll(
                client.ExecuteKeyValueTransactionScript(
                    script1,
                    null, 
                    [new() { Key = "@keyA", Value = keyNameA }], 
                    cancellationToken: TestContext.Current.CancellationToken
                ),
                client.ExecuteKeyValueTransactionScript(
                    script2,
                    null, 
                    [new() { Key = "@keyA", Value = keyNameA }], 
                    cancellationToken: TestContext.Current.CancellationToken
                )
            );
            
            Assert.False(true);
        }
        catch (KahunaException e)
        {
            oneFailed++;
            
            Assert.Equal(KeyValueResponseType.Aborted, e.KeyValueErrorCode);
        }
        
        Assert.Equal(1, oneFailed);
    }
    
    [Theory, CombinatorialData]
    public async Task TestWriteSkewAnomaly(
        [CombinatorialValues(KahunaCommunicationType.Grpc)] KahunaCommunicationType communicationType,
        [CombinatorialValues(KahunaClientType.SingleEndpoint)] KahunaClientType clientType
    )
    {
        KahunaClient client = GetClientByType(communicationType, clientType);

        int oneFailed = 0;

        List<KahunaKeyValue> doctors = await client.GetByPrefix(
            "doctors", 
            KeyValueDurability.Persistent, 
            cancellationToken: TestContext.Current.CancellationToken
        );
        
        foreach (KahunaKeyValue doctor in doctors)
            await doctor.Delete(cancellationToken: TestContext.Current.CancellationToken);
        
        await client.SetKeyValue("doctors/bob", "true", durability: KeyValueDurability.Persistent, cancellationToken: TestContext.Current.CancellationToken);
        await client.SetKeyValue("doctors/alice", "true", durability: KeyValueDurability.Persistent, cancellationToken: TestContext.Current.CancellationToken);

        const string script1 = """
           let oncall = get by prefix 'doctors'
           if count(oncall) = 2 then
              set 'doctors/alice' false ex 10000
           else
              throw 'no both doctors on call'
           end 
           """;
        
        const string script2 = """
           let oncall = get by prefix 'doctors'
           if count(oncall) = 2 then
              set @doctor false ex 10000
           else
              throw 'no both doctors on call'
           end
           """;

        try
        {
            await Task.WhenAll(
                client.ExecuteKeyValueTransactionScript(
                    script1,
                    null, 
                    [new() { Key = "@doctor", Value = "doctors/alice" }], 
                    cancellationToken: TestContext.Current.CancellationToken
                ),
                client.ExecuteKeyValueTransactionScript(
                    script2,
                    null, 
                    [new() { Key = "@doctor", Value = "doctors/bob" }],
                    cancellationToken: TestContext.Current.CancellationToken
                )
            );
            
            Assert.False(true);
        }
        catch (KahunaException e)
        {
            oneFailed++;
            
            Assert.Equal(KeyValueResponseType.Aborted, e.KeyValueErrorCode);
        }
        
        Assert.Equal(1, oneFailed);
        
        doctors = await client.GetByPrefix(
            "doctors", 
            KeyValueDurability.Persistent, 
            cancellationToken: TestContext.Current.CancellationToken
        );
        
        Assert.Equal(2, doctors.Count);
        Assert.Single(doctors, x => x.ValueAsString() == "true");
        Assert.Single(doctors, x => x.ValueAsString() == "false");
    }
    
    [Theory, CombinatorialData]
    public async Task TestStartTransactionAndRollback(
        [CombinatorialValues(KahunaCommunicationType.Grpc)] KahunaCommunicationType communicationType,
        [CombinatorialValues(KahunaClientType.SingleEndpoint, KahunaClientType.PoolOfEndpoints)] KahunaClientType clientType
    )
    {
        string keyName = GetRandomKeyName();
        
        KahunaClient client = GetClientByType(communicationType, clientType);

        {
            await using KahunaTransactionSession session = await client.StartTransactionSession(
                new() { Locking = KeyValueTransactionLocking.Pessimistic },
                cancellationToken: TestContext.Current.CancellationToken
            );

            Assert.True(session.TransactionId.L > 0);

            KahunaKeyValue result = await session.SetKeyValue(keyName, "some value", cancellationToken: TestContext.Current.CancellationToken);
            Assert.True(result.Success);

            result = await session.GetKeyValue(keyName, cancellationToken: TestContext.Current.CancellationToken);
            Assert.True(result.Success);
            Assert.Equal("some value", result.ValueAsString());
        }

        /// Value shouldn't exist outside the scope of the transaction
        KahunaKeyValue result2 = await client.GetKeyValue(keyName, cancellationToken: TestContext.Current.CancellationToken);
        Assert.False(result2.Success); 
    }
    
    [Theory, CombinatorialData]
    public async Task TestStartTransactionAndExplicitRollback(
        [CombinatorialValues(KahunaCommunicationType.Grpc)] KahunaCommunicationType communicationType,
        [CombinatorialValues(KahunaClientType.SingleEndpoint, KahunaClientType.PoolOfEndpoints)] KahunaClientType clientType
    )
    {
        KahunaClient client = GetClientByType(communicationType, clientType);

        await using KahunaTransactionSession session = await client.StartTransactionSession(
            new() { Locking = KeyValueTransactionLocking.Pessimistic },
            cancellationToken: TestContext.Current.CancellationToken
        );
        
        string keyName = GetRandomKeyName();
        
        Assert.True(session.TransactionId.L > 0);

        KahunaKeyValue result = await session.SetKeyValue(keyName, "some value", cancellationToken: TestContext.Current.CancellationToken);
        Assert.True(result.Success);
        
        result = await session.GetKeyValue(keyName, cancellationToken: TestContext.Current.CancellationToken);
        Assert.True(result.Success);        
        Assert.Equal("some value", result.ValueAsString());
        
        await session.Rollback(cancellationToken: TestContext.Current.CancellationToken);
        
        /// Value shouldn't exist outside the scope of the transaction
        result = await client.GetKeyValue(keyName, cancellationToken: TestContext.Current.CancellationToken);
        Assert.False(result.Success); 
    }
    
    [Theory, CombinatorialData]
    public async Task TestStartTransactionAndCommit(
        [CombinatorialValues(KahunaCommunicationType.Grpc)] KahunaCommunicationType communicationType,
        [CombinatorialValues(KahunaClientType.SingleEndpoint, KahunaClientType.PoolOfEndpoints)] KahunaClientType clientType
    )
    {
        KahunaClient client = GetClientByType(communicationType, clientType);

        await using KahunaTransactionSession session = await client.StartTransactionSession(
            new() { Locking = KeyValueTransactionLocking.Pessimistic },
            cancellationToken: TestContext.Current.CancellationToken
        );
        
        string keyName = GetRandomKeyName();
        
        Assert.True(session.TransactionId.L > 0);

        KahunaKeyValue result = await session.SetKeyValue(keyName, "some value", cancellationToken: TestContext.Current.CancellationToken);
        Assert.True(result.Success);
        
        result = await session.GetKeyValue(keyName, cancellationToken: TestContext.Current.CancellationToken);
        Assert.True(result.Success);        
        Assert.Equal("some value", result.ValueAsString());
        
        /// Value shouldn't exist outside the scope of the transaction
        //result = await client.GetKeyValue(keyName, cancellationToken: TestContext.Current.CancellationToken);
        //Assert.False(result.Success); 
        
        await session.Commit(cancellationToken: TestContext.Current.CancellationToken);
        
        // Value should exist after commit
        result = await client.GetKeyValue(keyName, cancellationToken: TestContext.Current.CancellationToken);
        Assert.True(result.Success);        
        Assert.Equal("some value", result.ValueAsString());
    }
    
    [Theory, CombinatorialData]
    public async Task TestStartTransactionAndCommitNoChanges(
        [CombinatorialValues(KahunaCommunicationType.Grpc)] KahunaCommunicationType communicationType,
        [CombinatorialValues(KahunaClientType.SingleEndpoint, KahunaClientType.PoolOfEndpoints)] KahunaClientType clientType
    )
    {
        KahunaClient client = GetClientByType(communicationType, clientType);

        await using KahunaTransactionSession session = await client.StartTransactionSession(
            new() { Locking = KeyValueTransactionLocking.Pessimistic },
            cancellationToken: TestContext.Current.CancellationToken
        );
        
        Assert.True(session.TransactionId.L > 0);
        
        await session.Commit(cancellationToken: TestContext.Current.CancellationToken);
    }
    
    [Theory, CombinatorialData]
    public async Task TestStartTransactionAndCommitMultiKey(
        [CombinatorialValues(KahunaCommunicationType.Grpc)] KahunaCommunicationType communicationType,
        [CombinatorialValues(KahunaClientType.SingleEndpoint, KahunaClientType.PoolOfEndpoints)] KahunaClientType clientType
    )
    {
        KahunaClient client = GetClientByType(communicationType, clientType);

        await using KahunaTransactionSession session = await client.StartTransactionSession(
            new() { Locking = KeyValueTransactionLocking.Pessimistic },
            cancellationToken: TestContext.Current.CancellationToken
        );
        
        string keyName1 = GetRandomKeyName();
        string keyName2 = GetRandomKeyName();
        
        Assert.True(session.TransactionId.L > 0);

        KahunaKeyValue result = await session.SetKeyValue(keyName1, "some value1", cancellationToken: TestContext.Current.CancellationToken);
        Assert.True(result.Success);
        
        result = await session.SetKeyValue(keyName2, "some value2", cancellationToken: TestContext.Current.CancellationToken);
        Assert.True(result.Success);
        
        await session.Commit(cancellationToken: TestContext.Current.CancellationToken);
        
        // Value should exist after commit
        result = await client.GetKeyValue(keyName1, cancellationToken: TestContext.Current.CancellationToken);
        Assert.True(result.Success);        
        Assert.Equal("some value1", result.ValueAsString());
        
        // Value should exist after commit
        result = await client.GetKeyValue(keyName2, cancellationToken: TestContext.Current.CancellationToken);
        Assert.True(result.Success);        
        Assert.Equal("some value2", result.ValueAsString());
    }
    
    [Theory, CombinatorialData]
    public async Task TestSnapshotIsolationConflictInteractive(
        [CombinatorialValues(KahunaCommunicationType.Grpc)] KahunaCommunicationType communicationType,
        [CombinatorialValues(KahunaClientType.SingleEndpoint)] KahunaClientType clientType
    )
    {
        KahunaClient client = GetClientByType(communicationType, clientType);
        
        string keyNameA = GetRandomKeyName();
        string keyNameB = GetRandomKeyName();
        
        KahunaKeyValue result = await client.SetKeyValue(keyNameA, "10", cancellationToken: TestContext.Current.CancellationToken);
        Assert.True(result.Success);
        
        result = await client.SetKeyValue(keyNameB, "10", cancellationToken: TestContext.Current.CancellationToken);
        Assert.True(result.Success);
        
        try
        {
            await Task.WhenAll(
                SnapshotIsolationConflictOne(client, keyNameA, keyNameB),
                SnapshotIsolationConflictTwo(client, keyNameA, keyNameB)
            );
            
            Assert.False(true);
        }
        catch (KahunaException e)
        {
            Assert.True(e.KeyValueErrorCode is KeyValueResponseType.Aborted or KeyValueResponseType.MustRetry);
        }
        
        KahunaKeyValue resultA = await client.GetKeyValue(keyNameA, cancellationToken: TestContext.Current.CancellationToken);
        KahunaKeyValue resultB = await client.GetKeyValue(keyNameB, cancellationToken: TestContext.Current.CancellationToken);
        
        Assert.True(
            ("10" == resultA.ValueAsString() && "0" == resultB.ValueAsString()) || 
            ("0" == resultA.ValueAsString() && "10" == resultB.ValueAsString())
        );
    }

    private static async Task SnapshotIsolationConflictOne(KahunaClient client, string keyNameA, string keyNameB)
    {
        await using KahunaTransactionSession session1 = await client.StartTransactionSession(
            new() { Locking = KeyValueTransactionLocking.Optimistic }, 
            cancellationToken: TestContext.Current.CancellationToken
        );
        
        KahunaKeyValue av1 = await session1.GetKeyValue(keyNameA, cancellationToken: TestContext.Current.CancellationToken);
        KahunaKeyValue bv1 = await session1.GetKeyValue(keyNameB, cancellationToken: TestContext.Current.CancellationToken);
        
        if (int.Parse(av1.ValueAsString() ?? "0") + int.Parse(bv1.ValueAsString() ?? "0") == 20)
            await session1.SetKeyValue(keyNameA, "0", cancellationToken: TestContext.Current.CancellationToken);

        await session1.Commit(cancellationToken: TestContext.Current.CancellationToken);
    }
    
    private static async Task SnapshotIsolationConflictTwo(KahunaClient client, string keyNameA, string keyNameB)
    {
        await using KahunaTransactionSession session2 = await client.StartTransactionSession(
            new() { Locking = KeyValueTransactionLocking.Optimistic }, 
            cancellationToken: TestContext.Current.CancellationToken
        );
        
        KahunaKeyValue av2 = await session2.GetKeyValue(keyNameA, cancellationToken: TestContext.Current.CancellationToken);
        KahunaKeyValue bv2 = await session2.GetKeyValue(keyNameB, cancellationToken: TestContext.Current.CancellationToken);
        
        if (int.Parse(av2.ValueAsString() ?? "0") + int.Parse(bv2.ValueAsString() ?? "0") == 20)
            await session2.SetKeyValue(keyNameA, "0", cancellationToken: TestContext.Current.CancellationToken);

        await session2.Commit(cancellationToken: TestContext.Current.CancellationToken);
    }
    
    [Theory, CombinatorialData]
    public async Task TestSerializableConflictInteractive(
        [CombinatorialValues(KahunaCommunicationType.Grpc)] KahunaCommunicationType communicationType,
        [CombinatorialValues(KahunaClientType.SingleEndpoint)] KahunaClientType clientType
    )
    {
        KahunaClient client = GetClientByType(communicationType, clientType);

        int oneFailed = 0;
        
        string keyNameA = GetRandomKeyName();
                
        try
        {
            await Task.WhenAll(
                SerializableConflictInteractiveOne(client, keyNameA), 
                SerializableConflictInteractiveTwo(client, keyNameA)
            );
            
            Assert.False(true);
        }
        catch (KahunaException e)
        {
            oneFailed++;
            
            Assert.True(e.KeyValueErrorCode is KeyValueResponseType.Aborted or KeyValueResponseType.MustRetry);
        }
        
        Assert.Equal(1, oneFailed);        
    }

    private static async Task SerializableConflictInteractiveOne(KahunaClient client, string keyName)
    {
        KahunaTransactionSession session1 = await client.StartTransactionSession(
            new() { Locking = KeyValueTransactionLocking.Pessimistic }, 
            cancellationToken: TestContext.Current.CancellationToken
        );
        
        KahunaKeyValue result = await session1.GetKeyValue(keyName, cancellationToken: TestContext.Current.CancellationToken);
        Assert.False(result.Success);
        
        await Task.Delay(500, TestContext.Current.CancellationToken);
        
        await session1.SetKeyValue(keyName, "30", cancellationToken: TestContext.Current.CancellationToken);

        await session1.Commit(TestContext.Current.CancellationToken);
    }
    
     private static async Task SerializableConflictInteractiveTwo(KahunaClient client, string keyName)
    {
        KahunaTransactionSession session2 = await client.StartTransactionSession(
            new() { Locking = KeyValueTransactionLocking.Pessimistic }, 
            cancellationToken: TestContext.Current.CancellationToken
        );
        
        await session2.SetKeyValue(keyName, "30", cancellationToken: TestContext.Current.CancellationToken);

        await session2.Commit(TestContext.Current.CancellationToken);
    }
     
    [Theory, CombinatorialData]
    public async Task TestStartTransactionSetExtendKeyAndRollback(
        [CombinatorialValues(KahunaCommunicationType.Grpc)] KahunaCommunicationType communicationType,
        [CombinatorialValues(KahunaClientType.SingleEndpoint, KahunaClientType.PoolOfEndpoints)] KahunaClientType clientType
    )
    {
        KahunaClient client = GetClientByType(communicationType, clientType);

        await using KahunaTransactionSession session = await client.StartTransactionSession(
            new() { Locking = KeyValueTransactionLocking.Pessimistic },
            cancellationToken: TestContext.Current.CancellationToken
        );
        
        string keyName = GetRandomKeyName();
        
        Assert.True(session.TransactionId.L > 0);

        KahunaKeyValue result = await session.SetKeyValue(keyName, "some value", cancellationToken: TestContext.Current.CancellationToken);
        Assert.True(result.Success);
        
        result = await session.GetKeyValue(keyName, cancellationToken: TestContext.Current.CancellationToken);
        Assert.True(result.Success);        
        Assert.Equal("some value", result.ValueAsString());
        
        result = await session.ExtendKeyValue(keyName, 500, cancellationToken: TestContext.Current.CancellationToken);
        Assert.True(result.Success);
        
        await Task.Delay(1000, TestContext.Current.CancellationToken);
        
        await session.Rollback(cancellationToken: TestContext.Current.CancellationToken);
        
        // Value should exist after commit
        result = await client.GetKeyValue(keyName, cancellationToken: TestContext.Current.CancellationToken);
        Assert.False(result.Success);
    }
    
    [Theory, CombinatorialData]
    public async Task TestStartTransactionSetExtendKeyAndCommit(
        [CombinatorialValues(KahunaCommunicationType.Grpc)] KahunaCommunicationType communicationType,
        [CombinatorialValues(KahunaClientType.SingleEndpoint, KahunaClientType.PoolOfEndpoints)] KahunaClientType clientType
    )
    {
        KahunaClient client = GetClientByType(communicationType, clientType);

        await using KahunaTransactionSession session = await client.StartTransactionSession(
            new() { Locking = KeyValueTransactionLocking.Pessimistic },
            cancellationToken: TestContext.Current.CancellationToken
        );
        
        string keyName = GetRandomKeyName();
        
        Assert.True(session.TransactionId.L > 0);

        KahunaKeyValue result = await session.SetKeyValue(keyName, "some value", cancellationToken: TestContext.Current.CancellationToken);
        Assert.True(result.Success);
        
        result = await session.GetKeyValue(keyName, cancellationToken: TestContext.Current.CancellationToken);
        Assert.True(result.Success);        
        Assert.Equal("some value", result.ValueAsString());
        
        result = await session.ExtendKeyValue(keyName, 500, cancellationToken: TestContext.Current.CancellationToken);
        Assert.True(result.Success);
        
        await Task.Delay(1000, TestContext.Current.CancellationToken);
        
        await session.Rollback(cancellationToken: TestContext.Current.CancellationToken);
        
        // Value should exist after commit
        result = await client.GetKeyValue(keyName, cancellationToken: TestContext.Current.CancellationToken);
        Assert.False(result.Success);
    }

    [Theory, CombinatorialData]
    public async Task TestRetryableTransactionRollback(
        [CombinatorialValues(KahunaCommunicationType.Grpc)]
        KahunaCommunicationType communicationType,
        [CombinatorialValues(KahunaClientType.SingleEndpoint, KahunaClientType.PoolOfEndpoints)]
        KahunaClientType clientType
    )
    {
        KahunaClient client = GetClientByType(communicationType, clientType);

        KahunaTransactionOptions txOptions = new() { Locking = KeyValueTransactionLocking.Pessimistic };
        
        string keyName = GetRandomKeyName();

        await client.RetryableTransaction(txOptions, async (session, cancellationToken) =>
        {
            await session.SetKeyValue(keyName, "value1", cancellationToken: cancellationToken);

            await session.Rollback(cancellationToken);
            
        }, TestContext.Current.CancellationToken);
        
        KahunaKeyValue result = await client.GetKeyValue(keyName, cancellationToken: TestContext.Current.CancellationToken);
        Assert.False(result.Success);
    }
    
    [Theory, CombinatorialData]
    public async Task TestRetryableTransactionCommit(
        [CombinatorialValues(KahunaCommunicationType.Grpc)]
        KahunaCommunicationType communicationType,
        [CombinatorialValues(KahunaClientType.SingleEndpoint, KahunaClientType.PoolOfEndpoints)]
        KahunaClientType clientType
    )
    {
        KahunaClient client = GetClientByType(communicationType, clientType);

        KahunaTransactionOptions txOptions = new() { Locking = KeyValueTransactionLocking.Pessimistic };
        
        string keyName = GetRandomKeyName();

        await client.RetryableTransaction(txOptions, async (session, cancellationToken) =>
        {
            await session.SetKeyValue(keyName, "value1", cancellationToken: cancellationToken);

            await session.Commit(cancellationToken);
            
        }, TestContext.Current.CancellationToken);
        
        KahunaKeyValue result = await client.GetKeyValue(keyName, cancellationToken: TestContext.Current.CancellationToken);
        Assert.True(result.Success);
    }
    
    [Theory, CombinatorialData]
    public async Task TestRetryableTransactionRace(
        [CombinatorialValues(KahunaCommunicationType.Grpc)]
        KahunaCommunicationType communicationType,
        [CombinatorialValues(KahunaClientType.SingleEndpoint, KahunaClientType.PoolOfEndpoints)]
        KahunaClientType clientType
    )
    {
        KahunaClient client = GetClientByType(communicationType, clientType);
        
        string keyName = GetRandomKeyName();
        
        await client.SetKeyValue(keyName, "0", cancellationToken: TestContext.Current.CancellationToken);

        List<Task> tasks = [];
        
        for (int i = 0; i < 10; i++)
            tasks.Add(ExecuteRetryableConcurrently(client, i, keyName));
        
        await Task.WhenAll(tasks);
        
        KahunaKeyValue result = await client.GetKeyValue(keyName, cancellationToken: TestContext.Current.CancellationToken);
        Assert.True(result.Success);
        //Assert.
    }

    private static async Task ExecuteRetryableConcurrently(KahunaClient client, int i, string keyName)
    {
        KahunaTransactionOptions txOptions = new() { Locking = KeyValueTransactionLocking.Pessimistic };
        
        await client.RetryableTransaction(txOptions, async (session, cancellationToken) =>
        {
            KahunaKeyValue currentKeyValue = await session.GetKeyValue(keyName, cancellationToken: cancellationToken);
                
            if (currentKeyValue.ValueAsLong() < i)
                await session.SetKeyValue(keyName, i.ToString(), cancellationToken: cancellationToken);

            await session.Commit(cancellationToken);
            
        }, TestContext.Current.CancellationToken);
    }

    private KahunaClient GetClientByType(KahunaCommunicationType communicationType, KahunaClientType clientType)
    {
        return clientType switch
        {
            KahunaClientType.SingleEndpoint => new(url, null, GetCommunicationByType(communicationType)),
            KahunaClientType.PoolOfEndpoints => new(urls, null, GetCommunicationByType(communicationType)),
            _ => throw new ArgumentOutOfRangeException(nameof(clientType), clientType, null)
        };
    }

    private static IKahunaCommunication GetCommunicationByType(KahunaCommunicationType communicationType)
    {
        return communicationType switch
        {
            KahunaCommunicationType.Grpc => new GrpcCommunication(null, null),
            KahunaCommunicationType.Rest => new RestCommunication(null),
            _ => throw new ArgumentOutOfRangeException(nameof(communicationType), communicationType, null)
        };
    }
    
    private static string GetRandomKeyName()
    {
        return Guid.NewGuid().ToString("N")[..16];
    }
}