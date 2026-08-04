using Kahuna.Communication.External.Grpc;
using Kahuna.Communication.External.Grpc.KeyValues;

namespace Kahuna.Server.Tests;

/// <summary>
/// A transactional batch operation arriving from a client without coordinator registration identity can
/// never commit — the server-owned working set never learns of it, so commit would report success while
/// writing nothing. The client-facing gRPC entry points reject such requests with InvalidInput instead of
/// accepting doomed work. These tests pin the guard's decision table: non-transactional batches and
/// registered transactional batches pass; unregistered transactional batches are rejected per item.
/// </summary>
public sealed class TestUnregisteredTransactionalBatchGuard
{
    private static GrpcTrySetManyKeyValueRequestItem SetItem(string key, long txPhysical) => new()
    {
        Key = key,
        TransactionIdPhysical = txPhysical,
        Durability = GrpcKeyValueDurability.Persistent
    };

    private static GrpcTryDeleteManyKeyValueRequestItem DeleteItem(string key, long txPhysical) => new()
    {
        Key = key,
        TransactionIdPhysical = txPhysical,
        Durability = GrpcKeyValueDurability.Persistent
    };

    [Fact]
    public void SetMany_NonTransactional_Passes()
    {
        GrpcTrySetManyKeyValueRequest request = new();
        request.Items.Add(SetItem("a", 0));
        request.Items.Add(SetItem("b", 0));

        Assert.Null(KeyValuesService.RejectUnregisteredTransactionalSetMany(request));
    }

    [Fact]
    public void SetMany_Transactional_RejectedPerItem()
    {
        GrpcTrySetManyKeyValueRequest request = new();
        request.Items.Add(SetItem("a", 0));
        request.Items.Add(SetItem("b", 100)); // one transactional item taints the whole batch

        GrpcTrySetManyKeyValueResponse? rejected = KeyValuesService.RejectUnregisteredTransactionalSetMany(request);

        Assert.NotNull(rejected);
        Assert.Equal(2, rejected!.Items.Count);
        Assert.All(rejected.Items, static i => Assert.Equal(GrpcKeyValueResponseType.TypeInvalidInput, i.Type));
        Assert.Equal("a", rejected.Items[0].Key);
        Assert.Equal("b", rejected.Items[1].Key);
    }

    [Fact]
    public void DeleteMany_NonTransactional_WithoutIdentity_Passes()
    {
        GrpcTryDeleteManyKeyValueRequest request = new();
        request.Items.Add(DeleteItem("a", 0));

        Assert.Null(KeyValuesService.RejectUnregisteredTransactionalDeleteMany(request));
    }

    [Fact]
    public void DeleteMany_Transactional_WithIdentity_Passes()
    {
        GrpcTryDeleteManyKeyValueRequest request = new()
        {
            CoordinatorKey = "coord",
            OperationIdHigh = 1,
            OperationIdLow = 2
        };
        request.Items.Add(DeleteItem("a", 100));

        Assert.Null(KeyValuesService.RejectUnregisteredTransactionalDeleteMany(request));
    }

    [Fact]
    public void DeleteMany_Transactional_WithoutIdentity_RejectedPerItem()
    {
        GrpcTryDeleteManyKeyValueRequest request = new();
        request.Items.Add(DeleteItem("a", 100));
        request.Items.Add(DeleteItem("b", 100));

        GrpcTryDeleteManyKeyValueResponse? rejected = KeyValuesService.RejectUnregisteredTransactionalDeleteMany(request);

        Assert.NotNull(rejected);
        Assert.Equal(2, rejected!.Items.Count);
        Assert.All(rejected.Items, static i => Assert.Equal(GrpcKeyValueResponseType.TypeInvalidInput, i.Type));
    }

    [Fact]
    public void DeleteMany_Transactional_WithPartialIdentity_RejectedPerItem()
    {
        // A coordinator key without an operation id (or vice versa) cannot register either.
        GrpcTryDeleteManyKeyValueRequest request = new() { CoordinatorKey = "coord" };
        request.Items.Add(DeleteItem("a", 100));

        Assert.NotNull(KeyValuesService.RejectUnregisteredTransactionalDeleteMany(request));
    }
}
