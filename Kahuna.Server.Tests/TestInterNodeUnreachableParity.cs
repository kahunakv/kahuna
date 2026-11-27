using System.Collections;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;

using Kahuna.Server.Communication.Internode;
using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Data;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.KeyValues.Writes;
using Kahuna.Server.Persistence.Pitr;
using Kahuna.Shared.KeyValue;
using Kahuna.Shared.Locks;
using Kahuna.Shared.Sequences;

using Kommander;
using Kommander.Time;

using Microsoft.Extensions.Logging.Abstractions;

namespace Kahuna.Server.Tests;

/// <summary>
/// The in-memory inter-node transport answers a peer it cannot reach as the gRPC transport answers an unreachable
/// peer, method by method. For each <see cref="IInterNodeCommunication"/> method the test compares three calls: the
/// gRPC transport to an address where nothing listens, the in-memory transport over a blocked link, and the in-memory
/// transport to a node that is not registered. The answers must be equal. When the gRPC transport throws, the
/// in-memory calls must throw too, and <see cref="RetryableFailureClassifier"/> must call each exception retryable.
/// </summary>
public sealed class TestInterNodeUnreachableParity
{
    /// <summary>Nothing listens on port 1, so the gRPC call fails with a retryable transport status at once.</summary>
    private const string UnreachableGrpcNode = "https://localhost:1";

    private const string Caller = "caller:1";

    private const string Peer = "peer:1";

    private const string NotRegistered = "nowhere:1";

    /// <summary>A bound for each call, so a call that waits for a reply that never comes fails the case.</summary>
    private static readonly TimeSpan CallDeadline = TimeSpan.FromSeconds(30);

    private static readonly HLCTimestamp TransactionId = new(1, 100, 0);

    private static readonly HLCTimestamp CommitId = new(1, 200, 0);

    private static readonly byte[] Owner = [1, 2, 3];

    private static readonly TransactionOperationId OperationId = new(7, 11);

    /// <summary>One call of each interface method, with fixed arguments. The key is the method name.</summary>
    private static readonly Dictionary<string, Func<IInterNodeCommunication, string, CancellationToken, Task<object?>>> Cases = new(StringComparer.Ordinal)
    {
        [nameof(IInterNodeCommunication.TryLock)] = Call((t, n, ct) => t.TryLock(n, "lock/r", Owner, 1000, LockDurability.Persistent, ct)),
        [nameof(IInterNodeCommunication.TryExtendLock)] = Call((t, n, ct) => t.TryExtendLock(n, "lock/r", Owner, 1000, LockDurability.Persistent, ct)),
        [nameof(IInterNodeCommunication.TryUnlock)] = Call((t, n, ct) => t.TryUnlock(n, "lock/r", Owner, LockDurability.Persistent, ct)),
        [nameof(IInterNodeCommunication.GetLock)] = Call((t, n, ct) => t.GetLock(n, "lock/r", LockDurability.Persistent, ct)),

        [nameof(IInterNodeCommunication.CreateSequence)] = Call((t, n, ct) => t.CreateSequence(n, "seq", 1, 1, null, null, SequenceDurability.Persistent, ct)),
        [nameof(IInterNodeCommunication.UpdateSequence)] = Call((t, n, ct) => t.UpdateSequence(n, "seq", new SequenceUpdate(Increment: 2), SequenceDurability.Persistent, ct)),
        [nameof(IInterNodeCommunication.GetSequence)] = Call((t, n, ct) => t.GetSequence(n, "seq", SequenceDurability.Persistent, ct)),
        [nameof(IInterNodeCommunication.NextSequenceValue)] = Call((t, n, ct) => t.NextSequenceValue(n, "seq", null, SequenceDurability.Persistent, ct)),
        [nameof(IInterNodeCommunication.ReserveSequenceRange)] = Call((t, n, ct) => t.ReserveSequenceRange(n, "seq", 5, null, SequenceDurability.Persistent, ct)),
        [nameof(IInterNodeCommunication.DeleteSequence)] = Call((t, n, ct) => t.DeleteSequence(n, "seq", SequenceDurability.Persistent, ct)),

        [nameof(IInterNodeCommunication.TrySetKeyValue)] = Call((t, n, ct) =>
            t.TrySetKeyValue(n, TransactionId, "kv/a", [1], null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, 0, ct)),
        [nameof(IInterNodeCommunication.TrySetManyNodeKeyValue)] = async (t, n, ct) =>
        {
            List<KahunaSetKeyValueResponseItem> responses = [];
            await t.TrySetManyNodeKeyValue(n, SetItems(), false, new Lock(), responses, ct);
            return responses;
        },
        [nameof(IInterNodeCommunication.TryDeleteManyNodeKeyValue)] = async (t, n, ct) =>
        {
            List<KahunaDeleteKeyValueResponseItem> responses = [];
            await t.TryDeleteManyNodeKeyValue(n, DeleteItems(), new Lock(), responses, ct);
            return responses;
        },
        [nameof(IInterNodeCommunication.TryDeleteKeyValue)] = Call((t, n, ct) => t.TryDeleteKeyValue(n, TransactionId, "kv/a", KeyValueDurability.Persistent, ct)),
        [nameof(IInterNodeCommunication.TryExtendKeyValue)] = Call((t, n, ct) => t.TryExtendKeyValue(n, TransactionId, "kv/a", 1000, KeyValueDurability.Persistent, ct)),
        [nameof(IInterNodeCommunication.TryGetValue)] = Call((t, n, ct) =>
            t.TryGetValue(n, TransactionId, "kv/a", -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct)),
        [nameof(IInterNodeCommunication.TryExistsValue)] = Call((t, n, ct) =>
            t.TryExistsValue(n, TransactionId, "kv/a", -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct)),
        [nameof(IInterNodeCommunication.TryGetManyNodeValues)] = async (t, n, ct) =>
        {
            List<(KeyValueResponseType, string, KeyValueDurability, ReadOnlyKeyValueEntry?)> responses = [];
            await t.TryGetManyNodeValues(n, TransactionId, HLCTimestamp.Zero, ReadKeys(), new Lock(), responses, ct);
            return responses;
        },
        [nameof(IInterNodeCommunication.TryProbeMany)] = Call((t, n, ct) =>
            t.TryProbeMany(n, TransactionId, HLCTimestamp.Zero, ["kv/a", "kv/b"], KeyValueProbeLock.None, 0, KeyValueDurability.Persistent, ct)),
        [nameof(IInterNodeCommunication.TryProbeManyRanges)] = Call((t, n, ct) =>
            t.TryProbeManyRanges(n, TransactionId, HLCTimestamp.Zero, "kv/", [new("kv/a", true, "kv/m", false, 10), new("kv/m", true, null, false, 10)],
                KeyValueProbeLock.None, 0, KeyValueDurability.Persistent, ct)),
        [nameof(IInterNodeCommunication.TryExistsManyNodeValues)] = async (t, n, ct) =>
        {
            List<(KeyValueResponseType, string, KeyValueDurability, ReadOnlyKeyValueEntry?)> responses = [];
            await t.TryExistsManyNodeValues(n, TransactionId, HLCTimestamp.Zero, ReadKeys(), new Lock(), responses, ct);
            return responses;
        },
        [nameof(IInterNodeCommunication.TryCheckWriteIntentValue)] = Call((t, n, ct) => t.TryCheckWriteIntentValue(n, TransactionId, "kv/a", KeyValueDurability.Persistent, ct)),
        [nameof(IInterNodeCommunication.TryCheckManyWriteIntents)] = Call((t, n, ct) =>
            t.TryCheckManyWriteIntents(n, TransactionId,
                [new("kv/a", KeyValueDurability.Persistent, KeyValueConflictChecks.None), new("kv/b", KeyValueDurability.Ephemeral, KeyValueConflictChecks.None, 4)], ct)),

        [nameof(IInterNodeCommunication.TryAcquireExclusiveLock)] = Call((t, n, ct) => t.TryAcquireExclusiveLock(n, TransactionId, "kv/a", 1000, KeyValueDurability.Ephemeral, ct)),
        [nameof(IInterNodeCommunication.TryAcquireExclusivePrefixLock)] = Call((t, n, ct) => t.TryAcquireExclusivePrefixLock(n, TransactionId, "kv/", 1000, KeyValueDurability.Persistent, ct)),
        [nameof(IInterNodeCommunication.TryAcquireRangeLock)] = Call((t, n, ct) =>
            t.TryAcquireRangeLock(n, TransactionId, "kv/", "kv/a", true, "kv/z", false, 1000, KeyValueDurability.Persistent, RangeLockMode.Shared, ct)),
        [nameof(IInterNodeCommunication.TryAcquireExclusiveRangeLock)] = Call((t, n, ct) =>
            t.TryAcquireExclusiveRangeLock(n, TransactionId, "kv/", "kv/a", true, "kv/z", false, 1000, KeyValueDurability.Persistent, ct)),
        [nameof(IInterNodeCommunication.TryAcquireNodeExclusiveLocks)] = async (t, n, ct) =>
        {
            List<(KeyValueResponseType, string, KeyValueDurability, HLCTimestamp, long)> responses = [];
            await t.TryAcquireNodeExclusiveLocks(n, TransactionId, [("kv/a", 1000, KeyValueDurability.Persistent), ("kv/b", 1000, KeyValueDurability.Ephemeral)],
                new Lock(), responses, ct);
            return responses;
        },
        [nameof(IInterNodeCommunication.TryReleaseExclusiveLock)] = Call((t, n, ct) => t.TryReleaseExclusiveLock(n, TransactionId, "kv/a", KeyValueDurability.Persistent, ct)),
        [nameof(IInterNodeCommunication.TryReleaseExclusivePrefixLock)] = Call((t, n, ct) => t.TryReleaseExclusivePrefixLock(n, TransactionId, "kv/", KeyValueDurability.Persistent, ct)),
        [nameof(IInterNodeCommunication.TryReleaseExclusiveRangeLock)] = Call((t, n, ct) =>
            t.TryReleaseExclusiveRangeLock(n, TransactionId, "kv/", "kv/a", true, "kv/z", false, KeyValueDurability.Persistent, ct)),
        [nameof(IInterNodeCommunication.TryReleaseNodeExclusiveLocks)] = async (t, n, ct) =>
        {
            List<(KeyValueResponseType, string, KeyValueDurability)> responses = [];
            await t.TryReleaseNodeExclusiveLocks(n, TransactionId, MutationKeys(), new Lock(), responses, ct);
            return responses;
        },

        [nameof(IInterNodeCommunication.TryPrepareMutations)] = Call((t, n, ct) =>
            t.TryPrepareMutations(n, TransactionId, CommitId, "kv/a", KeyValueDurability.Ephemeral, 0, ct, "kv/anchor")),
        [nameof(IInterNodeCommunication.TryPrepareNodeMutations)] = async (t, n, ct) =>
        {
            List<(KeyValueResponseType, HLCTimestamp, string, KeyValueDurability)> responses = [];
            await t.TryPrepareNodeMutations(n, TransactionId, CommitId, MutationKeys(), new Lock(), responses, ct, "kv/anchor");
            return responses;
        },
        [nameof(IInterNodeCommunication.TryCommitMutations)] = Call((t, n, ct) => t.TryCommitMutations(n, TransactionId, "kv/a", CommitId, KeyValueDurability.Persistent, ct)),
        [nameof(IInterNodeCommunication.TryCommitNodeMutations)] = async (t, n, ct) =>
        {
            List<(KeyValueResponseType, string, long, KeyValueDurability)> responses = [];
            await t.TryCommitNodeMutations(n, TransactionId, TicketKeys(), new Lock(), responses, ct);
            return responses;
        },
        [nameof(IInterNodeCommunication.TryRollbackMutations)] = Call((t, n, ct) => t.TryRollbackMutations(n, TransactionId, "kv/a", CommitId, KeyValueDurability.Persistent, ct)),
        [nameof(IInterNodeCommunication.TryRollbackNodeMutations)] = async (t, n, ct) =>
        {
            List<(KeyValueResponseType, string, long, KeyValueDurability)> responses = [];
            await t.TryRollbackNodeMutations(n, TransactionId, TicketKeys(), new Lock(), responses, ct);
            return responses;
        },

        [nameof(IInterNodeCommunication.DurableOperation)] = Call((t, n, ct) => t.DurableOperation(n, 1, 0, "kv", [1], ct)),
        [nameof(IInterNodeCommunication.DurableBundle)] = Call((t, n, ct) => t.DurableBundle(n, 1, [("kv", [1])], false, 0, [], ct)),
        [nameof(IInterNodeCommunication.DurableDecision)] = Call((t, n, ct) => t.DurableDecision(n, 1, [1], TransactionId, 1, [], ct)),
        [nameof(IInterNodeCommunication.DurableOnePhase)] = Call((t, n, ct) => t.DurableOnePhase(n, 1, [1], [2], [3], TransactionId, 1, CommitId, [], 0, ct)),
        [nameof(IInterNodeCommunication.LookupTransactionRecord)] = Call((t, n, ct) => t.LookupTransactionRecord(n, 1, TransactionId, 1, "kv/anchor", ct)),

        [nameof(IInterNodeCommunication.GetByBucket)] = Call((t, n, ct) => t.GetByBucket(n, TransactionId, "kv/", HLCTimestamp.Zero, KeyValueDurability.Persistent, ct)),
        [nameof(IInterNodeCommunication.GetByRange)] = Call((t, n, ct) =>
            t.GetByRange(n, TransactionId, "kv/", "kv/a", true, "kv/z", false, 10, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct)),
        [nameof(IInterNodeCommunication.ScanByPrefix)] = Call((t, n, ct) => t.ScanByPrefix(n, "kv/", HLCTimestamp.Zero, KeyValueDurability.Persistent, false, ct)),

        [nameof(IInterNodeCommunication.StartTransaction)] = Call((t, n, ct) => t.StartTransaction(n, new KeyValueTransactionOptions { CoordinatorKey = "coordinator" }, ct)),
        [nameof(IInterNodeCommunication.CommitTransaction)] = Call((t, n, ct) => t.CommitTransaction(n, new TransactionHandle(TransactionId, "coordinator", "kv/anchor"), ct)),
        [nameof(IInterNodeCommunication.RollbackTransaction)] = Call((t, n, ct) => t.RollbackTransaction(n, new TransactionHandle(TransactionId, "coordinator", "kv/anchor"), ct)),
        [nameof(IInterNodeCommunication.BeginOperation)] = Call((t, n, ct) => t.BeginOperation(n, "coordinator", TransactionId, OperationId, OperationKind.Set, null, ct, 1)),
        [nameof(IInterNodeCommunication.CompleteOperation)] = Call((t, n, ct) =>
            t.CompleteOperation(n, "coordinator", TransactionId, OperationId, new OperationCompletionPayload { ModifiedKey = "kv/a" }, ct)),
        [nameof(IInterNodeCommunication.GetTransactionWorkingSet)] = Call((t, n, ct) => t.GetTransactionWorkingSet(n, "coordinator", TransactionId, ct)),
        [nameof(IInterNodeCommunication.CloseTransaction)] = Call((t, n, ct) => t.CloseTransaction(n, "coordinator", TransactionId, ct)),

        [nameof(IInterNodeCommunication.EnsureKeyRangeSeeded)] = Call((t, n, ct) => t.EnsureKeyRangeSeeded(n, "space", ct)),
        [nameof(IInterNodeCommunication.EnsureKeyRangeRemoved)] = Call((t, n, ct) => t.EnsureKeyRangeRemoved(n, "space", ct)),
        [nameof(IInterNodeCommunication.GetRangeLocks)] = Call((t, n, ct) => t.GetRangeLocks(n, "space", ct)),
        [nameof(IInterNodeCommunication.ImportRangeLocks)] = async (t, n, ct) =>
        {
            await t.ImportRangeLocks(n, "space", [], ct);
            return "completed";
        },
        [nameof(IInterNodeCommunication.ImportCompletionReceipts)] = Call((t, n, ct) =>
            t.ImportCompletionReceipts(n, 1, [new CompletionReceiptRecord(TransactionId, "kv/a", null, KeyValueDurability.Persistent)], ct)),
        [nameof(IInterNodeCommunication.ReplicateKeyValueRangePage)] = Call((t, n, ct) => t.ReplicateKeyValueRangePage(n, 1, [1], ct)),
        [nameof(IInterNodeCommunication.GetRangeTransactionState)] = Call((t, n, ct) =>
            t.GetRangeTransactionState(n, 1, null, null, KeyValueRangeStateKinds.Receipts, null, 10, ct)),
        [nameof(IInterNodeCommunication.GetRangeHeldHistoryPage)] = Call((t, n, ct) =>
            t.GetRangeHeldHistoryPage(n, 1, "space", null, null, [], HLCTimestamp.Zero, null, ct)),
        [nameof(IInterNodeCommunication.GetStagedBaseVerdicts)] = Call((t, n, ct) => t.GetStagedBaseVerdicts(n, 1, TransactionId, 1, ["kv/a"], null, 0, ct)),

        [nameof(IInterNodeCommunication.AcquireSnapshotHold)] = Call((t, n, ct) => t.AcquireSnapshotHold(n, "holder", CommitId, 1000, ct)),
        [nameof(IInterNodeCommunication.RenewSnapshotHold)] = Call((t, n, ct) => t.RenewSnapshotHold(n, "hold", 1000, ct)),
        [nameof(IInterNodeCommunication.ReleaseSnapshotHold)] = Call((t, n, ct) => t.ReleaseSnapshotHold(n, "hold", ct)),
        [nameof(IInterNodeCommunication.GetSnapshotFloor)] = Call((t, n, ct) => t.GetSnapshotFloor(n, ct)),
        [nameof(IInterNodeCommunication.HistoryHold)] = Call((t, n, ct) => t.HistoryHold(n, new HistoryHoldRequest(HistoryHoldOperation.Acquire, "holder", [1], CommitId, 1000), ct)),
        [nameof(IInterNodeCommunication.GetPartitionApplyFingerprint)] = Call((t, n, ct) => t.GetPartitionApplyFingerprint(n, 1, ct)),
        [nameof(IInterNodeCommunication.GetAppliedLogTypes)] = Call((t, n, ct) => t.GetAppliedLogTypes(n, ct)),
        [nameof(IInterNodeCommunication.CaptureBackupPart)] = Call((t, n, ct) =>
            t.CaptureBackupPart(n, new BackupPartRequest(BackupPartKind.Full, [1], 1, []), new DiscardingSink(), ct)),
        [nameof(IInterNodeCommunication.GetPreparedIntentPresence)] = Call((t, n, ct) => t.GetPreparedIntentPresence(n, 1, TransactionId, 1, "kv/a", ct)),
        [nameof(IInterNodeCommunication.ConfirmRangeQuiesce)] = Call((t, n, ct) => t.ConfirmRangeQuiesce(n, 1, "space", null, null, TransactionId, ct)),
        [nameof(IInterNodeCommunication.SettleDecidedBlockers)] = Call((t, n, ct) => t.SettleDecidedBlockers(n, 1, ["kv/a"], TransactionId, 1, ct)),
    };

    public static TheoryData<string> Methods() => new(Cases.Keys.Order(StringComparer.Ordinal));

    [Theory]
    [MemberData(nameof(Methods))]
    public async Task UnreachablePeer_GetsTheGrpcAnswer_OnABlockedLinkAndForANodeThatIsNotRegistered(string method)
    {
        Func<IInterNodeCommunication, string, CancellationToken, Task<object?>> call = Cases[method];

        GrpcInterNodeCommunication grpc = new(new KahunaConfiguration(), new RaftTransportSecurityOptions(), NullLogger<GrpcInterNodeCommunication>.Instance);
        Outcome expected = await RunAsync(call, grpc, UnreachableGrpcNode);

        // An exception that the classifier does not call retryable would let a caller fail at once, on gRPC too.
        Assert.NotEqual(OutcomeKind.Failure, expected.Kind);

        (MemoryInterNodeCommmunication shared, MemoryInterNodeCommmunication caller) = BuildMemoryTransport();

        shared.BlockLink(Caller, Peer);
        Assert.Equal(expected, await RunAsync(call, caller, Peer));

        // The view checks the link in both directions.
        shared.UnblockAllLinks();
        shared.BlockLink(Peer, Caller);
        Assert.Equal(expected, await RunAsync(call, caller, Peer));

        Assert.Equal(expected, await RunAsync(call, caller, NotRegistered));
    }

    [Fact]
    public void EveryInterfaceMethod_HasAParityCase()
    {
        string[] methods = [.. typeof(IInterNodeCommunication).GetMethods().Select(static m => m.Name)];

        // A case is keyed by name, so an overload would share a case with another method.
        Assert.Equal(methods.Length, methods.Distinct(StringComparer.Ordinal).Count());

        Assert.Empty(methods.Except(Cases.Keys, StringComparer.Ordinal));
        Assert.Empty(Cases.Keys.Except(methods, StringComparer.Ordinal));
    }

    /// <summary>
    /// The unblocked view reaches the peer, so the parity cases prove the block and the missing registration, and not
    /// a view that never reaches any node.
    /// </summary>
    [Fact]
    public async Task UnblockedLink_RunsTheCallOnThePeer()
    {
        (_, MemoryInterNodeCommmunication caller) = BuildMemoryTransport();

        InvalidOperationException ran = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            caller.TryGetValue(Peer, TransactionId, "kv/a", -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, TestContext.Current.CancellationToken));

        Assert.Contains(nameof(IKahuna.LocateAndTryGetValue), ran.Message);
    }

    /// <summary>
    /// The message of an operation that has no typed answer still names the node, the caller and the reason, so a
    /// log shows which link was blocked.
    /// </summary>
    [Fact]
    public async Task OperationWithNoTypedAnswer_ThrowsARetryableFailure_ThatNamesTheLink()
    {
        (MemoryInterNodeCommmunication shared, MemoryInterNodeCommmunication caller) = BuildMemoryTransport();
        shared.BlockLink(Caller, Peer);

        KahunaServerException blocked = await Assert.ThrowsAsync<KahunaServerException>(() =>
            caller.DurableOperation(Peer, 1, 0, "kv", [1], TestContext.Current.CancellationToken));

        Assert.Equal(KeyValueResponseType.MustRetry, blocked.ResponseType);
        Assert.True(RetryableFailureClassifier.IsRetryable(blocked));
        Assert.Equal($"The node {Peer} is not reachable from {Caller}: the link between them is blocked.", blocked.Message);

        KahunaServerException unknown = await Assert.ThrowsAsync<KahunaServerException>(() =>
            caller.DurableOperation(NotRegistered, 1, 0, "kv", [1], TestContext.Current.CancellationToken));

        Assert.Equal(KeyValueResponseType.MustRetry, unknown.ResponseType);
        Assert.Equal($"The node {NotRegistered} does not exist.", unknown.Message);
    }

    /// <summary>A shared transport with the caller and the peer registered, and the caller's view of it.</summary>
    private static (MemoryInterNodeCommmunication Shared, MemoryInterNodeCommmunication Caller) BuildMemoryTransport()
    {
        MemoryInterNodeCommmunication shared = new();

        shared.SetNodes(new Dictionary<string, IKahuna>
        {
            [Caller] = DispatchProxy.Create<IKahuna, TargetMustNotRun>(),
            [Peer] = DispatchProxy.Create<IKahuna, TargetMustNotRun>(),
        });

        return (shared, shared.ForNode(Caller));
    }

    private enum OutcomeKind
    {
        Answer,
        RetryableFailure,
        Failure
    }

    /// <summary>What one call gave: an answer and its fields, or a failure.</summary>
    private readonly record struct Outcome(OutcomeKind Kind, string Detail)
    {
        public override string ToString() => $"{Kind}: {Detail}";
    }

    private static async Task<Outcome> RunAsync(Func<IInterNodeCommunication, string, CancellationToken, Task<object?>> call, IInterNodeCommunication transport, string node)
    {
        using CancellationTokenSource deadline = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        deadline.CancelAfter(CallDeadline);

        try
        {
            return new(OutcomeKind.Answer, Describe(await call(transport, node, deadline.Token), 0));
        }
        catch (Exception ex) when (RetryableFailureClassifier.IsRetryable(ex))
        {
            // The two transports throw different types for the same case; the caller sees only that it may retry.
            return new(OutcomeKind.RetryableFailure, "");
        }
        catch (Exception ex)
        {
            return new(OutcomeKind.Failure, $"{ex.GetType().Name}: {ex.Message}");
        }
    }

    private static Func<IInterNodeCommunication, string, CancellationToken, Task<object?>> Call<T>(Func<IInterNodeCommunication, string, CancellationToken, Task<T>> call) =>
        async (transport, node, ct) => await call(transport, node, ct);

    /// <summary>
    /// A text form of an answer that two equal answers share: the value of each public field and property, in name
    /// order, and the items of each list and tuple in order. The result types are classes with list fields, so their
    /// own equality compares references.
    /// </summary>
    private static string Describe(object? value, int depth)
    {
        if (value is null)
            return "null";

        if (depth > 8)
            return "...";

        switch (value)
        {
            case string text:
                return "\"" + text + "\"";

            case byte[] bytes:
                return "0x" + Convert.ToHexString(bytes);

            case HLCTimestamp timestamp:
                return timestamp.ToString();

            case Enum or bool or char or sbyte or byte or short or ushort or int or uint or long or ulong or float or double or decimal:
                return value.ToString()!;

            case ITuple tuple:
            {
                StringBuilder builder = new("(");
                for (int i = 0; i < tuple.Length; i++)
                {
                    if (i > 0)
                        builder.Append(", ");

                    builder.Append(Describe(tuple[i], depth + 1));
                }

                return builder.Append(')').ToString();
            }

            case IEnumerable items:
            {
                StringBuilder builder = new("[");
                bool first = true;
                foreach (object? item in items)
                {
                    if (!first)
                        builder.Append(", ");

                    builder.Append(Describe(item, depth + 1));
                    first = false;
                }

                return builder.Append(']').ToString();
            }
        }

        Type type = value.GetType();
        StringBuilder members = new(type.Name);
        members.Append('{');

        bool firstMember = true;

        foreach (MemberInfo member in type.GetMembers(BindingFlags.Public | BindingFlags.Instance)
                     .Where(static m => m is FieldInfo || m is PropertyInfo { CanRead: true } p && p.GetIndexParameters().Length == 0)
                     .OrderBy(static m => m.Name, StringComparer.Ordinal))
        {
            object? memberValue = member is FieldInfo field ? field.GetValue(value) : ((PropertyInfo)member).GetValue(value);

            if (!firstMember)
                members.Append(", ");

            members.Append(member.Name).Append('=').Append(Describe(memberValue, depth + 1));
            firstMember = false;
        }

        return members.Append('}').ToString();
    }

    private static List<KahunaSetKeyValueRequestItem> SetItems() =>
    [
        new() { TransactionId = TransactionId, Key = "kv/a", Value = [1], CompareRevision = -1, Flags = KeyValueFlags.Set, Durability = KeyValueDurability.Persistent },
        new() { TransactionId = TransactionId, Key = "kv/b", Value = [2], CompareRevision = -1, Flags = KeyValueFlags.Set, Durability = KeyValueDurability.Ephemeral },
    ];

    private static List<KahunaDeleteKeyValueRequestItem> DeleteItems() =>
    [
        new() { TransactionId = TransactionId, Key = "kv/a", Durability = KeyValueDurability.Persistent },
        new() { TransactionId = TransactionId, Key = "kv/b", Durability = KeyValueDurability.Ephemeral },
    ];

    private static List<(string key, long revision, KeyValueDurability durability)> ReadKeys() =>
        [("kv/a", -1, KeyValueDurability.Persistent), ("kv/b", 3, KeyValueDurability.Ephemeral)];

    private static List<(string key, KeyValueDurability durability)> MutationKeys() =>
        [("kv/a", KeyValueDurability.Persistent), ("kv/b", KeyValueDurability.Ephemeral)];

    private static List<(string key, HLCTimestamp ticketId, KeyValueDurability durability)> TicketKeys() =>
        [("kv/a", CommitId, KeyValueDurability.Persistent), ("kv/b", CommitId, KeyValueDurability.Ephemeral)];

    /// <summary>A node that fails every call made on it, so a case shows a call that ran on the target.</summary>
    public class TargetMustNotRun : DispatchProxy
    {
        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args) =>
            throw new InvalidOperationException($"{targetMethod?.Name} ran on the target node.");
    }

    /// <summary>A backup sink that keeps nothing. No case reaches it: the call fails before a share is captured.</summary>
    private sealed class DiscardingSink : IBackupPartSink
    {
        public ValueTask<HLCTimestamp> ProposeCutAsync(HLCTimestamp minInFlight, HLCTimestamp maxCommitted, CancellationToken ct) => ValueTask.FromResult(maxCommitted);

        public ValueTask BeginFileAsync(string relativePath, CancellationToken ct) => ValueTask.CompletedTask;

        public ValueTask WriteAsync(ReadOnlyMemory<byte> data, CancellationToken ct) => ValueTask.CompletedTask;

        public ValueTask EndFileAsync(CancellationToken ct) => ValueTask.CompletedTask;

        public ValueTask CapturedAsync(BackupManifest manifest, CancellationToken ct) => ValueTask.CompletedTask;
    }
}
