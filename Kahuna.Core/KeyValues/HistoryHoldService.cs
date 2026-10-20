using Kommander;

using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues;

/// <summary>
/// The history hold surface: routes each request to the meta-partition leader, which alone commits registry
/// mutations, and answers it there. A list is read from the leader's registry under a read-index confirmation, never
/// from local belief, because a node that only thinks it leads may not have applied the latest mutations.
/// </summary>
internal sealed class HistoryHoldService
{
    private readonly KeyValuesRuntime runtime;

    internal HistoryHoldService(KeyValuesRuntime runtime) => this.runtime = runtime;

    private IRaft raft => runtime.Raft;

    private HistoryHoldStore store => runtime.HistoryHoldStore;

    public async Task<HistoryHoldAnswer> Handle(HistoryHoldRequest request, CancellationToken ct)
    {
        if (!raft.Joined)
            return HistoryHoldAnswer.Refused(KeyValueResponseType.MustRetry);

        if (await raft.AmILeaderIfHosted(RangeMapStore.MetaPartitionId, ct).ConfigureAwait(false))
            return await HandleLocally(request, ct).ConfigureAwait(false);

        // A forwarded request that reached a node which does not lead either: the two nodes disagree on the leader,
        // and a further hop could bounce it. The caller retries against a fresh leader view.
        if (ForwardedRequestScope.IsActive)
            return HistoryHoldAnswer.Refused(KeyValueResponseType.MustRetry);

        string? leader = await raft.TryResolveLeader(RangeMapStore.MetaPartitionId, ct).ConfigureAwait(false);
        if (leader is null)
            return HistoryHoldAnswer.Refused(KeyValueResponseType.MustRetry);

        if (leader == raft.GetLocalEndpoint())
            return await HandleLocally(request, ct).ConfigureAwait(false);

        return await runtime.InterNodeCommunication.HistoryHold(leader, request, ct).ConfigureAwait(false);
    }

    private async Task<HistoryHoldAnswer> HandleLocally(HistoryHoldRequest request, CancellationToken ct)
    {
        switch (request.Operation)
        {
            case HistoryHoldOperation.Acquire:
                return await store.AcquireAsync(request.HolderId ?? string.Empty, request.Partitions ?? [], request.Timestamp, request.LeaseMs, ct).ConfigureAwait(false);

            case HistoryHoldOperation.Renew:
                return await store.RenewAsync(request.HoldIds ?? [], request.LeaseMs, request.Timestamp, ct).ConfigureAwait(false);

            case HistoryHoldOperation.Release:
                return await store.ReleaseAsync(request.HoldIds ?? [], ct).ConfigureAwait(false);

            case HistoryHoldOperation.List:
                if (!await raft.ConfirmLeadershipIfHosted(RangeMapStore.MetaPartitionId, ct).ConfigureAwait(false))
                    return HistoryHoldAnswer.Refused(KeyValueResponseType.MustRetry);

                return new HistoryHoldAnswer(KeyValueResponseType.Get, store.List(request.HolderId));

            default:
                return HistoryHoldAnswer.Refused(KeyValueResponseType.InvalidInput);
        }
    }
}
