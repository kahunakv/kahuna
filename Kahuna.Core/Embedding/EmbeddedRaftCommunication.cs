using Kommander;
using Kommander.Communication;
using GossipPingRequest = Kommander.Gossip.PingRequest;
using GossipPingResponse = Kommander.Gossip.PingResponse;
using GossipPingReqRequest = Kommander.Gossip.PingReqRequest;
using GossipPingReqResponse = Kommander.Gossip.PingReqResponse;
using Kommander.Data;

namespace Kahuna;

internal sealed class EmbeddedRaftCommunication : ICommunication
{
    /// <summary>
    /// The two phantom peers that grant a lone embedded node a ceremonial majority so it can elect
    /// itself leader. They hold no state, run no coordinator, and discard every entry they are
    /// sent, so no read is ever served from one and no commit depends on one catching up.
    /// </summary>
    public static readonly List<RaftNode> Witnesses =
    [
        new("embedded-witness-1:0"),
        new("embedded-witness-2:0")
    ];

    private static readonly Task<HandshakeResponse> HandshakeResponse = Task.FromResult(new HandshakeResponse());

    private static readonly Task<RequestVotesResponse> RequestVotesResponse = Task.FromResult(new RequestVotesResponse());

    private static readonly Task<VoteResponse> VoteResponse = Task.FromResult(new VoteResponse());

    private static readonly Task<AppendLogsResponse> AppendLogsResponse = Task.FromResult(new AppendLogsResponse());

    private static readonly Task<CompleteAppendLogsResponse> CompleteAppendLogsResponse = Task.FromResult(new CompleteAppendLogsResponse());

    private static readonly Task<BatchRequestsResponse> BatchRequestsResponse = Task.FromResult(new BatchRequestsResponse());

    private static readonly Task<SnapshotResponse> SnapshotRefused = Task.FromResult(new SnapshotResponse(false));

    private static readonly Task<JoinResponse> JoinResponse = Task.FromResult(new JoinResponse(false));

    private static readonly Task<LeaveResponse> LeaveResponse = Task.FromResult(new LeaveResponse(false));

    private static readonly Task<GossipPingResponse> GossipPingResponseAlive = Task.FromResult(new GossipPingResponse(true, 0));

    private static readonly Task<GossipPingReqResponse> GossipPingReqResponseReached = Task.FromResult(new GossipPingReqResponse(true));

    // An embedded node's only peers are the two synthetic witnesses, which run no coordinator, so
    // no remote node can ever commit a roster role transition on its behalf. Refusing as
    // "member not found" rather than reporting success keeps a decommission drain from believing
    // it started: an embedded node has no survivor to evacuate its replicas onto anyway.
    private static readonly Task<SetMemberRoleResponse> SetMemberRoleRefused =
        Task.FromResult(new SetMemberRoleResponse(false, null, RaftOperationStatus.MemberNotFound));

    public Task<HandshakeResponse> Handshake(RaftManager manager, RaftNode node, HandshakeRequest request)
    {
        return HandshakeResponse;
    }

    public Task<RequestVotesResponse> RequestVotes(RaftManager manager, RaftNode node, RequestVotesRequest request)
    {
        manager.Vote(new(request.Partition, request.Term, request.MaxLogId, request.LastLogTerm, request.Time, node.Endpoint, preVote: request.PreVote));
        return RequestVotesResponse;
    }

    public Task<VoteResponse> Vote(RaftManager manager, RaftNode node, VoteRequest request)
    {
        return VoteResponse;
    }

    public Task<AppendLogsResponse> AppendLogs(RaftManager manager, RaftNode node, AppendLogsRequest request)
    {
        CompleteAppendLogs(manager, node, request);
        return AppendLogsResponse;
    }

    public Task<CompleteAppendLogsResponse> CompleteAppendLogs(RaftManager manager, RaftNode node, CompleteAppendLogsRequest request)
    {
        return CompleteAppendLogsResponse;
    }

    public Task<JoinResponse> SendJoin(RaftManager manager, RaftNode node, JoinRequest request)
    {
        return JoinResponse;
    }

    public Task<LeaveResponse> SendLeave(RaftManager manager, RaftNode node, LeaveRequest request, CancellationToken cancellationToken = default)
    {
        return LeaveResponse;
    }

    public Task<SetMemberRoleResponse> SendSetMemberRole(RaftManager manager, RaftNode node, SetMemberRoleRequest request, CancellationToken cancellationToken = default)
    {
        return SetMemberRoleRefused;
    }

    public Task<GossipPingResponse> SendPing(RaftManager manager, RaftNode node, GossipPingRequest request, CancellationToken cancellationToken = default)
    {
        return GossipPingResponseAlive;
    }

    public Task<GossipPingReqResponse> SendPingReq(RaftManager manager, RaftNode node, GossipPingReqRequest request, CancellationToken cancellationToken = default)
    {
        return GossipPingReqResponseReached;
    }

    /// <summary>
    /// Refuses a snapshot install for a witness. Stated explicitly rather than inherited from the
    /// interface default, because the two mean different things: the default reads as "this
    /// transport has not implemented the RPC yet", while here installing a snapshot on a peer that
    /// stores nothing is meaningless by construction. The leader must never get this far — it asks
    /// for a snapshot only after a catch-up batch is refused, and catch-up is off for a
    /// witness-only quorum — so a refusal reaching an operator's snapshot statuses is a signal that
    /// something is trying to make a witness carry state.
    /// </summary>
    public Task<SnapshotResponse> SendInstallSnapshot(RaftManager manager, RaftNode node, SnapshotRequest request, CancellationToken cancellationToken = default)
    {
        return SnapshotRefused;
    }

    public Task<BatchRequestsResponse> BatchRequests(RaftManager manager, RaftNode node, BatchRequestsRequest request)
    {
        if (request.Requests is null)
            return BatchRequestsResponse;

        foreach (BatchRequestsRequestItem item in request.Requests)
        {
            switch (item.Type)
            {
                case BatchRequestsRequestType.RequestVote when item.RequestVotes is not null:
                    RequestVotes(manager, node, item.RequestVotes);
                    break;

                case BatchRequestsRequestType.AppendLogs when item.AppendLogs is not null:
                    AppendLogs(manager, node, item.AppendLogs);
                    break;
            }
        }

        return BatchRequestsResponse;
    }

    /// <summary>
    /// Acknowledges an AppendLogs on behalf of a witness. A witness persists nothing and can never
    /// serve a read, so the only coherent frontier to report for it is the leader's own: by
    /// construction it is never behind, which is the whole point of the ceremonial quorum.
    /// </summary>
    /// <remarks>
    /// Deriving the frontier from the payload alone made an <b>empty</b> heartbeat — the leader's
    /// steady-state message to every peer — ACK with 0. A follower's self-reported frontier is
    /// recorded last-writer-wins (a genuine crash-restart regression must be able to lower it), so
    /// the witnesses were pinned at 0 forever and the leader read a whole-log gap on every
    /// heartbeat: a bounded WAL range read per partition whose result is discarded, an anchored
    /// batch refused as non-contiguous once compaction moved the readable floor above the anchor,
    /// and — because the refusal routes to the snapshot fallback — a real partition-state export
    /// streamed into a transport that rejects it, retried forever.
    /// <para>
    /// The leader's frontier is read straight from the partition's in-memory commit counter, with no
    /// lock and no scheduler round-trip. That matters here: this runs synchronously inside the
    /// leader's own send path, so anything that blocked or round-tripped a mailbox would deadlock
    /// the hottest Raft path against itself.
    /// </para>
    /// <para>
    /// Kommander holds the system partition apart from its per-partition map, so the lookup answers
    /// -1 for it. That is the "no report" sentinel: it seeds a peer that has never acked, and never
    /// erases a frontier already recorded from an entry-carrying ACK.
    /// </para>
    /// </remarks>
    private static void CompleteAppendLogs(RaftManager manager, RaftNode node, AppendLogsRequest request)
    {
        long commitIndex = request.Logs is { Count: > 0 } logs
            ? logs.Max(log => log.Id)
            : manager.GetCommitIndex(request.Partition);

        manager.CompleteAppendLogs(new(
            request.Partition,
            request.Term,
            request.Time,
            node.Endpoint,
            RaftOperationStatus.Success,
            commitIndex
        ));
    }
}
