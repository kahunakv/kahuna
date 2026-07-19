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

    private static readonly Task<JoinResponse> JoinResponse = Task.FromResult(new JoinResponse(false));

    private static readonly Task<LeaveResponse> LeaveResponse = Task.FromResult(new LeaveResponse(false));

    private static readonly Task<GossipPingResponse> GossipPingResponseAlive = Task.FromResult(new GossipPingResponse(true, 0));

    private static readonly Task<GossipPingReqResponse> GossipPingReqResponseReached = Task.FromResult(new GossipPingReqResponse(true));

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

    public Task<GossipPingResponse> SendPing(RaftManager manager, RaftNode node, GossipPingRequest request, CancellationToken cancellationToken = default)
    {
        return GossipPingResponseAlive;
    }

    public Task<GossipPingReqResponse> SendPingReq(RaftManager manager, RaftNode node, GossipPingReqRequest request, CancellationToken cancellationToken = default)
    {
        return GossipPingReqResponseReached;
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

    private static void CompleteAppendLogs(RaftManager manager, RaftNode node, AppendLogsRequest request)
    {
        long commitIndex = request.Logs is null || request.Logs.Count == 0 ? 0 : request.Logs.Max(log => log.Id);

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
