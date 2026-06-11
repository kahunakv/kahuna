using Kommander;
using Kommander.Communication;
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

    private static readonly Task<AppendLogsBatchResponse> AppendLogsBatchResponse = Task.FromResult(new AppendLogsBatchResponse());

    private static readonly Task<CompleteAppendLogsResponse> CompleteAppendLogsResponse = Task.FromResult(new CompleteAppendLogsResponse());

    private static readonly Task<CompleteAppendLogsBatchResponse> CompleteAppendLogsBatchResponse = Task.FromResult(new CompleteAppendLogsBatchResponse());

    private static readonly Task<BatchRequestsResponse> BatchRequestsResponse = Task.FromResult(new BatchRequestsResponse());

    public Task<HandshakeResponse> Handshake(RaftManager manager, RaftNode node, HandshakeRequest request)
    {
        return HandshakeResponse;
    }

    public Task<RequestVotesResponse> RequestVotes(RaftManager manager, RaftNode node, RequestVotesRequest request)
    {
        manager.Vote(new(request.Partition, request.Term, request.MaxLogId, request.Time, node.Endpoint, preVote: request.PreVote));
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

    public Task<AppendLogsBatchResponse> AppendLogsBatch(RaftManager manager, RaftNode node, AppendLogsBatchRequest request)
    {
        if (request.AppendLogs is not null)
        {
            foreach (AppendLogsRequest appendLogsRequest in request.AppendLogs)
                CompleteAppendLogs(manager, node, appendLogsRequest);
        }

        return AppendLogsBatchResponse;
    }

    public Task<CompleteAppendLogsResponse> CompleteAppendLogs(RaftManager manager, RaftNode node, CompleteAppendLogsRequest request)
    {
        return CompleteAppendLogsResponse;
    }

    public Task<CompleteAppendLogsBatchResponse> CompleteAppendLogsBatch(RaftManager manager, RaftNode node, CompleteAppendLogsBatchRequest request)
    {
        return CompleteAppendLogsBatchResponse;
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
