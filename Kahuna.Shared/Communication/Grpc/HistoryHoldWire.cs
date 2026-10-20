using Kahuna.Shared.Communication.Rest;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace Kahuna.Shared.Communication.Grpc;

/// <summary>
/// Converts history hold requests and answers to and from their gRPC and REST shapes. The server, the node that
/// forwards to the meta-partition leader, and the client all go through these methods, so they cannot disagree on a
/// field.
/// </summary>
public static class HistoryHoldWire
{
    public static GrpcHistoryHoldRequest ToGrpc(HistoryHoldRequest request)
    {
        GrpcHistoryHoldRequest wire = new()
        {
            Operation = (int)request.Operation,
            TimestampNode = request.Timestamp.N,
            TimestampPhysical = request.Timestamp.L,
            TimestampCounter = request.Timestamp.C,
            LeaseMs = request.LeaseMs
        };

        if (request.HolderId is not null)
            wire.HolderId = request.HolderId;

        if (request.Partitions is not null)
            wire.Partitions.AddRange(request.Partitions);

        if (request.HoldIds is not null)
            wire.HoldIds.AddRange(request.HoldIds);

        return wire;
    }

    public static HistoryHoldRequest FromGrpc(GrpcHistoryHoldRequest wire) => new(
        (HistoryHoldOperation)wire.Operation,
        wire.HasHolderId ? wire.HolderId : null,
        wire.Partitions.Count > 0 ? [.. wire.Partitions] : null,
        new HLCTimestamp(wire.TimestampNode, wire.TimestampPhysical, wire.TimestampCounter),
        wire.LeaseMs,
        wire.HoldIds.Count > 0 ? [.. wire.HoldIds] : null);

    public static GrpcHistoryHoldResponse ToGrpc(HistoryHoldAnswer answer)
    {
        GrpcHistoryHoldResponse wire = new() { Type = (GrpcKeyValueResponseType)answer.Type };

        foreach (HistoryHoldInfo hold in answer.Holds)
        {
            wire.Holds.Add(new GrpcHistoryHoldInfo
            {
                HoldId = hold.HoldId,
                HolderId = hold.HolderId,
                PartitionId = hold.PartitionId,
                TimestampNode = hold.Timestamp.N,
                TimestampPhysical = hold.Timestamp.L,
                TimestampCounter = hold.Timestamp.C,
                LeaseExpiryNode = hold.LeaseExpiry.N,
                LeaseExpiryPhysical = hold.LeaseExpiry.L,
                LeaseExpiryCounter = hold.LeaseExpiry.C
            });
        }

        return wire;
    }

    public static HistoryHoldAnswer FromGrpc(GrpcHistoryHoldResponse wire)
    {
        List<HistoryHoldInfo> holds = new(wire.Holds.Count);

        foreach (GrpcHistoryHoldInfo hold in wire.Holds)
        {
            holds.Add(new HistoryHoldInfo(
                hold.HoldId,
                hold.HolderId,
                hold.PartitionId,
                new HLCTimestamp(hold.TimestampNode, hold.TimestampPhysical, hold.TimestampCounter),
                new HLCTimestamp(hold.LeaseExpiryNode, hold.LeaseExpiryPhysical, hold.LeaseExpiryCounter)));
        }

        return new HistoryHoldAnswer((KeyValueResponseType)wire.Type, holds);
    }

    public static KahunaHistoryHoldRequest ToRest(HistoryHoldRequest request) => new()
    {
        Operation = request.Operation,
        HolderId = request.HolderId,
        Partitions = request.Partitions is null ? null : [.. request.Partitions],
        Timestamp = request.Timestamp,
        LeaseMs = request.LeaseMs,
        HoldIds = request.HoldIds is null ? null : [.. request.HoldIds]
    };

    public static HistoryHoldRequest FromRest(KahunaHistoryHoldRequest wire) => new(
        wire.Operation,
        wire.HolderId,
        wire.Partitions,
        wire.Timestamp,
        wire.LeaseMs,
        wire.HoldIds);

    public static KahunaHistoryHoldResponse ToRest(HistoryHoldAnswer answer) => new()
    {
        Type = answer.Type,
        Holds = answer.Holds.Select(h => new KahunaHistoryHoldInfoResponse
        {
            HoldId = h.HoldId,
            HolderId = h.HolderId,
            PartitionId = h.PartitionId,
            Timestamp = h.Timestamp,
            LeaseExpiry = h.LeaseExpiry
        }).ToList()
    };

    public static HistoryHoldAnswer FromRest(KahunaHistoryHoldResponse wire) => new(
        wire.Type,
        wire.Holds?.Select(h => new HistoryHoldInfo(h.HoldId, h.HolderId, h.PartitionId, h.Timestamp, h.LeaseExpiry)).ToList() ?? []);
}
