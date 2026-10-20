using Google.Protobuf;
using Kahuna.Shared.Communication.Rest;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace Kahuna.Shared.Communication.Grpc;

/// <summary>
/// Converts watch requests and frames to and from their gRPC and REST shapes. The server encodes with these
/// methods and the client decodes with them, so the two sides cannot disagree on a field.
///
/// <para>An event value is shared, never copied, on the way out: it can be the array the writer sent, and
/// nothing on the path mutates it. A null value is sent as an absent field (gRPC) or a JSON null (REST), so a
/// delete or a keys-only event stays apart from a zero-byte value.</para>
/// </summary>
public static class KeyValueWatchWire
{
    /// <summary>
    /// Builds the scope a gRPC open request names. Returns null when the scope is malformed (an empty value,
    /// or a range whose start is not below its end).
    /// </summary>
    public static KeyValueWatchScope? ScopeFromGrpc(GrpcWatchRequest request) =>
        TryCreateScope(
            (KeyValueWatchScopeKind)request.ScopeKind,
            request.ScopeValue,
            request.HasStartKey ? request.StartKey : null,
            request.HasEndKey ? request.EndKey : null);

    /// <summary>Builds the scope a REST request names, or null when it is malformed.</summary>
    public static KeyValueWatchScope? ScopeFromRest(KahunaWatchRequest request) =>
        TryCreateScope(request.ScopeKind, request.ScopeValue, request.StartKey, request.EndKey);

    private static KeyValueWatchScope? TryCreateScope(KeyValueWatchScopeKind kind, string? value, string? startKey, string? endKey)
    {
        if (string.IsNullOrEmpty(value))
            return null;

        switch (kind)
        {
            case KeyValueWatchScopeKind.Key:
                return KeyValueWatchScope.ForKey(value);

            case KeyValueWatchScopeKind.Prefix:
                return KeyValueWatchScope.ForPrefix(value);

            case KeyValueWatchScopeKind.Range:
                if (startKey is not null && endKey is not null && string.CompareOrdinal(startKey, endKey) >= 0)
                    return null;

                return KeyValueWatchScope.ForRange(value, startKey, endKey);

            default:
                return null;
        }
    }

    /// <summary>The gRPC open request for <paramref name="request"/> under <paramref name="watchId"/>.</summary>
    public static GrpcWatchRequest ToGrpcOpen(long watchId, KeyValueWatchRequest request)
    {
        GrpcWatchRequest open = new()
        {
            Type = GrpcWatchRequestType.WatchRequestOpen,
            WatchId = watchId,
            ScopeKind = (GrpcWatchScopeKind)request.Scope.Kind,
            ScopeValue = request.Scope.Value,
            Durability = (GrpcKeyValueDurability)request.Durability,
            KeysOnly = request.KeysOnly,
            Capacity = request.Capacity
        };

        if (request.Scope.StartKey is not null)
            open.StartKey = request.Scope.StartKey;

        if (request.Scope.EndKey is not null)
            open.EndKey = request.Scope.EndKey;

        if (request.Partitions is not null)
            open.Partitions.AddRange(request.Partitions);

        open.ResumeFromNode = request.ResumeFrom.N;
        open.ResumeFromPhysical = request.ResumeFrom.L;
        open.ResumeFromCounter = request.ResumeFrom.C;

        if (request.ResumePartitions is not null)
        {
            foreach ((int partitionId, HLCTimestamp cursor) in request.ResumePartitions)
                open.ResumePartitions.Add(new GrpcWatchCursor { PartitionId = partitionId, TimestampNode = cursor.N, TimestampPhysical = cursor.L, TimestampCounter = cursor.C });
        }

        return open;
    }

    /// <summary>The request a gRPC open names, over <paramref name="scope"/> (see <see cref="ScopeFromGrpc"/>).</summary>
    public static KeyValueWatchRequest RequestFromGrpc(GrpcWatchRequest request, KeyValueWatchScope scope)
    {
        Dictionary<int, HLCTimestamp>? cursors = null;
        foreach (GrpcWatchCursor cursor in request.ResumePartitions)
            (cursors ??= [])[cursor.PartitionId] = new HLCTimestamp(cursor.TimestampNode, cursor.TimestampPhysical, cursor.TimestampCounter);

        return new KeyValueWatchRequest(
            scope,
            (KeyValueDurability)request.Durability,
            request.KeysOnly,
            request.Capacity,
            request.Partitions.Count > 0 ? [.. request.Partitions] : null,
            new HLCTimestamp(request.ResumeFromNode, request.ResumeFromPhysical, request.ResumeFromCounter),
            cursors);
    }

    /// <summary>The request a REST body names, over <paramref name="scope"/> (see <see cref="ScopeFromRest"/>).</summary>
    public static KeyValueWatchRequest RequestFromRest(KahunaWatchRequest request, KeyValueWatchScope scope)
    {
        Dictionary<int, HLCTimestamp>? cursors = null;
        if (request.ResumePartitions is not null)
        {
            foreach (KahunaWatchCursor cursor in request.ResumePartitions)
                (cursors ??= [])[cursor.PartitionId] = cursor.Timestamp;
        }

        return new KeyValueWatchRequest(scope, request.Durability, request.KeysOnly, request.Capacity, request.Partitions, request.ResumeFrom, cursors);
    }

    /// <summary>The REST body for <paramref name="request"/>.</summary>
    public static KahunaWatchRequest ToRest(KeyValueWatchRequest request) => new()
    {
        ScopeKind = request.Scope.Kind,
        ScopeValue = request.Scope.Value,
        StartKey = request.Scope.StartKey,
        EndKey = request.Scope.EndKey,
        Durability = request.Durability,
        KeysOnly = request.KeysOnly,
        Capacity = request.Capacity,
        Partitions = request.Partitions is null ? null : [.. request.Partitions],
        ResumeFrom = request.ResumeFrom,
        ResumePartitions = request.ResumePartitions?.Select(c => new KahunaWatchCursor { PartitionId = c.Key, Timestamp = c.Value }).ToList()
    };

    public static GrpcWatchFrame ToGrpc(long watchId, KeyValueWatchFrame frame)
    {
        GrpcWatchFrame wire = new()
        {
            WatchId = watchId,
            Type = (GrpcWatchFrameType)frame.Type,
            PartitionId = frame.PartitionId,
            LagReason = (int)frame.LagReason,
            Refusal = (GrpcKeyValueResponseType)frame.Refusal,
            TimestampNode = frame.Timestamp.N,
            TimestampPhysical = frame.Timestamp.L,
            TimestampCounter = frame.Timestamp.C,
            TransactionIdNode = frame.TransactionId.N,
            TransactionIdPhysical = frame.TransactionId.L,
            TransactionIdCounter = frame.TransactionId.C
        };

        if (frame.Event is { } @event)
        {
            wire.Key = @event.Key;
            wire.Kind = (GrpcWatchEventKind)@event.Kind;
            wire.Revision = @event.Revision;
            wire.ExpiresNode = @event.Expires.N;
            wire.ExpiresPhysical = @event.Expires.L;
            wire.ExpiresCounter = @event.Expires.C;
            wire.CommitTimestampNode = @event.CommitTimestamp.N;
            wire.CommitTimestampPhysical = @event.CommitTimestamp.L;
            wire.CommitTimestampCounter = @event.CommitTimestamp.C;
            wire.TransactionIdNode = @event.TransactionId.N;
            wire.TransactionIdPhysical = @event.TransactionId.L;
            wire.TransactionIdCounter = @event.TransactionId.C;

            if (@event.Value is not null)
                wire.Value = UnsafeByteOperations.UnsafeWrap(@event.Value);

            if (@event.Origin is not null)
                wire.Origin = @event.Origin;
        }

        for (int i = 0; i < frame.Partitions.Count; i++)
            wire.Partitions.Add(frame.Partitions[i]);

        for (int i = 0; i < frame.Routes.Count; i++)
        {
            KeyValueWatchRoute route = frame.Routes[i];
            GrpcWatchRoute wireRoute = new() { PartitionId = route.PartitionId, HostedHere = route.HostedHere };
            wireRoute.Endpoints.AddRange(route.Endpoints);
            wire.Routes.Add(wireRoute);
        }

        return wire;
    }

    public static KeyValueWatchFrame FromGrpc(GrpcWatchFrame wire)
    {
        HLCTimestamp timestamp = new(wire.TimestampNode, wire.TimestampPhysical, wire.TimestampCounter);
        HLCTimestamp transactionId = new(wire.TransactionIdNode, wire.TransactionIdPhysical, wire.TransactionIdCounter);

        switch (wire.Type)
        {
            case GrpcWatchFrameType.WatchFrameEvent:
                return KeyValueWatchFrame.ForEvent(new(
                    wire.Key,
                    (KeyValueWatchEventKind)wire.Kind,
                    ByteStringPayload.GetArrayOrNull(wire.HasValue, wire.Value),
                    wire.Revision,
                    new(wire.ExpiresNode, wire.ExpiresPhysical, wire.ExpiresCounter),
                    new(wire.CommitTimestampNode, wire.CommitTimestampPhysical, wire.CommitTimestampCounter),
                    transactionId,
                    wire.PartitionId,
                    wire.HasOrigin ? wire.Origin : null));

            case GrpcWatchFrameType.WatchFrameProgress:
                return KeyValueWatchFrame.ForProgress(wire.PartitionId, timestamp);

            case GrpcWatchFrameType.WatchFrameTransactionEnd:
                return KeyValueWatchFrame.ForTransactionEnd(wire.PartitionId, transactionId);

            case GrpcWatchFrameType.WatchFrameLagged:
                return KeyValueWatchFrame.ForLagged(wire.PartitionId, (KeyValueWatchLagReason)wire.LagReason, timestamp);

            case GrpcWatchFrameType.WatchFrameHistoryUnavailable:
                return KeyValueWatchFrame.ForHistoryUnavailable(wire.PartitionId);

            case GrpcWatchFrameType.WatchFrameOpened:
                return KeyValueWatchFrame.ForOpened([.. wire.Partitions]);

            default:
                List<KeyValueWatchRoute> routes = new(wire.Routes.Count);
                foreach (GrpcWatchRoute route in wire.Routes)
                    routes.Add(new(route.PartitionId, route.HostedHere, [.. route.Endpoints]));

                return KeyValueWatchFrame.ForRefused(wire.PartitionId, (KeyValueResponseType)wire.Refusal, routes);
        }
    }

    public static KahunaWatchFrameResponse ToRest(KeyValueWatchFrame frame)
    {
        KahunaWatchFrameResponse wire = new()
        {
            Frame = frame.Type,
            Type = frame.Type == KeyValueWatchFrameType.Refused ? frame.Refusal : KeyValueResponseType.Get,
            PartitionId = frame.PartitionId,
            Timestamp = frame.Timestamp,
            TransactionId = frame.TransactionId,
            LagReason = frame.LagReason
        };

        if (frame.Event is { } @event)
        {
            wire.Key = @event.Key;
            wire.Kind = @event.Kind;
            wire.Value = @event.Value;
            wire.Revision = @event.Revision;
            wire.Expires = @event.Expires;
            wire.CommitTimestamp = @event.CommitTimestamp;
            wire.TransactionId = @event.TransactionId;
            wire.Origin = @event.Origin;
        }

        if (frame.Partitions.Count > 0)
            wire.Partitions = [.. frame.Partitions];

        if (frame.Routes.Count > 0)
        {
            wire.Routes = new(frame.Routes.Count);
            foreach (KeyValueWatchRoute route in frame.Routes)
                wire.Routes.Add(new() { PartitionId = route.PartitionId, HostedHere = route.HostedHere, Endpoints = [.. route.Endpoints] });
        }

        return wire;
    }

    public static KeyValueWatchFrame FromRest(KahunaWatchFrameResponse wire)
    {
        switch (wire.Frame)
        {
            case KeyValueWatchFrameType.Event:
                return KeyValueWatchFrame.ForEvent(new(
                    wire.Key ?? "",
                    wire.Kind,
                    wire.Value,
                    wire.Revision,
                    wire.Expires,
                    wire.CommitTimestamp,
                    wire.TransactionId,
                    wire.PartitionId,
                    wire.Origin));

            case KeyValueWatchFrameType.Progress:
                return KeyValueWatchFrame.ForProgress(wire.PartitionId, wire.Timestamp);

            case KeyValueWatchFrameType.TransactionEnd:
                return KeyValueWatchFrame.ForTransactionEnd(wire.PartitionId, wire.TransactionId);

            case KeyValueWatchFrameType.Lagged:
                return KeyValueWatchFrame.ForLagged(wire.PartitionId, wire.LagReason, wire.Timestamp);

            case KeyValueWatchFrameType.HistoryUnavailable:
                return KeyValueWatchFrame.ForHistoryUnavailable(wire.PartitionId);

            case KeyValueWatchFrameType.Opened:
                return KeyValueWatchFrame.ForOpened(wire.Partitions ?? []);

            default:
                List<KeyValueWatchRoute> routes = new(wire.Routes?.Count ?? 0);
                if (wire.Routes is not null)
                {
                    foreach (KahunaWatchRouteResponse route in wire.Routes)
                        routes.Add(new(route.PartitionId, route.HostedHere, route.Endpoints ?? []));
                }

                return KeyValueWatchFrame.ForRefused(wire.PartitionId, wire.Type, routes);
        }
    }
}
