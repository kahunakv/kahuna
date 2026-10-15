
using Kahuna.Shared.Communication.Grpc;

namespace Kahuna.Server.KeyValues;

/// <summary>
/// Maps <see cref="KeyValueScanObservation"/> to and from its gRPC message. An absent message (a server
/// that predates it) maps to null, never to a zero observation.
/// </summary>
internal static class ScanObservationWire
{
    public static GrpcScanObservation ToGrpc(in KeyValueScanObservation observation) => new()
    {
        EntriesInspected = observation.EntriesInspected,
        DeadEntriesSkipped = observation.DeadEntriesSkipped,
        RevisionRowsExamined = observation.RevisionRowsExamined,
        RevisionRunSeeks = observation.RevisionRunSeeks,
        IntentsMerged = observation.IntentsMerged,
        PageRetries = observation.PageRetries,
        ServedRemotely = observation.ServedRemotely,
        DiskFallback = observation.DiskFallback
    };

    public static KeyValueScanObservation? FromGrpc(GrpcScanObservation? observation) =>
        observation is null
            ? null
            : new KeyValueScanObservation(
                observation.EntriesInspected,
                observation.DeadEntriesSkipped,
                observation.RevisionRowsExamined,
                observation.RevisionRunSeeks,
                observation.IntentsMerged,
                observation.PageRetries,
                observation.ServedRemotely,
                observation.DiskFallback);
}
