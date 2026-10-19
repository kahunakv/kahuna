using System.Text.Json;
using Google.Protobuf;
using Grpc.Core;
using Kommander.Time;

using Kahuna.Server.Persistence.Pitr;

namespace Kahuna.Server.Communication.Internode.Grpc;

/// <summary>
/// The inter-node encoding of a backup share's request and of the stream that answers it, shared by the node
/// that composes the backup and the node that captures the share. Manifests travel as the same JSON document
/// the catalog stores, so a share's manifest arrives with every field the composition checks.
/// </summary>
internal static class BackupPartWire
{
    public static GrpcCaptureBackupPartRequest ToGrpc(BackupPartRequest request)
    {
        GrpcCaptureBackupPartRequest message = new()
        {
            Kind = (int)request.Kind,
            ExpectedTopologyGeneration = request.ExpectedTopologyGeneration
        };

        message.Partitions.AddRange(request.Partitions);

        foreach (BackupManifest ancestor in request.ParentChain)
            message.ParentChainJson.Add(JsonSerializer.Serialize(ancestor, PitrIndentedJsonContext.Default.BackupManifest));

        return message;
    }

    public static BackupPartRequest FromGrpc(GrpcCaptureBackupPartRequest message)
    {
        List<BackupManifest> chain = new(message.ParentChainJson.Count);
        foreach (string json in message.ParentChainJson)
            chain.Add(DeserializeManifest(json));

        return new BackupPartRequest(
            (BackupPartKind)message.Kind,
            [.. message.Partitions],
            message.ExpectedTopologyGeneration,
            chain);
    }

    public static GrpcBackupPartResult ToGrpc(BackupPartResult result)
    {
        GrpcBackupPartResult message = new()
        {
            Outcome = (int)result.Outcome,
            Message = result.Message ?? "",
            ManifestJson = result.Manifest is null ? "" : JsonSerializer.Serialize(result.Manifest, PitrIndentedJsonContext.Default.BackupManifest)
        };
        message.LaggingPartitions.AddRange(result.LaggingPartitions);
        return message;
    }

    public static BackupPartResult FromGrpc(GrpcBackupPartResult message) => new(
        (BackupPartOutcome)message.Outcome,
        string.IsNullOrEmpty(message.Message) ? null : message.Message,
        string.IsNullOrEmpty(message.ManifestJson) ? null : DeserializeManifest(message.ManifestJson))
    {
        LaggingPartitions = [.. message.LaggingPartitions]
    };

    private static GrpcBackupHlc ToGrpc(HLCTimestamp time) => new() { Node = time.N, Physical = time.L, Counter = time.C };

    private static HLCTimestamp FromGrpc(GrpcBackupHlc? time) =>
        time is null ? HLCTimestamp.Zero : new HLCTimestamp(time.Node, time.Physical, time.Counter);

    private static BackupManifest DeserializeManifest(string json) =>
        JsonSerializer.Deserialize(json, PitrIndentedJsonContext.Default.BackupManifest)
        ?? throw new BackupArtifactException("A backup share carried an empty manifest.");

    /// <summary>
    /// Replays a share's chunk stream into the composing node's sink, and returns the result that ends it. When
    /// the share reports its capture, the sink's barrier is awaited and Proceed is sent back on
    /// <paramref name="control"/>. A stream that ends without a result was cut off, which is a transport failure,
    /// not a refusal.
    /// </summary>
    public static async Task<BackupPartResult> ReceiveAsync(
        IAsyncStreamReader<GrpcBackupPartChunk> stream, IClientStreamWriter<GrpcBackupPartControl>? control,
        IBackupPartSink sink, string node, CancellationToken ct)
    {
        while (await stream.MoveNext(ct).ConfigureAwait(false))
        {
            GrpcBackupPartChunk chunk = stream.Current;
            switch (chunk.BodyCase)
            {
                case GrpcBackupPartChunk.BodyOneofCase.CutProposal:
                    HLCTimestamp cut = await sink.ProposeCutAsync(
                        FromGrpc(chunk.CutProposal.MinInFlight), FromGrpc(chunk.CutProposal.MaxCommitted), ct).ConfigureAwait(false);
                    if (control is not null)
                        await control.WriteAsync(new GrpcBackupPartControl { Cut = ToGrpc(cut) }).ConfigureAwait(false);
                    break;

                case GrpcBackupPartChunk.BodyOneofCase.CapturedManifestJson:
                    await sink.CapturedAsync(DeserializeManifest(chunk.CapturedManifestJson), ct).ConfigureAwait(false);
                    if (control is not null)
                        await control.WriteAsync(new GrpcBackupPartControl { Proceed = true }).ConfigureAwait(false);
                    break;

                case GrpcBackupPartChunk.BodyOneofCase.BeginFile:
                    await sink.BeginFileAsync(chunk.BeginFile, ct).ConfigureAwait(false);
                    break;

                case GrpcBackupPartChunk.BodyOneofCase.Data:
                    await sink.WriteAsync(chunk.Data.Memory, ct).ConfigureAwait(false);
                    break;

                case GrpcBackupPartChunk.BodyOneofCase.EndFile:
                    await sink.EndFileAsync(ct).ConfigureAwait(false);
                    break;

                case GrpcBackupPartChunk.BodyOneofCase.Result:
                    return FromGrpc(chunk.Result);
            }
        }

        throw new IOException($"The backup share stream from node {node} ended before its result.");
    }

    /// <summary>
    /// The capturing node's side: every sink call becomes one message of the response stream; a proposed cut
    /// waits for the agreed Cut, and a reported capture for Proceed, on <paramref name="control"/>.
    /// </summary>
    public sealed class StreamingSink(
        IServerStreamWriter<GrpcBackupPartChunk> stream, IAsyncStreamReader<GrpcBackupPartControl> control) : IBackupPartSink
    {
        public async ValueTask<HLCTimestamp> ProposeCutAsync(HLCTimestamp minInFlight, HLCTimestamp maxCommitted, CancellationToken ct)
        {
            await stream.WriteAsync(new GrpcBackupPartChunk
            {
                CutProposal = new GrpcBackupCutProposal { MinInFlight = ToGrpc(minInFlight), MaxCommitted = ToGrpc(maxCommitted) }
            }).ConfigureAwait(false);

            if (!await control.MoveNext(ct).ConfigureAwait(false) || control.Current.BodyCase != GrpcBackupPartControl.BodyOneofCase.Cut)
                throw new OperationCanceledException("The composing node abandoned the backup before the shares agreed on a cut.");

            return FromGrpc(control.Current.Cut);
        }

        public async ValueTask CapturedAsync(BackupManifest manifest, CancellationToken ct)
        {
            await stream.WriteAsync(new GrpcBackupPartChunk
            {
                CapturedManifestJson = JsonSerializer.Serialize(manifest, PitrIndentedJsonContext.Default.BackupManifest)
            }).ConfigureAwait(false);

            // The composing node abandons the backup by closing its side; the share then must not continue.
            if (!await control.MoveNext(ct).ConfigureAwait(false) || control.Current.BodyCase != GrpcBackupPartControl.BodyOneofCase.Proceed)
                throw new OperationCanceledException("The composing node abandoned the backup before every share was captured.");
        }

        public async ValueTask BeginFileAsync(string relativePath, CancellationToken ct) =>
            await stream.WriteAsync(new GrpcBackupPartChunk { BeginFile = relativePath }).ConfigureAwait(false);

        public async ValueTask WriteAsync(ReadOnlyMemory<byte> data, CancellationToken ct) =>
            await stream.WriteAsync(new GrpcBackupPartChunk { Data = ByteString.CopyFrom(data.Span) }).ConfigureAwait(false);

        public async ValueTask EndFileAsync(CancellationToken ct) =>
            await stream.WriteAsync(new GrpcBackupPartChunk { EndFile = true }).ConfigureAwait(false);
    }
}
