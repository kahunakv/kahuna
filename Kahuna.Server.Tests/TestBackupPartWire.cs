using System.Text;
using Google.Protobuf;
using Grpc.Core;
using Kahuna.Communication.External.Grpc;
using Kahuna.Server.Communication.Internode.Grpc;
using Kahuna.Server.Persistence.Pitr;
using Kommander.Time;

namespace Kahuna.Server.Tests;

/// <summary>
/// A backup share crosses the gRPC transport as a server stream: the capturing node's <see cref="BackupsService"/>
/// turns each sink call into a chunk and ends with the result, and the composing node replays the chunks into its
/// own sink. These cases run both halves over wire-round-tripped messages, so a field the codec drops or a chunk
/// the replay mishandles fails here and not only on a real cluster.
/// </summary>
public sealed class TestBackupPartWire
{
    // A physical component above int.MaxValue and a large counter, so a narrowed or transposed field shows.
    private static readonly HLCTimestamp SampleTs = new(3, 5_000_000_000L, 4_000_000_123u);

    private static T WireRoundTrip<T>(T message, MessageParser<T> parser) where T : IMessage<T> =>
        parser.ParseFrom(message.ToByteArray());

    private static BackupManifest SampleManifest()
    {
        BackupManifest manifest = BackupManifest.CreateFull([PartitionBackupRange.Create(2, 1, default, 7, SampleTs, 4)]);
        manifest.ClusterPartitions = [1, 2, 3];
        manifest.CoveredPartitions = [2];
        manifest.TopologyGeneration = -8_123_456_789_012L;
        manifest.Checksums = new() { ["checkpoint/a.sst"] = "aa", ["intent_frontier.pb"] = "bb" };
        manifest.Sizes = new() { ["checkpoint/a.sst"] = 3, ["intent_frontier.pb"] = 0 };
        manifest.SetBaseCut(SampleTs);
        manifest.SetClusterSnapshotTime(SampleTs);
        return manifest;
    }

    [Fact]
    public void Request_SurvivesTheWire_WithItsParentChain()
    {
        BackupManifest parent = SampleManifest();
        BackupPartRequest request = new(BackupPartKind.Incremental, [2, 5], -42, [parent]);

        BackupPartRequest decoded = BackupPartWire.FromGrpc(
            WireRoundTrip(BackupPartWire.ToGrpc(request), GrpcCaptureBackupPartRequest.Parser));

        Assert.Equal(BackupPartKind.Incremental, decoded.Kind);
        Assert.Equal([2, 5], decoded.Partitions);
        Assert.Equal(-42, decoded.ExpectedTopologyGeneration);
        BackupManifest chainEntry = Assert.Single(decoded.ParentChain);
        Assert.Equal(parent.BackupId, chainEntry.BackupId);
        Assert.Equal(parent.CoveredPartitions, chainEntry.CoveredPartitions);
        Assert.Equal(parent.TopologyGeneration, chainEntry.TopologyGeneration);
    }

    [Fact]
    public async Task Share_StreamsEveryArtifactAndTheResult_AndTheReplayRebuildsThem()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        BackupManifest manifest = SampleManifest();
        byte[] big = new byte[700_000];
        Random.Shared.NextBytes(big);

        SharingKahuna capturing = new(manifest, [("checkpoint/a.sst", Encoding.UTF8.GetBytes("abc")), ("checkpoint/big.sst", big), ("intent_frontier.pb", [])]);
        RecordingStreamWriter written = new();

        await new BackupsService(capturing).CaptureBackupPart(
            Controls(new BackupPartRequest(BackupPartKind.Full, [2], 7, []), cut: AgreedTs, proceed: true), written, new StubServerCallContext());

        // The share proposed, then captured at the cut the composing node sent back.
        Assert.Equal(AgreedTs, capturing.AgreedCut);
        Assert.True(capturing.Proceeded);

        using RecordingSink replayed = new();
        RecordingControlWriter control = new();
        BackupPartResult result = await BackupPartWire.ReceiveAsync(new ReplayStreamReader(written.Chunks), control, replayed, "node-x", ct);

        // The proposal reached the composing node's sink first, the capture report after every artifact, and the
        // agreed cut and then Proceed went back.
        Assert.Equal((SampleTs, MaxCommittedTs), replayed.Proposal);
        Assert.Equal(manifest.BackupId, replayed.Captured!.BackupId);
        Assert.Equal(3, replayed.FilesAtCapture);
        Assert.Equal(
            [GrpcBackupPartControl.BodyOneofCase.Cut, GrpcBackupPartControl.BodyOneofCase.Proceed],
            control.Sent.Select(c => c.BodyCase));
        Assert.Equal(RecordingSink.AgreedCut, new HLCTimestamp(control.Sent[0].Cut.Node, control.Sent[0].Cut.Physical, control.Sent[0].Cut.Counter));

        Assert.Equal(BackupPartOutcome.Ok, result.Outcome);
        Assert.Equal(manifest.BackupId, result.Manifest!.BackupId);
        Assert.Equal(manifest.Checksums, result.Manifest.Checksums);
        Assert.Equal(SampleTs, result.Manifest.BaseCut);
        Assert.Equal(["checkpoint/a.sst", "checkpoint/big.sst", "intent_frontier.pb"], replayed.Files.Keys.Order());
        Assert.Equal("abc", Encoding.UTF8.GetString(replayed.Files["checkpoint/a.sst"]));
        Assert.Equal(big, replayed.Files["checkpoint/big.sst"]);
        Assert.Empty(replayed.Files["intent_frontier.pb"]);
    }

    [Fact]
    public async Task Refusal_CarriesItsOutcomeAndMessage()
    {
        SharingKahuna refusing = new(null, [], BackupPartResult.Refused(BackupPartOutcome.CutUnverified, "in flight"));
        RecordingStreamWriter written = new();

        await new BackupsService(refusing).CaptureBackupPart(
            Controls(new BackupPartRequest(BackupPartKind.Full, [1], 7, []), cut: null, proceed: false), written, new StubServerCallContext());

        BackupPartResult result = await BackupPartWire.ReceiveAsync(
            new ReplayStreamReader(written.Chunks), new RecordingControlWriter(), new RecordingSink(), "node-x", TestContext.Current.CancellationToken);

        Assert.Equal(BackupPartOutcome.CutUnverified, result.Outcome);
        Assert.Equal("in flight", result.Message);
        Assert.Null(result.Manifest);
    }

    [Fact]
    public async Task StreamCutOffBeforeItsResult_IsATransportFailure()
    {
        List<GrpcBackupPartChunk> chunks = [new() { BeginFile = "checkpoint/a.sst" }, new() { Data = ByteString.CopyFromUtf8("ab") }];

        IOException ex = await Assert.ThrowsAsync<IOException>(() => BackupPartWire.ReceiveAsync(
            new ReplayStreamReader(chunks), new RecordingControlWriter(), new RecordingSink(), "node-x", TestContext.Current.CancellationToken));
        Assert.Contains("node-x", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ComposingNodeThatClosesBeforeProceed_AbandonsTheShare()
    {
        BackupManifest manifest = SampleManifest();
        SharingKahuna capturing = new(manifest, [("checkpoint/a.sst", [1, 2, 3])]);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => new BackupsService(capturing).CaptureBackupPart(
            Controls(new BackupPartRequest(BackupPartKind.Full, [2], 7, []), cut: AgreedTs, proceed: false),
            new RecordingStreamWriter(), new StubServerCallContext()));
        Assert.Equal(AgreedTs, capturing.AgreedCut);
        Assert.False(capturing.Proceeded);
    }

    [Fact]
    public async Task ComposingNodeThatClosesBeforeTheCut_AbandonsTheShareBeforeItCaptures()
    {
        SharingKahuna capturing = new(SampleManifest(), [("checkpoint/a.sst", [1, 2, 3])]);
        RecordingStreamWriter written = new();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => new BackupsService(capturing).CaptureBackupPart(
            Controls(new BackupPartRequest(BackupPartKind.Full, [2], 7, []), cut: null, proceed: false),
            written, new StubServerCallContext()));
        Assert.Null(capturing.AgreedCut);
        Assert.Equal([GrpcBackupPartChunk.BodyOneofCase.CutProposal], written.Chunks.Select(c => c.BodyCase));
    }

    // What the fake share proposes, and what the composing node answers.
    private static readonly HLCTimestamp MaxCommittedTs = new(2, 5_000_000_100L, 9);

    private static readonly HLCTimestamp AgreedTs = new(1, 5_000_000_050L, 7);

    private static ReplayStreamReader<GrpcBackupPartControl> Controls(BackupPartRequest request, HLCTimestamp? cut, bool proceed)
    {
        List<GrpcBackupPartControl> controls = [new() { Request = BackupPartWire.ToGrpc(request) }];
        if (cut is { } agreed)
            controls.Add(new GrpcBackupPartControl { Cut = new GrpcBackupHlc { Node = agreed.N, Physical = agreed.L, Counter = agreed.C } });
        if (proceed)
            controls.Add(new GrpcBackupPartControl { Proceed = true });
        return new ReplayStreamReader<GrpcBackupPartControl>([.. controls.Select(c => WireRoundTrip(c, GrpcBackupPartControl.Parser))]);
    }

    private sealed class SharingKahuna(BackupManifest? manifest, (string Path, byte[] Bytes)[] files, BackupPartResult? refusal = null)
        : FakeKahunaBase
    {
        public BackupPartRequest? Received { get; private set; }

        public bool Proceeded { get; private set; }

        public HLCTimestamp? AgreedCut { get; private set; }

        public override async Task<BackupPartResult> CaptureBackupPartAsync(
            BackupPartRequest request, IBackupPartSink sink, CancellationToken ct = default)
        {
            Received = request;
            if (refusal is not null)
                return refusal;

            AgreedCut = await sink.ProposeCutAsync(SampleTs, MaxCommittedTs, ct);

            foreach ((string path, byte[] bytes) in files)
            {
                await sink.BeginFileAsync(path, ct);
                // Two writes per file, so the replay must append, not replace.
                int half = bytes.Length / 2;
                await sink.WriteAsync(bytes.AsMemory(0, half), ct);
                await sink.WriteAsync(bytes.AsMemory(half), ct);
                await sink.EndFileAsync(ct);
            }

            await sink.CapturedAsync(manifest!, ct);
            Proceeded = true;

            return new BackupPartResult(BackupPartOutcome.Ok, null, manifest);
        }
    }

    private sealed class RecordingSink : IBackupPartSink, IDisposable
    {
        private string? current;
        private MemoryStream? buffer;

        public Dictionary<string, byte[]> Files { get; } = new(StringComparer.Ordinal);

        public void Dispose() => buffer?.Dispose();

        public BackupManifest? Captured { get; private set; }

        public static readonly HLCTimestamp AgreedCut = new(4, 5_000_000_070L, 3);

        public (HLCTimestamp MinInFlight, HLCTimestamp MaxCommitted)? Proposal { get; private set; }

        public ValueTask<HLCTimestamp> ProposeCutAsync(HLCTimestamp minInFlight, HLCTimestamp maxCommitted, CancellationToken ct)
        {
            Proposal = (minInFlight, maxCommitted);
            return ValueTask.FromResult(AgreedCut);
        }

        public int FilesAtCapture { get; private set; }

        public ValueTask CapturedAsync(BackupManifest manifest, CancellationToken ct)
        {
            Captured = manifest;
            FilesAtCapture = Files.Count;
            return ValueTask.CompletedTask;
        }

        public ValueTask BeginFileAsync(string relativePath, CancellationToken ct)
        {
            Assert.Null(current);
            current = relativePath;
            buffer = new MemoryStream();
            return ValueTask.CompletedTask;
        }

        public ValueTask WriteAsync(ReadOnlyMemory<byte> data, CancellationToken ct)
        {
            Assert.NotNull(buffer);
            buffer.Write(data.Span);
            return ValueTask.CompletedTask;
        }

        public ValueTask EndFileAsync(CancellationToken ct)
        {
            Files[current!] = buffer!.ToArray();
            current = null;
            buffer = null;
            return ValueTask.CompletedTask;
        }
    }

    /// <summary>Records every chunk the service writes, after a wire round trip.</summary>
    private sealed class RecordingStreamWriter : IServerStreamWriter<GrpcBackupPartChunk>
    {
        public List<GrpcBackupPartChunk> Chunks { get; } = [];

        public WriteOptions? WriteOptions { get; set; }

        public Task WriteAsync(GrpcBackupPartChunk message)
        {
            Chunks.Add(WireRoundTrip(message, GrpcBackupPartChunk.Parser));
            return Task.CompletedTask;
        }
    }

    private sealed class ReplayStreamReader<T>(IReadOnlyList<T> messages) : IAsyncStreamReader<T>
    {
        private int next = -1;

        public T Current => messages[next];

        public Task<bool> MoveNext(CancellationToken cancellationToken) => Task.FromResult(++next < messages.Count);
    }

    private sealed class ReplayStreamReader(IReadOnlyList<GrpcBackupPartChunk> chunks) : IAsyncStreamReader<GrpcBackupPartChunk>
    {
        private readonly ReplayStreamReader<GrpcBackupPartChunk> inner = new(chunks);

        public GrpcBackupPartChunk Current => inner.Current;

        public Task<bool> MoveNext(CancellationToken cancellationToken) => inner.MoveNext(cancellationToken);
    }

    private sealed class RecordingControlWriter : IClientStreamWriter<GrpcBackupPartControl>
    {
        public List<GrpcBackupPartControl> Sent { get; } = [];

        public WriteOptions? WriteOptions { get; set; }

        public Task WriteAsync(GrpcBackupPartControl message)
        {
            Sent.Add(WireRoundTrip(message, GrpcBackupPartControl.Parser));
            return Task.CompletedTask;
        }

        public Task CompleteAsync() => Task.CompletedTask;
    }

    /// <summary>Minimal context: the service reads only the cancellation token.</summary>
    private sealed class StubServerCallContext : ServerCallContext
    {
        protected override CancellationToken CancellationTokenCore => CancellationToken.None;
        protected override string MethodCore => "test";
        protected override string HostCore => "test";
        protected override string PeerCore => "test";
        protected override DateTime DeadlineCore => DateTime.MaxValue;
        protected override Metadata RequestHeadersCore => new();
        protected override Metadata ResponseTrailersCore => new();
        protected override Status StatusCore { get; set; }
        protected override WriteOptions? WriteOptionsCore { get; set; }
        protected override AuthContext AuthContextCore => throw new NotSupportedException();
        protected override ContextPropagationToken CreatePropagationTokenCore(ContextPropagationOptions? options) => throw new NotSupportedException();
        protected override Task WriteResponseHeadersAsyncCore(Metadata responseHeaders) => throw new NotSupportedException();
    }
}
