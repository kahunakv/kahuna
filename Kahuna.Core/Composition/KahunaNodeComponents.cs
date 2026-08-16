
using Nixie;
using Kahuna.Server.KeyValues;
using Kahuna.Server.Locks;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Server.Sequencer;
using Kommander.WAL.IO;

namespace Kahuna.Server.Composition;

/// <summary>
/// The object graph one Kahuna node is built from, as produced by <see cref="KahunaNodeComposer"/>.
/// It is a hand-off record, not a service locator: the manager assigns each member to its own field
/// once and never consults it again.
/// </summary>
/// <param name="PersistenceBackend">
/// The storage backend behind the committed-but-unflushed overlay. Every consumer — actors, scans,
/// the background writer, PITR — sees this same wrapped instance.
/// </param>
/// <param name="BackendReadScheduler">Backend read pool. Constructed but <b>not started</b>.</param>
/// <param name="BackendWriteScheduler">Background flush pool. Constructed but <b>not started</b>.</param>
/// <param name="DurabilityProvider">
/// Kahuna's application-durability provider, which hosts assign to Kommander before the node joins.
/// </param>
internal sealed record KahunaNodeComponents(
    IPersistenceBackend PersistenceBackend,
    FairReadScheduler BackendReadScheduler,
    FairReadScheduler BackendWriteScheduler,
    KahunaDurabilityProvider DurabilityProvider,
    IActorRef<BackgroundWriterActor, BackgroundWriteRequest> BackgroundWriter,
    LockManager Locks,
    KeyValuesManager KeyValues,
    SequencerManager Sequencer);
