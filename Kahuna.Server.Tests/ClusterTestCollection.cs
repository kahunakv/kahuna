using Xunit;

namespace Kahuna.Server.Tests;

// Groups the tests that observe the process-global "kahuna.snapshot_floor.missing_protected_version_total"
// counter. That counter lives on a static Meter shared by every node, so a test that deliberately drives
// it non-zero (defective-prune injection) must never run concurrently with a test that asserts it stayed
// zero. Assigning all such tests to one collection makes xUnit run them sequentially relative to each
// other; the collection itself still runs in parallel with the rest of the suite, because no test outside
// this group ever pushes that counter above zero under correct behavior.
[CollectionDefinition("SnapshotFloorMetrics")]
public sealed class SnapshotFloorMetricsCollection { }
