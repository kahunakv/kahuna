# Key-value watch streams guide

A watch is a stream of the committed mutations to a key, a prefix, or a key range. A client opens a watch and
then receives an event each time a key in the scope is set, deleted, or has its expiry extended. This guide
covers the scopes, the delivery and ordering contract, progress watermarks, resuming from a cursor, history
holds, the ways a watch ends, the three surfaces (in-process, gRPC, REST), replica placement, change-feed
consumers, and the metrics.

## Scopes

| Scope | Covers | Factory |
|---|---|---|
| Key | One exact key | `KeyValueWatchScope.ForKey("orders/42")` |
| Prefix | Every key that starts with the prefix | `KeyValueWatchScope.ForPrefix("db:t|")` |
| Range | Every key of one key space in `[startKey, endKey)` | `KeyValueWatchScope.ForRange("orders", "orders/100", "orders/200")` |

All comparisons are ordinal. A key belongs to the key space before its last `/`, so a range scope matches
`orders/150` but not `orders/150/lines`.

A prefix that contains `|` and no `/` before it (a placement-group prefix such as `db:t|`) is held by one
partition, because hash routing places a whole placement group on one partition. Any other prefix can match
keys on every partition, so its watch covers every hash partition plus every key range that can hold a
matching key.

## What an event carries

| Field | Meaning |
|---|---|
| `Key` | The key that changed. |
| `Kind` | `Set`, `Deleted` or `Extended`. |
| `Value` | The new value of a `Set`. Null for a delete, an extend, and every event of a keys-only watch. A zero-byte value is an empty array, never null. |
| `Revision` | The key's revision after the mutation. |
| `Expires` | The key's expiry. |
| `CommitTimestamp` | The transaction's commit timestamp, or the write's last-modified timestamp for a write outside a transaction. |
| `TransactionId` | The transaction, or zero for a write outside a transaction. |
| `PartitionId` | The data partition whose log carried the mutation. |
| `Origin` | What produced the transaction that wrote the key, as the transaction named it at start (for example a schema rewrite). Null for an ordinary write. Set only on a cluster that enables transaction origin tags (see "Change-feed consumers"). |

A keys-only watch (`KeysOnly: true`) gets every field except the value. Use it when the consumer only needs to
know which key changed, for example to evict a cache entry; the node then never sends the value bytes.

There are no expiry events: a key whose expiry passes is not reported. Expiry is a local cache eviction on each
node, not a replicated transition, so there is no committed moment to report. There is also no before-image: an
event carries the new value only, not the value it replaced.

## Delivery and ordering

- **Registration is synchronous.** When the call that opens the watch returns, the watch is registered on every
  partition of its scope. Every write committed after that produces an event, once the serving replica applied
  it.
- **Delivery is at-least-once.** For one key, the first delivery of each revision arrives in revision order, and
  the commit timestamps of one key increase with its revisions. A repeat of an older revision can arrive later
  (a transaction re-driven after a failure can settle twice). A consumer drops an event whose revision is at or
  below the last revision it holds for that key.
- **Across keys of one partition**, events arrive in the partition's log order. That order is not commit
  timestamp order: a durable transaction's event is sent when the transaction settles on the partition, which
  can come after later writes to other keys.
- **Across partitions** no order is promised. The events of one transaction share its commit timestamp and its
  transaction id, so a consumer can group them.
- **Visibility.** A committed value can be readable before its event arrives. A durable transaction's value
  becomes readable at commit, and its event comes when the transaction settles on the partition.

Besides events, a stream carries two kinds of frames:

- **`Progress`** (only with the commit admission window on, see below): a watermark W for one partition. No event
  with a commit timestamp at or below W arrives later on that partition. Events between two progress frames can
  arrive out of commit-timestamp order. A consumer that needs a consistent cut over several partitions uses the
  lowest watermark over them.
- **`TransactionEnd`** (only with the commit admission window on): a durable transaction has no more events on
  the partition. It follows the transaction's last event there, and goes only to watches that received an event
  of that transaction.

## Progress watermarks and the commit admission window

A commit timestamp is minted before its entry reaches a partition's log, so without a bound a low timestamp can
land after higher ones, and no watermark could hold. The commit admission window bounds it. With the setting
`CommitAdmissionLagMs` (CLI `--commit-admission-lag-ms`) at L > 0, every replica admits a prepare, or a write
made outside a transaction, only when its commit timestamp falls inside `(entry time − L, entry time + 5 s)`.
The entry time is the leader's clock when it proposed the entry, the same on every replica. An entry outside the
window installs nothing on any replica, and its writer gets a retryable answer:

- a write outside a transaction answers `MustRetry`;
- a transaction aborts with the refusal class `CommitTimestampOutsideWindow`. A script answers `MustRetry`; an
  interactive session answers `Aborted` with that class, and the client starts a new transaction.

A transaction is refused when its commit takes longer than L to reach a participant's log, or when its
coordinator's clock runs more than L behind (or more than 5 s ahead of) the participant's leader. A larger L
refuses fewer commits and holds the watermark further behind: W trails the newest entry by about L. On an idle
partition the leader proposes an empty tick entry about every L/2 (between 100 ms and 1 s), so the watermark
keeps moving with no writes. A pending durable intent holds W below its commit timestamp until it settles.

**Enable it only when every node runs a version that reads the bound, and give every node the same value.** The
bound travels in each entry, so nodes with different values judge an entry the same way; but a node with a
smaller value can send a watermark that a later event goes below. A watch that sees that ends with
`Lagged(WatermarkViolated)`. With the window off (the default), streams carry no progress frames, no
`TransactionEnd`, and a watch cannot resume.

## Resume

A request can name a cursor: `KeyValueWatchRequest.ResumeFrom` (every partition) and `ResumePartitions` (per
partition). The node then replays, from its retained log, every event above the cursor, and then switches to the
live stream with no gap:

```csharp
KeyValueWatchOpening opening = await kahuna.WatchKeyValues(
    new KeyValueWatchRequest(KeyValueWatchScope.ForPrefix("db:t|"), ResumeFrom: cursor), ct);
```

- **The cursor is a watermark**, the last `Progress` timestamp the consumer received for the partition, never
  the timestamp of its last event (events between two watermarks can arrive out of order).
- Events between the cursor and the end of the replay can arrive again; the per-key rule above drops them.
- The replay reads from the first entry at or after `cursor − 5 s`, which the window makes sufficient.
- A partition whose history the node cannot prove complete ends the watch with `HistoryUnavailable`: the window is
  off, a snapshot install covers the cursor, or the log below the cursor was compacted. **The documented recovery
  is a new snapshot**, then a new watch from the snapshot's timestamp. A client never hides it.

## History holds

A node keeps its log only until compaction, so a resume works only while the log still reaches back to the cursor.
A history hold keeps a partition's log retained from a timestamp on every replica (also after a restart or a
replica move), so a resume on any replica is likely to succeed:

```csharp
HistoryHoldAnswer held = await client.AcquireHistoryHolds("cdc-orders", partitions, cursor, leaseMs: 600_000);
// ... as the consumer commits its progress:
await client.RenewHistoryHolds([.. held.Holds.Select(h => h.HoldId)], 600_000, newCursor);
// ... when done:
await client.ReleaseHistoryHolds([.. held.Holds.Select(h => h.HoldId)]);
```

- **Who takes one:** only a consumer that asks. A watch never takes one.
- **Advance it** with a renew that names a later timestamp, as the consumer commits its own progress, so the
  retained log shrinks. A renew never moves a hold back.
- **Leases:** a hold stays registered after its lease lapses until the reaper purges it, and a renew before the
  purge still holds the history. `DoesNotExist` on a renew means the hold was purged or released: presume the
  history gone. After a full restart, holds loaded from disk get a grace window before the reaper may purge them.
- **Bounds:** `HistoryHoldMaxLeaseMs` (default one day), `HistoryHoldMaxPerHolder` (4096) and
  `HistoryHoldMaxTotal` (65536). Past a bound an acquire answers `HistoryHoldLimitReached`.
- **Availability, not safety.** A replica that compacted before it applied a hold answers `HistoryUnavailable`;
  it never replays a gap.

The surfaces are `IKahuna.HistoryHold`, the gRPC rpc `HistoryHold`, `POST /v1/kv/history-holds`, the client
methods above, and the control commands below.

## How a watch ends

A watch ends with one terminal frame, and nothing comes after it. Events between the last delivered event and the
end can be missing. A consumer opens a new watch from its last watermarks, which replays them (see "Resume"); with
no watermark (the commit admission window is off), or after `HistoryUnavailable`, it treats everything the watch
covered as possibly changed.

| Terminal frame | Reason | Cause |
|---|---|---|
| `Lagged` | `Overflow` | The consumer fell a full capacity behind. The node dropped the buffered frames instead of buffering without limit. |
| `Lagged` | `PartitionStateReplaced` | The serving replica installed a snapshot of the partition, or stopped hosting it. A snapshot carries no per-key events. |
| `Lagged` | `ApplyDiscontinuity` | The serving replica failed to apply a committed entry, so it cannot prove that it reported every mutation. |
| `Lagged` | `RangeMapChanged` | A split, a merge, or a key-range registration changed the partitions the scope covers. |
| `Lagged` | `Shutdown` | The serving node is shutting down. |
| `Lagged` | `Disconnected` | The connection to the serving node ended without a terminal frame (set by the client). |
| `Lagged` | `WatermarkViolated` | An event arrived below a watermark already sent: the nodes run the commit admission window with different settings. |
| `HistoryUnavailable` | — | A resume whose history the node cannot prove complete (see "Resume"). Take a new snapshot. |
| `Refused` | the refusal | The watch could not be opened. |

The capacity is the number of frames a node buffers for one watch. The request sets it (`Capacity`); zero takes
the node default of 4096 frames.

A watch on ephemeral keys is refused with `WatchUnsupportedOnEphemeral` on every surface. An ephemeral write never
reaches Raft, so there is no replicated event to report. The refusal is permanent; a retry gets the same answer.

## In-process

An embedded application calls `IKahuna.WatchKeyValues`. Any replica serves a watch, leader or follower, and this
node must host every partition the scope covers.

```csharp
KeyValueWatchOpening opening = await kahuna.WatchKeyValues(
    new KeyValueWatchRequest(KeyValueWatchScope.ForPrefix("db:t|"), KeysOnly: true), ct);

if (opening.Watch is not { } watch)
    throw new InvalidOperationException($"watch refused: {opening.Type}");

await using (watch)
{
    await foreach (KeyValueWatchFrame frame in watch.ReadAllAsync(ct))
    {
        if (frame.Event is { } e)
            Evict(e.Key);
        else if (frame.IsTerminal)
            EvictEverything();
    }
}
```

Refusals: `WatchUnsupportedOnEphemeral`, `DoesNotExist` (this node does not host a partition of the scope; the
opening's `Routes` name the replicas of each partition), and `MustRetry` (the range map does not cover the scope
yet, or kept changing while the watch registered).

## Client

`KahunaClient.WatchKeyValues` returns a `KahunaWatch`, which is an `IAsyncEnumerable<KeyValueWatchFrame>`.
Dispose it to close the watch on every node.

```csharp
await using KahunaWatch watch = await client.WatchKeyValues(
    new KeyValueWatchRequest(KeyValueWatchScope.ForPrefix("orders/")), ct);

await foreach (KeyValueWatchFrame frame in watch)
{
    if (frame.Event is { } e)
        Console.WriteLine($"{e.Kind} {e.Key} r{e.Revision}");
}
```

The client works over both transports. Over gRPC each watch is one stream on the shared channel to its node; the
channel opens more HTTP/2 connections when one connection reaches its stream limit.

`KahunaWatch.Watermarks` holds the last watermark of each partition: the cursor to store. With
`WatchKeyValues(request, autoResume: true)`, a node stream that ends with `Lagged` (overflow, a dropped
connection, a shutdown, a replaced partition state, a failed apply) is reopened from those watermarks, on its node
or on another replica, and the node replays what was missed: the consumer sees no lagged frame, but can see
events again. A stream is resumed only when every one of its partitions has a watermark. `HistoryUnavailable`, a
refusal, a range-map change and a watermark violation still end the watch.

**Replica placement.** When the replication factor is below the node count, one node can host only some
partitions of a scope. The node the client asks then refuses with `DoesNotExist` and names the replicas of each
partition. The client opens the watch on those replicas, one node for each group of partitions, each limited to
the partitions that node hosts (`KeyValueWatchRequest.Partitions`). A terminal frame from any of them ends the
whole watch and closes the others. The client dials a named replica only when it resolves to a configured
endpoint, through `KahunaOptions.RoutingEndpointMap`, or when `KahunaOptions.AllowUnlistedRoutingEndpoints` is
on, the same rule that applies to routing hints. A client that cannot reach a replica of every partition gets
`DoesNotExist`.

## gRPC

```
rpc Watch (stream GrpcWatchRequest) returns (stream GrpcWatchResponse);
```

One call carries any number of watches. The client names each watch with a watch id it chooses (not zero, and
unique among the open watches of the call):

1. A `WATCH_REQUEST_OPEN` request opens a watch. The server answers with one frame for that watch id before any
   other frame of it: `WATCH_FRAME_OPENED` (with the partitions it registered on) or `WATCH_FRAME_REFUSED` (with a
   typed refusal, and the replica routes for `TYPE_DOES_NOT_EXIST`).
2. The server then sends the watch's frames, several per `GrpcWatchResponse`, until a terminal frame.
3. A `WATCH_REQUEST_CANCEL` request closes one watch. The server sends nothing more for it.
4. Ending the request stream ends the call and closes every watch on it. So does a client that goes away, with a
   clean close or an aborted connection.

A refused watch never ends the call: a malformed scope or a reused watch id gets `TYPE_INVALID_INPUT` for that
id, and the other watches stay open. A retryable server failure while a watch opens is refused with
`TYPE_MUST_RETRY`; one that ends the whole call is reported as status `Unavailable`.

An event's value is a proto3 `optional bytes` field: absent for no value, present and empty for zero bytes.

A resume names its cursor in the open request: `ResumeFrom*` (every partition) and `ResumePartitions` (a cursor per
partition, which overrides it).

## REST

`POST /v1/kv/watch` with a JSON body opens one watch and answers with a server-sent-events stream
(`text/event-stream`).

```json
{ "scopeKind": 1, "scopeValue": "orders/", "durability": 1, "keysOnly": false, "capacity": 0 }
```

`scopeKind` is `0` (key), `1` (prefix) or `2` (range, with `startKey` and `endKey`). `durability` is `1`
(persistent), the default when it is left out. Each event of the stream is one `data:` line that holds a frame:

```
data: {"frame":6,"type":3,"partitionId":-1,"partitions":[1,2], ...}

data: {"frame":0,"type":3,"partitionId":2,"key":"orders/7","kind":0,"value":"djE=","revision":3, ...}
```

`frame` is the frame type (`0` event, `1` progress, `2` transaction end, `3` lagged, `4` history unavailable,
`5` refused, `6` opened). A resume adds `"resumeFrom": {"n":1,"l":...,"c":0}` or `"resumePartitions":
[{"partitionId":1,"timestamp":{...}}]` to the body. `type` is the opening's answer: `3`
(`Get`) on an opened watch and on its frames, the refusal code on a refused one. The first frame is always opened or refused. The stream
ends after a terminal frame. Closing the request closes and unregisters the watch.

A request the node could not start at all (a retryable failure before the stream began) gets a plain JSON body
`{"type":101}` (`MustRetry`) instead of a stream, so read the content type first.

## Control tool

```
kahuna-control --watch orders/            # every event under orders/, until Ctrl+C
kahuna-control --watch orders/ --watch-keys-only
kahuna-control --watch orders/ --format json
kahuna-control --watch orders/ --watch-from 1:1792504674003:0   # replay above a watermark first

kahuna-control --history-hold-acquire cdc-orders --partitions 1,2 --history-from 1:1792504674003:0 --lease-ms 600000
kahuna-control --history-hold-renew <id>,<id> --history-from <later timestamp>
kahuna-control --history-hold-release <id>,<id>
kahuna-control --history-hold-list [--holder cdc-orders] [--format json]
```

A timestamp is `node:physical:counter`, as the watch prints it in its progress lines, or the physical
milliseconds alone.

The interactive shell has the same command. It prints events until the watch ends or you press Ctrl+C or Esc, then
returns to the prompt:

```
kahuna-cli> watch orders/
kahuna-cli> watch orders/ keys-only
```

## Metrics

| Metric | Meaning |
|---|---|
| `kahuna.kv.watch.active` | Open watches on the node, in-process and remote. |
| `kahuna.kv.watch.remote_streams` | Open remote watch streams, by `transport` (`grpc`, `rest`). One gRPC stream can carry many watches. |
| `kahuna.kv.watch.remote_frames_sent_total` | Frames written to remote streams, by `transport`. |
| `kahuna.kv.watch.events_delivered_total` | Events handed to watches, one per matched watch. |
| `kahuna.kv.watch.frames_dropped_total` | Buffered frames dropped when a watch ended with `Lagged`. |
| `kahuna.kv.watch.streams_lagged_total` | Watches ended with `Lagged`, by `reason`. |
| `kahuna.kv.watch.refused_total` | Watch requests refused, by `type`. |
| `kahuna.kv.watch.replays_total` | Resumes replayed per partition, by `outcome` (`replayed`, `history_unavailable`). |
| `kahuna.kv.watch.watermark_lag_ms` | The largest gap on the node between a partition's newest entry and its last watermark. |
| `kahuna.kv.watch.progress_ticks_total` | Idle-partition tick entries this node proposed. |
| `kahuna.kv.commit_admission.refused_total` | Entries the admission window refused at apply on this node, by `kind` (`prepare`, `write`) and `bound` (`floor`: too old; `lead`: too far ahead). |
| `kahuna.kv.commit_admission.oldest_pending_intent_age_ms` | Age of the oldest pending durable intent on the node. A stuck intent holds its partition's watermark back. |
| `kahuna.kv.history_holds.live` | Live history holds in the registry. |
| `kahuna.kv.history_holds.lowest_timestamp_age_ms` | Age of the lowest held timestamp, by `partition`. A hold nobody advances keeps it growing. |
| `kahuna.kv.history_holds.pinned_entries` | Log entries a history hold keeps retained on this node, by `partition`. |
| `kahuna.kv.history_holds.refused_total` | Acquires refused by a bound, by `bound` (`holder`, `total`). |
| `kahuna.kv.history_holds.purged_total` | Holds the reaper purged, by `reason` (`expired`, `partition_gone`). |

A steady `streams_lagged_total{reason="Overflow"}` means consumers read too slowly for their capacity. Any other
reason follows a snapshot install, an apply failure, a range change or a shutdown.

## Change-feed consumers

A change-data-capture consumer needs a complete, resumable feed. The flow:

1. Take an MVCC snapshot hold at a timestamp W (`AcquireSnapshotHold`) and a history hold at W on every partition.
2. Read the snapshot at W.
3. Open the watch from W (`ResumeFrom: W`).
4. Release the snapshot hold.
5. Store the watermarks as the consumer commits its progress, and renew the history holds forward to them.

**Commit timestamps.** A commit answers the transaction's commit timestamp, the one its writes carry and its watch
events report (`TransactionCommitAnswer.CommitTimestamp`; the client's `KahunaTransactionSession.CommitWithTimestamp`).
It is zero when the commit wrote no persistent key through the durable path.

**Maintenance writes.** A transaction can name an origin at start (`KahunaTransactionOptions.Origin`, at most 64
characters, for example `"schema-rewrite"` or `"ttl-sweep"`). The origin is stored on every prepared intent and
reported on every watch event of the transaction, so the consumer can drop maintenance writes. It is kept only on a
cluster that enables `TransactionOriginTags` (CLI `--transaction-origin-tags`); enable it only when every node
reads it. A write outside a transaction carries no origin.

**No before-image.** An event carries the new value only. A consumer that needs the replaced value keeps its own
state. There are no expiry events either: a key whose expiry passes is not reported.
