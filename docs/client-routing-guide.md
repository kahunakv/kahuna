# Client leader-aware routing

Kahuna accepts every operation at every node. A node that does not own the resource an operation
addresses resolves the owner and forwards the operation there, so a client that picks nodes at
random still gets correct results — it just pays an extra inter-node hop for most of them.

This guide covers the opt-in client behaviour that removes that hop: the client learns which node
owns a resource, and sends the next operation on that resource straight there.

Nothing here changes what an operation does. Routing changes which node a request reaches first.
Every node re-resolves the resource, re-applies range fences and re-checks leadership on arrival,
so a routing answer that is stale or wrong costs a forward and never a wrong result.

---

## 1. What it buys

With `N` reachable nodes and one eligible owner per point operation, uniform rotation lands
`(N-1)/N` of them on a node that must forward. Direct routing removes that forward.

It does not remove anything else. Consensus replication, the leadership confirmation a read needs,
and cross-partition coordination all stay exactly as they were. Expect the gain on repeated point
operations against a stable cluster; expect nothing on a stream of resources the client has never
seen until metadata mode is on.

---

## 2. Modes

Set `KahunaOptions.Routing`.

| Mode | Behaviour |
| --- | --- |
| `Auto` | **Default.** `Learned` when the client was given several endpoints, `RoundRobin` when it was given one. |
| `RoundRobin` | Rotate over the configured endpoints. The behaviour of every client before routing existed. |
| `Learned` | Reuse the destination a previous response reported for the same resource. A resource the client has not seen falls back to rotation. |
| `Metadata` | `Learned`, plus resolution of unseen resources from routing metadata the client reads once and refreshes in the background. |

```csharp
// Learns, because it was given several endpoints.
KahunaClient pooled = new(["https://node1:8082", "https://node2:8084", "https://node3:8086"]);

// Rotates, because it was given one.
KahunaClient single = new("https://node1:8082");

// Either default can be overridden.
KahunaClient forced = new("https://node1:8082",
    options: new KahunaOptions { Routing = KahunaRoutingMode.Learned });
```

### Why a single-endpoint client stays on rotation

It could not act on most hints. A hint names whichever node owns the resource, and only endpoints the
client was configured with are dialled (see §5), so hints naming the other nodes are refused. Such a
client would keep a cache it could rarely use.

Give the client every endpoint if you want it to route. `KahunaClient.EffectiveRouting` reports the
mode actually in force, with `Auto` already resolved — read that rather than `KahunaOptions.Routing`
when you want to know how a client behaves.

### If the advertised addresses are not the ones you dial

`Auto` turns learning on for a pooled client, but §5 still governs which endpoints may be dialled. In a
deployment where nodes advertise addresses this client cannot reach and no `RoutingEndpointMap` is
configured, every hint is refused: the client keeps working, on rotation, and gains nothing. The
counter `hints_rejected` with `reason=endpoint_rejected` is what makes that visible — check it before
concluding the feature is not helping.

---

## 3. Which operations route

| Operation family | Treatment |
| --- | --- |
| Point key-value: get, exists, set, conditional set, delete, extend | **Routed** by the exact key. |
| Locks: acquire, extend, release, inspect | **Routed** by the exact lock resource, in its own name space. |
| Sequences: create, get, next, reserve, delete | **Routed** by the sequence name, resolved through the server's storage-key rule. |
| Batched point reads and writes (get-many, exists-many, set-many, delete-many) | **Server-dispatched**, and every item's route is learned from the response. |
| Prefix, bucket and range scans | **Server-dispatched**: one coordinator owns the fan-out, the pagination and the merge. |
| Transaction scripts and interactive transaction sessions | **Coordinator-routed** by the existing coordinator identity, never by a data key. |
| Cluster, range, backup and snapshot administration | **Unchanged.** Node-scoped by nature, and an explicit `nodeUrl` is always honoured. |

A batched call keeps server dispatch on purpose. The server already fans the batch out per key and
reports one outcome per item; splitting it in the client would turn that into several independent
requests whose transport failure has an ambiguous outcome per group, which the per-item response
contract cannot express. The client still learns every item's route, so the following point
operations on those resources go direct.

An explicit `nodeUrl` argument is never substituted. Neither is the affinity a lock handle carries
under `UpgradeUrls`: that affinity takes precedence over a learned route, while still passing the
endpoint policy and the failure cooldown below.

---

## 4. Server side: advertising an endpoint

A hint names a node, and a client dials URLs. Those are not always the same text — container port
mapping and split internal/external host names both break the assumption — so a node publishes what
it advertises rather than the address its peers route on.

| Flag | Meaning |
| --- | --- |
| `--advertised-client-endpoint <url>` | The base URL this node tells clients to use. Empty derives it from the Raft endpoint and the scheme below. |
| `--advertised-client-scheme <scheme>` | Scheme prefixed to a Raft endpoint to name a node. Empty follows `--raft-grpc-scheme`. |
| `--disable-peer-endpoint-advertisement` | Never name a peer in a hint. Set it where a peer's client URL cannot be derived from its Raft endpoint. |
| `--disable-routing-hints` | Return no hints at all. Clients then keep their configured endpoint selection. |

The default derivation is `<scheme><raft-host>:<raft-port>`. That is correct wherever the Raft port
is also a public listener, which is the common single-listener deployment. Where it is not, set
`--advertised-client-endpoint` explicitly on each node.

---

## 5. Client side: which endpoints may be dialled

A response must not be able to steer a client — and its credentials and TLS trust — at an address
the operator never chose. So a hint is accepted only after it resolves to a configured endpoint.

| Option | Meaning |
| --- | --- |
| `RoutingEndpointMap` | Maps an advertised endpoint onto the URL this client dials. |
| `AllowUnlistedRoutingEndpoints` | Off by default. On, a well-formed HTTP(S) endpoint that is neither configured nor mapped may be dialled. |

Both sides of the map are compared without a trailing slash and without case distinction. A hint
that resolves through the map is dialled as the **configured URL instance**, so the transport's
existing connection pool is reused rather than a second one opened for another spelling of the same
address.

Turn `AllowUnlistedRoutingEndpoints` on only for a cluster that grows nodes the client was not
started with, and only when every node's advertised address is one the client may dial.

### The Docker example

In `docker/local.yml` a node routes on `172.30.0.2:8082` and a client outside the network reaches it
at `https://localhost:8082`. Map the two:

```csharp
new KahunaOptions
{
    Routing = KahunaRoutingMode.Learned,
    RoutingEndpointMap = new Dictionary<string, string>
    {
        ["https://172.30.0.2:8082"] = "https://localhost:8082",
        ["https://172.30.0.3:8084"] = "https://localhost:8084",
        ["https://172.30.0.4:8086"] = "https://localhost:8086"
    }
}
```

Without the map every hint is refused, the client stays on rotation, and every operation still
succeeds.

---

## 6. Cache behaviour

| Option | Default | Meaning |
| --- | --- | --- |
| `RouteCacheCapacity` | 4096 | Learned routes held. Past it the least recently added are dropped. |
| `RouteHintLifetime` | 60 s | How long a learned route is used before it must be observed again. |
| `RoutingEndpointCooldown` | 5 s | How long an endpoint is held out of routing after a transport failure. |
| `RoutingMetadataLifetime` | 60 s | How long one routing-metadata map is kept. `Metadata` mode only. |

These are tuning values to measure, not consistency guarantees. Nothing in the cache is authoritative:

* **Exact resources only.** Nothing is coalesced by prefix. Two keys that share a prefix today can be
  separated by a range split tomorrow, and a prefix-keyed entry would then send one of them to the
  wrong partition on every request until it expired.
* **Domains are separate.** A lock and a key may share a name and live on different partitions, so a
  route learned in one name space never answers for another.
* **A late response cannot undo a newer one.** A response may replace a cached route only while that
  route still names the endpoint the request was sent to. A reply that arrives after another response
  already moved the route describes a state that is gone, and is dropped.
* **A failed endpoint is held out, not trusted as dead.** The cooldown stops following operations from
  queueing behind a node that stopped answering; a later response from that node clears it at once.
* **Bounded work and memory.** A cache hit allocates nothing and takes no lock. Learning a resource the
  cache has not seen takes one uncontended per-shard lock.

### What a failure report does not mean

A transport failure says the request did not complete, not that it did not run. A failure after the
request was submitted has an ambiguous outcome. The routing cache never decides that an operation may
be retried; that stays each operation's own contract, exactly as it was before routing existed.

The gRPC transport reports a failure from the point where every operation to a node passes through.
The REST transport reports one from its shared send path. Neither report changes any operation's
result.

---

## 7. Metadata mode

`Metadata` adds one read of `GET /v1/cluster/routing` (or the `Cluster.GetRoutingMetadata` gRPC call),
kept for `RoutingMetadataLifetime` and refreshed in the background. The read never sits on an
operation's own path: an operation that finds no usable map goes out on rotation now, and the map it
started loading serves the operations after it. Concurrent misses coalesce onto one read.

The payload carries, separately:

* the hash rule for hash-routed key spaces — algorithm identifier
  (`kahuna.placement-group-jump-xxh32-v1`), the key-space separator (`/`, last occurrence in a key),
  the placement-group separator (`|`, first occurrence in a key space; key spaces that share a group
  hash together), pool size and partition offset;
* the storage-key rule for sequences and the reserved key prefix;
* per key space, the routing mode and, for a key-range space, its descriptor intervals with their
  generations;
* per partition, the advisory endpoint of its believed leader.

Ownership and leadership are separate lists on purpose: a leader election replaces entries in one, a
split, merge or move replaces entries in the other.

The client refuses rather than approximates. An unrecognised schema version, a hash algorithm or a
separator it does not implement exactly, a map the answering node could not read coherently, or a
node that has not finished initializing all leave the client on learned routes. A key space that routes by range with a
gap over the key, and a partition with no known leader, resolve to nothing rather than to a guess.

The routing modes a node reports are its own and are not replicated, so two nodes may legitimately
disagree about one key space. Reading the map from a different node simply produces a different, and
equally advisory, answer.

---

## 8. Metrics

The client publishes counters under the meter `Kahuna.Client.Routing`:

| Counter | What it counts |
| --- | --- |
| `cache_hits` / `cache_misses` | Operations that found, or did not find, a live cached route. |
| `metadata_hits` | Operations routed from the metadata map rather than a learned route. |
| `hints_learned` | Response hints accepted and stored. |
| `hints_rejected` | Hints dropped, with a `reason` of `endpoint_rejected`, `superseded` or `unknown_provenance`. |
| `endpoints_suppressed` | Endpoints put into a failure cooldown. |
| `suppressed_routes_skipped` | Operations that skipped a cached route because its endpoint was in cooldown. |
| `metadata_refreshes` / `metadata_refreshes_coalesced` | Metadata reads issued, and reads another caller's in-flight read served. |
| `metadata_refresh_failures` | Reads that produced no usable map, with a `reason`. |

Every dimension is a small fixed set. Resource names, owner tokens, transaction ids and endpoint URLs
are never used as labels: each is unbounded in a real workload.

`hints_rejected` with `reason=endpoint_rejected` climbing while `cache_hits` stays flat is the
signature of an endpoint policy that does not match what the servers advertise — check
`--advertised-client-endpoint` and `RoutingEndpointMap`.

---

## 9. Measured effect

Three local nodes, in-memory storage, cleartext gRPC, 12 partitions with leadership spread 4/5/3, a
fixed seed, and the routing modes interleaved inside each repetition so host drift lands on every arm.
Medians:

| Workload | RoundRobin | Learned | Metadata |
| --- | --- | --- | --- |
| get, 2 000 keys, concurrency 64 | 72 188 rps, p99 3.50 ms | **116 808 (+62%)**, p99 1.95 ms | 114 404 (+58%), p99 1.92 ms |
| get, 50 000 keys, concurrency 64 | 58 008 rps, p99 3.75 ms | 57 902 (−0.2%), p99 3.70 ms | **87 696 (+51%)**, p99 1.18 ms |
| set, 2 000 keys, concurrency 8 | 3 423 rps, p99 10.81 ms | **3 782 (+10.5%)**, p99 9.92 ms | — |
| mixed 50/50, 2 000 keys, concurrency 8 | 5 937 rps, p99 8.05 ms | **7 309 (+23%)**, p99 6.79 ms | — |

Reads gain most, and the tail gains more than the median: removing a hop removes a source of variance,
not only a fixed cost.

The 50 000-key row is the one to reason from when sizing `RouteCacheCapacity`. Against the default
4 096-entry cache, every key there is evicted before it recurs, so learned routing never hits — and
lands within 0.2% of endpoint rotation. It does not pay, and it does not cost. Metadata mode is what
helps a working set larger than the cache, because it resolves a resource it has never seen without
needing an entry for it at all.

Writes gain less, which is expected: a write is dominated by consensus replication, so the hop is a
smaller share of it. Treat the write and mixed figures as "no regression, probably a modest gain" —
they were measured on a host whose own run-to-run spread reached 30% for writes and 67% for mixed
traffic, against between-mode differences of 10% and 23%.

Measure your own deployment before quoting any of this. A single-host loopback cluster understates the
hop, because the forward it removes crosses no real network.

## 10. Rolling it out

1. Upgrade the servers. Hints are on by default and cost nothing to a client that ignores them.
2. Check what a node advertises: `GET /v1/cluster/routing` reports `localEndpoint`. Confirm a client
   can dial it, and add a `RoutingEndpointMap` if it cannot.
3. Upgrade the clients. A client given several endpoints starts learning on its own; one given a
   single endpoint does not change. Watch `cache_hits` against `hints_rejected` — the latter climbing
   with `reason=endpoint_rejected` means step 2 is unfinished.
4. Set `Routing = RoundRobin` explicitly on any client you want held back.
5. Move to `Metadata` when a workload's cost is dominated by resources the client has not seen before,
   or when its working set is larger than `RouteCacheCapacity` (see §9).

Old and new mix freely in both directions. A new client against an old server receives no hints and
stays on rotation; an old client against a new server ignores the additional response fields.
