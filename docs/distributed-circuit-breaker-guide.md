# Distributed circuit breaker guide

A circuit breaker stops a caller from sending traffic to a dependency that already fails. The caller
counts recent outcomes. When too many of them fail, the caller rejects immediately instead of
waiting for a timeout.

Inside one process a breaker is a small array and a timestamp. Across a fleet it is not. Twenty
replicas keep twenty windows, so the dependency absorbs roughly twenty times the damage before the
first breaker trips. The fix is one shared window. This guide shows how to hold that shared window
in Kahuna, and which Kahuna tools each part of the problem needs.

This guide is for a consumer of a Kahuna cluster. It changes no Kahuna code.

## 1. The state machine

A breaker has three states and four transitions.

| From | To | Trigger |
| --- | --- | --- |
| closed | open | the failure ratio reaches the threshold, after a minimum number of observations |
| open | half-open | the first admission attempt after the open period elapses |
| half-open | closed | enough probe successes |
| half-open | open | any probe failure |

Two edges are absent on purpose. A closed breaker never jumps to half-open. An open breaker never
closes without a probe, because the passage of time is not evidence of recovery.

Move a breaker out of `open` lazily. Do not schedule a timer. The call that notices the open period
elapsed is the same call that wants the first probe slot, so one transaction does both. A timer
splits those two events apart, and every replica then races into the gap at once.

## 2. Which Kahuna tool answers which part

| Part of the problem | Kahuna tool |
| --- | --- |
| Decide and mutate without interleave | a script transaction (`TryExecuteTransactionScript`) |
| Ship the script once, run it by hash | `LoadTransactionScript` / `KahunaTransactionScript` |
| One clock for every replica | `current_time()`, read on the node that runs the script |
| Retire old observations | `SET … EX <milliseconds>` |
| Hold the observation set | keys under one prefix, read with `GET BY BUCKET` |
| Hold the probe budget | one key per claim, each with its own `EX` lease |
| Reclaim a dead replica's probe slot | the `EX` lease on the claim key |
| Throw a stale window away | an epoch number in the state, repeated in each observation value |

The script transaction is the important one. It is what makes reading, deciding and writing a single
step. Without it a replica reads the window, computes a ratio, and writes `open` while a dozen other
replicas do the same.

## 3. Key layout

A key space is the part of a key before its **last** `/`. A placement group is the part of a key
space before its **first** `|`. Kahuna hashes the placement group, so two key spaces that share a
group land on one partition.

Use that rule to put a whole breaker on one partition. Put the scope first and the subspace second:

| Key | Holds |
| --- | --- |
| `<scope>\|cb/state` | `closed`, `open` or `half` |
| `<scope>\|cb/gen` | the current epoch, as a decimal string |
| `<scope>\|cb/openedat` | the millisecond stamp of the last open |
| `<scope>\|cb/probeok` | probe successes so far in this recovery |
| `<scope>\|cb.obs/<uuid>` | one observation, `<epoch>:f` or `<epoch>:s` |
| `<scope>\|cb.probe/<uuid>` | one probe claim |

Every one of these keys has the placement group `<scope>`, so all of them resolve to one partition
and one Raft leader. Three consequences follow, and all three matter:

1. The transaction touches one partition, so it can take the one-phase commit path.
2. One leader answers every call, so `current_time()` reads one clock.
3. The two buckets stay separate, because `<scope>|cb.obs` and `<scope>|cb.probe` are distinct key
   spaces.

Choose the scope deliberately. Everything mapped to one scope shares one window, one state and one
probe budget. A global scope reacts fastest and punishes everyone for one bad tenant. A narrow scope
contains the damage and fills its window more slowly. Scope keys are visible to anyone who can read
the cluster, so keep them stable and put nothing sensitive in them.

## 4. The epoch

A local breaker empties its window on every transition. A shared breaker must not, because clearing
it costs another round trip and leaves a gap where the state moved but the observations did not.

Label each observation with the epoch it belongs to instead. Counting reads only the members that
carry the current epoch. A transition then costs one increment, and the superseded observations age
out under their own expiry.

The epoch moves when the breaker opens and when a probe failure re-opens it. The epoch does **not**
move on the `open → half-open` edge, because recovery continues the same epoch.

Kahuna script has no substring function, so a script cannot read the epoch out of a value. Compare
for exact equality against a mark the script builds instead:

```
LET fmark = concat(to_string(gen), ":f")
LET smark = concat(to_string(gen), ":s")
```

A member that carries any other epoch matches neither mark, so the count ignores it. This works
because an observation holds exactly two fields. It does not extend to a third.

## 5. The scripts

Three scripts cover the whole breaker. Each one is one transaction.

Config values appear here as literals: a minimum throughput of 4, a threshold of 500 thousandths, an
open period of 500 ms, a budget of 2 probes, a probe lease of 5000 ms, and an observation retention
of 60000 ms. Build one script text per policy. Every replica that shares a scope must use the same
values, because nothing stops one replica from holding a different threshold against the same
window.

### 5.1 Record an outcome

```
BEGIN (locking=pessimistic, timeout=20000)
  LET st = GET @state
  LET g = GET @gen
  LET gen = 0
  IF g != null THEN
    LET gen = to_int(g)
  END
  LET state = "closed"
  IF st != null THEN
    LET state = to_string(st)
  END
  LET verdict = "ignored"
  IF state == "closed" THEN
    LET fmark = concat(to_string(gen), ":f")
    LET smark = concat(to_string(gen), ":s")
    LET mark = smark
    IF @outcome == "f" THEN
      LET mark = fmark
    END
    LET w = GET BY BUCKET @obs
    LET total = 1
    LET fails = 0
    IF @outcome == "f" THEN
      LET fails = 1
    END
    SET @obskey mark EX 60000
    FOR v IN w DO
      IF v == fmark THEN
        LET fails = fails + 1
        LET total = total + 1
      END
      IF v == smark THEN
        LET total = total + 1
      END
    END
    LET verdict = concat("recorded:", concat(to_string(fails), concat("/", to_string(total))))
    IF total >= 4 && fails * 1000 >= 500 * total THEN
      SET @state "open"
      SET @gen to_string(gen + 1)
      SET @openedat to_string(current_time())
      LET verdict = "opened"
    END
  END
  LET answer = verdict
  COMMIT
END
```

The threshold is an integer numerator of thousandths, and the test multiplies instead of dividing.
A ratio comparison written as `fails / total >= 0.5` invites a floating-point surprise at the exact
boundary the breaker exists to act on. A division also raises a script error when the window is
empty, because Kahuna refuses division by zero.

The script counts the window **before** it writes the new observation, then adds that observation in
arithmetic. Section 8 explains why.

A non-closed breaker records nothing. It returns `ignored`.

### 5.2 Admit a call

```
BEGIN (locking=pessimistic, timeout=20000)
  LET st = GET @state
  LET state = "closed"
  IF st != null THEN
    LET state = to_string(st)
  END
  LET verdict = "closed"
  IF state == "open" THEN
    LET oa = GET @openedat
    LET openedAt = 0
    IF oa != null THEN
      LET openedAt = to_int(oa)
    END
    IF current_time() - openedAt >= 500 THEN
      SET @state "half"
      SET @probeok "0"
      LET state = "half"
    ELSE
      LET verdict = "rejected"
    END
  END
  IF state == "half" THEN
    LET p = GET BY BUCKET @probes
    IF count(p) < 2 THEN
      SET @probekey "1" EX 5000
      LET verdict = "probe"
    ELSE
      LET verdict = "rejected"
    END
  END
  LET answer = verdict
  COMMIT
END
```

The verdict is one of three values:

- `closed` — call the dependency and record the outcome.
- `probe` — call the dependency, then settle the probe with the claim key you sent.
- `rejected` — fail immediately. Do not call the dependency.

The `open → half-open` move and the probe claim sit in one transaction. This is the whole point of
the lazy transition. No other replica can arrive between the two.

The probe budget is a set of claim keys, not a counter. A counter cannot recover from a replica that
claims a slot and then dies, because the decrement never arrives. A claim key with a lease expires
on its own.

### 5.3 Settle a probe

```
BEGIN (locking=pessimistic, timeout=20000)
  LET st = GET @state
  LET state = "closed"
  IF st != null THEN
    LET state = to_string(st)
  END
  LET verdict = "ignored"
  IF state == "half" THEN
    LET claim = GET @probekey
    IF claim == null THEN
      LET verdict = "stale"
    ELSE
      DELETE @probekey
      IF @outcome == "f" THEN
        LET g = GET @gen
        LET gen = 0
        IF g != null THEN
          LET gen = to_int(g)
        END
        SET @state "open"
        SET @gen to_string(gen + 1)
        SET @openedat to_string(current_time())
        LET verdict = "reopened"
      ELSE
        LET ok = GET @probeok
        LET successes = 1
        IF ok != null THEN
          LET successes = to_int(ok) + 1
        END
        IF successes >= 2 THEN
          SET @state "closed"
          SET @probeok "0"
          LET verdict = "recovered"
        ELSE
          SET @probeok to_string(successes)
          LET verdict = "progress"
        END
      END
    END
  END
  LET answer = verdict
  COMMIT
END
```

The missing claim key is the staleness check. A probe that was slow rather than dead loses its slot
when the lease expires. Its result then arrives against a slot that belongs to someone else, and the
script must not count it. The absent key says exactly that, and the verdict is `stale`.

A probe that settles normally releases its slot at once. The lease is a backstop for a probe that
never reports, not a timer for a probe that takes a while.

## 6. Wiring it up

1. Build one script text per policy, with your configuration substituted into the literals.
2. Load each script once per client with `LoadTransactionScript`. The client hashes the text with
   Blake3 and the server caches the parse, so later calls send the hash rather than the text.
3. Derive the scope from the call, then build the parameter list.
4. Call the admit script before every outbound call.
5. Call the record script or the settle script after the outbound call settles.

The parameters are key names, not values, apart from `@outcome`:

| Placeholder | Value |
| --- | --- |
| `@state` | `<scope>\|cb/state` |
| `@gen` | `<scope>\|cb/gen` |
| `@openedat` | `<scope>\|cb/openedat` |
| `@probeok` | `<scope>\|cb/probeok` |
| `@obs` | `<scope>\|cb.obs/` |
| `@probes` | `<scope>\|cb.probe/` |
| `@obskey` | `<scope>\|cb.obs/<a fresh uuid>` |
| `@probekey` | `<scope>\|cb.probe/<a fresh uuid>` |
| `@outcome` | `f` or `s` |

The client generates the uuid. A script cannot build a key name, so the client must name every key
the script touches. Section 8 covers the consequences.

Put a timeout **inside** the breaker, around the wrapped call. Its job there is not to improve
latency. Its job is to guarantee that every admitted attempt settles, so a hung call cannot hold a
probe slot until the lease expires.

### Retry handling

Retry a result of `MustRetry`. Neither `MustRetry` nor `Aborted` commits anything, so a retry is
safe. Retry the same admit call with the **same** claim key, so a retry cannot claim two slots.

### When the cluster is unreachable

Kahuna does not answer this for you, and neither does any other store. Decide it in your client:

- A scope the replica last saw as `open` or `half` rejects. The breaker was protecting something,
  nothing said the dependency recovered, and the only reason you cannot confirm that is the cluster
  being unreachable.
- A scope last seen closed, or never seen, is genuinely ambiguous. Make it a configuration option
  with a documented default. Blocking all traffic because a coordinator is unreachable turns one
  outage into two, and whether that is right depends on what the call does.
- An outcome that cannot be recorded is dropped. The call already completed, so there is nothing
  useful to do with the result, and one lost datapoint moves a window of a hundred by one percent.

Do not fall back to a local in-process breaker. That rebuilds the per-replica window the shared one
replaced, at the moment the system is already under stress, while the breaker still reports itself
as distributed.

Keep only the non-closed scopes in that memory. `closed` and `never heard of it` lead to the same
decision, so storing `closed` grows the map with every scope the process ever sees.

## 7. Settings that constrain each other

Each of these looks reasonable alone. Check them together.

1. **Retention against minimum throughput.** An observation leaves the window by age. If
   observations expire after 60 s and the breaker needs 20 of them, a scope that receives 10 calls
   a minute never holds 20 at once. It can fail every call and stay closed forever. Check any
   low-volume scope by hand.
2. **The probe lease against the slowest probe.** The lease must exceed the timeout inside the
   breaker. A lease shorter than that hands a live probe's slot to someone else, so more probes run
   than the budget allows, and the original result is discarded as stale.
3. **The probe lease against the open period.** Keep the lease at or below the open period. A
   re-opened breaker cannot clear the claim keys of the previous attempt, because a script cannot
   build their key names. They expire instead. If the lease outlives the open period, stale claims
   still occupy the budget when the breaker next reaches half-open, and recovery stalls.
4. **Successes to close against the probe budget.** Any single probe failure resets progress. A high
   success requirement on a small budget makes recovery depend on a long unbroken run, so traffic
   stays shut off well after the dependency recovers.
5. **The state record must not expire.** A missing state record reads as `closed`. An expired open
   breaker therefore admits everything and loses the epoch that labels the current window. Give an
   expiry only to a closed breaker, and keep that expiry above the observation retention.

## 8. Language limits to design around

These are properties of Kahuna script today. Each one shaped a decision above.

**`RETURN` inside a transaction stops the script.** A `COMMIT` written after it never runs, and the
transaction aborts with the reason `Transaction aborted`. Produce the result with a trailing `LET`
instead, immediately before `COMMIT`. The transaction returns the value of the last statement it
executed.

**A key name cannot be computed.** The grammar accepts an identifier, a placeholder, or a string
literal in key position. A script cannot build a key name from an expression. Three consequences
follow:

- The client must name every key, which is why each observation and each probe claim carries a
  client-generated uuid.
- The window cannot trim by count, only by age. A ring buffer would need the script to address slot
  *i*, and it cannot.
- Superseded observations and abandoned probe claims cannot be deleted in bulk. They expire.

**There is no substring, prefix test, or split.** Compare against a value the script builds with
`concat`, as section 4 does. A value with more than two fields cannot be read at all.

**`current_time()` is a wall clock, not the HLC.** It reads the clock of the node that runs the
script, which removes client skew from the decision. It does not remove skew between Kahuna nodes.
The layout in section 3 keeps one leader as the clock source for a scope, so this is sound in
practice, and the clock source changes on a leader change.

**A bucket read does not see the transaction's own write.** Write the observation and then scan the
bucket, and the scan does not return the key you just wrote. Count first, then add the new
observation in arithmetic, as section 5.1 does.

## 9. Bucket names

A bucket is a key space: the text of a key before its last `/`. `GET BY BUCKET` accepts the key
space bare (`svc|cb.probe`) or with a trailing slash (`svc|cb.probe/`). Both spellings are served by
the actor that owns the key space, the same one every point read and write of `svc|cb.probe/<id>`
goes to, and both take the same prefix lock under pessimistic locking. A scan and a point read of
one member inside one transaction therefore agree, and a released claim is gone from the next scan
the moment the release commits.

A partial prefix that names no key space (`svc|cb.pro`) is also accepted. It reads through the
persisted rows and is correct, but it cannot be served from the owning actor's memory, so prefer the
key space when you have it.

## 10. Cost

Each decision is a transaction on one partition, which is a Raft commit when it writes. That is
more than one round trip to one process. Two things keep it reasonable:

- The layout in section 3 keeps the whole breaker on one partition, which admits the one-phase
  commit path.
- The admit script writes only when it claims a probe slot or moves the state. A closed breaker's
  admission is a read.

Record every outcome, but admit through the breaker only where the protection is worth a round trip.

## 11. Verification status

The three scripts in section 5 run in `Kahuna.Server.Tests/TestCircuitBreakerScripts.cs`, against a
three-node embedded cluster with four partitions and the memory backend. The following was observed
directly:

- Six outcomes recorded across three nodes accumulate in one window, and the breaker opens at the
  configured ratio.
- An open breaker rejects, and a further outcome is ignored because the epoch moved.
- Twenty concurrent admissions against a freshly elapsed open breaker, spread across three nodes,
  admit exactly two probes and reject eighteen. This is the race the shared budget exists to
  prevent. It held over three runs.
- A stale claim settles as `stale` and makes no progress.
- Two probe successes close the breaker, and a single probe failure re-opens it.
- Two settled probes release their slots the moment their transactions commit: the next scan of the
  probe bucket counts zero claims, and a breaker re-opened on the same scope admits a fresh probe
  against that empty budget.

The following is **not** verified:

- Any backend other than `memory`.
- Behaviour across a partition leader change, including the clock source change in section 8.
- Behaviour under a node failure while a probe is in flight.
- Configuration passed as placeholders rather than literals. The grammar accepts a placeholder in
  expression position, so it should work, and it was not tested.
