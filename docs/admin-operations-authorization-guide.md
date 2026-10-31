# Admin operations authorization guide

Some operations change the cluster itself, or write large amounts of data to disk. A node can require an
**admin key** for them, separate from anything a data client uses. This guide explains which operations
need the key, how to turn the key on, and how to rotate it.

---

## The operations

| Operation | REST | gRPC |
|---|---|---|
| Remove a node from the cluster | `POST /v1/cluster/leave` | `Cluster.Leave` |
| Change the replication factor | `POST /v1/cluster/replication-factor` | `Cluster.SetReplicationFactor` |
| Register or remove a key-range space | `POST /v1/ranges/register`, `/v1/ranges/unregister` | `KeyValuer.RegisterKeyRange`, `RemoveKeyRange` |
| Split or merge ranges | `POST /v1/ranges/split`, `/v1/ranges/merge` | `Cluster.SplitRange`, `MergeRanges` |
| Take a backup | `POST /v1/backups/full`, `/incremental`, `/coordinated` | `Backups.TakeFullBackup`, `TakeIncrementalBackup`, `TakeCoordinatedBackup` |
| Backup garbage collection and chain validation | `POST /v1/backups/gc`, `/v1/backups/validate-chain` | `Backups.RunBackupGarbageCollection`, `ValidateChain` |
| Restore | `POST /v1/restore` | `Backups.Restore` |

These are never gated:

- data operations (key-values, locks, sequences, transactions, watches);
- read-only views: membership, placement, ranges, routing metadata, the backup list and a backup chain, the
  dashboard and `/v1/cluster/health`;
- node-to-node traffic (`Backups.CaptureBackupPart` and the forwarding streams). Node mTLS covers it; see
  `docs/node-transport-security-guide.md`.

## Turn it on

1. Generate a key of at least 32 characters:

   ```sh
   openssl rand -base64 32 > /etc/kahuna/admin-keys
   chmod 600 /etc/kahuna/admin-keys
   ```

2. Start every node with the same file:

   ```sh
   kahuna-server ... --admin-api-key-file /etc/kahuna/admin-keys
   ```

3. Give the key to the admin callers only:

   - `kahuna-control --admin-key-file /etc/kahuna/admin-keys ...`
   - `KahunaClient`: set `KahunaOptions.AdminApiKey`. The client sends the key with its admin calls, and
     with no other call.
   - Any other HTTP caller: send the header `X-Kahuna-Admin-Key: <key>`. Any other gRPC caller: send the
     metadata entry `x-kahuna-admin-key`.

A call without a valid key gets `401 Unauthorized` (REST) or `StatusCode.Unauthenticated` (gRPC). The
operation does not run. The node logs the refusal at Warning with the operation and the caller's address,
and never logs the key.

Without `--admin-api-key-file` the operations are open to any caller of the client port, and the node logs
a warning at startup.

## The key file

- One key per line. Blank lines and lines that start with `#` are ignored.
- Every key must have at least 32 characters.
- A file that cannot be read, that holds no key, or that holds a short key stops the node at startup. A
  typo cannot leave the operations open while you believe they are closed.
- Keep the file readable only by the server user. Do not pass keys on the command line: the command line is
  visible in `ps`.

`kahuna-control --admin-key-file` reads the first key in the same format, so one file can serve both.

## Rotation

1. Add the new key as a second line on every node, and restart the nodes one at a time. Both keys now work.
2. Move every admin caller to the new key.
3. Remove the old key from the file on every node, and restart the nodes one at a time.

## Embedded nodes and custom hosts

`EmbeddedKahunaOptions.AdminApiKeyFile` carries the path. The gate itself lives in the host that serves the
REST routes and the gRPC services: such a host must call `services.AddAdminGate(AdminGate.LoadKeys(path))`.
A host that does not call it leaves the operations open.
