# Kahuna node transport security guide

This guide explains how to authenticate traffic between Kahuna nodes with mutual TLS (mTLS). One
certificate per node authenticates every node-to-node connection: Kommander's Raft traffic and Kahuna's
own inter-node gRPC.

The shared mechanism (modes, thumbprint pinning, the validity window, the HMAC signature format) belongs
to Kommander. Its reference is `docs/node-transport-authentication-developer-guide.md` in the Kommander
repository. This guide covers what is specific to Kahuna.

---

## What is covered

| Traffic | `MutualTls` | `SharedSecret` |
|---|---|---|
| Raft over gRPC and REST (Kommander) | Authenticated | Authenticated |
| Key-value and lock forwarding (the `BatchServerKeyValueRequests` / `BatchServerLockRequests` streams) | Authenticated | **Not authenticated** |
| Sequence forwarding (unary calls with the `kahuna-forwarded` header) | Authenticated | **Not authenticated** |
| The unary 2PC participant methods (`TryPrepare*`, `TryCommit*`, `TryRollback*` on `KeyValuer`) | Authenticated | **Not authenticated** |
| Application clients (`KahunaClient`, REST, `Kahuna.Control`) | Not covered | Not covered |

Use `MutualTls` for full node-to-node coverage. `SharedSecret` protects Raft only, because Kahuna's own
messages carry no signature. The node logs a warning at startup in that mode.

**Administrative surfaces are not gated.** Leave, split, merge, replication factor, key-range
registration, backups, restore, the membership, placement, range and routing views, the dashboard and
`/v1/cluster/health` accept any caller that can reach the port, in every mode. `Kahuna.Control` reaches
them through `KahunaClient`, which cannot present a client certificate. Restrict these ports at the
network layer until application authentication exists.

---

## Quick start

On every node, with that node's own certificate:

```sh
kahuna-server \
  --initial-cluster node2:8082 node3:8082 \
  --raft-host node1 --raft-port 8082 \
  --https-certificate /etc/kahuna/node1.pfx \
  --https-ports 2071 8082 \
  --node-auth-mode MutualTls \
  --trusted-client-cert-thumbprint <node1> <node2> <node3> \
  --trusted-server-cert-thumbprint <node1> <node2> <node3> \
  --advertised-client-endpoint https://node1:2071 \
  --disable-peer-endpoint-advertisement
```

In this example, port 8082 is the cluster listener and port 2071 is the application listener. Mutual
TLS needs the two on separate ports, so the application port here is not the standalone default (8082).

`docker/local-mtls.yml` is a complete three-node example with per-node development certificates.

---

## The flags

| Flag | Meaning | Default |
|---|---|---|
| `--node-auth-mode` | `Disabled`, `SharedSecret` or `MutualTls`. | `Disabled` |
| `--client-certificate` | PKCS#12 certificate this node presents to its peers. | `--https-certificate` |
| `--client-certificate-password` | Password of `--client-certificate`. | `--https-certificate-password` when `--client-certificate` is not set |
| `--trusted-client-cert-thumbprint` | SHA-256 thumbprints of the peer certificates this node accepts. Space-separated. | none |
| `--trusted-server-cert-thumbprint` | SHA-256 thumbprints this node pins when it dials a peer. Space-separated. | none |
| `--node-shared-secret` | Secret for `SharedSecret` mode. | none |
| `--node-auth-header` | Signature header name for `SharedSecret` mode. | Kommander's default |
| `--node-require-tls` | `true` or `false`. Refuses node requests that did not arrive over TLS. | `true` |
| `--node-auth-clock-skew` | Accepted clock skew for signed requests, in seconds. | `60` |
| `--allow-plaintext-listener` | Binds the cleartext listeners beside HTTPS. | off |

Without `--trusted-server-cert-thumbprint`, a node validates its peers' server certificates against the
system trust store and the host name. Self-signed per-node certificates then fail that validation, so
pin them.

---

## The listeners

A node can bind four kinds of listener. Under mTLS they behave as follows:

| Listener | Flag | Under `MutualTls` |
|---|---|---|
| **Cluster listener** | the HTTPS port equal to `--raft-port` | Requires a client certificate in the TLS handshake. HTTP/1.1 and HTTP/2 only. |
| **Application listeners** | the other `--https-ports` | Server TLS only, as before. HTTP/3 stays available. |
| **Cleartext HTTP** | `--http-ports` | Not bound, unless `--allow-plaintext-listener`. |
| **Cleartext HTTP/2 (h2c)** | `--grpc-cleartext-ports` | Not bound, unless `--allow-plaintext-listener`. |

Point peers (`--initial-cluster`) at the cluster listener. Point application clients at an application
listener. A client without a certificate cannot complete a handshake with the cluster listener.

The cluster listener accepts any certificate at the TLS layer. The trust decision is the thumbprint
allow-list, which Kommander checks on each Raft request and Kahuna checks on each node-only request.
This is what lets self-signed per-node certificates work.

Every gRPC service is mapped on every listener, so the listener is not the only control. Each node-only
surface checks the peer certificate itself. A node-only request that arrives on an application listener
or a cleartext listener is refused with `Unauthenticated`, because those listeners never carry a client
certificate.

### Cleartext listeners changed for everyone

When an HTTPS certificate is configured, Kahuna no longer binds `--http-ports` (default 8081) or the
standalone h2c default (8083), in any mode. Pass `--allow-plaintext-listener` to keep them. A clustered
node that still puts `--raft-port` on a cleartext listener refuses to start and names that flag.

---

## Schemes

`--raft-grpc-scheme` and `--raft-http-scheme` must be `https://` under mTLS. The cleartext inter-node
mode (Raft on the h2c port with `http://`) is for development only and cannot carry a certificate.

---

## Routing hints under mTLS

A routing hint that names a peer is built from that peer's Raft endpoint. Under mTLS that address is the
cluster listener, and a `KahunaClient` that follows the hint fails its handshake. Therefore, a node with
routing hints enabled must set:

- `--advertised-client-endpoint` to its own application listener, and
- `--disable-peer-endpoint-advertisement`, because a node cannot derive a peer's application URL.

Clients then learn routes only from the node that answers. To turn hints off instead, pass
`--disable-routing-hints`.

---

## Thumbprints

A thumbprint is the SHA-256 hash of the DER-encoded certificate, in hexadecimal:

```sh
openssl x509 -in node1.crt -noout -fingerprint -sha256
openssl pkcs12 -in node1.pfx -nokeys -passin pass: | openssl x509 -noout -fingerprint -sha256
```

Colons, spaces and letter case are ignored. It is **not** the SHA-1 value that some tools and
`X509Certificate2.Thumbprint` show.

There is no chain building and no revocation check. A compromised certificate stays trusted until its
thumbprint is removed from every node and those nodes restart.

---

## One certificate or one per node

By default a node presents its HTTPS server certificate as its client certificate, so an existing
deployment needs no new key material.

If all nodes share one certificate, that certificate proves cluster membership, not node identity. It
cannot be revoked for one node, and it cannot be rotated one node at a time. Use one certificate per node
in production.

---

## Rotation

Rotation needs restarts. A node loads its certificate once, at startup.

1. Add the new thumbprint to `--trusted-client-cert-thumbprint` and `--trusted-server-cert-thumbprint` on
   every node.
2. Restart those nodes one at a time.
3. Switch the rotating node to the new certificate.
4. Restart that node.
5. Remove the old thumbprint from every node.
6. Restart those nodes one at a time.

The overlap in steps 1 to 5 lets the cluster roll one node at a time.

---

## Startup checks

A node refuses to start, with a message that names the flag to fix, when:

| Condition | Fix |
|---|---|
| `MutualTls` without `--https-certificate` | Set `--https-certificate`. |
| `MutualTls` with `--raft-allow-insecure-certificate-validation` | Remove that flag. |
| `MutualTls` without a usable `--trusted-client-cert-thumbprint` | Add the peers' thumbprints. An empty allow-list would trust any certificate. |
| `MutualTls` with `--raft-port` not in `--https-ports` | Add the Raft port to `--https-ports`. |
| `MutualTls` with `--raft-grpc-scheme` or `--raft-http-scheme` other than `https://` | Set both to `https://`. |
| `MutualTls` with routing hints on, but no `--advertised-client-endpoint` or no `--disable-peer-endpoint-advertisement` | Set both, or pass `--disable-routing-hints`. |
| `MutualTls` with `--http-ports` or `--grpc-cleartext-ports` | Pass `--allow-plaintext-listener`, or remove them. |
| `MutualTls` with a client certificate that cannot be loaded (missing file, wrong password, no private key) | Fix the certificate. The message names the file. |
| `SharedSecret` without `--node-shared-secret` | Set the secret. |
| A certificate is configured and `--raft-port` is only on a cleartext listener | Add the Raft port to `--https-ports`, or pass `--allow-plaintext-listener`. |

---

## Diagnosing refusals

A refused node-only call is logged on the receiving node at Warning level:

```
Node-only call /Sequencer/GetSequence rejected: CertificateUntrusted (remote 10.0.0.7)
```

Kommander logs refused Raft calls in the same way (`[RaftService] gRPC mTLS auth rejected for ...`). The
status tells you what to fix:

| Status | Meaning |
|---|---|
| `CertificateRequired` | No certificate on the connection: the call reached an application or cleartext listener, or the caller has no client certificate. |
| `CertificateUntrusted` | The thumbprint is not in the receiving node's allow-list. |
| `CertificateExpired` | The certificate is outside its validity window. |
| `TlsRequired` | The call did not arrive over TLS. |

A caller without any client certificate never reaches these checks on the cluster listener. Its TLS
handshake fails, and the caller sees a transport error.

---

## Embedded nodes and custom hosts

`EmbeddedKahunaOptions.TransportSecurity` passes the same options object to Kommander. Set
`ClientCertificate` on it to supply a certificate without a PKCS#12 file.

A host that serves Kahuna's gRPC services itself must:

- call `services.AddNodeTransportGate()` before it builds the application. `MapGrpcKahunaRoutes()` refuses
  to start without it;
- construct `GrpcInterNodeCommunication` with the transport-security options of the node's own
  `RaftConfiguration`. Kahuna and Kommander share one channel pool per peer URL, and the first caller's
  options decide whether that pool presents a certificate. Different options there can strip the
  certificate from Raft traffic too.
