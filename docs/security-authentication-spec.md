# Security and Authentication Spec

## Goal

Define and implement a layered security model for Kahuna that covers:

1. **Transport security** — TLS on all wire connections with proper certificate validation.
2. **Node-to-node authentication** — every inter-node request (Raft via Kommander, and
   Kahuna's own gRPC inter-node batching) proves cluster membership before any state is
   mutated.
3. **Client authentication** — external callers identify themselves before accessing
   locks, key-value pairs, or sequences.

Today Kahuna runs with insecure defaults: certificate validation is unconditionally
bypassed on both server and client, no client credentials are checked, and inter-node
Kahuna traffic (outside of Raft) carries no authentication. This spec closes those gaps
incrementally and establishes an operational security baseline.

---

## Current State

### Transport layer
- Kestrel binds HTTPS on port 2071 using a configured certificate, giving encrypted
  channels.
- `FlurlHttp.Clients.WithDefaults(...)` in `Program.cs` sets
  `ServerCertificateCustomValidationCallback = (a, b, c, d) => true` — validation is
  off for all Flurl REST clients by default.
- `GrpcInterNodeCommunication.cs` sets
  `RemoteCertificateValidationCallback = delegate { return true; }` for gRPC channels.
- `KahunaClient / GrpcCommunication.cs` (client library) likewise returns `true` for all
  server certificates.

### Node authentication
- Raft inter-node requests go through Kommander, which now supports
  `RaftNodeAuthenticationMode.SharedSecret` (HMAC-SHA256) and `MutualTls` (reserved).
  Kahuna does not yet wire any `TransportSecurity` options into the `RaftConfiguration`
  it builds.
- Kahuna's own inter-node gRPC batching (locks, key-value) carries no authentication
  headers; any host on the network can send signed-looking Raft traffic or call internal
  endpoints.

### Client authentication
- No middleware, interceptor, or filter validates client credentials on any gRPC or REST
  endpoint.
- `KahunaClient` has no credential configuration surface.
- Protobuf messages carry no auth fields.

---

## Non-Goals

- No per-user RBAC in the initial implementation; authentication grants full cluster
  access.
- No message-level encryption beyond TLS.
- No in-memory transport authentication (single-process testing).
- No authentication on internal in-process calls between handlers.
- No OAuth2 / OIDC integration in the first phase (reserved for a future phase).
- Do not expose cluster-internal endpoints to the public internet; network isolation
  remains the first line of defense.

---

## Security Model

Two distinct caller populations require different treatment:

### Population 1: Cluster nodes (inter-node)
Nodes in the same cluster are peers. They must prove they belong to the same cluster
before a receiving node processes any Raft vote, append, handshake, lock replication,
or key-value mutation.

The mechanism mirrors Kommander's proven design:
- **SharedSecret** (default): HMAC-SHA256 over a canonical request representation using a
  cluster-wide shared secret. Simple to deploy and already battle-tested in Kommander.
- **MutualTls** (optional, future): Kestrel requires and validates client certificates for
  inter-node endpoints.

### Population 2: External clients (client-to-server)
Client applications connect via `KahunaClient`. They need a credential to prove they are
authorized to access the cluster.

The mechanism for phase 1:
- **API key** (static bearer token): A pre-shared key sent in every request as an HTTP
  header / gRPC metadata entry. Simple, easy to rotate, supports multiple keys for
  key-per-service.

Both populations rely on TLS for transport confidentiality. Neither mechanism is a
substitute for the other; they protect different trust boundaries.

---

## Configuration Types

### Cluster-node security (server-side and inter-node)

Extend `KahunaCommandLineOptions` and a new `KahunaSecurityOptions` class:

```csharp
public enum KahunaClientAuthenticationMode
{
    Disabled = 0,      // No client credential required (default, not for production)
    ApiKey   = 1,      // Static bearer/API key
}

public sealed class KahunaSecurityOptions
{
    // ------ Client authentication ------
    public KahunaClientAuthenticationMode ClientAuthMode { get; set; } =
        KahunaClientAuthenticationMode.Disabled;

    // One or more accepted API keys (any match is valid).
    // Multiple keys support rotation: add the new key, roll nodes, remove the old key.
    public IReadOnlyCollection<string> ApiKeys { get; set; } = [];

    public string ClientAuthHeaderName { get; set; } = "X-Kahuna-Api-Key";

    // ------ TLS / transport ------
    public bool RequireTls { get; set; } = true;
    public bool AllowInsecureCertificateValidation { get; set; } = false;
    public IReadOnlyCollection<string> TrustedServerCertificateThumbprints { get; set; } = [];

    // ------ Inter-node (non-Raft) ------
    // Re-uses the same SharedSecret and HMAC approach as Kommander.
    // Leave null to inherit from RaftTransportSecurityOptions.SharedSecret.
    public string? InterNodeSharedSecret { get; set; }
    public string InterNodeAuthHeaderName { get; set; } = "X-Kahuna-Node-Auth";
    public bool ShareRaftSecret { get; set; } = true;   // inherit from Raft config by default
}
```

### Raft-layer security (delegated to Kommander)

Wire `KahunaCommandLineOptions` values into `RaftConfiguration.TransportSecurity`:

```csharp
RaftTransportSecurityOptions raftSecurity = new()
{
    NodeAuthenticationMode = opts.RaftNodeAuthMode,   // new CLI flag
    SharedSecret           = opts.RaftSharedSecret,   // existing --raft-http-auth-bearer-token replacement
    RequireTls             = opts.RaftRequireTls,
    AllowInsecureCertificateValidation = opts.AllowInsecureCertificateValidation,
    TrustedServerCertificateThumbprints = opts.TrustedServerCertThumbprints,
};
```

---

## Client Authentication Protocol (API Key)

### Header / metadata

| Transport | Credential location |
|-----------|---------------------|
| gRPC      | `Metadata` entry: `x-kahuna-api-key: <key>` |
| REST      | HTTP header: `X-Kahuna-Api-Key: <key>` |

The key is sent in plaintext — TLS provides confidentiality. Do not log key values.

### Server validation

1. If `ClientAuthMode == Disabled`, skip validation (emit startup warning).
2. Check that the header/metadata entry is present; return `401` / `Unauthenticated` if
   absent.
3. Compare the supplied key against each entry in `ApiKeys` using fixed-time comparison
   (`CryptographicOperations.FixedTimeEquals`); accept if any matches.
4. Return `401` / `Unauthenticated` for no match. Do not reveal which keys exist.
5. Log authentication failures at `Warning` level (key prefix only, never full value).

### Key format

- Minimum 32 bytes of cryptographic randomness, base64url-encoded (43 chars minimum).
- Recommend 256-bit keys: `openssl rand -base64 32`.
- No structure or claims embedded in the key; treat as an opaque token.

---

## Inter-Node Authentication Protocol (HMAC-SHA256)

Kahuna's own inter-node gRPC batching (not the Raft layer) uses the same HMAC-SHA256
design already proven in Kommander. This avoids maintaining two different auth
implementations.

### Header / metadata fields

| Name | Content |
|------|---------|
| `X-Kahuna-Node-Auth` | base64url HMAC-SHA256 signature |
| `X-Kahuna-Node-Id`   | sender node identifier |
| `X-Kahuna-Node-Ts`   | Unix timestamp, milliseconds |
| `X-Kahuna-Node-Nonce`| random 128-bit nonce, base64url |

### Signature input (canonical string)

```text
grpc-method-name + "\n" +
sender-node-id   + "\n" +
timestamp        + "\n" +
nonce
```

Body hashing is omitted for streaming calls where the body is not available at
authentication time. For unary calls, append:

```text
+ "\n" + sha256hex(request-body-bytes)
```

### Signature algorithm

```text
HMACSHA256(UTF8(interNodeSharedSecret), UTF8(canonicalString))
```

Encode result as base64url.

### Validation rules

- Reject missing fields: `StatusCode.Unauthenticated`.
- Reject malformed base64url: `StatusCode.Unauthenticated`.
- Reject timestamp skew > 60 seconds (configurable): `StatusCode.Unauthenticated`.
- Reject replayed nonce (same sender + nonce within skew window): `StatusCode.Unauthenticated`.
- Compare HMAC using `CryptographicOperations.FixedTimeEquals`.
- If `RequireTls = true` and the inbound channel is plaintext: reject.
- Never log the secret, raw signature, or full header value.

### Secret sharing with Raft

When `ShareRaftSecret = true` (default), the inter-node HMAC secret is derived from the
same `RaftTransportSecurityOptions.SharedSecret`. Operators configure one cluster secret;
both Raft and Kahuna inter-node channels are protected. Set `InterNodeSharedSecret` to
override.

---

## Certificate Validation

Remove unconditional bypass callbacks; gate them behind explicit developer configuration.

### Current bypasses to fix

| File | Location | Fix |
|------|----------|-----|
| `Kahuna.Server/Program.cs` | `FlurlHttp...ServerCertificateCustomValidationCallback` | Remove; use proper handler |
| `Kahuna.Core/Communication/InterNode/GrpcInterNodeCommunication.cs` | `RemoteCertificateValidationCallback = delegate { return true; }` | Gate behind `AllowInsecureCertificateValidation` |
| `Kahuna.Client/Communication/GrpcCommunication.cs` | Same callback | Gate; expose option to `KahunaOptions` |
| `Kahuna.Client/Communication/RestCommunication.cs` | Same callback | Gate; expose option to `KahunaOptions` |

### Production behavior

1. Platform trust chain + hostname verification (default; no code change needed once
   bypasses are removed).
2. Certificate pinning: if `TrustedServerCertificateThumbprints` is non-empty, accept
   only certificates whose SHA-256 thumbprint (hex) is in the list.
3. Development bypass: `AllowInsecureCertificateValidation = true` must be explicitly
   set; emit a single `Warning` log line at startup.

### KahunaOptions (client library)

```csharp
public sealed class KahunaOptions
{
    // ... existing options ...
    public bool AllowInsecureCertificateValidation { get; set; } = false;
    public IReadOnlyCollection<string> TrustedServerCertificateThumbprints { get; set; } = [];
    public string? ApiKey { get; set; }
}
```

The `ApiKey` value is attached to every outgoing gRPC call as metadata and every REST
call as a header.

---

## Implementation Phases

### Phase 1 — Fix certificate validation (lowest risk, high value)
Remove bypass callbacks. Expose `AllowInsecureCertificateValidation` as an opt-in
developer flag. Update development Docker Compose files to use the flag explicitly.
This can be merged independently without changing the auth model.

**Acceptance:** A `KahunaClient` connecting to a node with the development self-signed
cert fails by default; succeeds with `AllowInsecureCertificateValidation = true` or with
a matching thumbprint configured.

### Phase 2 — Wire Raft/Kommander shared-secret auth
Pass `RaftTransportSecurityOptions` with `NodeAuthenticationMode.SharedSecret` and a
configured secret into `RaftManager` from `Program.cs`. Add CLI flags
`--raft-node-auth-mode` and `--raft-node-shared-secret`. The existing bearer-token option
(`--raft-http-auth-bearer-token`) becomes a deprecated alias that is mapped to
`SharedSecret` when the new option is absent.

Validation happens entirely inside Kommander. Kahuna's task is to configure it correctly.

**Acceptance:** Two-node cluster with matching secrets elects a leader. Node with wrong
secret is rejected by the leader.

### Phase 3 — Kahuna inter-node HMAC auth
Implement `KahunaNodeAuthenticator` (modelled on Kommander's `RaftTransportAuthenticator`)
for signing and validating Kahuna's own inter-node gRPC calls. Add a gRPC interceptor or
explicit validation at the entry point of `GrpcServerBatcher` and any internal-only gRPC
routes.

Add CLI flag `--kahuna-node-shared-secret` (defaults to inheriting from Raft secret).

**Acceptance:** Inter-node calls without valid HMAC headers are rejected before entering
the Kahuna handler pipeline.

### Phase 4 — Client API key authentication
Implement `KahunaApiKeyAuthenticator`. Add a gRPC server interceptor and an ASP.NET
endpoint filter for REST routes. Protect all public-facing endpoints:
- `/v1/kahuna/*` REST routes
- `Locker`, `KeyValues`, `Sequencer` gRPC services

Add CLI flags `--client-auth-mode` and `--client-api-key` (repeatable for multiple keys).
Add `KahunaOptions.ApiKey` in the client library.

**Acceptance:** A client without an API key receives `401 Unauthorized` / `StatusCode.Unauthenticated`.
A client with a valid key can perform all operations. A client with an invalid key is
rejected. Two keys are accepted simultaneously (rotation scenario).

---

## Command-Line Options

Add to `KahunaCommandLineOptions`:

```
# Raft-layer auth (delegated to Kommander)
--raft-node-auth-mode            Disabled|SharedSecret|MutualTls  (default: Disabled)
--raft-node-shared-secret        Cluster shared secret for Raft HMAC auth
--raft-node-auth-header          Custom header name (default: X-Kommander-Cluster-Auth)
--raft-require-tls               Require TLS for inter-node Raft (default: true)

# Kahuna inter-node auth
--node-auth-mode                 Disabled|SharedSecret             (default: Disabled)
--node-shared-secret             Secret for Kahuna inter-node HMAC (inherits Raft secret)
--node-auth-header               Custom header name (default: X-Kahuna-Node-Auth)

# Client auth
--client-auth-mode               Disabled|ApiKey                   (default: Disabled)
--client-api-key                 Accepted API key (repeatable for multiple keys)
--client-auth-header             Custom header name (default: X-Kahuna-Api-Key)

# TLS
--allow-insecure-certificate-validation  Skip cert validation (dev only)
--trusted-server-cert-thumbprint         SHA-256 thumbprint, hex (repeatable)
```

Fail fast at startup:
- `SharedSecret` mode selected with empty secret → error and exit.
- `ApiKey` mode selected with no `--client-api-key` values → error and exit.
- `RequireTls = true` but HTTP scheme is `http://` → error and exit.

---

## gRPC Interceptor Design

```csharp
// Server interceptor for client auth (Phase 4)
public sealed class KahunaAuthInterceptor : Interceptor
{
    public override async Task<TResponse> UnaryServerHandler<TRequest, TResponse>(
        TRequest request,
        ServerCallContext context,
        UnaryServerMethod<TRequest, TResponse> continuation)
    {
        ValidateClientAuth(context);
        return await continuation(request, context);
    }

    public override async Task<TResponse> ClientStreamingServerHandler<TRequest, TResponse>(
        IAsyncStreamReader<TRequest> requestStream,
        ServerCallContext context,
        ClientStreamingServerMethod<TRequest, TResponse> continuation)
    {
        ValidateClientAuth(context);
        return await continuation(requestStream, context);
    }

    // ... override ServerStreamingServerHandler, DuplexStreamingServerHandler ...

    private void ValidateClientAuth(ServerCallContext context)
    {
        if (_options.ClientAuthMode == KahunaClientAuthenticationMode.Disabled)
            return;

        string? key = context.RequestHeaders.GetValue(_options.ClientAuthHeaderName);
        if (!_authenticator.ValidateApiKey(key))
            throw new RpcException(new Status(StatusCode.Unauthenticated, "Invalid or missing API key"));
    }
}
```

Register in `Program.cs`:

```csharp
builder.Services.AddGrpc(opts =>
{
    opts.Interceptors.Add<KahunaAuthInterceptor>();
});
```

---

## REST Middleware Design

```csharp
// Endpoint filter for public REST routes (Phase 4)
public sealed class KahunaApiKeyEndpointFilter : IEndpointFilter
{
    public async ValueTask<object?> InvokeAsync(
        EndpointFilterInvocationContext context,
        EndpointFilterDelegate next)
    {
        if (_options.ClientAuthMode == KahunaClientAuthenticationMode.Disabled)
            return await next(context);

        string? key = context.HttpContext.Request.Headers[_options.ClientAuthHeaderName];
        if (!_authenticator.ValidateApiKey(key))
            return Results.Unauthorized();

        return await next(context);
    }
}
```

---

## Replay Attack Prevention (Inter-Node)

Identical design to Kommander:

- Global `ConcurrentDictionary<string, long>` keyed on
  `sha256hex(secret) + "\n" + sender + "\n" + nonce`.
- Value is the expiry Unix-ms (`timestamp + skewMs`).
- On validation: check presence → reject with `ReplayDetected`; insert with expiry → proceed.
- Background timer prunes expired entries every 30 seconds.
- Nonce namespace (the secret hash prefix) prevents cross-cluster replay if two clusters
  share a nonce by coincidence.

---

## Tests

### Unit tests

- `KahunaApiKeyAuthenticator`:
  - Returns `true` for any matching key in the configured set.
  - Uses fixed-time comparison (no short-circuit on mismatch).
  - Returns `false` for empty key, null key, wrong key.
  - Accepts second key during rotation (two keys configured).

- `KahunaNodeAuthenticator` (Phase 3):
  - HMAC signing is deterministic for fixed inputs.
  - Tampering with method, sender, timestamp, nonce, or body hash fails validation.
  - Stale timestamp is rejected.
  - Replayed nonce is rejected after first use.
  - Expired nonce is accepted again after skew window passes.

- `KahunaSecurityOptions`:
  - Fails validation when `ApiKey` mode is set with no keys.
  - Fails validation when `SharedSecret` mode is set with empty secret.

### Integration tests

- Client with correct API key reaches a `TrySetKV` handler.
- Client without API key receives `StatusCode.Unauthenticated`.
- Client with wrong API key receives `StatusCode.Unauthenticated`.
- Two-node cluster with matching node secrets accepts inter-node calls.
- Node with wrong secret has its inter-node calls rejected.
- `KahunaClient` with `AllowInsecureCertificateValidation = true` connects to a
  self-signed cert successfully.
- `KahunaClient` without that flag fails against self-signed cert.

---

## Reuse from Kommander

The following Kommander types can be copied or extracted into a shared library rather
than re-implemented:

| Kommander type | Purpose | Reuse strategy |
|---------------|---------|----------------|
| `RaftTransportAuthenticator` | HMAC sign/verify, replay cache | Copy into `Kahuna.Core` as `KahunaNodeAuthenticator`; adjust header names and canonical input |
| `RaftTransportAuthenticationHeaders` | Header/metadata struct | Copy; rename fields |
| `RaftTransportAuthenticationStatus` | Validation result enum | Copy; add `ApiKeyMissing`, `ApiKeyInvalid` |
| `RaftTransportAuthenticationResult` | Result wrapper | Copy |
| `RaftTransportSecurityOptions` | TLS config shape | Inherit or copy; `KahunaSecurityOptions` wraps it |

If Kommander is ever extracted into a standalone NuGet package with a public API for
these types, prefer a direct package reference instead of copying.

---

## Operational Guidance

### Secret generation

```bash
# 256-bit cluster secret (for both Raft and Kahuna inter-node)
openssl rand -base64 32

# 256-bit client API key
openssl rand -base64 32
```

### Secret distribution

- Use Docker secrets, Kubernetes `Secret` objects, or a secrets manager (Vault, AWS
  Secrets Manager).
- Never commit secrets to source control.
- Pass secrets via environment variables or mounted files, not CLI arguments on shared
  systems (CLI args appear in `ps` output).

### Secret rotation (API keys)

1. Add the new key to `--client-api-key` on all nodes (two keys active).
2. Roll all clients to use the new key.
3. Remove the old key from node configuration; restart nodes.

### Secret rotation (cluster secrets)

Use the same window approach: accept both primary and secondary secrets during
transition, then remove the old one. This requires a secondary-secret field in
`KahunaSecurityOptions` (can be added when needed).

### Deployment checklist

- [ ] HTTPS certificate from a trusted CA or pinned self-signed cert.
- [ ] `AllowInsecureCertificateValidation` is absent or `false` in production config.
- [ ] `--raft-node-auth-mode SharedSecret` with a strong random secret.
- [ ] `--node-auth-mode SharedSecret` (same or derived secret).
- [ ] `--client-auth-mode ApiKey` with at least one key configured.
- [ ] Raft and Kahuna inter-node endpoints bound to private network interfaces.
- [ ] Client-facing endpoints (2071) firewalled to permitted client CIDR ranges.

---

## Backward Compatibility

| Phase | Breaking? | Notes |
|-------|-----------|-------|
| Phase 1 (cert validation) | Yes for clients using self-signed certs without opt-in | Must update Docker Compose / test harness with explicit flag |
| Phase 2 (Raft auth) | No — `Disabled` default preserved | Existing clusters keep working; operators opt in |
| Phase 3 (inter-node auth) | No — `Disabled` default | Same opt-in model |
| Phase 4 (client auth) | No — `Disabled` default | Existing clients unaffected until operator enables |

Target for a future major release: make `SharedSecret` the default for both inter-node
modes and `ApiKey` the default for client auth. `Disabled` becomes an explicit opt-in
for trusted-network deployments.

---

## Done Criteria

- All certificate-validation bypasses are removed from non-dev code paths.
- Raft layer uses Kommander's `SharedSecret` mode when configured.
- Kahuna inter-node gRPC calls carry HMAC headers and are validated before handlers run.
- All public gRPC services and REST endpoints reject clients without a valid API key when
  `ClientAuthMode = ApiKey`.
- `KahunaClient` can be configured with an API key and with certificate pinning.
- Tests cover: wrong key, missing key, valid key, wrong node secret, replay, stale
  timestamp, cert bypass opt-in.
- Startup fails fast on misconfigured auth (mode set but no secret/key provided).
- README / operational docs describe the threat model and deployment checklist.
