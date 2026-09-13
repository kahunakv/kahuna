Development-only certificates for the mutual TLS profile (`docker/local-mtls.yml`). One per node, so the
cluster exercises per-node identity rather than a shared certificate. Never use them outside development.

| Node    | SHA-256 thumbprint                                                 |
|---------|--------------------------------------------------------------------|
| kahuna1 | `915F02B125C299836F8E86AEF125EDEFD82AC100E8452816605FF1A6C197C6F3` |
| kahuna2 | `375A7AAF2F36DFBF45F519E540E91361A77BEF49AEA8BE25FDC7627AC11719A3` |
| kahuna3 | `C2327F352487F41C0C4C92B26DA6FD50CE60635241259455666EA4CC83F183EA` |

Regenerate a node's certificate (no password), then update its thumbprint here and in `docker/local-mtls.yml`:

```sh
openssl req -x509 -newkey rsa:2048 -nodes -keyout node1.key -out node1.crt -days 3650 -config node1.cnf
openssl pkcs12 -export -out node1.pfx -inkey node1.key -in node1.crt -passout pass:
openssl x509 -in node1.crt -noout -fingerprint -sha256
rm node1.key
```
