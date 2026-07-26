# Security Policy

WalFlux runs with a privileged database role and turns config- and WAL-derived
data into SQL. We take that seriously; please report problems privately.

## Reporting a vulnerability

Use GitHub's private reporting:
[Security → Report a vulnerability](https://github.com/sedai77/walflux-postgres-incremental-views/security/advisories/new).
Please do not open a public issue for anything you believe is exploitable.

You will get an acknowledgment within 7 days. We aim for a fix and a
coordinated advisory within 90 days, and will keep you in the loop on the
timeline.

## Supported versions

| Version | Supported |
|---|---|
| 0.1.x | yes |
| earlier | no |

## Scope

Every SQL identifier WalFlux emits goes through
`walflux.common.quote_ident()` (validate-then-quote) and every value is
parameterized — never interpolated. Particularly in scope: any path that gets
config- or replication-stream-controlled text into SQL outside those two
mechanisms, privilege escalation via the `walflux` schema, and leakage of the
DSN (credentials) into logs or error output.
