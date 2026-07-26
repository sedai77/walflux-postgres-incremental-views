# Changelog

All notable changes to WalFlux are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions follow
[semver](https://semver.org/) with the usual 0.x caveat: breaking changes may
land in minor versions (0.2.0), fixes in patch versions (0.1.1). Anything
behavior-affecting — and anything that requires `walflux setup --force` — is
called out explicitly in its entry.

## [Unreleased]

## [0.1.0] - 2026-07-26

Initial release.

### Added

- `pgoutput` (protocol v1) decoder for logical replication messages.
- Incrementally maintained `count` / `sum` / `avg` aggregate tables with
  `GROUP BY` — including NULL group keys and global (no `group_by`) views.
- Exactly-once apply: each batch's deltas and its replication checkpoint commit
  in one target transaction; redelivered transactions are discarded by commit
  LSN. Proven across `kill -9` by the integration suite and `make kill9`.
- Consistent snapshot backfill: `walflux setup` builds targets from the slot's
  exported snapshot, so backfill and stream meet with no gap and no overlap.
- CLI: `walflux setup` / `run` / `status` / `teardown`, plus
  `python -m walflux` as an alternative entry point.
- A PEP 561 `py.typed` marker: the package's type annotations are visible to
  mypy/pyright in downstream projects.
- A wrong or unreachable DSN fails as one tidy `cannot connect to database`
  error line instead of a raw psycopg2 traceback.

[Unreleased]: https://github.com/sedai77/walflux-postgres-incremental-views/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/sedai77/walflux-postgres-incremental-views/releases/tag/v0.1.0
