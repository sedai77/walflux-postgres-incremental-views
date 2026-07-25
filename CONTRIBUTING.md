# Contributing to WalFlux

Thanks for helping out. This is a small, sharply-scoped codebase — the fastest way to
contribute well is to read [`docs/SPEC.md`](docs/SPEC.md) first. It is the authoritative
contract for every module's interface, and pull requests are reviewed against it.
The reasoning behind the design lives in [`DESIGN.md`](DESIGN.md).

## Development setup

WalFlux uses [uv](https://docs.astral.sh/uv/) for everything. There is nothing to
"install" beyond cloning:

```bash
git clone https://github.com/nexzensoftware-ship-it/walflux.git
cd walflux
uv run --python 3.12 --extra dev pytest -m "not integration"
```

`uv run` resolves and caches the environment (runtime deps: `psycopg2-binary`,
`PyYAML`; dev deps: `pytest`, `ruff`) on first use.

## Running the tests

Unit tests need no database:

```bash
uv run --python 3.12 --extra dev pytest -m "not integration"
```

The integration suite needs a Postgres 15+ with `wal_level=logical` and a
superuser-ish DSN in `WALFLUX_TEST_DSN` (it creates replication slots, publications,
and scratch schemas). The easiest way is docker:

```bash
docker run -d --name walflux-test-pg \
  -e POSTGRES_PASSWORD=postgres -p 5432:5432 postgres:16 \
  -c wal_level=logical -c max_replication_slots=8 -c max_wal_senders=8

WALFLUX_TEST_DSN=postgresql://postgres:postgres@localhost:5432/postgres \
  uv run --python 3.12 --extra dev pytest -m integration -v

docker rm -f walflux-test-pg   # when done
```

Without `WALFLUX_TEST_DSN` the integration tests skip with an explanatory reason —
that is expected locally.

CI runs three jobs on every push and pull request: `ruff check`, the unit suite on
Python 3.10 and 3.13 (the oldest and newest supported), and the integration suite
against Postgres 16. All three must pass.

## Style

- `ruff check .` must be clean (`uv run --python 3.12 --extra dev ruff check .`);
  configuration lives in `pyproject.toml`.
- Full type hints, `from __future__ import annotations` at the top of every module.
- Docstrings on public functions. Comments are reserved for invariants the code
  cannot express (the skip rule, the snapshot handshake, feedback ordering).
- SQL identifiers go through `walflux.common.quote_ident()`; values are always
  parameterized (`%s`), never interpolated.
- No new runtime dependencies. `psycopg2-binary` + `PyYAML` is the complete list,
  and keeping it that way is a feature. psycopg2 imports stay inside the modules
  that talk to the database (`replication.py`, `bootstrap.py`, `checkpoint.py`,
  `daemon.py`) so everything else is importable without a database.

## Making changes

1. Check whether your change conflicts with [`docs/SPEC.md`](docs/SPEC.md). If it
   does, the PR should update the spec too — interface changes are spec changes.
2. Add or update tests. Behavior changes to the protocol decoder, delta arithmetic,
   or config validation need unit tests; anything touching the apply/checkpoint/ack
   path needs to keep the kill -9 integration test honest.
3. Keep PRs focused. Small, reviewable diffs with a clear "why" in the description
   land quickly.

Bug reports are most useful with: Postgres version, WalFlux version, the config's
`views:` section, and daemon logs around the failure (the daemon logs every flush
with its LSN, which usually pinpoints the moment things went wrong).

## Repository metadata (for maintainers)

Suggested GitHub "About" description:

> Millisecond-fresh materialized views for Postgres via logical replication —
> no extensions, no triggers, no second database.

Suggested topics: `postgres`, `postgresql`, `materialized-views`,
`incremental-view-maintenance`, `change-data-capture`, `logical-replication`,
`pgoutput`, `cdc`, `streaming`, `python`.
