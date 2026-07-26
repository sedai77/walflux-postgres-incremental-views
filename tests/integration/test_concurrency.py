"""Concurrency stress: racing writers + rapid add/delete churn + kill -9.

What this proves beyond ``test_end_to_end``: logical decoding serializes
concurrent writers by commit order and WalFlux's whole-transaction batching
preserves that order — so aggregates converge to the exact ``GROUP BY`` truth
even while six sessions race each other with interleaved inserts, group-moving
updates, overlapping deletes, same-transaction insert+delete pairs (net-zero
transactions), and whole groups being emptied and immediately recreated. A
SIGKILL lands mid-storm, with the writers continuing against a dead consumer,
to confirm crash recovery composes with concurrency.

Writers tolerate deadlocks (rollback and continue): concurrent multi-row
updates with overlapping predicates make them possible, and an aborted
transaction never reaches the WAL — so tolerating them is both realistic and
harmless to the ground-truth comparison.
"""

from __future__ import annotations

import os
import random
import secrets
import subprocess
import threading
import time
from decimal import Decimal
from pathlib import Path

import psycopg2
import psycopg2.errorcodes
import pytest
from test_end_to_end import _RUNNER, _start_daemon, _wait_slot_released

from walflux.bootstrap import setup, teardown
from walflux.common import parse_lsn
from walflux.config import load_config

DSN = os.environ.get("WALFLUX_TEST_DSN")

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not DSN,
        reason="WALFLUX_TEST_DSN not set (needs Postgres 15+ with wal_level=logical)",
    ),
]

GROUPS = ["alpha", "beta", "gamma", "delta", "epsilon", "zeta", None]
WRITERS = 6
ITERATIONS = 80


def _config_yaml(schema: str, slot: str, grouped_view: str, global_view: str) -> str:
    aggregates = (
        "    aggregates:\n"
        "      - fn: count\n        as: n_items\n"
        "      - fn: count\n        column: tag\n        as: n_tagged\n"
        "      - fn: sum\n        column: val\n        as: total_val\n"
        "      - fn: avg\n        column: val\n        as: avg_val\n"
    )
    return (
        f'database:\n  dsn: "{DSN}"\n'
        f"slot: {slot}\n"
        f"publication: {slot}\n"
        "batch:\n  max_ms: 100\n  max_txns: 50\n"
        "views:\n"
        f"  - name: {grouped_view}\n"
        f"    source: {schema}.items\n"
        "    group_by: [grp]\n"
        f"{aggregates}"
        f"  - name: {global_view}\n"
        f"    source: {schema}.items\n"
        "    group_by: []\n"
        f"{aggregates}"
    )


def _writer(schema: str, seed: int, errors: list[str], deadlocks: list[int]) -> None:
    """One racing session: seeded RNG so the op mix is reproducible per thread."""
    rng = random.Random(seed)
    conn = psycopg2.connect(DSN)
    try:
        with conn.cursor() as cur:
            for i in range(ITERATIONS):
                try:
                    op = rng.random()
                    if op < 0.40:
                        # Burst insert into random groups; occasionally a brand-new
                        # unique group appears mid-stream.
                        rows = [
                            (
                                f"w{seed}g{i}" if rng.random() < 0.05 else rng.choice(GROUPS),
                                Decimal(rng.randrange(-500, 5000)) / 100
                                if rng.random() > 0.15
                                else None,
                                f"t{i}" if rng.random() < 0.3 else None,
                            )
                            for _ in range(rng.randrange(1, 8))
                        ]
                        cur.executemany(
                            f'INSERT INTO "{schema}".items (grp, val, tag) VALUES (%s, %s, %s)',
                            rows,
                        )
                    elif op < 0.60:
                        # Group-moving update over an overlapping id slice: racing
                        # writers contend on the same rows (row-lock serialization).
                        cur.execute(
                            f'UPDATE "{schema}".items SET grp = %s, val = %s '
                            "WHERE id %% 13 = %s",
                            (
                                rng.choice(GROUPS),
                                Decimal(rng.randrange(0, 9999)) / 100,
                                rng.randrange(13),
                            ),
                        )
                    elif op < 0.75:
                        cur.execute(
                            f'DELETE FROM "{schema}".items WHERE id %% 17 = %s',
                            (rng.randrange(17),),
                        )
                    elif op < 0.90:
                        # Rapid add/delete inside ONE transaction: nets to zero and
                        # must cancel exactly (a net-zero group may briefly exist as
                        # an upserted row but can never survive with rows <= 0).
                        cur.execute(
                            f'INSERT INTO "{schema}".items (grp, val, tag) '
                            "VALUES (%s, %s, %s) RETURNING id",
                            (rng.choice(GROUPS), Decimal("13.37"), "flash"),
                        )
                        fid = cur.fetchone()[0]
                        cur.execute(f'DELETE FROM "{schema}".items WHERE id = %s', (fid,))
                    else:
                        # Empty an entire group, sometimes recreating it immediately
                        # in the next transaction: exercises target-row delete then
                        # re-insert across (or within) batches.
                        grp = rng.choice(GROUPS[:-1])
                        cur.execute(f'DELETE FROM "{schema}".items WHERE grp = %s', (grp,))
                        if rng.random() < 0.5:
                            conn.commit()
                            cur.execute(
                                f'INSERT INTO "{schema}".items (grp, val, tag) '
                                "VALUES (%s, %s, %s)",
                                (grp, Decimal("1.00"), None),
                            )
                    conn.commit()
                    if rng.random() < 0.3:
                        time.sleep(rng.random() * 0.01)
                except psycopg2.Error as exc:
                    if exc.pgcode in (
                        psycopg2.errorcodes.DEADLOCK_DETECTED,
                        psycopg2.errorcodes.SERIALIZATION_FAILURE,
                    ):
                        deadlocks.append(1)
                        conn.rollback()
                    else:
                        raise
    except Exception as exc:  # noqa: BLE001 - surfaced via the errors list
        errors.append(f"writer {seed}: {exc!r}")
    finally:
        conn.close()


def _wait_caught_up(
    conn: psycopg2.extensions.connection, slot: str, schema: str, log_path: Path
) -> None:
    """Same barrier as test_end_to_end._wait_caught_up, for the items table."""
    deadline = time.monotonic() + 90.0
    with conn.cursor() as cur:
        cur.execute("SELECT pg_current_wal_lsn()::text")
        target = parse_lsn(cur.fetchone()[0])
        cur.execute(
            f'INSERT INTO "{schema}".items (grp, val, tag) VALUES (%s, %s, %s)',
            ("nudge", Decimal("1.00"), None),
        )
        while True:
            cur.execute("SELECT commit_lsn FROM walflux.checkpoint WHERE slot = %s", (slot,))
            row = cur.fetchone()
            if row is not None and int(row[0]) > target:
                return
            if time.monotonic() > deadline:
                tail = log_path.read_text()[-2000:] if log_path.exists() else "<no log>"
                pytest.fail(f"daemon did not catch up (checkpoint={row}); log tail:\n{tail}")
            time.sleep(0.25)


def _assert_views_match(
    conn: psycopg2.extensions.connection, schema: str, grouped_view: str, global_view: str
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT grp, count(*), count(tag), sum(val), avg(val) "
            f'FROM "{schema}".items GROUP BY grp ORDER BY grp NULLS LAST'
        )
        truth = cur.fetchall()
        cur.execute(
            f"SELECT grp, n_items, n_tagged, total_val, avg_val "
            f'FROM walflux."{grouped_view}" ORDER BY grp NULLS LAST'
        )
        got = cur.fetchall()
        assert len(truth) >= 5, "churn should leave several live groups"
        assert got == truth

        cur.execute(f'SELECT count(*), count(tag), sum(val), avg(val) FROM "{schema}".items')
        truth_global = cur.fetchall()
        cur.execute(
            f'SELECT n_items, n_tagged, total_val, avg_val FROM walflux."{global_view}"'
        )
        assert cur.fetchall() == truth_global

        cur.execute(f'SELECT count(*) FROM walflux."{grouped_view}" WHERE __walflux_rows <= 0')
        assert cur.fetchone()[0] == 0, "churned-empty groups must be deleted, never negative"


def test_concurrent_writers_rapid_churn(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("WALFLUX_DSN", raising=False)
    suffix = secrets.token_hex(3)
    schema = f"wf_conc_{suffix}"
    slot = f"walflux_conc_{suffix}"
    grouped_view = f"conc_by_grp_{suffix}"
    global_view = f"conc_global_{suffix}"
    log_path = tmp_path / "daemon.log"

    admin = psycopg2.connect(DSN)
    admin.autocommit = True
    config = None
    proc: subprocess.Popen[bytes] | None = None
    try:
        with admin.cursor() as cur:
            cur.execute(f'CREATE SCHEMA "{schema}"')
            cur.execute(
                f'CREATE TABLE "{schema}".items ('
                "id bigserial PRIMARY KEY, grp text, val numeric(12,2), tag text)"
            )
            cur.execute(
                f'INSERT INTO "{schema}".items (grp, val, tag) '
                "SELECT (ARRAY['alpha', 'beta', 'gamma'])[1 + i % 3], i::numeric(12,2), "
                "CASE WHEN i % 5 = 0 THEN 't' || i END "
                "FROM generate_series(1, 40) AS g(i)"
            )

        cfg_path = tmp_path / "walflux.yaml"
        cfg_path.write_text(_config_yaml(schema, slot, grouped_view, global_view))
        config = load_config(str(cfg_path))
        setup(config, force=True)

        runner = tmp_path / "run_daemon.py"
        runner.write_text(_RUNNER)
        proc = _start_daemon(runner, cfg_path, log_path)

        errors: list[str] = []
        deadlocks: list[int] = []
        threads = [
            threading.Thread(target=_writer, args=(schema, seed, errors, deadlocks))
            for seed in range(WRITERS)
        ]
        for t in threads:
            t.start()

        # SIGKILL while all six writers keep hammering: WAL accumulates against
        # a dead consumer, then the restarted daemon must catch up through the
        # full interleaved backlog without double-applying the pre-kill batches.
        time.sleep(1.5)
        proc.kill()
        proc.wait(timeout=10)
        _wait_slot_released(admin, slot)
        proc = _start_daemon(runner, cfg_path, log_path)

        for t in threads:
            t.join(timeout=120)
            assert not t.is_alive(), "writer thread hung"
        assert not errors, f"writer failures: {errors}"

        _wait_caught_up(admin, slot, schema, log_path)
        _assert_views_match(admin, schema, grouped_view, global_view)

        # The churn should have actually exercised contention at least once in
        # a while; zero deadlocks across all runs is fine, this is informational.
        print(f"writers finished: {WRITERS * ITERATIONS} txn attempts, "
              f"{len(deadlocks)} deadlock rollbacks")
    finally:
        if proc is not None and proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=10)
        if config is not None:
            try:
                teardown(config)
            except Exception:  # noqa: BLE001 - best-effort cleanup
                pass
        with admin.cursor() as cur:
            cur.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        admin.close()
