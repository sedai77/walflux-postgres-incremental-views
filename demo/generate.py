#!/usr/bin/env python3
"""Steady mixed write workload for the WalFlux demo.

Applies roughly 30 single-row transactions per second to the ``orders``
table: 70% inserts, 15% updates (each picks a fresh random status, so most
updates move a row between groups), 15% deletes. About one in ten totals
and two in three coupons are NULL, so the daemon's NULL handling is
exercised continuously. Prints a heartbeat line every 5 seconds and exits
cleanly on SIGINT/SIGTERM.
"""

from __future__ import annotations

import os
import random
import signal
import sys
import time
from decimal import Decimal
from types import FrameType

import psycopg2
import psycopg2.extensions

DSN = os.environ.get("WALFLUX_DSN", "postgresql://walflux:walflux@postgres:5432/demo")
OPS_PER_SEC = 30.0
HEARTBEAT_EVERY_S = 5.0
STATUSES = ("pending", "paid", "shipped", "cancelled")
COUPONS = ("SAVE10", "VIP", "BOGO")

_stop = False


def _request_stop(signum: int, frame: FrameType | None) -> None:
    global _stop
    _stop = True


def log(msg: str) -> None:
    """Print one generator log line, unbuffered so `docker logs` streams it."""
    print(f"[generator] {msg}", flush=True)


def connect() -> psycopg2.extensions.connection:
    """Connect to the demo database, retrying while Postgres finishes booting."""
    last_error: Exception | None = None
    for _ in range(30):
        try:
            return psycopg2.connect(DSN)
        except psycopg2.OperationalError as exc:
            last_error = exc
            time.sleep(1.0)
    raise SystemExit(f"[generator] could not connect to {DSN!r}: {last_error}")


def random_total() -> Decimal | None:
    """A plausible order total in dollars; NULL roughly 10% of the time."""
    if random.random() < 0.10:
        return None
    return Decimal(random.randint(500, 50_000)) / 100


def random_coupon() -> str | None:
    """A coupon code; NULL roughly two thirds of the time."""
    return random.choice(COUPONS) if random.random() < 0.35 else None


def do_insert(cur: psycopg2.extensions.cursor) -> None:
    """Insert one new order with random status/total/coupon."""
    cur.execute(
        "INSERT INTO orders (status, total, coupon) VALUES (%s, %s, %s)",
        (random.choice(STATUSES), random_total(), random_coupon()),
    )


def do_update(cur: psycopg2.extensions.cursor) -> None:
    """Rewrite one random order; the fresh status moves it between groups ~75% of the time."""
    cur.execute(
        "UPDATE orders SET status = %s, total = %s, coupon = %s"
        " WHERE id = (SELECT id FROM orders ORDER BY random() LIMIT 1)",
        (random.choice(STATUSES), random_total(), random_coupon()),
    )


def do_delete(cur: psycopg2.extensions.cursor) -> None:
    """Delete one random order (a no-op when the table happens to be empty)."""
    cur.execute(
        "DELETE FROM orders WHERE id = (SELECT id FROM orders ORDER BY random() LIMIT 1)"
    )


def main() -> int:
    """Run the paced workload loop until asked to stop."""
    signal.signal(signal.SIGINT, _request_stop)
    signal.signal(signal.SIGTERM, _request_stop)

    conn = connect()
    cur = conn.cursor()
    log(f"connected; writing ~{OPS_PER_SEC:.0f} single-row transactions/sec"
        " (70% insert, 15% update, 15% delete)")

    counts = {"insert": 0, "update": 0, "delete": 0}
    interval = 1.0 / OPS_PER_SEC
    start = time.monotonic()
    next_op = start
    next_beat = start + HEARTBEAT_EVERY_S

    while not _stop:
        roll = random.random()
        if roll < 0.70:
            do_insert(cur)
            counts["insert"] += 1
        elif roll < 0.85:
            do_update(cur)
            counts["update"] += 1
        else:
            do_delete(cur)
            counts["delete"] += 1
        conn.commit()  # one source transaction per operation

        now = time.monotonic()
        if now >= next_beat:
            next_beat = now + HEARTBEAT_EVERY_S
            cur.execute("SELECT count(*) FROM orders")
            live_rows = cur.fetchone()[0]
            conn.commit()
            total_ops = sum(counts.values())
            log(f"t={now - start:5.0f}s ops={total_ops}"
                f" (insert={counts['insert']} update={counts['update']}"
                f" delete={counts['delete']}) rate={total_ops / (now - start):.1f}/s"
                f" live_rows={live_rows}")

        # Pace to ~OPS_PER_SEC without accumulating sleep debt after stalls.
        next_op = max(next_op + interval, now - interval)
        time.sleep(max(0.0, next_op - time.monotonic()))

    total_ops = sum(counts.values())
    log(f"stopping cleanly after {total_ops} transactions"
        f" (insert={counts['insert']} update={counts['update']} delete={counts['delete']})")
    cur.close()
    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
