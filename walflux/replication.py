"""Thin wrapper over psycopg2's logical replication protocol client."""

from __future__ import annotations

import logging
import time
from collections.abc import Iterator
from select import select

import psycopg2
from psycopg2.extras import LogicalReplicationConnection

logger = logging.getLogger("walflux.replication")

#: select() timeout — lets the consumer flush idle batches and notice shutdown.
_WAKE_SECONDS = 1.0
#: Send standby status at least this often, even with no progress to report.
_KEEPALIVE_SECONDS = 10.0


class ReplicationStream:
    """One started pgoutput stream on a logical replication slot.

    ``messages()`` yields raw XLogData payloads for :func:`protocol.decode_message`.
    ``ack`` reports a flush position to the server, which then may discard the
    WAL before it — so it must be called only after the target transaction
    containing the matching checkpoint has committed.
    """

    def __init__(self, dsn: str, slot: str, publication: str) -> None:
        self._conn = psycopg2.connect(dsn, connection_factory=LogicalReplicationConnection)
        self._cursor = self._conn.cursor()
        self._cursor.start_replication(
            slot_name=slot,
            decode=False,
            options={"proto_version": "1", "publication_names": publication},
        )
        self._flush_lsn = 0
        self._last_feedback = time.monotonic()
        self._stopping = False
        logger.info("replication started on slot %r (publication %r)", slot, publication)

    def messages(self, wake_seconds: float = _WAKE_SECONDS) -> Iterator[tuple[int, bytes] | None]:
        """Yield ``(wal_end, payload)`` per XLogData, or ``None`` on a wake.

        A ``None`` is yielded whenever *wake_seconds* elapse without a decoded
        message — whether the stream is idle or a large message is trickling in
        — so the caller can honor its flush deadline and notice shutdown;
        keepalive feedback is sent every ~10s regardless of progress so the
        server never deems the client dead.
        """
        last_yield = time.monotonic()
        while not self._stopping:
            msg = self._cursor.read_message()
            if msg is not None:
                last_yield = time.monotonic()
                yield msg.wal_end, bytes(msg.payload)
                continue
            self._keepalive_if_due()
            remaining = wake_seconds - (time.monotonic() - last_yield)
            if remaining > 0:
                select([self._cursor], [], [], remaining)
            if time.monotonic() - last_yield >= wake_seconds and not self._stopping:
                last_yield = time.monotonic()
                yield None

    def ack(self, flush_lsn: int) -> None:
        """Report *flush_lsn* as durably applied.

        Called ONLY after the target transaction containing the matching
        checkpoint has committed: feedback is what lets the server discard
        WAL, and WAL must outlive any state a crashed consumer could need.
        """
        self._flush_lsn = flush_lsn
        self._cursor.send_feedback(flush_lsn=flush_lsn, force=True)
        self._last_feedback = time.monotonic()

    def stop(self) -> None:
        """Ask ``messages()`` to finish; safe to call from a signal handler."""
        self._stopping = True

    def close(self) -> None:
        """Stop the stream and close the replication connection."""
        self._stopping = True
        for closeable in (self._cursor, self._conn):
            try:
                closeable.close()
            except Exception:  # noqa: BLE001 - best-effort teardown
                pass

    def _keepalive_if_due(self) -> None:
        if time.monotonic() - self._last_feedback >= _KEEPALIVE_SECONDS:
            # Re-report the last flushed position (0 = "no information" to the
            # server); force=True sends the packet immediately.
            self._cursor.send_feedback(flush_lsn=self._flush_lsn, force=True)
            self._last_feedback = time.monotonic()
