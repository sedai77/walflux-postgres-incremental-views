"""pgoutput protocol version 1 binary message decoder.

Decodes the payload bytes of XLogData frames produced by the built-in
``pgoutput`` logical decoding plugin, as specified in the PostgreSQL
documentation ("Logical Replication Message Formats", PG 15/16, protocol
version 1). All integers are big-endian; strings are NUL-terminated UTF-8;
timestamps are microseconds since 2000-01-01 00:00:00 UTC.

Protocol version 1 only: streamed in-progress transactions (protocol >= 2)
are never negotiated, so no message carries the optional leading Xid field
and binary ('b') tuple columns never appear.
"""

from __future__ import annotations

import struct
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from walflux.common import UNCHANGED_TOAST, ProtocolError, format_lsn, parse_lsn

__all__ = [
    "UNCHANGED_TOAST",
    "Begin",
    "Column",
    "ColumnValue",
    "Commit",
    "Delete",
    "Insert",
    "Message",
    "Origin",
    "Relation",
    "Truncate",
    "TupleData",
    "TypeInfo",
    "Update",
    "convert_value",
    "decode_message",
    "format_lsn",
    "parse_lsn",
]

#: A column value in a decoded tuple: None (NULL) | UNCHANGED_TOAST | str (text format).
ColumnValue = object | str | None
TupleData = tuple[ColumnValue, ...]

_PG_EPOCH = datetime(2000, 1, 1, tzinfo=timezone.utc)

_REPLICA_IDENTITIES = frozenset("dnfi")


@dataclass(frozen=True)
class Column:
    name: str
    type_oid: int
    type_mod: int
    is_key: bool  # bit 1 of the per-column flags byte


@dataclass(frozen=True)
class Relation:
    rel_id: int
    namespace: str  # "" in the wire format means pg_catalog; passed through as decoded
    name: str
    replica_identity: str  # one of 'd' (default), 'n' (nothing), 'f' (full), 'i' (index)
    columns: tuple[Column, ...]


@dataclass(frozen=True)
class Begin:
    final_lsn: int  # LSN of the transaction's commit record
    commit_ts: datetime  # UTC
    xid: int


@dataclass(frozen=True)
class Commit:
    flags: int
    commit_lsn: int
    end_lsn: int  # first LSN after this transaction — used for checkpoints
    commit_ts: datetime


@dataclass(frozen=True)
class Insert:
    rel_id: int
    new: TupleData


@dataclass(frozen=True)
class Update:
    rel_id: int
    old: TupleData | None  # present iff 'O' submessage (REPLICA IDENTITY FULL)
    key: TupleData | None  # present iff 'K' submessage (identity-index columns only)
    new: TupleData


@dataclass(frozen=True)
class Delete:
    rel_id: int
    old: TupleData | None  # 'O' variant
    key: TupleData | None  # 'K' variant


@dataclass(frozen=True)
class Truncate:
    options: int  # bit 0: CASCADE, bit 1: RESTART IDENTITY
    rel_ids: tuple[int, ...]


@dataclass(frozen=True)
class Origin:  # decoded and ignored by the daemon
    commit_lsn: int
    name: str


@dataclass(frozen=True)
class TypeInfo:  # 'Y' message; decoded and ignored by the daemon
    type_oid: int
    namespace: str
    name: str


Message = Begin | Commit | Relation | Insert | Update | Delete | Truncate | Origin | TypeInfo


class _Reader:
    """Bounds-checked cursor over one message buffer.

    Every read raises :class:`ProtocolError` naming the message type and the
    offset where data ran out, so truncation errors are self-diagnosing.
    """

    __slots__ = ("buf", "kind", "pos")

    def __init__(self, buf: bytes, kind: str) -> None:
        self.buf = buf
        self.kind = kind
        self.pos = 1  # past the message-type byte

    def take(self, n: int) -> bytes:
        end = self.pos + n
        if end > len(self.buf):
            raise ProtocolError(
                f"truncated {self.kind!r} message: need {n} byte(s) at offset {self.pos}, "
                f"buffer has {len(self.buf)}"
            )
        chunk = self.buf[self.pos : end]
        self.pos = end
        return chunk

    def tag(self) -> str:
        return chr(self.take(1)[0])

    def uint8(self) -> int:
        return self.take(1)[0]

    def uint16(self) -> int:
        return struct.unpack(">H", self.take(2))[0]

    def int32(self) -> int:
        return struct.unpack(">i", self.take(4))[0]

    def uint32(self) -> int:
        return struct.unpack(">I", self.take(4))[0]

    def uint64(self) -> int:
        return struct.unpack(">Q", self.take(8))[0]

    def timestamp(self) -> datetime:
        """Int64 microseconds since the Postgres epoch, as an aware UTC datetime."""
        micros = struct.unpack(">q", self.take(8))[0]
        return _PG_EPOCH + timedelta(microseconds=micros)

    def cstring(self) -> str:
        end = self.buf.find(b"\x00", self.pos)
        if end < 0:
            raise ProtocolError(
                f"unterminated string in {self.kind!r} message at offset {self.pos}"
            )
        text = self.buf[self.pos : end].decode("utf-8")
        self.pos = end + 1
        return text


def _decode_tuple_data(r: _Reader) -> TupleData:
    """Int16 column count, then per column: 'n' NULL | 'u' unchanged TOAST |
    't' text (Int32 length + bytes)."""
    count = r.uint16()
    values: list[ColumnValue] = []
    for _ in range(count):
        category = r.tag()
        if category == "n":
            values.append(None)
        elif category == "u":
            values.append(UNCHANGED_TOAST)
        elif category == "t":
            length = r.int32()
            values.append(r.take(length).decode("utf-8"))
        elif category == "b":
            raise ProtocolError(
                f"binary column format 'b' in {r.kind!r} message at offset {r.pos - 1}: "
                "requires pgoutput protocol >= 2, but WalFlux negotiates version 1"
            )
        else:
            raise ProtocolError(
                f"unknown tuple column category {category!r} in {r.kind!r} message "
                f"at offset {r.pos - 1}"
            )
    return tuple(values)


def _expect_tag(r: _Reader, expected: str, got: str) -> None:
    if got != expected:
        raise ProtocolError(
            f"expected {expected!r} submessage in {r.kind!r} message at offset {r.pos - 1}, "
            f"got {got!r}"
        )


def _decode_begin(r: _Reader) -> Begin:
    return Begin(final_lsn=r.uint64(), commit_ts=r.timestamp(), xid=r.uint32())


def _decode_commit(r: _Reader) -> Commit:
    return Commit(
        flags=r.uint8(),
        commit_lsn=r.uint64(),
        end_lsn=r.uint64(),
        commit_ts=r.timestamp(),
    )


def _decode_origin(r: _Reader) -> Origin:
    return Origin(commit_lsn=r.uint64(), name=r.cstring())


def _decode_relation(r: _Reader) -> Relation:
    rel_id = r.uint32()
    namespace = r.cstring()
    name = r.cstring()
    replica_identity = r.tag()
    if replica_identity not in _REPLICA_IDENTITIES:
        raise ProtocolError(
            f"unknown replica identity {replica_identity!r} in 'R' message at offset {r.pos - 1}"
        )
    columns = []
    for _ in range(r.uint16()):
        # Per column: Int8 flags (bit 1: part of the key), String name,
        # Int32 type OID, Int32 type modifier (atttypmod).
        flags = r.uint8()
        col_name = r.cstring()
        type_oid = r.uint32()
        type_mod = r.int32()
        columns.append(
            Column(name=col_name, type_oid=type_oid, type_mod=type_mod, is_key=bool(flags & 1))
        )
    return Relation(
        rel_id=rel_id,
        namespace=namespace,
        name=name,
        replica_identity=replica_identity,
        columns=tuple(columns),
    )


def _decode_type(r: _Reader) -> TypeInfo:
    return TypeInfo(type_oid=r.uint32(), namespace=r.cstring(), name=r.cstring())


def _decode_insert(r: _Reader) -> Insert:
    rel_id = r.uint32()
    _expect_tag(r, "N", r.tag())
    return Insert(rel_id=rel_id, new=_decode_tuple_data(r))


def _decode_update(r: _Reader) -> Update:
    rel_id = r.uint32()
    old: TupleData | None = None
    key: TupleData | None = None
    tag = r.tag()
    if tag == "K":
        key = _decode_tuple_data(r)
        tag = r.tag()
    elif tag == "O":
        old = _decode_tuple_data(r)
        tag = r.tag()
    _expect_tag(r, "N", tag)
    return Update(rel_id=rel_id, old=old, key=key, new=_decode_tuple_data(r))


def _decode_delete(r: _Reader) -> Delete:
    rel_id = r.uint32()
    tag = r.tag()
    if tag == "K":
        return Delete(rel_id=rel_id, old=None, key=_decode_tuple_data(r))
    if tag == "O":
        return Delete(rel_id=rel_id, old=_decode_tuple_data(r), key=None)
    raise ProtocolError(
        f"expected 'K' or 'O' submessage in 'D' message at offset {r.pos - 1}, got {tag!r}"
    )


def _decode_truncate(r: _Reader) -> Truncate:
    count = r.uint32()
    options = r.uint8()
    return Truncate(options=options, rel_ids=tuple(r.uint32() for _ in range(count)))


_DECODERS = {
    "B": _decode_begin,
    "C": _decode_commit,
    "O": _decode_origin,
    "R": _decode_relation,
    "Y": _decode_type,
    "I": _decode_insert,
    "U": _decode_update,
    "D": _decode_delete,
    "T": _decode_truncate,
}


def decode_message(buf: bytes) -> Message:
    """Decode one pgoutput protocol-version-1 message.

    ``buf`` is the payload of a single XLogData frame. Raises
    :class:`~walflux.common.ProtocolError` for unknown message types,
    truncated buffers, or protocol >= 2 constructs.
    """
    if not buf:
        raise ProtocolError("empty pgoutput message buffer")
    kind = chr(buf[0])
    decoder = _DECODERS.get(kind)
    if decoder is None:
        raise ProtocolError(
            f"unknown pgoutput message type {kind!r} (byte 0x{buf[0]:02X}) at offset 0"
        )
    return decoder(_Reader(buf, kind))


_INT_OIDS = frozenset({20, 21, 23, 26})  # int8, int2, int4, oid
_FLOAT_OIDS = frozenset({700, 701})  # float4, float8
_BOOL_OID = 16
_NUMERIC_OID = 1700


def convert_value(text: str | None, type_oid: int) -> bool | int | float | Decimal | str | None:
    """Convert a text-format column value to a Python value by type OID.

    NULL stays ``None``; bool/int/float/numeric get native types; every other
    type is returned as its unchanged text representation. Callers must handle
    :data:`UNCHANGED_TOAST` before calling this.
    """
    if text is None:
        return None
    if type_oid == _BOOL_OID:
        return text == "t"
    if type_oid in _INT_OIDS:
        return int(text)
    if type_oid in _FLOAT_OIDS:
        return float(text)
    if type_oid == _NUMERIC_OID:
        return Decimal(text)
    return text
