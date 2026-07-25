"""Tests for the pgoutput protocol-version-1 decoder.

The ``encode_*`` helpers below build wire bytes with ``struct.pack`` following
the layouts in the PostgreSQL docs ("Logical Replication Message Formats",
PG 16, protocol version 1) — they double as executable format documentation.
"""

from __future__ import annotations

import struct
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from walflux.common import ProtocolError
from walflux.protocol import (
    UNCHANGED_TOAST,
    Begin,
    Column,
    Commit,
    Delete,
    Insert,
    Origin,
    Relation,
    Truncate,
    TypeInfo,
    Update,
    convert_value,
    decode_message,
    format_lsn,
    parse_lsn,
)

PG_EPOCH = datetime(2000, 1, 1, tzinfo=timezone.utc)


# --- wire-format builders -------------------------------------------------


def cstring(text: str) -> bytes:
    """String: NUL-terminated UTF-8."""
    return text.encode() + b"\x00"


def pg_micros(ts: datetime) -> int:
    """TimestampTz: Int64 microseconds since 2000-01-01 00:00:00 UTC."""
    return (ts - PG_EPOCH) // timedelta(microseconds=1)


def encode_tuple_data(values: tuple[object, ...]) -> bytes:
    """TupleData: Int16 column count, then per column a category byte:
    'n' NULL, 'u' unchanged TOAST, 't' text (Int32 length + bytes)."""
    out = struct.pack(">H", len(values))
    for value in values:
        if value is None:
            out += b"n"
        elif value is UNCHANGED_TOAST:
            out += b"u"
        else:
            assert isinstance(value, str)
            data = value.encode()
            out += b"t" + struct.pack(">i", len(data)) + data
    return out


def encode_begin(final_lsn: int, commit_ts: datetime, xid: int) -> bytes:
    """Begin: 'B', Int64 final LSN, Int64 commit timestamp, Int32 xid."""
    return b"B" + struct.pack(">QqI", final_lsn, pg_micros(commit_ts), xid)


def encode_commit(flags: int, commit_lsn: int, end_lsn: int, commit_ts: datetime) -> bytes:
    """Commit: 'C', Int8 flags, Int64 commit LSN, Int64 end LSN, Int64 timestamp."""
    return b"C" + struct.pack(">BQQq", flags, commit_lsn, end_lsn, pg_micros(commit_ts))


def encode_origin(commit_lsn: int, name: str) -> bytes:
    """Origin: 'O', Int64 origin commit LSN, String name."""
    return b"O" + struct.pack(">Q", commit_lsn) + cstring(name)


def encode_relation(
    rel_id: int,
    namespace: str,
    name: str,
    replica_identity: str,
    columns: list[tuple[int, str, int, int]],  # (flags, name, type_oid, type_mod)
) -> bytes:
    """Relation: 'R', Int32 OID, String namespace, String name, Int8 replica
    identity, Int16 column count, then per column: Int8 flags, String name,
    Int32 type OID, Int32 type modifier."""
    out = b"R" + struct.pack(">I", rel_id) + cstring(namespace) + cstring(name)
    out += replica_identity.encode() + struct.pack(">H", len(columns))
    for flags, col_name, type_oid, type_mod in columns:
        out += struct.pack(">B", flags) + cstring(col_name) + struct.pack(">Ii", type_oid, type_mod)
    return out


def encode_type(type_oid: int, namespace: str, name: str) -> bytes:
    """Type: 'Y', Int32 type OID, String namespace, String name."""
    return b"Y" + struct.pack(">I", type_oid) + cstring(namespace) + cstring(name)


def encode_insert(rel_id: int, new: tuple[object, ...]) -> bytes:
    """Insert: 'I', Int32 relation OID, 'N', TupleData."""
    return b"I" + struct.pack(">I", rel_id) + b"N" + encode_tuple_data(new)


def encode_update(
    rel_id: int,
    new: tuple[object, ...],
    old: tuple[object, ...] | None = None,
    key: tuple[object, ...] | None = None,
) -> bytes:
    """Update: 'U', Int32 relation OID, optional 'K' or 'O' TupleData
    (never both), then mandatory 'N' TupleData."""
    out = b"U" + struct.pack(">I", rel_id)
    if key is not None:
        out += b"K" + encode_tuple_data(key)
    if old is not None:
        out += b"O" + encode_tuple_data(old)
    return out + b"N" + encode_tuple_data(new)


def encode_delete(
    rel_id: int,
    old: tuple[object, ...] | None = None,
    key: tuple[object, ...] | None = None,
) -> bytes:
    """Delete: 'D', Int32 relation OID, exactly one of 'K' or 'O', TupleData."""
    out = b"D" + struct.pack(">I", rel_id)
    if key is not None:
        return out + b"K" + encode_tuple_data(key)
    assert old is not None
    return out + b"O" + encode_tuple_data(old)


def encode_truncate(options: int, rel_ids: tuple[int, ...]) -> bytes:
    """Truncate: 'T', Int32 relation count, Int8 options (1 CASCADE,
    2 RESTART IDENTITY), then Int32 relation OIDs."""
    out = b"T" + struct.pack(">IB", len(rel_ids), options)
    for rel_id in rel_ids:
        out += struct.pack(">I", rel_id)
    return out


# --- transaction control --------------------------------------------------


def test_begin_roundtrip() -> None:
    ts = datetime(2024, 5, 1, 12, 34, 56, 789012, tzinfo=timezone.utc)
    msg = decode_message(encode_begin(0x16B374D848, ts, 771))
    assert msg == Begin(final_lsn=0x16B374D848, commit_ts=ts, xid=771)


def test_begin_timestamp_epoch() -> None:
    # Zero microseconds is exactly the Postgres epoch, timezone-aware UTC.
    msg = decode_message(encode_begin(1, PG_EPOCH, 1))
    assert isinstance(msg, Begin)
    assert msg.commit_ts == PG_EPOCH
    assert msg.commit_ts.tzinfo is not None
    one_day = decode_message(encode_begin(1, PG_EPOCH + timedelta(days=1), 1))
    assert isinstance(one_day, Begin)
    assert one_day.commit_ts == datetime(2000, 1, 2, tzinfo=timezone.utc)


def test_commit_roundtrip() -> None:
    ts = datetime(2026, 7, 25, 8, 0, 0, tzinfo=timezone.utc)
    msg = decode_message(encode_commit(0, 0x1000, 0x1058, ts))
    assert msg == Commit(flags=0, commit_lsn=0x1000, end_lsn=0x1058, commit_ts=ts)


# --- relation / type / origin ---------------------------------------------


def test_relation_roundtrip() -> None:
    wire = encode_relation(
        16385,
        "public",
        "orders",
        "f",
        [(1, "id", 20, -1), (0, "status", 25, -1), (0, "total", 1700, 655366)],
    )
    msg = decode_message(wire)
    assert msg == Relation(
        rel_id=16385,
        namespace="public",
        name="orders",
        replica_identity="f",
        columns=(
            Column(name="id", type_oid=20, type_mod=-1, is_key=True),
            Column(name="status", type_oid=25, type_mod=-1, is_key=False),
            Column(name="total", type_oid=1700, type_mod=655366, is_key=False),
        ),
    )


def test_relation_empty_namespace_passes_through() -> None:
    # "" on the wire means pg_catalog; the decoder must not rewrite it.
    msg = decode_message(encode_relation(1259, "", "pg_class", "d", []))
    assert isinstance(msg, Relation)
    assert msg.namespace == ""
    assert msg.columns == ()


def test_relation_unknown_replica_identity() -> None:
    with pytest.raises(ProtocolError, match="replica identity"):
        decode_message(encode_relation(1, "public", "t", "x", []))


def test_type_info_roundtrip() -> None:
    msg = decode_message(encode_type(16400, "public", "mood"))
    assert msg == TypeInfo(type_oid=16400, namespace="public", name="mood")


def test_origin_roundtrip() -> None:
    msg = decode_message(encode_origin(0xDEADBEEF, "upstream"))
    assert msg == Origin(commit_lsn=0xDEADBEEF, name="upstream")


# --- row changes ----------------------------------------------------------


def test_insert_with_null_and_toast_columns() -> None:
    msg = decode_message(encode_insert(16385, ("42", None, UNCHANGED_TOAST, "héllo")))
    assert isinstance(msg, Insert)
    assert msg.rel_id == 16385
    assert msg.new == ("42", None, UNCHANGED_TOAST, "héllo")
    assert msg.new[2] is UNCHANGED_TOAST


def test_update_new_only() -> None:
    msg = decode_message(encode_update(7, new=("1", "shipped")))
    assert msg == Update(rel_id=7, old=None, key=None, new=("1", "shipped"))


def test_update_with_old_tuple() -> None:
    # REPLICA IDENTITY FULL: full old row arrives as an 'O' submessage.
    msg = decode_message(encode_update(7, new=("1", "shipped"), old=("1", "pending")))
    assert msg == Update(rel_id=7, old=("1", "pending"), key=None, new=("1", "shipped"))


def test_update_with_key_tuple() -> None:
    # Identity-index key change: only the key columns arrive, as 'K'.
    msg = decode_message(encode_update(7, new=("2", "shipped"), key=("1", None)))
    assert msg == Update(rel_id=7, old=None, key=("1", None), new=("2", "shipped"))


def test_delete_key_variant() -> None:
    msg = decode_message(encode_delete(9, key=("5",)))
    assert msg == Delete(rel_id=9, old=None, key=("5",))


def test_delete_old_variant() -> None:
    msg = decode_message(encode_delete(9, old=("5", "cancelled", None)))
    assert msg == Delete(rel_id=9, old=("5", "cancelled", None), key=None)


def test_delete_requires_key_or_old_submessage() -> None:
    wire = b"D" + struct.pack(">I", 9) + b"N" + encode_tuple_data(("5",))
    with pytest.raises(ProtocolError, match="'K' or 'O'"):
        decode_message(wire)


def test_truncate_multiple_relations() -> None:
    msg = decode_message(encode_truncate(3, (16385, 16389, 16401)))
    assert msg == Truncate(options=3, rel_ids=(16385, 16389, 16401))


# --- LSN helpers (re-exported from walflux.common) ------------------------


def test_lsn_parse_format_roundtrip() -> None:
    assert parse_lsn("16/B374D848") == 0x16B374D848
    assert format_lsn(0x16B374D848) == "16/B374D848"
    assert parse_lsn(format_lsn(0)) == 0
    assert format_lsn(parse_lsn("0/0")) == "0/0"


# --- convert_value --------------------------------------------------------


def test_convert_value_null() -> None:
    assert convert_value(None, 25) is None
    assert convert_value(None, 1700) is None


def test_convert_value_bool() -> None:
    assert convert_value("t", 16) is True
    assert convert_value("f", 16) is False


@pytest.mark.parametrize("oid", [20, 21, 23, 26])
def test_convert_value_integers(oid: int) -> None:
    assert convert_value("-42", oid) == -42
    assert isinstance(convert_value("0", oid), int)


@pytest.mark.parametrize("oid", [700, 701])
def test_convert_value_floats(oid: int) -> None:
    assert convert_value("1.5", oid) == 1.5
    assert isinstance(convert_value("1.5", oid), float)


def test_convert_value_numeric_is_exact_decimal() -> None:
    value = convert_value("0.1", 1700)
    assert value == Decimal("0.1")
    assert isinstance(value, Decimal)
    assert convert_value("0.1", 1700) + convert_value("0.2", 1700) == Decimal("0.3")


def test_convert_value_other_types_stay_text() -> None:
    assert convert_value("2024-05-01", 1082) == "2024-05-01"
    assert convert_value("hello", 25) == "hello"


# --- errors ---------------------------------------------------------------


def test_empty_buffer() -> None:
    with pytest.raises(ProtocolError, match="empty"):
        decode_message(b"")


def test_unknown_message_type() -> None:
    with pytest.raises(ProtocolError, match="'Z'"):
        decode_message(b"Z\x00\x00")


def test_truncated_begin() -> None:
    with pytest.raises(ProtocolError, match="truncated 'B'"):
        decode_message(b"B\x00\x00\x00\x00")


def test_truncated_tuple_text_value() -> None:
    # Declared text length exceeds the remaining buffer.
    wire = b"I" + struct.pack(">I", 1) + b"N" + struct.pack(">H", 1) + b"t"
    wire += struct.pack(">i", 100) + b"short"
    with pytest.raises(ProtocolError, match="truncated 'I'"):
        decode_message(wire)


def test_unterminated_string() -> None:
    wire = b"O" + struct.pack(">Q", 1) + b"no-nul-terminator"
    with pytest.raises(ProtocolError, match="unterminated"):
        decode_message(wire)


def test_binary_tuple_category_rejected() -> None:
    # 'b' columns are protocol >= 2; we negotiate version 1.
    wire = b"I" + struct.pack(">I", 1) + b"N" + struct.pack(">H", 1) + b"b"
    wire += struct.pack(">i", 1) + b"\x01"
    with pytest.raises(ProtocolError, match="protocol >= 2"):
        decode_message(wire)


def test_unknown_tuple_category() -> None:
    wire = b"I" + struct.pack(">I", 1) + b"N" + struct.pack(">H", 1) + b"z"
    with pytest.raises(ProtocolError, match="category 'z'"):
        decode_message(wire)


def test_insert_requires_new_submessage() -> None:
    wire = b"I" + struct.pack(">I", 1) + b"O" + encode_tuple_data(("1",))
    with pytest.raises(ProtocolError, match="expected 'N'"):
        decode_message(wire)
