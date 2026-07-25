"""Load and validate the WalFlux YAML config into typed view specs.

The public surface is :func:`load_config`, which returns a :class:`Config`
holding fully validated :class:`~walflux.aggregates.ViewSpec` /
:class:`~walflux.aggregates.AggSpec` objects.  Every rejected input raises
:class:`~walflux.common.ConfigError` with a message naming the offending
view, key, and value, so users can fix the file without reading source.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass

import yaml

from walflux.aggregates import AggSpec, ViewSpec
from walflux.common import ConfigError

__all__ = ["Config", "load_config"]

_IDENT_RE = re.compile(r"^[a-z_][a-z0-9_]*$")
_MAX_IDENT_BYTES = 63
_AGG_FNS = ("count", "sum", "avg")
# aggregates.py derives storage columns "<alias>__sum" / "<alias>__nn" and the
# bookkeeping column "__walflux_rows"; aliases must stay clear of all three.
_RESERVED_SUFFIXES = ("__sum", "__nn")
_STORAGE_SUFFIX_BYTES = len("__sum")
_ROWS_COLUMN = "__walflux_rows"

_DEFAULT_SLOT = "walflux"
_DEFAULT_PUBLICATION = "walflux"
_DEFAULT_BATCH_MAX_MS = 200
_DEFAULT_BATCH_MAX_TXNS = 500


@dataclass
class Config:
    """Validated runtime configuration for setup, the daemon, and the CLI."""

    dsn: str
    slot: str
    publication: str
    batch_max_ms: int
    batch_max_txns: int
    views: list[ViewSpec]


def _require_mapping(value: object, what: str) -> dict:
    if not isinstance(value, dict):
        raise ConfigError(f"{what} must be a mapping, got {type(value).__name__}")
    return value


def _check_ident(value: object, what: str) -> str:
    if not isinstance(value, str):
        raise ConfigError(f"{what} must be a string, got {value!r}")
    if not _IDENT_RE.match(value):
        raise ConfigError(
            f"{what} {value!r} is not a valid identifier (expected [a-z_][a-z0-9_]*)"
        )
    if len(value.encode()) > _MAX_IDENT_BYTES:
        raise ConfigError(f"{what} {value!r} is longer than {_MAX_IDENT_BYTES} bytes")
    return value


def _positive_int(value: object, what: str, default: int) -> int:
    if value is None:
        return default
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ConfigError(f"{what} must be a positive integer, got {value!r}")
    return value


def _parse_source(value: object, view_name: str) -> tuple[str, str]:
    if not isinstance(value, str):
        raise ConfigError(f"view {view_name!r}: source must be a string, got {value!r}")
    parts = value.split(".")
    if len(parts) == 1:
        schema, table = "public", parts[0]
    elif len(parts) == 2:
        schema, table = parts
    else:
        raise ConfigError(
            f'view {view_name!r}: source {value!r} must be "table" or "schema.table"'
        )
    _check_ident(schema, f"view {view_name!r}: source schema")
    _check_ident(table, f"view {view_name!r}: source table")
    return schema, table


def _parse_aggregate(
    raw: object, view_name: str, group_by: list[str], seen_aliases: set[str]
) -> AggSpec:
    agg = _require_mapping(raw, f"view {view_name!r}: each aggregate")

    fn = agg.get("fn")
    if fn is None:
        raise ConfigError(f"view {view_name!r}: aggregate is missing required key 'fn'")
    if fn not in _AGG_FNS:
        raise ConfigError(
            f"view {view_name!r}: unknown aggregate fn {fn!r} (expected one of: count, sum, avg)"
        )

    alias_raw = agg.get("as")
    if alias_raw is None:
        raise ConfigError(f"view {view_name!r}: aggregate ({fn}) is missing required key 'as'")
    alias = _check_ident(alias_raw, f"view {view_name!r}: aggregate alias")

    column = agg.get("column")
    if fn in ("sum", "avg") and column is None:
        raise ConfigError(f"view {view_name!r}: aggregate {alias!r}: {fn} requires a column")
    if column is not None:
        column = _check_ident(column, f"view {view_name!r}: aggregate {alias!r}: column")

    if alias in seen_aliases:
        raise ConfigError(f"view {view_name!r}: duplicate aggregate alias {alias!r}")
    if alias in group_by:
        raise ConfigError(
            f"view {view_name!r}: aggregate alias {alias!r} collides with a group_by column"
        )
    if alias == _ROWS_COLUMN or alias.endswith(_RESERVED_SUFFIXES):
        raise ConfigError(
            f"view {view_name!r}: aggregate alias {alias!r} is reserved:"
            f" '{_ROWS_COLUMN}' and the '__sum'/'__nn' suffixes name WalFlux storage columns"
        )
    if fn in ("sum", "avg") and len(alias.encode()) + _STORAGE_SUFFIX_BYTES > _MAX_IDENT_BYTES:
        raise ConfigError(
            f"view {view_name!r}: aggregate alias {alias!r} is too long to leave room for its"
            f" '__sum'/'__nn' storage columns"
            f" (at most {_MAX_IDENT_BYTES - _STORAGE_SUFFIX_BYTES} bytes for {fn})"
        )

    seen_aliases.add(alias)
    return AggSpec(fn=fn, column=column, alias=alias)


def _parse_view(raw: object, seen_names: set[str]) -> ViewSpec:
    view = _require_mapping(raw, "each view")

    name_raw = view.get("name")
    if name_raw is None:
        raise ConfigError("view is missing required key 'name'")
    name = _check_ident(name_raw, "view name")
    if name in seen_names:
        raise ConfigError(f"duplicate view name {name!r}")
    seen_names.add(name)

    source = view.get("source")
    if source is None:
        raise ConfigError(f"view {name!r} is missing required key 'source'")
    schema, table = _parse_source(source, name)

    raw_group_by = view.get("group_by")
    if raw_group_by is None:
        raw_group_by = []
    if not isinstance(raw_group_by, list):
        raise ConfigError(f"view {name!r}: group_by must be a list of column names")
    group_by: list[str] = []
    for col_raw in raw_group_by:
        col = _check_ident(col_raw, f"view {name!r}: group_by column")
        if col == _ROWS_COLUMN:
            raise ConfigError(
                f"view {name!r}: group_by column {col!r} is reserved for WalFlux bookkeeping"
            )
        if col in group_by:
            raise ConfigError(f"view {name!r}: group_by column {col!r} is repeated")
        group_by.append(col)

    raw_aggs = view.get("aggregates")
    if raw_aggs is None or raw_aggs == []:
        raise ConfigError(f"view {name!r} must declare at least one aggregate")
    if not isinstance(raw_aggs, list):
        raise ConfigError(
            f"view {name!r}: 'aggregates' must be a list, got {type(raw_aggs).__name__}"
        )
    seen_aliases: set[str] = set()
    aggregates = [_parse_aggregate(a, name, group_by, seen_aliases) for a in raw_aggs]

    return ViewSpec(
        name=name,
        source_schema=schema,
        source_table=table,
        group_by=group_by,
        aggregates=aggregates,
    )


def load_config(path: str) -> Config:
    """Read and validate the YAML config at ``path``.

    The ``WALFLUX_DSN`` environment variable, when set and non-empty,
    overrides ``database.dsn``; at least one of the two must be present.
    Raises :class:`~walflux.common.ConfigError` on any invalid input.
    """
    try:
        with open(path, encoding="utf-8") as fh:
            raw = yaml.safe_load(fh)
    except FileNotFoundError:
        raise ConfigError(f"config file not found: {path}") from None
    except OSError as exc:
        raise ConfigError(f"cannot read config file {path}: {exc}") from exc
    except yaml.YAMLError as exc:
        raise ConfigError(f"invalid YAML in {path}: {exc}") from exc

    doc = _require_mapping(raw, "top-level config")

    database = _require_mapping(doc.get("database") or {}, "'database' section")
    dsn = os.environ.get("WALFLUX_DSN") or database.get("dsn")
    if dsn is None:
        raise ConfigError(
            "no DSN configured: set database.dsn in the config file"
            " or the WALFLUX_DSN environment variable"
        )
    if not isinstance(dsn, str):
        raise ConfigError(f"database.dsn must be a string, got {dsn!r}")

    slot_raw = doc.get("slot")
    slot = _DEFAULT_SLOT if slot_raw is None else _check_ident(slot_raw, "slot name")
    pub_raw = doc.get("publication")
    publication = (
        _DEFAULT_PUBLICATION if pub_raw is None else _check_ident(pub_raw, "publication name")
    )

    batch = _require_mapping(doc.get("batch") or {}, "'batch' section")
    batch_max_ms = _positive_int(batch.get("max_ms"), "batch.max_ms", _DEFAULT_BATCH_MAX_MS)
    batch_max_txns = _positive_int(
        batch.get("max_txns"), "batch.max_txns", _DEFAULT_BATCH_MAX_TXNS
    )

    raw_views = doc.get("views")
    if raw_views is None or raw_views == []:
        raise ConfigError("config must declare at least one view under 'views'")
    if not isinstance(raw_views, list):
        raise ConfigError(f"'views' must be a list, got {type(raw_views).__name__}")
    seen_names: set[str] = set()
    views = [_parse_view(v, seen_names) for v in raw_views]

    return Config(
        dsn=dsn,
        slot=slot,
        publication=publication,
        batch_max_ms=batch_max_ms,
        batch_max_txns=batch_max_txns,
        views=views,
    )
