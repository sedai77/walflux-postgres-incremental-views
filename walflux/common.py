"""Shared primitives: exceptions, wire sentinels, LSN and identifier helpers."""

from __future__ import annotations

import re


class WalfluxError(Exception):
    """Base class for all WalFlux errors."""


class ProtocolError(WalfluxError):
    """Malformed or unsupported pgoutput wire data."""


class ConfigError(WalfluxError):
    """Invalid configuration file."""


class SchemaDriftError(WalfluxError):
    """A source table no longer matches what a view needs."""


class SetupError(WalfluxError):
    """`walflux setup` cannot proceed (existing slot, missing table, ...)."""


class _UnchangedToast:
    __slots__ = ()

    def __repr__(self) -> str:
        return "UNCHANGED_TOAST"


#: Sentinel for a TOASTed column the server did not re-send ('u' in the wire format).
UNCHANGED_TOAST = _UnchangedToast()


def parse_lsn(text: str) -> int:
    """Parse a textual LSN like ``16/B374D848`` into an integer."""
    try:
        hi, lo = text.split("/")
        return (int(hi, 16) << 32) | int(lo, 16)
    except ValueError as exc:
        raise WalfluxError(f"invalid LSN: {text!r}") from exc


def format_lsn(lsn: int) -> str:
    """Format an integer LSN as ``16/B374D848``."""
    return f"{lsn >> 32:X}/{lsn & 0xFFFFFFFF:X}"


_IDENT_RE = re.compile(r"^[a-z_][a-z0-9_]*$")


def quote_ident(name: str) -> str:
    """Validate-then-quote a SQL identifier.

    WalFlux only ever generates identifiers matching the conservative
    ``[a-z_][a-z0-9_]*`` set (config validation enforces this), so anything
    else reaching this function is a bug or an injection attempt — reject it
    rather than trying to escape it.
    """
    if not _IDENT_RE.match(name) or len(name.encode()) > 63:
        raise WalfluxError(f"invalid identifier: {name!r}")
    return f'"{name}"'
