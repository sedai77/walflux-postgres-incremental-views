"""CLI-facing behavior: ``python -m walflux`` and tidy connection errors (no database)."""

from __future__ import annotations

import subprocess
import sys

import pytest

from walflux import __version__, bootstrap, daemon
from walflux.aggregates import AggSpec, ViewSpec
from walflux.cli import main
from walflux.common import WalfluxError
from walflux.config import Config

#: A DSN that fails fast with OperationalError (nonexistent unix-socket dir, no network).
BAD_DSN = "postgresql://walflux@/walflux?host=/nonexistent-walflux-socket"


def _config() -> Config:
    view = ViewSpec(
        name="orders_by_status",
        source_schema="public",
        source_table="orders",
        group_by=["status"],
        aggregates=[AggSpec(fn="count", column=None, alias="order_count")],
    )
    return Config(
        dsn=BAD_DSN,
        slot="walflux",
        publication="walflux",
        batch_max_ms=200,
        batch_max_txns=500,
        views=[view],
    )


def test_python_dash_m_walflux_version() -> None:
    result = subprocess.run(
        [sys.executable, "-m", "walflux", "--version"],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0
    assert __version__ in result.stdout


@pytest.mark.parametrize(
    "call",
    [
        lambda config: bootstrap.setup(config),
        lambda config: bootstrap.teardown(config),
        lambda config: daemon.status(config),
    ],
    ids=["setup", "teardown", "status"],
)
def test_unreachable_dsn_is_a_walflux_error(call) -> None:
    """A wrong or unreachable DSN is user-fixable, not a bug report: it must
    surface as WalfluxError (one tidy CLI line), never a raw traceback."""
    with pytest.raises(WalfluxError, match="cannot connect to database"):
        call(_config())


def test_cli_renders_connect_error_as_one_line(tmp_path, monkeypatch, capsys) -> None:
    monkeypatch.delenv("WALFLUX_DSN", raising=False)
    config = tmp_path / "walflux.yaml"
    config.write_text(
        f'database:\n  dsn: "{BAD_DSN}"\n'
        "views:\n"
        "  - name: orders_by_status\n"
        "    source: public.orders\n"
        "    group_by: [status]\n"
        "    aggregates:\n"
        "      - { fn: count, as: order_count }\n",
        encoding="utf-8",
    )
    assert main(["status", "-c", str(config)]) == 1
    err = capsys.readouterr().err
    assert err.startswith("walflux: error: cannot connect to database:")
    assert err.strip().count("\n") == 0
