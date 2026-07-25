"""``walflux`` command-line entry point.

Subcommand handlers import :mod:`walflux.bootstrap` / :mod:`walflux.daemon`
lazily so that ``--help`` and ``--version`` never load psycopg2 or open a
database connection.  Exit codes: 0 success, 1 error, 2 usage.
"""

from __future__ import annotations

import argparse
import sys

from walflux import __version__
from walflux.common import WalfluxError, format_lsn
from walflux.config import load_config

__all__ = ["main"]


def _cmd_setup(args: argparse.Namespace) -> int:
    config = load_config(args.config)
    from walflux import bootstrap

    bootstrap.setup(config, force=args.force)
    return 0


def _cmd_run(args: argparse.Namespace) -> int:
    config = load_config(args.config)
    from walflux.daemon import Daemon

    Daemon(config).run()
    return 0


def _cmd_status(args: argparse.Namespace) -> int:
    config = load_config(args.config)
    from walflux import daemon

    _print_status(daemon.status(config))
    return 0


def _cmd_teardown(args: argparse.Namespace) -> int:
    if not args.yes:
        print(
            "walflux: teardown drops the replication slot, the publication, and the"
            " walflux schema; re-run with --yes to confirm",
            file=sys.stderr,
        )
        return 2
    config = load_config(args.config)
    from walflux import bootstrap

    bootstrap.teardown(config)
    return 0


def _print_status(info: dict[str, object]) -> None:
    """Render ``daemon.status()`` output as a small aligned plain-text table."""

    def lsn(value: object) -> str:
        if value is None:
            return "-"
        if isinstance(value, int):
            return format_lsn(value)
        return str(value)

    active = "active" if info.get("active") else "inactive"
    rows: list[tuple[str, str]] = [
        ("slot", f"{info.get('slot', '-')} ({active})"),
        ("restart_lsn", lsn(info.get("restart_lsn"))),
        ("confirmed_flush_lsn", lsn(info.get("confirmed_flush_lsn"))),
        ("current_lsn", lsn(info.get("current_lsn"))),
        ("lag_bytes", "-" if info.get("lag_bytes") is None else str(info["lag_bytes"])),
        ("checkpoint_lsn", lsn(info.get("checkpoint_lsn"))),
    ]
    views = info.get("views") or {}
    view_items = views.items() if isinstance(views, dict) else views
    for name, count in view_items:
        rows.append((f"view {name}", "no target table" if count is None else f"{count} rows"))

    width = max(len(label) for label, _ in rows)
    for label, value in rows:
        print(f"{label:<{width}}  {value}")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="walflux",
        description="Incrementally-maintained materialized views for Postgres.",
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    sub = parser.add_subparsers(dest="command", metavar="command", required=True)

    def subcommand(name: str, help_text: str, handler) -> argparse.ArgumentParser:
        p = sub.add_parser(name, help=help_text, description=help_text)
        p.add_argument(
            "-c",
            "--config",
            required=True,
            metavar="CONFIG",
            help="path to the walflux YAML config file",
        )
        p.set_defaults(handler=handler)
        return p

    setup = subcommand(
        "setup",
        "Create the publication, replication slot, and target tables, then backfill."
        " Destructive to targets: existing walflux target tables are dropped and"
        " rebuilt (they are derived data).",
        _cmd_setup,
    )
    setup.add_argument(
        "--force",
        action="store_true",
        help="drop and recreate the replication slot if it already exists, then re-backfill",
    )

    subcommand("run", "Run the daemon in the foreground, logging to stderr.", _cmd_run)
    subcommand(
        "status",
        "Show slot state, replication lag, checkpoint, and per-view row counts.",
        _cmd_status,
    )
    teardown = subcommand(
        "teardown",
        "Drop the replication slot, the publication, and the walflux schema.",
        _cmd_teardown,
    )
    teardown.add_argument("--yes", action="store_true", help="confirm the teardown (required)")

    return parser


def main(argv: list[str] | None = None) -> int:
    """Parse ``argv`` (defaults to ``sys.argv[1:]``) and run one subcommand.

    Returns the process exit code.  Expected, user-fixable failures
    (:class:`~walflux.common.WalfluxError`) become one tidy stderr line and
    exit code 1; unexpected exceptions propagate so the full traceback — a
    bug report — is printed.
    """
    args = _build_parser().parse_args(argv)
    try:
        return args.handler(args)
    except WalfluxError as exc:
        print(f"walflux: error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
