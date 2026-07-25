"""Delta accumulation and SQL generation for aggregate target tables (Module B).

Pure logic — no database connections.  The daemon feeds decoded row events into
a :class:`DeltaBatch` and executes ``statements()`` inside a single target
transaction together with the checkpoint write; :func:`target_ddl` and
:func:`backfill_sql` generate the setup-time SQL.

Storage scheme: SQL ``SUM`` over zero non-NULL inputs is NULL, not 0, so a
running total alone cannot tell "no inputs" from "inputs summing to 0".  Each
``sum``/``avg`` aggregate therefore stores two plain columns — the running
total ``"<alias>__sum"`` and the non-NULL input count ``"<alias>__nn"`` — and
the user-facing ``"<alias>"`` is a stored generated column that yields NULL
when ``__nn`` is 0.  Generated columns are never inserted into; only the
storage columns are written.  All aggregate arithmetic uses
:class:`decimal.Decimal`.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation

from walflux.common import UNCHANGED_TOAST, SchemaDriftError, WalfluxError, quote_ident

#: Schema every target table lives in.
_TARGET_SCHEMA = "walflux"
#: Bookkeeping column counting live source rows per group.
_ROWS_COLUMN = "__walflux_rows"
#: Constant key column giving global (no ``group_by``) targets a one-row upsert key.
_GLOBAL_KEY_COLUMN = "__walflux_all"


@dataclass
class AggSpec:
    """One aggregate of a view; ``column`` is None only for ``COUNT(*)``."""

    fn: str  # 'count' | 'sum' | 'avg'
    column: str | None
    alias: str


@dataclass
class ViewSpec:
    """One incrementally-maintained aggregate view over a single source table."""

    name: str
    source_schema: str
    source_table: str
    group_by: list[str]
    aggregates: list[AggSpec]


@dataclass(frozen=True)
class _Storage:
    """A physical (non-generated) aggregate column of a target table."""

    name: str
    ddl_type: str
    backfill_expr: str
    zero: int | Decimal


def _agg_storage(agg: AggSpec) -> list[_Storage]:
    """The storage columns backing one aggregate, in target-table order."""
    if agg.fn == "count":
        expr = "count(*)" if agg.column is None else f"count({quote_ident(agg.column)})"
        return [_Storage(agg.alias, "bigint NOT NULL", expr, 0)]
    if agg.fn in ("sum", "avg"):
        if agg.column is None:
            raise WalfluxError(f"aggregate {agg.alias!r}: {agg.fn} requires a column")
        src = quote_ident(agg.column)
        # COALESCE keeps the stored running total non-NULL so the upsert's
        # addition stays well-defined; NULL semantics live in the generated column.
        return [
            _Storage(f"{agg.alias}__sum", "numeric", f"coalesce(sum({src}), 0)", Decimal(0)),
            _Storage(f"{agg.alias}__nn", "bigint NOT NULL", f"count({src})", 0),
        ]
    raise WalfluxError(f"aggregate {agg.alias!r}: unknown aggregate function {agg.fn!r}")


def _target(view: ViewSpec) -> str:
    return f"{quote_ident(_TARGET_SCHEMA)}.{quote_ident(view.name)}"


def _key_columns(view: ViewSpec) -> list[str]:
    return view.group_by if view.group_by else [_GLOBAL_KEY_COLUMN]


def target_ddl(view: ViewSpec, group_col_types: dict[str, str]) -> list[str]:
    """DROP TABLE IF EXISTS + CREATE TABLE + CREATE UNIQUE INDEX for one view.

    ``group_col_types`` maps each group column to its SQL type; Module D
    resolves these from ``pg_catalog`` via ``format_type`` at setup time
    (types cannot be parameterized, so they are trusted server-provided text).
    """
    target = _target(view)
    columns: list[str] = []
    if view.group_by:
        for col in view.group_by:
            try:
                col_type = group_col_types[col]
            except KeyError:
                raise WalfluxError(
                    f"view {view.name!r}: no resolved type for group column {col!r}"
                ) from None
            columns.append(f"{quote_ident(col)} {col_type}")
    else:
        columns.append(f"{quote_ident(_GLOBAL_KEY_COLUMN)} boolean NOT NULL DEFAULT true")
    columns.append(f"{quote_ident(_ROWS_COLUMN)} bigint NOT NULL")
    for agg in view.aggregates:
        columns.extend(f"{quote_ident(s.name)} {s.ddl_type}" for s in _agg_storage(agg))
        if agg.fn in ("sum", "avg"):
            total = quote_ident(f"{agg.alias}__sum")
            nn = quote_ident(f"{agg.alias}__nn")
            value = total if agg.fn == "sum" else f"{total} / {nn}"
            columns.append(
                f"{quote_ident(agg.alias)} numeric GENERATED ALWAYS AS "
                f"(CASE WHEN {nn} > 0 THEN {value} END) STORED"
            )
    keys = ", ".join(quote_ident(c) for c in _key_columns(view))
    body = ",\n    ".join(columns)
    return [
        f"DROP TABLE IF EXISTS {target}",
        f"CREATE TABLE {target} (\n    {body}\n)",
        # NULLS NOT DISTINCT (PG 15+) makes NULL group keys upsert into a single
        # row, matching GROUP BY semantics where all NULLs form one group.
        f"CREATE UNIQUE INDEX ON {target} ({keys}) NULLS NOT DISTINCT",
    ]


def backfill_sql(view: ViewSpec) -> str:
    """One INSERT ... SELECT populating the target from its source table.

    Writes only key and storage columns — generated columns are computed by
    Postgres.  With empty ``group_by`` the plain aggregate SELECT (no GROUP BY)
    yields exactly one row, matching ``SELECT sum(x) FROM t`` on an empty table.
    """
    storage = [s for agg in view.aggregates for s in _agg_storage(agg)]
    insert_cols = [*_key_columns(view), _ROWS_COLUMN, *(s.name for s in storage)]
    key_exprs = [quote_ident(c) for c in view.group_by] if view.group_by else ["true"]
    select_exprs = [*key_exprs, "count(*)", *(s.backfill_expr for s in storage)]
    source = f"{quote_ident(view.source_schema)}.{quote_ident(view.source_table)}"
    sql = (
        f"INSERT INTO {_target(view)} ({', '.join(quote_ident(c) for c in insert_cols)})\n"
        f"SELECT {', '.join(select_exprs)}\n"
        f"FROM {source}"
    )
    if view.group_by:
        sql += f"\nGROUP BY {', '.join(quote_ident(c) for c in view.group_by)}"
    return sql


def _as_decimal(value: object, view_name: str, column: str) -> Decimal:
    """Convert a non-NULL aggregated value to Decimal, or raise WalfluxError."""
    try:
        if isinstance(value, Decimal):
            result = value
        else:
            # Floats go through str() so a float column contributes its
            # shortest round-trip representation rather than its full binary
            # expansion.
            result = Decimal(str(value)) if isinstance(value, float) else Decimal(value)
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise WalfluxError(
            f"view {view_name!r}: cannot aggregate column {column!r} "
            f"value of type {type(value).__name__}"
        ) from exc
    if not result.is_finite():
        # NaN/Infinity cannot be maintained additively: NaN - NaN = NaN, so a
        # later DELETE could never cancel this row's contribution and the
        # aggregate would silently diverge from ground truth forever. Halt
        # loudly instead (same philosophy as schema drift).
        raise WalfluxError(
            f"view {view_name!r}: non-finite value {value} in aggregated "
            f"column {column!r}; WalFlux cannot maintain NaN/Infinity "
            f"incrementally — exclude such rows or use a finite sentinel"
        )
    return result


def _agg_deltas(agg: AggSpec, row: dict[str, object], view_name: str) -> tuple[int | Decimal, ...]:
    """Per-storage-column contribution of one row event (before sign)."""
    if agg.fn == "count":
        if agg.column is None:
            return (1,)
        return (1,) if row[agg.column] is not None else (0,)
    value = row[agg.column]  # sum/avg always have a column (validated at plan build)
    if value is None:
        return (Decimal(0), 0)
    return (_as_decimal(value, view_name, agg.column), 1)


@dataclass(frozen=True)
class _Plan:
    """Precomputed per-view apply plan."""

    view: ViewSpec
    needed: tuple[str, ...]  # source columns the view reads from every row event
    zeros: tuple[int | Decimal, ...]  # delta template: __walflux_rows + storage columns
    upsert_sql: str


def _build_plan(view: ViewSpec) -> _Plan:
    storage = [s for agg in view.aggregates for s in _agg_storage(agg)]
    agg_columns = [a.column for a in view.aggregates if a.column is not None]
    needed = tuple(dict.fromkeys([*view.group_by, *agg_columns]))
    target = _target(view)
    insert_cols = [*_key_columns(view), _ROWS_COLUMN, *(s.name for s in storage)]
    delta_cols = [_ROWS_COLUMN, *(s.name for s in storage)]
    updates = ",\n  ".join(
        f"{quote_ident(c)} = {target}.{quote_ident(c)} + EXCLUDED.{quote_ident(c)}"
        for c in delta_cols
    )
    keys = ", ".join(quote_ident(c) for c in _key_columns(view))
    upsert_sql = (
        f"INSERT INTO {target} ({', '.join(quote_ident(c) for c in insert_cols)})\n"
        f"VALUES ({', '.join(['%s'] * len(insert_cols))})\n"
        f"ON CONFLICT ({keys}) DO UPDATE SET\n"
        f"  {updates}"
    )
    zeros = (0, *(s.zero for s in storage))
    return _Plan(view=view, needed=needed, zeros=zeros, upsert_sql=upsert_sql)


class DeltaBatch:
    """Accumulates per-group deltas for many views across one flush batch.

    ``statements()`` emits, in order: full-table DELETEs for truncated views,
    one additive upsert per touched group, then the zero-row cleanup DELETE for
    each touched grouped view.  Global (empty ``group_by``) views keep their
    single row forever — with no live source rows its generated columns yield
    NULL and its counts read 0, matching SQL aggregates over an empty table —
    so the cleanup is skipped for them.
    """

    def __init__(self, views: list[ViewSpec]) -> None:
        self._plans: dict[str, _Plan] = {v.name: _build_plan(v) for v in views}
        self._deltas: dict[str, dict[tuple[object, ...], list[int | Decimal]]] = {
            name: {} for name in self._plans
        }
        self._truncated: set[str] = set()

    def _plan(self, view_name: str) -> _Plan:
        try:
            return self._plans[view_name]
        except KeyError:
            raise WalfluxError(f"unknown view: {view_name!r}") from None

    def apply_insert(self, view_name: str, row: dict[str, object]) -> None:
        """Fold one inserted source row into the batch."""
        self._apply(view_name, row, 1)

    def apply_delete(self, view_name: str, row: dict[str, object]) -> None:
        """Fold one deleted source row into the batch."""
        self._apply(view_name, row, -1)

    def apply_update(self, view_name: str, old: dict[str, object], new: dict[str, object]) -> None:
        """Fold one updated source row into the batch: delete(old) + insert(new)."""
        self._apply(view_name, old, -1)
        self._apply(view_name, new, 1)

    def apply_truncate(self, view_name: str) -> None:
        """Discard deltas accumulated so far for the view and mark it truncated."""
        plan = self._plan(view_name)
        groups = self._deltas[view_name]
        groups.clear()
        self._truncated.add(view_name)
        if not plan.view.group_by:
            # A global target keeps its single row even when the source is empty;
            # seed a zero delta so the upsert re-creates the row after the DELETE.
            groups[()] = list(plan.zeros)

    def is_empty(self) -> bool:
        """True when a flush would execute no statements."""
        return not self._truncated and not any(self._deltas.values())

    def _apply(self, view_name: str, row: dict[str, object], sign: int) -> None:
        plan = self._plan(view_name)
        view = plan.view
        for col in plan.needed:
            try:
                value = row[col]
            except KeyError:
                raise SchemaDriftError(
                    f"view {view_name!r}: source row is missing column {col!r}; "
                    f"re-run `walflux setup --force`"
                ) from None
            if value is UNCHANGED_TOAST:
                # The daemon already substitutes old values for unchanged-TOAST
                # columns in an UPDATE's new tuple; a marker reaching this point
                # sits in a row image that should have been complete (an
                # old/delete tuple), so the identity really is not FULL.
                raise SchemaDriftError(
                    f"view {view_name!r}: column {col!r} arrived as unchanged TOAST in a "
                    f"row image that should be complete; ensure "
                    f"{view.source_schema}.{view.source_table} has REPLICA IDENTITY FULL "
                    f"and re-run `walflux setup --force`"
                )
        key = tuple(row[c] for c in view.group_by)
        group = self._deltas[view_name].setdefault(key, list(plan.zeros))
        group[0] += sign
        index = 1
        for agg in view.aggregates:
            for delta in _agg_deltas(agg, row, view_name):
                group[index] += sign * delta
                index += 1

    def statements(self) -> list[tuple[str, tuple]]:
        """The batch as an ordered ``(sql, params)`` list for one target transaction.

        Ordering is load-bearing: truncate DELETEs run first so post-truncate
        deltas insert into an empty table as absolute values; upserts next; the
        zero-row cleanup last, after cancellations have been summed in.
        """
        out: list[tuple[str, tuple]] = []
        for name, plan in self._plans.items():
            if name in self._truncated:
                out.append((f"DELETE FROM {_target(plan.view)}", ()))
        for name, plan in self._plans.items():
            key_prefix: tuple[object, ...] = () if plan.view.group_by else (True,)
            for key, deltas in self._deltas[name].items():
                out.append((plan.upsert_sql, (*key_prefix, *key, *deltas)))
        for name, plan in self._plans.items():
            if not plan.view.group_by:
                continue
            if name in self._truncated or self._deltas[name]:
                cleanup = (
                    f"DELETE FROM {_target(plan.view)} WHERE {quote_ident(_ROWS_COLUMN)} <= 0"
                )
                out.append((cleanup, ()))
        return out
