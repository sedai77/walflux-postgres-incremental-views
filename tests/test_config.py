"""Tests for walflux.config: happy path, defaults, env override, every validation rule."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from walflux.common import ConfigError
from walflux.config import load_config


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep the ambient environment from leaking into DSN-resolution tests."""
    monkeypatch.delenv("WALFLUX_DSN", raising=False)


def base_config() -> dict:
    """A minimal valid config; tests mutate copies of it to hit one rule each."""
    return {
        "database": {"dsn": "postgresql://u:p@localhost:5432/db"},
        "views": [
            {
                "name": "orders_by_status",
                "source": "public.orders",
                "group_by": ["status"],
                "aggregates": [
                    {"fn": "count", "as": "order_count"},
                    {"fn": "count", "column": "coupon", "as": "with_coupon"},
                    {"fn": "sum", "column": "total", "as": "revenue"},
                    {"fn": "avg", "column": "total", "as": "avg_order_value"},
                ],
            }
        ],
    }


def write(tmp_path: Path, data: dict) -> str:
    path = tmp_path / "walflux.yaml"
    path.write_text(yaml.safe_dump(data), encoding="utf-8")
    return str(path)


def load(tmp_path: Path, data: dict):
    return load_config(write(tmp_path, data))


def load_error(tmp_path: Path, data: dict, match: str) -> None:
    with pytest.raises(ConfigError, match=match):
        load(tmp_path, data)


# --- happy path, defaults, env override -----------------------------------


def test_happy_path(tmp_path: Path) -> None:
    data = base_config()
    data.update(
        slot="my_slot",
        publication="my_pub",
        batch={"max_ms": 50, "max_txns": 100},
    )
    data["views"].append(
        {
            "name": "orders_global",
            "source": "orders",
            "aggregates": [{"fn": "sum", "column": "total", "as": "grand_total"}],
        }
    )

    config = load(tmp_path, data)

    assert config.dsn == "postgresql://u:p@localhost:5432/db"
    assert config.slot == "my_slot"
    assert config.publication == "my_pub"
    assert config.batch_max_ms == 50
    assert config.batch_max_txns == 100

    grouped = config.views[0]
    assert grouped.name == "orders_by_status"
    assert grouped.source_schema == "public"
    assert grouped.source_table == "orders"
    assert grouped.group_by == ["status"]
    assert [(a.fn, a.column, a.alias) for a in grouped.aggregates] == [
        ("count", None, "order_count"),
        ("count", "coupon", "with_coupon"),
        ("sum", "total", "revenue"),
        ("avg", "total", "avg_order_value"),
    ]

    global_view = config.views[1]
    assert global_view.source_schema == "public"  # schema defaults to public
    assert global_view.group_by == []  # omitted group_by means a global aggregate


def test_defaults(tmp_path: Path) -> None:
    config = load(tmp_path, base_config())
    assert config.slot == "walflux"
    assert config.publication == "walflux"
    assert config.batch_max_ms == 200
    assert config.batch_max_txns == 500


def test_env_dsn_overrides_file_dsn(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("WALFLUX_DSN", "postgresql://env@elsewhere/db")
    config = load(tmp_path, base_config())
    assert config.dsn == "postgresql://env@elsewhere/db"


def test_env_dsn_suffices_without_database_section(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("WALFLUX_DSN", "postgresql://env@elsewhere/db")
    data = base_config()
    del data["database"]
    assert load(tmp_path, data).dsn == "postgresql://env@elsewhere/db"


def test_missing_dsn(tmp_path: Path) -> None:
    data = base_config()
    del data["database"]
    load_error(tmp_path, data, "WALFLUX_DSN")


def test_dsn_must_be_string(tmp_path: Path) -> None:
    data = base_config()
    data["database"]["dsn"] = 12345
    load_error(tmp_path, data, "database.dsn must be a string")


# --- file-level failures ---------------------------------------------------


def test_config_file_not_found(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="config file not found"):
        load_config(str(tmp_path / "absent.yaml"))


def test_invalid_yaml(tmp_path: Path) -> None:
    path = tmp_path / "walflux.yaml"
    path.write_text("views: [unclosed\n", encoding="utf-8")
    with pytest.raises(ConfigError, match="invalid YAML"):
        load_config(str(path))


def test_top_level_must_be_mapping(tmp_path: Path) -> None:
    path = tmp_path / "walflux.yaml"
    path.write_text("- just\n- a\n- list\n", encoding="utf-8")
    with pytest.raises(ConfigError, match="top-level config must be a mapping"):
        load_config(str(path))


# --- slot / publication / batch --------------------------------------------


@pytest.mark.parametrize("key", ["slot", "publication"])
def test_slot_and_publication_must_be_identifiers(tmp_path: Path, key: str) -> None:
    data = base_config()
    data[key] = "Bad-Name"
    load_error(tmp_path, data, "not a valid identifier")


@pytest.mark.parametrize("key", ["max_ms", "max_txns"])
@pytest.mark.parametrize("bad", [0, -5, "fast", True])
def test_batch_values_must_be_positive_ints(tmp_path: Path, key: str, bad: object) -> None:
    data = base_config()
    data["batch"] = {key: bad}
    load_error(tmp_path, data, f"batch.{key} must be a positive integer")


# --- views section shape ----------------------------------------------------


@pytest.mark.parametrize("views", [None, []])
def test_at_least_one_view_required(tmp_path: Path, views: object) -> None:
    data = base_config()
    if views is None:
        del data["views"]
    else:
        data["views"] = views
    load_error(tmp_path, data, "at least one view")


def test_views_must_be_a_list(tmp_path: Path) -> None:
    data = base_config()
    data["views"] = "orders_by_status"
    load_error(tmp_path, data, "'views' must be a list")


def test_view_name_required(tmp_path: Path) -> None:
    data = base_config()
    del data["views"][0]["name"]
    load_error(tmp_path, data, "missing required key 'name'")


@pytest.mark.parametrize("bad", ["Orders", "1st", "with-dash", ""])
def test_view_name_must_be_identifier(tmp_path: Path, bad: str) -> None:
    data = base_config()
    data["views"][0]["name"] = bad
    load_error(tmp_path, data, "not a valid identifier")


def test_view_names_must_be_unique(tmp_path: Path) -> None:
    data = base_config()
    data["views"].append(dict(data["views"][0]))
    load_error(tmp_path, data, "duplicate view name 'orders_by_status'")


# --- source -----------------------------------------------------------------


def test_source_required(tmp_path: Path) -> None:
    data = base_config()
    del data["views"][0]["source"]
    load_error(tmp_path, data, "missing required key 'source'")


def test_source_shape(tmp_path: Path) -> None:
    data = base_config()
    data["views"][0]["source"] = "db.public.orders"
    load_error(tmp_path, data, 'must be "table" or "schema.table"')


@pytest.mark.parametrize("bad", ["public.Orders", "Public.orders", ".orders", "orders."])
def test_source_parts_must_be_identifiers(tmp_path: Path, bad: str) -> None:
    data = base_config()
    data["views"][0]["source"] = bad
    load_error(tmp_path, data, "not a valid identifier")


# --- group_by ---------------------------------------------------------------


def test_group_by_must_be_a_list(tmp_path: Path) -> None:
    data = base_config()
    data["views"][0]["group_by"] = "status"
    load_error(tmp_path, data, "group_by must be a list")


def test_group_by_columns_must_not_repeat(tmp_path: Path) -> None:
    data = base_config()
    data["views"][0]["group_by"] = ["status", "region", "status"]
    load_error(tmp_path, data, "group_by column 'status' is repeated")


def test_group_by_columns_must_be_identifiers(tmp_path: Path) -> None:
    data = base_config()
    data["views"][0]["group_by"] = ["Status"]
    load_error(tmp_path, data, "not a valid identifier")


def test_group_by_column_must_not_be_bookkeeping_column(tmp_path: Path) -> None:
    data = base_config()
    data["views"][0]["group_by"] = ["__walflux_rows"]
    load_error(tmp_path, data, "reserved")


# --- aggregates -------------------------------------------------------------


@pytest.mark.parametrize("aggs", [None, []])
def test_at_least_one_aggregate_required(tmp_path: Path, aggs: object) -> None:
    data = base_config()
    if aggs is None:
        del data["views"][0]["aggregates"]
    else:
        data["views"][0]["aggregates"] = aggs
    load_error(tmp_path, data, "at least one aggregate")


def test_aggregate_fn_required(tmp_path: Path) -> None:
    data = base_config()
    data["views"][0]["aggregates"][0] = {"as": "order_count"}
    load_error(tmp_path, data, "missing required key 'fn'")


def test_unknown_aggregate_fn(tmp_path: Path) -> None:
    data = base_config()
    data["views"][0]["aggregates"][0] = {"fn": "median", "column": "total", "as": "med"}
    load_error(tmp_path, data, "unknown aggregate fn 'median'")


@pytest.mark.parametrize("fn", ["sum", "avg"])
def test_sum_and_avg_require_a_column(tmp_path: Path, fn: str) -> None:
    data = base_config()
    data["views"][0]["aggregates"][0] = {"fn": fn, "as": "broken"}
    load_error(tmp_path, data, f"{fn} requires a column")


def test_count_column_is_optional_both_ways(tmp_path: Path) -> None:
    # COUNT(*) has no column; COUNT(col) counts non-NULLs — both are valid.
    config = load(tmp_path, base_config())
    count_star, count_col = config.views[0].aggregates[:2]
    assert (count_star.fn, count_star.column) == ("count", None)
    assert (count_col.fn, count_col.column) == ("count", "coupon")


def test_aggregate_column_must_be_identifier(tmp_path: Path) -> None:
    data = base_config()
    data["views"][0]["aggregates"][2]["column"] = "Total Price"
    load_error(tmp_path, data, "not a valid identifier")


def test_alias_required(tmp_path: Path) -> None:
    data = base_config()
    data["views"][0]["aggregates"][0] = {"fn": "count"}
    load_error(tmp_path, data, "missing required key 'as'")


def test_alias_must_be_identifier(tmp_path: Path) -> None:
    data = base_config()
    data["views"][0]["aggregates"][0]["as"] = "Order Count"
    load_error(tmp_path, data, "not a valid identifier")


def test_alias_length_limit(tmp_path: Path) -> None:
    data = base_config()
    data["views"][0]["aggregates"][0]["as"] = "a" * 64
    load_error(tmp_path, data, "longer than 63 bytes")


def test_alias_must_be_unique_within_view(tmp_path: Path) -> None:
    data = base_config()
    data["views"][0]["aggregates"][1]["as"] = "order_count"
    load_error(tmp_path, data, "duplicate aggregate alias 'order_count'")


def test_alias_must_not_collide_with_group_column(tmp_path: Path) -> None:
    data = base_config()
    data["views"][0]["aggregates"][0]["as"] = "status"
    load_error(tmp_path, data, "collides with a group_by column")


@pytest.mark.parametrize("bad", ["__walflux_rows", "revenue__sum", "revenue__nn"])
def test_alias_reserved_names(tmp_path: Path, bad: str) -> None:
    data = base_config()
    data["views"][0]["aggregates"][0]["as"] = bad
    load_error(tmp_path, data, "is reserved")


def test_sum_alias_must_leave_room_for_storage_suffix(tmp_path: Path) -> None:
    # 60 bytes passes the plain 63-byte check but "<alias>__sum" would not fit.
    data = base_config()
    data["views"][0]["aggregates"][2]["as"] = "a" * 60
    load_error(tmp_path, data, "too long to leave room")
