from __future__ import annotations

import random
import sys
from pathlib import Path
from typing import Any

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "demo"))

import producer  # noqa: E402


class _Result:
    def list(self) -> list[dict[str, Any]]:
        return []


class _Runner:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, object]]] = []

    def sql(self, query: str, *parameters: object, **named_parameters: object) -> _Result:
        assert parameters == ()
        self.calls.append((query, named_parameters))
        return _Result()


def test_batches_for_phase_splits_on_table_boundaries() -> None:
    args = _args(batch_min=2, batch_max=2)
    table_a = producer.TableRef("s", "a")
    table_b = producer.TableRef("s", "b")
    actions = [
        producer.Action("insert", table_a, 1, "a1", 1),
        producer.Action("insert", table_b, 1, "b1", 3),
        producer.Action("insert", table_a, 2, "a2", 2),
        producer.Action("insert", table_b, 2, "b2", 4),
    ]

    batches = producer.build_batches(actions, args, random.Random(1))
    phase_batches = producer.batches_for_phase(batches, "insert", args)

    assert [[action.table.table for action in batch] for batch in phase_batches] == [
        ["a", "a"],
        ["b", "b"],
    ]


def test_apply_action_group_uses_range_insert_for_contiguous_inserts() -> None:
    runner = _Runner()
    args = _args()
    table = producer.TableRef("s", "events")
    actions = [
        producer.Action("insert", table, 1, "p1", 1),
        producer.Action("insert", table, 2, "p2", 2),
    ]

    producer.apply_action_group(runner, actions, args)

    assert len(runner.calls) == 1
    query, params = runner.calls[0]
    assert "INSERT INTO" in query
    assert "range($first_row_id, $end_row_id)" in query
    assert params["first_row_id"] == 1
    assert params["end_row_id"] == 3
    assert params["payload_prefix"] == "s.events."


def test_apply_action_group_uses_values_insert_for_non_contiguous_inserts() -> None:
    runner = _Runner()
    args = _args()
    table = producer.TableRef("s", "events")
    actions = [
        producer.Action("insert", table, 1, "p1", 1),
        producer.Action("insert", table, 3, "p3", 3),
    ]

    producer.apply_action_group(runner, actions, args)

    assert len(runner.calls) == 1
    query, params = runner.calls[0]
    assert "INSERT INTO" in query
    assert "range($first_row_id, $end_row_id)" not in query
    assert query.count("$id_") == 2
    assert params["id_0"] == 1
    assert params["id_1"] == 3


def test_apply_action_group_uses_one_bulk_update_statement() -> None:
    runner = _Runner()
    args = _args()
    table = producer.TableRef("s", "events")
    actions = [
        producer.Action("update", table, 1, "updated", 1),
        producer.Action("update", table, 2, "updated", 2),
    ]

    producer.apply_action_group(runner, actions, args)

    assert len(runner.calls) == 1
    query, params = runner.calls[0]
    assert "UPDATE" in query
    assert "FROM" in query
    assert query.count("$id_") == 2
    assert params["payload_0"] == "updated"


def test_parse_args_insert_rate_derives_inserts_per_table() -> None:
    args = producer.parse_args(
        [
            "--schemas",
            "1",
            "--tables",
            "10",
            "--insert-rate",
            "10000",
            "--duration",
            "30",
            "--update",
            "0",
            "--delete",
            "0",
        ]
    )

    assert args.insert_rate == 10000.0
    assert args.inserts == 30000


def test_parse_args_insert_rate_requires_insert_only_workload() -> None:
    with pytest.raises(SystemExit):
        producer.parse_args(["--insert-rate", "100", "--duration", "1"])


def test_progress_counter_reports_periodically(capsys: pytest.CaptureFixture[str]) -> None:
    progress = producer.ProgressCounter(
        label="insert",
        total_commits=2,
        total_actions=10,
        interval_s=100.0,
    )

    progress.record(commits=1, actions=4)
    assert capsys.readouterr().out == ""
    progress.record(commits=1, actions=6)

    output = capsys.readouterr().out
    assert "insert progress 2/2 commits, 10/10 actions" in output


def _args(*, batch_min: int = 1, batch_max: int = 10) -> producer.Args:
    return producer.Args(
        schemas=1,
        tables=1,
        inserts=2,
        insert_rate=None,
        update=25.0,
        delete=10.0,
        duration=0.0,
        profile="flat",
        batch_min=batch_min,
        batch_max=batch_max,
        workers=1,
        catalog=None,
        catalog_backend=None,
        storage=None,
        reset=False,
    )
