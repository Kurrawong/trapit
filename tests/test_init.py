import time

import lmdb
import pytest

from trapit import (
    COMPLETED,
    ERROR,
    ERROR_MARKER,
    SKIPPED,
    SUCCESS_MARKER,
    TrackedParallelIterator,
    _QueuedMarkerWriter,
    _key_bytes,
    _worker_process_item,
)


def double(item):
    return item * 2


def add_config(item, multiplier, offset=0):
    return item * multiplier + offset


def key(item):
    return f"item-{item}"


def fail_on_three(item):
    if item == 3:
        raise ValueError("boom")
    return item * 10


def sleep_inverse(item):
    time.sleep((4 - item) * 0.01)
    return item


def should_process_even(item):
    return item % 2 == 0


def marker_state(db_path, keys):
    env = lmdb.open(str(db_path), readonly=True, lock=False)
    try:
        with env.begin() as txn:
            return {k: txn.get(k.encode()) for k in keys}
    finally:
        env.close()


def run_iterator(tmp_path, items, func=double, **kwargs):
    db_path = tmp_path / "db"
    options = {
        "db_path": str(db_path),
        "mode": "singlethreaded",
        "show_progress": False,
    }
    options.update(kwargs)
    with TrackedParallelIterator(items, func, key_func=key, **options) as pit:
        results = list(pit)
        counts = (pit.completed, pit.errors, pit.skipped)
    return results, counts, db_path


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"mode": "bad"}, "mode must be"),
        ({"workers": 0}, "workers must be"),
        ({"chunksize": 0}, "chunksize must be"),
        ({"map_size": 0}, "map_size must be"),
        ({"key_func": "not-callable"}, "key_func must be callable"),
        ({"repro": "bad"}, "repro must be"),
        ({"chunksize": 2, "worker_timeout": 1}, "worker_timeout requires"),
        ({"map_resize_threshold": 0}, "map_resize_threshold"),
        ({"map_resize_threshold": 1}, "map_resize_threshold"),
        ({"map_resize_factor": 1}, "map_resize_factor"),
        ({"write_batch_size": 0}, "write_batch_size"),
        ({"write_flush_interval": -0.1}, "write_flush_interval"),
    ],
)
def test_constructor_validation(tmp_path, kwargs, message):
    kwargs.setdefault("db_path", str(tmp_path / "db"))
    with pytest.raises(ValueError, match=message):
        TrackedParallelIterator([1], double, show_progress=False, **kwargs)


def test_func_args_are_normalized(tmp_path):
    assert TrackedParallelIterator([], double, func_args=None).func_args == ()
    assert TrackedParallelIterator([], double, func_args=3).func_args == (3,)
    assert TrackedParallelIterator([], double, func_args="x").func_args == ("x",)
    assert TrackedParallelIterator([], double, func_args=[1, 2]).func_args == (1, 2)


def test_iterator_must_be_context_managed():
    pit = TrackedParallelIterator([1], double, mode="singlethreaded", show_progress=False)
    with pytest.raises(RuntimeError, match="context manager"):
        list(pit)


def test_context_cannot_be_reentered(tmp_path):
    pit = TrackedParallelIterator(
        [1], double, db_path=str(tmp_path / "db"), mode="singlethreaded", show_progress=False
    )
    with pit:
        with pytest.raises(RuntimeError, match="re-entered"):
            pit.__enter__()


def test_singlethreaded_processes_items_and_persists_success_markers(tmp_path):
    results, counts, db_path = run_iterator(tmp_path, [1, 2, 3])

    assert results == [(1, "item-1", 2), (2, "item-2", 4), (3, "item-3", 6)]
    assert counts == (3, 0, 0)
    assert marker_state(db_path, ["item-1", "item-2", "item-3"]) == {
        "item-1": SUCCESS_MARKER,
        "item-2": SUCCESS_MARKER,
        "item-3": SUCCESS_MARKER,
    }


def test_default_key_func_is_str_and_default_func_kwargs(tmp_path):
    db_path = tmp_path / "db"
    with TrackedParallelIterator(
        [1, 2], double, db_path=str(db_path), mode="singlethreaded", show_progress=False
    ) as pit:
        assert list(pit) == [(1, "1", 2), (2, "2", 4)]
    assert marker_state(db_path, ["1", "2"]) == {
        "1": SUCCESS_MARKER,
        "2": SUCCESS_MARKER,
    }


def test_func_args_and_kwargs_are_passed(tmp_path):
    results, counts, _ = run_iterator(
        tmp_path,
        [1, 2],
        add_config,
        func_args=3,
        func_kwargs={"offset": 5},
    )
    assert results == [(1, "item-1", 8), (2, "item-2", 11)]
    assert counts == (2, 0, 0)


def test_worker_errors_are_counted_not_yielded_and_persisted(tmp_path):
    results, counts, db_path = run_iterator(tmp_path, [1, 3, 4], fail_on_three)

    assert results == [(1, "item-1", 10), (4, "item-4", 40)]
    assert counts == (2, 1, 0)
    state = marker_state(db_path, ["item-1", "item-3", "item-4"])
    assert state["item-1"] == SUCCESS_MARKER
    assert state["item-3"] == ERROR_MARKER
    assert state["item-4"] == SUCCESS_MARKER


def test_repro_none_skips_success_and_error_records(tmp_path):
    _, first_counts, db_path = run_iterator(tmp_path, [1, 3], fail_on_three)
    assert first_counts == (1, 1, 0)

    with TrackedParallelIterator(
        [1, 3],
        double,
        key_func=key,
        db_path=str(db_path),
        mode="singlethreaded",
        repro="none",
        show_progress=False,
    ) as pit:
        assert list(pit) == []
        assert (pit.completed, pit.errors, pit.skipped) == (0, 0, 2)


def test_repro_errors_retries_only_errors_and_clears_error_marker(tmp_path):
    _, _, db_path = run_iterator(tmp_path, [1, 3], fail_on_three)

    with TrackedParallelIterator(
        [1, 3],
        double,
        key_func=key,
        db_path=str(db_path),
        mode="singlethreaded",
        repro="errors",
        show_progress=False,
    ) as pit:
        assert list(pit) == [(3, "item-3", 6)]
        assert (pit.completed, pit.errors, pit.skipped) == (1, 0, 1)

    assert marker_state(db_path, ["item-1", "item-3"])["item-3"] == SUCCESS_MARKER


def test_repro_all_reprocesses_existing_records(tmp_path):
    _, _, db_path = run_iterator(tmp_path, [1])

    with TrackedParallelIterator(
        [1],
        add_config,
        key_func=key,
        db_path=str(db_path),
        mode="singlethreaded",
        repro="all",
        func_args=(10,),
        show_progress=False,
    ) as pit:
        assert list(pit) == [(1, "item-1", 10)]
        assert (pit.completed, pit.errors, pit.skipped) == (1, 0, 0)


def test_callable_repro_ignores_lmdb_for_decision(tmp_path):
    _, _, db_path = run_iterator(tmp_path, [1, 2])

    with TrackedParallelIterator(
        [1, 2, 3, 4],
        double,
        key_func=key,
        db_path=str(db_path),
        mode="singlethreaded",
        repro=should_process_even,
        show_progress=False,
    ) as pit:
        assert list(pit) == [(2, "item-2", 4), (4, "item-4", 8)]
        assert (pit.completed, pit.errors, pit.skipped) == (2, 0, 2)


def test_log_error_marks_error_and_clears_success(tmp_path):
    _, _, db_path = run_iterator(tmp_path, [1])

    pit = TrackedParallelIterator(
        [], double, key_func=key, db_path=str(db_path), mode="singlethreaded", show_progress=False
    )
    pit.log_error("item-1")

    assert marker_state(db_path, ["item-1"])["item-1"] == ERROR_MARKER


def test_batch_writes_persist_success_and_error_markers(tmp_path):
    results, counts, db_path = run_iterator(
        tmp_path,
        [1, 2, 3, 4],
        fail_on_three,
        batch_writes=True,
        write_batch_size=2,
        write_flush_interval=0,
    )

    assert results == [(1, "item-1", 10), (2, "item-2", 20), (4, "item-4", 40)]
    assert counts == (3, 1, 0)
    state = marker_state(db_path, ["item-1", "item-2", "item-3", "item-4"])
    assert state["item-3"] == ERROR_MARKER
    assert state["item-4"] == SUCCESS_MARKER


def test_batch_writes_flush_when_iteration_exits_early(tmp_path):
    db_path = tmp_path / "db"
    with TrackedParallelIterator(
        [1, 2, 3],
        double,
        key_func=key,
        db_path=str(db_path),
        mode="singlethreaded",
        batch_writes=True,
        write_batch_size=100,
        write_flush_interval=60,
        show_progress=False,
    ) as pit:
        assert next(iter(pit)) == (1, "item-1", 2)

    assert marker_state(db_path, ["item-1"])["item-1"] == SUCCESS_MARKER


def test_multithreading_preserve_order(tmp_path):
    results, counts, _ = run_iterator(
        tmp_path,
        [1, 2, 3],
        sleep_inverse,
        mode="multithreading",
        workers=3,
        preserve_order=True,
    )
    assert results == [(1, "item-1", 1), (2, "item-2", 2), (3, "item-3", 3)]
    assert counts == (3, 0, 0)


def test_multithreading_unordered_completes_all_items(tmp_path):
    results, counts, _ = run_iterator(
        tmp_path,
        [1, 2, 3],
        sleep_inverse,
        mode="multithreading",
        workers=3,
        preserve_order=False,
    )
    assert sorted(results) == [(1, "item-1", 1), (2, "item-2", 2), (3, "item-3", 3)]
    assert counts == (3, 0, 0)


def test_multiprocessing_preserve_order(tmp_path):
    results, counts, db_path = run_iterator(
        tmp_path,
        [1, 2, 3],
        double,
        mode="multiprocessing",
        workers=2,
        preserve_order=True,
        worker_timeout=5,
    )
    assert results == [(1, "item-1", 2), (2, "item-2", 4), (3, "item-3", 6)]
    assert counts == (3, 0, 0)
    assert marker_state(db_path, ["item-1"])["item-1"] == SUCCESS_MARKER


def test_key_func_must_return_string(tmp_path):
    db_path = tmp_path / "db"
    env = lmdb.open(str(db_path), map_size=1024 * 1024)
    try:
        with pytest.raises(TypeError, match="key_func must return str"):
            _worker_process_item(1, double, lambda item: item, env=env)
    finally:
        env.close()


def test_worker_process_item_statuses_and_deferred_writes(tmp_path):
    db_path = tmp_path / "db"
    env = lmdb.open(str(db_path), map_size=1024 * 1024)
    try:
        assert _worker_process_item(1, double, key, env=env) == (
            COMPLETED,
            1,
            "item-1",
            2,
            None,
        )
        assert _worker_process_item(1, double, key, env=env)[0] == SKIPPED

        status, item, item_key, data, payload = _worker_process_item(
            3, fail_on_three, key, env=env, defer_writes=True, repro="all"
        )
        assert (status, item, item_key, payload) == (ERROR, 3, "item-3", ERROR_MARKER)
        assert isinstance(data, ValueError)
        # defer_writes=True returned an error payload but did not persist it.
        with env.begin() as txn:
            assert txn.get(b"item-3") is None
    finally:
        env.close()


def test_key_bytes_rejects_non_string_key():
    with pytest.raises(TypeError, match="key_func must return str"):
        _key_bytes(123)  # type: ignore[arg-type]


def test_queued_marker_writer_coalesces_and_rejects_enqueue_after_close(tmp_path):
    db_path = tmp_path / "db"
    writer = _QueuedMarkerWriter(
        str(db_path),
        map_size=1024 * 1024,
        threshold=0.8,
        resize_factor=2.0,
        batch_size=10,
        flush_interval=60,
    )
    writer.enqueue("a", ERROR_MARKER)
    writer.enqueue("a", None)
    writer.enqueue("b", ERROR_MARKER)
    writer.close()

    assert marker_state(db_path, ["a", "b"]) == {
        "a": SUCCESS_MARKER,
        "b": ERROR_MARKER,
    }
    with pytest.raises(RuntimeError, match="closed"):
        writer.enqueue("c", None)


def test_progress_bar_starts_and_stops_when_enabled(tmp_path):
    db_path = tmp_path / "db"
    with TrackedParallelIterator(
        [1], double, db_path=str(db_path), mode="singlethreaded", show_progress=True
    ) as pit:
        assert pit._progress is not None
        assert list(pit) == [(1, "1", 2)]
    assert pit._progress is None


def test_progress_bar_tracks_status_counts(tmp_path):
    db_path = tmp_path / "db"
    with TrackedParallelIterator(
        [1], double, db_path=str(db_path), mode="singlethreaded", show_progress=False
    ) as pit:
        assert list(pit) == [(1, "1", 2)]

    with TrackedParallelIterator(
        [1, 2, 3],
        fail_on_three,
        db_path=str(db_path),
        mode="singlethreaded",
        show_progress=True,
    ) as pit:
        assert list(pit) == [(2, "2", 20)]
        fields = pit._progress.tasks[0].fields
        assert fields["completed_count"] == 1
        assert fields["error_count"] == 1
        assert fields["skipped_count"] == 1


def test_generator_iterable_has_unknown_total_and_processes(tmp_path):
    db_path = tmp_path / "db"
    items = (i for i in [1, 2])
    with TrackedParallelIterator(
        items,
        double,
        key_func=key,
        db_path=str(db_path),
        mode="singlethreaded",
        show_progress=False,
    ) as pit:
        assert pit._total is None
        assert list(pit) == [(1, "item-1", 2), (2, "item-2", 4)]
