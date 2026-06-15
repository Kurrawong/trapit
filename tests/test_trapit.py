"""Tests for trapit package."""

import os
import shutil

import lmdb
import pytest

from trapit import TrackedParallelIterator


@pytest.fixture
def db_path():
    """Create a temporary directory for LMDB database."""
    path = "./test_lmdb_db"
    if os.path.exists(path):
        shutil.rmtree(path)
    os.makedirs(path, exist_ok=True)
    yield path
    if os.path.exists(path):
        shutil.rmtree(path)


@pytest.fixture
def attempt_count():
    """Track function call attempts."""
    return {}


def process_item_success(item: int) -> int:
    """Simple processing function that always succeeds."""
    return item * 2


def process_item_fail_on_3(item: int) -> int:
    """Fails on item 3, succeeds on others."""
    if item == 3:
        raise ValueError(f"Simulated error for item {item}")
    return item * 2


def process_item_with_retry(item: int, attempt_count: dict) -> int:
    """Fails on first attempt for item 3, succeeds on retry."""
    attempt_count[item] = attempt_count.get(item, 0) + 1
    if item == 3 and attempt_count[item] == 1:
        raise ValueError(f"Error on attempt {attempt_count[item]}")
    return item * 2


# Repro callable functions (defined at module level for pickling)
def repro_only_even(item: int) -> bool:
    """Callable that only processes even items."""
    return item % 2 == 0


def repro_process_all(item: int) -> bool:
    """Callable that processes all items."""
    return True


def repro_only_gt_2(item: int) -> bool:
    """Callable that only processes items > 2."""
    return item > 2


def repro_only_gt_3(item: int) -> bool:
    """Callable that only processes items > 3."""
    return item > 3


def fail_on_3_multiproc(item: int) -> int:
    """Fails on item 3, succeeds on others. For multiprocessing tests."""
    if item == 3:
        raise ValueError("Error")
    return item * 2


def fail_on_2_and_4(item: int) -> int:
    """Fails on items 2 and 4, succeeds on others."""
    if item in (2, 4):
        raise ValueError(f"Error on {item}")
    return item * 2


def get_key(item: int) -> str:
    """Generate key for an item."""
    return f"item_{item}"


def process_with_args(item: int, multiplier: int = 2) -> int:
    """Processing function that accepts additional arguments."""
    return item * multiplier


def process_with_args_and_kwargs(item: int, multiplier: int, offset: int = 0) -> int:
    """Processing function that accepts both args and kwargs."""
    return (item * multiplier) + offset


class TestBasicProcessing:
    """Test basic processing functionality."""

    def test_process_all_items(self, db_path):
        """Test that all items are processed."""
        items = [1, 2, 3, 4, 5]
        processed = []

        with TrackedParallelIterator(
            items, process_item_success, get_key, db_path, mode="multithreading"
        ) as pit:
            for item, item_key, result in pit:
                processed.append((item, item_key, result))

        assert len(processed) == 5
        assert processed[0] == (1, "item_1", 2)
        assert processed[1] == (2, "item_2", 4)
        # Test the count properties
        assert pit.completed == 5
        assert pit.errors == 0
        assert pit.skipped == 0

    def test_skip_already_processed(self, db_path):
        """Test that already processed items are skipped with repro='none'."""
        items = [1, 2, 3, 4, 5]

        # First run
        with TrackedParallelIterator(
            items, process_item_success, get_key, db_path, mode="multithreading"
        ) as pit:
            results = list(pit)
        assert len(results) == 5

        # Second run with repro='none' should skip all
        with TrackedParallelIterator(
            items,
            process_item_success,
            get_key,
            db_path,
            mode="multithreading",
            repro="none",
        ) as pit:
            results = list(pit)
            # All items should be skipped
            assert len(results) == 0
            assert pit.completed == 0
            assert pit.skipped == 5
            assert pit.errors == 0


class TestErrorHandling:
    """Test error handling and tracking."""

    def test_error_tracking(self, db_path):
        """Test that errors are tracked in LMDB."""
        items = [1, 2, 3, 4, 5]

        with TrackedParallelIterator(
            items, process_item_fail_on_3, get_key, db_path, mode="multithreading"
        ) as pit:
            results = list(pit)

        # Item 3 should have failed, so only 4 items should have results
        assert len(results) == 4
        # Check the count properties
        assert pit.completed == 4
        assert pit.errors == 1
        assert pit.skipped == 0

        # Check error is in database
        env = lmdb.open(db_path, readonly=True)
        with env.begin() as txn:
            error_data = txn.get(b"error:item_3")
            success_marker = txn.get(b"item_3")
        env.close()

        assert error_data is not None
        assert success_marker is None

    def test_log_error_method(self, db_path):
        """Test the log_error method."""
        items = [1, 2, 3]

        with TrackedParallelIterator(
            items, process_item_success, get_key, db_path, mode="multithreading"
        ) as pit:
            # Process items
            list(pit)
            # Log an error for item_1
            try:
                raise RuntimeError("Test error")
            except RuntimeError as e:
                pit.log_error("item_1", e)

        # Check error is in database
        env = lmdb.open(db_path, readonly=True)
        with env.begin() as txn:
            error_data = txn.get(b"error:item_1")
            success_marker = txn.get(b"item_1")
        env.close()

        assert error_data is not None
        # Success marker should be cleared when error is logged
        assert success_marker is None


class TestReproModes:
    """Test reprocessing modes."""

    def test_repro_all(self, db_path):
        """Test repro='all' processes everything."""
        items = [1, 2, 3, 4, 5]

        # First run
        with TrackedParallelIterator(
            items, process_item_success, get_key, db_path, mode="multithreading"
        ) as pit:
            list(pit)

        # Second run with repro='all' should process all again
        with TrackedParallelIterator(
            items,
            process_item_success,
            get_key,
            db_path,
            mode="multithreading",
            repro="all",
        ) as pit:
            results = list(pit)
            assert len(results) == 5
            assert pit.completed == 5
            assert pit.errors == 0
            assert pit.skipped == 0

    def test_repro_errors(self, db_path):
        """Test repro='errors' only retries items that had errors."""
        items = [1, 2, 3, 4, 5]

        # First run with a function that fails on item 3
        def fail_on_3(item: int) -> int:
            if item == 3:
                raise ValueError("Error")
            return item * 2

        with TrackedParallelIterator(
            items, fail_on_3, get_key, db_path, mode="multithreading"
        ) as pit:
            list(pit)

        # Second run with repro='errors' should only retry item 3
        def succeed_always(item: int) -> int:
            return item * 2

        with TrackedParallelIterator(
            items,
            succeed_always,
            get_key,
            db_path,
            mode="multithreading",
            repro="errors",
        ) as pit:
            results = list(pit)

        # Only item 3 should be processed
        assert len(results) == 1
        assert results[0][1] == "item_3"  # Now it's (item, key, result)
        assert results[0][0] == 3  # item
        assert results[0][2] == 6  # result
        assert pit.completed == 1
        assert pit.skipped == 4  # Items 1, 2, 4, 5 were skipped
        assert pit.errors == 0

    def test_repro_none_default(self, db_path):
        """Test repro='none' is the default."""
        items = [1, 2, 3]

        with TrackedParallelIterator(
            items, process_item_success, get_key, db_path, mode="multithreading"
        ) as pit:
            list(pit)

        with TrackedParallelIterator(
            items, process_item_success, get_key, db_path, mode="multithreading"
        ) as pit:
            results = list(pit)
            # All items should be skipped in second run
            assert len(results) == 0
            assert pit.completed == 0
            assert pit.skipped == 3
            assert pit.errors == 0


class TestStatusCleanup:
    """Test that status markers are cleaned up when state changes."""

    def test_success_clears_error(self, db_path):
        """Test that when an item succeeds after error, error marker is cleared."""
        items = [1, 2, 3]
        attempt_count = {}

        def fail_then_succeed(item: int) -> int:
            attempt_count[item] = attempt_count.get(item, 0) + 1
            if item == 3 and attempt_count[item] == 1:
                raise ValueError("First attempt fails")
            return item * 2

        # First run - item 3 fails
        with TrackedParallelIterator(
            items, fail_then_succeed, get_key, db_path, mode="multithreading"
        ) as pit:
            list(pit)

        # Check error marker exists
        env = lmdb.open(db_path, readonly=True)
        with env.begin() as txn:
            has_error = txn.get(b"error:item_3") is not None
            has_success = txn.get(b"item_3") is not None
        env.close()
        assert has_error
        assert not has_success

        # Second run with repro='errors' - item 3 succeeds
        with TrackedParallelIterator(
            items,
            fail_then_succeed,
            get_key,
            db_path,
            mode="multithreading",
            repro="errors",
        ) as pit:
            list(pit)

        # Check error marker is cleared, success marker exists
        env = lmdb.open(db_path, readonly=True)
        with env.begin() as txn:
            has_error = txn.get(b"error:item_3") is not None
            has_success = txn.get(b"item_3") is not None
        env.close()
        assert not has_error
        assert has_success

    def test_error_clears_success(self, db_path):
        """Test that when an item errors after success, success marker is cleared."""
        items = [1, 2, 3]
        attempt_count = {}

        def succeed_then_fail(item: int) -> int:
            attempt_count[item] = attempt_count.get(item, 0) + 1
            if item == 3 and attempt_count[item] == 2:
                raise ValueError("Second attempt fails")
            return item * 2

        # First run - all succeed
        with TrackedParallelIterator(
            items, succeed_then_fail, get_key, db_path, mode="multithreading"
        ) as pit:
            list(pit)

        # Check success marker exists
        env = lmdb.open(db_path, readonly=True)
        with env.begin() as txn:
            has_success = txn.get(b"item_3") is not None
        env.close()
        assert has_success

        # Second run with repro='all' - item 3 fails
        with TrackedParallelIterator(
            items,
            succeed_then_fail,
            get_key,
            db_path,
            mode="multithreading",
            repro="all",
        ) as pit:
            list(pit)

        # Check success marker is cleared, error marker exists
        env = lmdb.open(db_path, readonly=True)
        with env.begin() as txn:
            has_error = txn.get(b"error:item_3") is not None
            has_success = txn.get(b"item_3") is not None
        env.close()
        assert has_error
        assert not has_success


class TestMultiprocessingMode:
    """Test multiprocessing mode."""

    def test_multiprocessing_basic(self, db_path):
        """Test basic multiprocessing works."""
        items = [1, 2, 3, 4, 5]
        processed = []

        with TrackedParallelIterator(
            items,
            process_item_success,
            get_key,
            db_path,
            mode="multiprocessing",
            workers=2,
        ) as pit:
            for item, item_key, result in pit:
                processed.append((item, item_key, result))

        assert len(processed) == 5
        assert pit.completed == 5
        assert pit.errors == 0
        assert pit.skipped == 0

    def test_multiprocessing_error_tracking(self, db_path):
        """Test error tracking in multiprocessing mode."""
        items = [1, 2, 3, 4, 5]

        with TrackedParallelIterator(
            items,
            process_item_fail_on_3,
            get_key,
            db_path,
            mode="multiprocessing",
            workers=2,
        ) as pit:
            results = list(pit)

        # Item 3 should have failed
        assert len(results) == 4
        assert pit.completed == 4
        assert pit.errors == 1
        assert pit.skipped == 0

        # Check error is in database
        env = lmdb.open(db_path, readonly=True)
        with env.begin() as txn:
            error_data = txn.get(b"error:item_3")
        env.close()
        assert error_data is not None


class TestReproCallable:
    """Test callable repro function."""

    def test_callable_repro_filters_items(self, db_path):
        """Test that callable repro filters items correctly."""
        items = [1, 2, 3, 4, 5, 6]

        with TrackedParallelIterator(
            items,
            process_item_success,
            get_key,
            db_path,
            mode="multithreading",
            repro=repro_only_even,
        ) as pit:
            results = list(pit)

        # Only even items should be processed: 2, 4, 6
        assert len(results) == 3
        keys = [r[1] for r in results]  # Now it's (item, key, result)
        assert "item_2" in keys
        assert "item_4" in keys
        assert "item_6" in keys
        assert "item_1" not in keys
        assert "item_3" not in keys
        assert "item_5" not in keys
        # 3 items completed, 3 items skipped (1, 3, 5)
        assert pit.completed == 3
        assert pit.skipped == 3
        assert pit.errors == 0

    def test_callable_repro_ignores_lmdb_tracker(self, db_path):
        """Test that callable repro ignores existing LMDB state."""
        items = [1, 2, 3, 4]

        # First, process all items to mark them as done
        with TrackedParallelIterator(
            items,
            process_item_success,
            get_key,
            db_path,
            mode="multithreading",
            repro="all",
        ) as pit:
            list(pit)

        # Now use a callable that says to process all
        # Even though all items are marked as processed in LMDB,
        # the callable should cause them to be reprocessed
        with TrackedParallelIterator(
            items,
            process_item_success,
            get_key,
            db_path,
            mode="multithreading",
            repro=repro_process_all,
        ) as pit:
            results = list(pit)

        # All items should be processed again
        assert len(results) == 4
        assert pit.completed == 4
        assert pit.errors == 0
        assert pit.skipped == 0

    def test_callable_repro_still_tracks_state(self, db_path):
        """Test that LMDB tracking still works with callable repro."""
        items = [1, 2, 3, 4]

        with TrackedParallelIterator(
            items,
            process_item_success,
            get_key,
            db_path,
            mode="multithreading",
            repro=repro_only_gt_2,
        ) as pit:
            list(pit)

        # Check that processed items (3, 4) have success markers
        env = lmdb.open(db_path, readonly=True)
        with env.begin() as txn:
            assert txn.get(b"item_3") is not None
            assert txn.get(b"item_4") is not None
            # Items 1, 2 should NOT have markers
            assert txn.get(b"item_1") is None
            assert txn.get(b"item_2") is None
        env.close()
        # Check counters
        assert pit.completed == 2
        assert pit.skipped == 2
        assert pit.errors == 0

    def test_callable_repro_error_tracking(self, db_path):
        """Test that error tracking works with callable repro."""
        items = [1, 2, 3, 4]

        with TrackedParallelIterator(
            items,
            process_item_fail_on_3,
            get_key,
            db_path,
            mode="multithreading",
            repro=repro_process_all,
        ) as pit:
            list(pit)

        # Check that item 3 has an error marker
        env = lmdb.open(db_path, readonly=True)
        with env.begin() as txn:
            assert txn.get(b"error:item_3") is not None
            # Other items should have success markers
            assert txn.get(b"item_1") is not None
            assert txn.get(b"item_2") is not None
            assert txn.get(b"item_4") is not None
        env.close()
        # Check counters
        assert pit.completed == 3
        assert pit.errors == 1
        assert pit.skipped == 0

    def test_callable_repro_multiprocessing(self, db_path):
        """Test that callable repro works with multiprocessing mode."""
        items = [1, 2, 3, 4, 5, 6]

        with TrackedParallelIterator(
            items,
            process_item_success,
            get_key,
            db_path,
            mode="multiprocessing",
            workers=2,
            repro=repro_only_gt_3,
        ) as pit:
            results = list(pit)

        # Only items 4, 5, 6 should be processed
        assert len(results) == 3
        keys = [r[1] for r in results]  # Now it's (item, key, result)
        assert "item_4" in keys
        assert "item_5" in keys
        assert "item_6" in keys
        assert pit.completed == 3
        assert pit.skipped == 3
        assert pit.errors == 0


class TestProcessingCounts:
    """Test the completed(), errors(), and skipped() counting methods."""

    def test_all_completed_no_errors(self, db_path):
        """Test counting when all items complete successfully."""
        items = [1, 2, 3, 4, 5]

        with TrackedParallelIterator(
            items, process_item_success, get_key, db_path, mode="multithreading"
        ) as pit:
            results = list(pit)

        assert len(results) == 5
        assert pit.completed == 5
        assert pit.errors == 0
        assert pit.skipped == 0

    def test_mixed_completed_and_errors(self, db_path):
        """Test counting with a mix of completed and errored items."""
        items = [1, 2, 3, 4, 5]

        with TrackedParallelIterator(
            items, fail_on_2_and_4, get_key, db_path, mode="multithreading"
        ) as pit:
            results = list(pit)

        assert len(results) == 3  # items 1, 3, 5
        assert pit.completed == 3
        assert pit.errors == 2  # items 2, 4
        assert pit.skipped == 0

    def test_all_skipped(self, db_path):
        """Test counting when all items are skipped."""
        items = [1, 2, 3]

        # First run to process all
        with TrackedParallelIterator(
            items, process_item_success, get_key, db_path, mode="multithreading"
        ) as pit:
            list(pit)

        # Second run with repro='none' skips all
        with TrackedParallelIterator(
            items,
            process_item_success,
            get_key,
            db_path,
            mode="multithreading",
            repro="none",
        ) as pit:
            results = list(pit)

        assert len(results) == 0
        assert pit.completed == 0
        assert pit.errors == 0
        assert pit.skipped == 3

    def test_mixed_all_states(self, db_path):
        """Test counting with completed, errored, and skipped items."""
        items = [1, 2, 3, 4, 5, 6]

        # First run: process all with item 3 failing
        def fail_on_3(item: int) -> int:
            if item == 3:
                raise ValueError("Error")
            return item * 2

        with TrackedParallelIterator(
            items, fail_on_3, get_key, db_path, mode="multithreading"
        ) as pit:
            list(pit)

        # Second run with repro='errors' and callable that only processes items > 2
        # This should:
        # - Reprocess item 3 (was errored, and > 2) -> might succeed or fail again
        # - Skip items 1, 2 (< 2)
        # - Skip items 4, 5, 6 (no error marker, repro='errors')
        def succeed_all(item: int) -> int:
            return item * 2

        with TrackedParallelIterator(
            items,
            succeed_all,
            get_key,
            db_path,
            mode="multithreading",
            repro="errors",
        ) as pit:
            results = list(pit)

        # Only item 3 should be processed (it had an error)
        assert len(results) == 1
        assert results[0][0] == 3  # item
        assert pit.completed == 1
        assert pit.skipped == 5  # items 1, 2, 4, 5, 6
        assert pit.errors == 0

    def test_counters_reset_between_runs(self, db_path):
        """Test that counters are reset when entering a new context."""
        items = [1, 2, 3]

        with TrackedParallelIterator(
            items, process_item_success, get_key, db_path, mode="multithreading"
        ) as pit:
            list(pit)
            first_completed = pit.completed

        # Counters should be reset in new context
        with TrackedParallelIterator(
            items,
            process_item_success,
            get_key,
            db_path,
            mode="multithreading",
            repro="all",
        ) as pit:
            list(pit)
            second_completed = pit.completed

        assert first_completed == 3
        assert second_completed == 3

    def test_counters_multiprocessing(self, db_path):
        """Test that counters work correctly in multiprocessing mode."""
        items = [1, 2, 3, 4, 5]

        with TrackedParallelIterator(
            items,
            fail_on_3_multiproc,
            get_key,
            db_path,
            mode="multiprocessing",
            workers=2,
        ) as pit:
            results = list(pit)

        assert len(results) == 4
        assert pit.completed == 4
        assert pit.errors == 1
        assert pit.skipped == 0


class TestFuncArgsKwargs:
    """Test func_args and func_kwargs parameters."""

    def test_func_args_multithreading(self, db_path):
        """Test that func_args works with multithreading mode."""
        items = [1, 2, 3, 4, 5]
        multiplier = 3

        with TrackedParallelIterator(
            items,
            process_with_args,
            get_key,
            db_path,
            mode="multithreading",
            func_args=(multiplier,),
        ) as pit:
            results = list(pit)

        assert len(results) == 5
        assert results[0][2] == 1 * multiplier  # result
        assert results[1][2] == 2 * multiplier
        assert pit.completed == 5
        assert pit.errors == 0

    def test_func_kwargs_multithreading(self, db_path):
        """Test that func_kwargs works with multithreading mode."""
        items = [1, 2, 3, 4, 5]

        with TrackedParallelIterator(
            items,
            process_with_args,
            get_key,
            db_path,
            mode="multithreading",
            func_kwargs={"multiplier": 4},
        ) as pit:
            results = list(pit)

        assert len(results) == 5
        assert results[0][2] == 1 * 4
        assert results[1][2] == 2 * 4
        assert pit.completed == 5

    def test_func_args_and_kwargs_multithreading(self, db_path):
        """Test that func_args and func_kwargs work together with multithreading."""
        items = [1, 2, 3]

        with TrackedParallelIterator(
            items,
            process_with_args_and_kwargs,
            get_key,
            db_path,
            mode="multithreading",
            func_args=(2,),  # multiplier
            func_kwargs={"offset": 10},  # offset
        ) as pit:
            results = list(pit)

        assert len(results) == 3
        # (item * multiplier) + offset
        assert results[0][2] == (1 * 2) + 10  # 12
        assert results[1][2] == (2 * 2) + 10  # 14
        assert results[2][2] == (3 * 2) + 10  # 16
        assert pit.completed == 3

    def test_func_args_multiprocessing(self, db_path):
        """Test that func_args works with multiprocessing mode."""
        items = [1, 2, 3, 4, 5]
        multiplier = 5

        with TrackedParallelIterator(
            items,
            process_with_args,
            get_key,
            db_path,
            mode="multiprocessing",
            workers=2,
            func_args=(multiplier,),
        ) as pit:
            results = list(pit)

        assert len(results) == 5
        assert results[0][2] == 1 * multiplier
        assert results[1][2] == 2 * multiplier
        assert pit.completed == 5
        assert pit.errors == 0

    def test_func_kwargs_multiprocessing(self, db_path):
        """Test that func_kwargs works with multiprocessing mode."""
        items = [1, 2, 3, 4, 5]

        with TrackedParallelIterator(
            items,
            process_with_args,
            get_key,
            db_path,
            mode="multiprocessing",
            workers=2,
            func_kwargs={"multiplier": 6},
        ) as pit:
            results = list(pit)

        assert len(results) == 5
        assert results[0][2] == 1 * 6
        assert results[1][2] == 2 * 6
        assert pit.completed == 5

    def test_func_args_and_kwargs_multiprocessing(self, db_path):
        """Test that func_args and func_kwargs work together with multiprocessing."""
        items = [1, 2, 3]

        with TrackedParallelIterator(
            items,
            process_with_args_and_kwargs,
            get_key,
            db_path,
            mode="multiprocessing",
            workers=2,
            func_args=(3,),  # multiplier
            func_kwargs={"offset": 5},  # offset
        ) as pit:
            results = list(pit)

        assert len(results) == 3
        # (item * multiplier) + offset
        assert results[0][2] == (1 * 3) + 5  # 8
        assert results[1][2] == (2 * 3) + 5  # 11
        assert results[2][2] == (3 * 3) + 5  # 14
        assert pit.completed == 3

    def test_func_args_default_empty(self, db_path):
        """Test that func_args defaults to empty tuple."""
        items = [1, 2, 3]

        # Should work without specifying func_args
        with TrackedParallelIterator(
            items,
            process_with_args,
            get_key,
            db_path,
            mode="multithreading",
            # func_args not specified - should default to ()
            func_kwargs={"multiplier": 2},
        ) as pit:
            results = list(pit)

        assert len(results) == 3
        assert results[0][2] == 2
        assert pit.completed == 3


class TestInvalidInputs:
    """Test error handling for invalid inputs."""

    def test_invalid_repro_mode(self):
        """Test that invalid repro mode raises ValueError."""
        with pytest.raises(ValueError, match="repro must be"):
            TrackedParallelIterator(
                [1, 2, 3],
                process_item_success,
                get_key,
                "./test_db",
                repro="invalid",
            )

    def test_invalid_mode(self, db_path):
        """Test that invalid mode raises ValueError."""
        with pytest.raises(ValueError, match="mode must be"):
            with TrackedParallelIterator(
                [1, 2, 3],
                process_item_success,
                get_key,
                db_path,
                mode="invalid",
            ):
                pass
