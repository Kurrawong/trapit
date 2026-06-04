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


def get_key(item: int) -> str:
    """Generate key for an item."""
    return f"item_{item}"


class TestBasicProcessing:
    """Test basic processing functionality."""

    def test_process_all_items(self, db_path):
        """Test that all items are processed."""
        items = [1, 2, 3, 4, 5]
        processed = []

        with TrackedParallelIterator(
            items, process_item_success, get_key, db_path, mode="multithreading"
        ) as pit:
            for item_key, result in pit:
                processed.append((item_key, result))

        assert len(processed) == 5
        assert processed[0] == ("item_1", 2)
        assert processed[1] == ("item_2", 4)

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
        assert len(results) == 0


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
        assert results[0][0] == "item_3"

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

        assert len(results) == 0


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
            for item_key, result in pit:
                processed.append((item_key, result))

        assert len(processed) == 5

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

        # Check error is in database
        env = lmdb.open(db_path, readonly=True)
        with env.begin() as txn:
            error_data = txn.get(b"error:item_3")
        env.close()
        assert error_data is not None


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
