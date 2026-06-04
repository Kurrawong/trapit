"""
TRAPIT - Tracked Async/Parallel Iterator

A parallel processing utility that tracks processed items in LMDB to support
resumable processing with configurable reprocessing modes.
"""

import pickle
import traceback
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from functools import partial
from multiprocessing import Pool
from typing import Callable, Hashable, Iterable, Optional, TypeVar

import lmdb

T = TypeVar("T")
R = TypeVar("R")
K = TypeVar("K", bound=Hashable)


def _worker_process_item(
    item: T,
    func: Callable[[T], R],
    key_func: Callable[[T], str],
    env: Optional[lmdb.Environment] = None,
    db_path: Optional[str] = None,
    map_size: Optional[int] = None,
    repro: str = "none",
) -> Optional[tuple[str, R]]:
    # Determine which environment to use
    if env is None:
        # Multiprocessing: each process opens its own
        assert db_path is not None
        assert map_size is not None
        env = lmdb.open(db_path, map_size=map_size, writemap=True, readonly=False)

    key = key_func(item)
    try:
        with env.begin() as txn:
            has_success = txn.get(key.encode()) is not None
            has_error = txn.get(f"error:{key}".encode()) is not None

            if repro == "none":
                # Skip if already processed (success or error)
                if has_success or has_error:
                    return None
            elif repro == "errors":
                # Only process items that have errors but no success (retry failures)
                if not has_error or has_success:
                    return None
            elif repro == "all":
                # Process everything, ignore existing state
                pass
            else:
                raise ValueError(
                    f"repro must be 'none', 'errors', or 'all', got '{repro}'"
                )

        result = func(item)
        with env.begin(write=True) as txn:
            txn.put(key.encode(), b"1")
            # Clear any existing error marker for this key
            txn.delete(f"error:{key}".encode())
        return (key, result)
    except Exception as e:
        error_data = {
            "timestamp": datetime.now().isoformat(),
            "error_type": type(e).__name__,
            "error_message": str(e),
            "traceback": traceback.format_exc(),
        }
        with env.begin(write=True) as txn:
            txn.put(f"error:{key}".encode(), pickle.dumps(error_data))
            # Clear any existing success marker for this key
            txn.delete(key.encode())
        return None
    finally:
        # Close if we opened it ourselves (multiprocessing mode)
        if env is not None and db_path is not None:
            env.close()


class TrackedParallelIterator:
    """
    A parallel iterator that tracks processed items in LMDB.

    Supports multiprocessing and multithreading modes with configurable
    reprocessing behavior.

    Args:
        iterable: The input items to process
        func: Function to apply to each item
        key_func: Function to generate a unique string key for each item
        db_path: Path to the LMDB database directory
        mode: 'multiprocessing' or 'multithreading'
        workers: Number of parallel workers
        chunksize: For multiprocessing, number of items per chunk
        map_size: LMDB map size in bytes
        repro: Reprocessing mode - 'none', 'errors', or 'all'
            - 'none': Skip items already processed (success or error)
            - 'errors': Only reprocess items that previously errored
            - 'all': Process all items, ignoring existing state

    Example:
        with TrackedParallelIterator(
            items, process_item, get_key, "./tracker_db", mode="multithreading"
        ) as pit:
            for item_key, result in pit:
                print(f"Processed {item_key}: {result}")
    """

    def __init__(
        self,
        iterable: Iterable[T],
        func: Callable[[T], R],
        key_func: Callable[[T], str],
        db_path: str,
        mode: str = "multiprocessing",
        workers: int = 4,
        chunksize: int = 1,
        map_size: int = 1024 * 1024 * 1024,
        repro: str = "none",
    ):
        if repro not in ("none", "errors", "all"):
            raise ValueError(f"repro must be 'none', 'errors', or 'all', got '{repro}'")
        self.iterable = iterable
        self.func = func
        self.key_func = key_func
        self.db_path = db_path
        self.mode = mode
        self.workers = workers
        self.chunksize = chunksize
        self.map_size = map_size
        self.repro = repro
        self._pool = None
        self._executor = None
        self._env = None  # Shared environment for multithreading

    def __enter__(self):
        if self.mode == "multiprocessing":
            self._pool = Pool(self.workers)
            worker = partial(
                _worker_process_item,
                func=self.func,
                key_func=self.key_func,
                db_path=self.db_path,
                map_size=self.map_size,
                repro=self.repro,
            )
            self._iterator = self._pool.imap(
                worker, self.iterable, chunksize=self.chunksize
            )
        elif self.mode == "multithreading":
            self._executor = ThreadPoolExecutor(max_workers=self.workers)
            # Open environment once for all threads to share
            self._env = lmdb.open(
                self.db_path, map_size=self.map_size, writemap=True, readonly=False
            )
            worker = partial(
                _worker_process_item,
                func=self.func,
                key_func=self.key_func,
                env=self._env,
                repro=self.repro,
            )
            self._iterator = self._executor.map(worker, self.iterable)
        else:
            raise ValueError("mode must be 'multiprocessing' or 'multithreading'")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.mode == "multiprocessing" and self._pool:
            self._pool.close()
            self._pool.join()
        elif self.mode == "multithreading" and self._executor:
            self._executor.shutdown(wait=True)
            if self._env:
                self._env.close()
                self._env = None

    def __iter__(self):
        return (result for result in self._iterator if result is not None)

    def log_error(self, key: str, error: Exception) -> None:
        """
        Log an error to the LMDB database with the given key.

        This is useful for logging errors that occur outside the worker
        (e.g., during post-processing).

        Args:
            key: The item key (without 'error:' prefix)
            error: The exception that occurred
        """
        error_data = {
            "timestamp": datetime.now().isoformat(),
            "error_type": type(error).__name__,
            "error_message": str(error),
            "traceback": traceback.format_exc(),
        }

        # Use existing environment if available (multithreading mode)
        if self._env is not None:
            with self._env.begin(write=True) as txn:
                txn.put(f"error:{key}".encode(), pickle.dumps(error_data))
                # Clear any existing success marker
                txn.delete(key.encode())
        else:
            # Open a new environment for multiprocessing mode or external usage
            env = lmdb.open(
                self.db_path, map_size=self.map_size, writemap=True, readonly=False
            )
            try:
                with env.begin(write=True) as txn:
                    txn.put(f"error:{key}".encode(), pickle.dumps(error_data))
                    # Clear any existing success marker
                    txn.delete(key.encode())
            finally:
                env.close()
