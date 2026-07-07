"""
TRAPIT - Tracked Async/Parallel Iterator with Rich Progress Bar

Extended to include a Rich progress bar with ETA, only displayed if running in a TTY.
"""

import logging
import pickle
import sys
import threading
import traceback
from collections.abc import Iterable
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from datetime import datetime
from functools import partial
from multiprocessing import Pool, TimeoutError as MultiprocessingTimeoutError, cpu_count
from multiprocessing import util as multiprocessing_util
from typing import Callable, Hashable, Optional, TypeVar, Union

import lmdb
from rich.progress import BarColumn, Console, Progress, TextColumn, TimeRemainingColumn

T = TypeVar("T")
R = TypeVar("R")
K = TypeVar("K", bound=Hashable)

# Type alias for repro parameter: can be a string mode or a callable
ReproType = Union[str, Callable[[T], bool]]

# Status constants for worker results
SKIPPED = "__SKIPPED__"
ERROR = "__ERROR__"
COMPLETED = "__COMPLETED__"

# Per-process LMDB environment used by multiprocessing workers.  This avoids
# opening and closing the environment for every item, which can exhaust file
# handles on Windows.
_WORKER_ENV: Optional[lmdb.Environment] = None
_WORKER_ENV_FINALIZER: Optional[multiprocessing_util.Finalize] = None
_MAP_RESIZE_LOCK = threading.RLock()


def _lmdb_used_bytes(env: lmdb.Environment) -> tuple[int, int]:
    """Return approximate used bytes and configured map size for an LMDB env."""
    info = env.info()
    stat = env.stat()
    used_bytes = (info["last_pgno"] + 1) * stat["psize"]
    return used_bytes, info["map_size"]


def _grow_map_if_needed(
    env: lmdb.Environment,
    threshold: float,
    resize_factor: float,
    required_size: Optional[int] = None,
) -> None:
    """Grow an LMDB map when usage is close to or beyond the current map size."""
    with _MAP_RESIZE_LOCK:
        used_bytes, map_size = _lmdb_used_bytes(env)
        target_size = required_size if required_size is not None else map_size
        if used_bytes < map_size * threshold and target_size <= map_size:
            return

        new_size = max(
            int(map_size * resize_factor),
            int(used_bytes * resize_factor),
            int(target_size * resize_factor),
            map_size + 1,
        )
        env.set_mapsize(new_size)
        logging.info("Increased LMDB map size from %s to %s bytes", map_size, new_size)


def _write_with_dynamic_map(
    env: lmdb.Environment,
    write_func: Callable[[lmdb.Transaction], None],
    threshold: float,
    resize_factor: float,
) -> None:
    """Run a write transaction, growing the map preemptively and on MapFullError."""
    _grow_map_if_needed(env, threshold, resize_factor)
    while True:
        try:
            with env.begin(write=True) as txn:
                write_func(txn)
            return
        except lmdb.MapFullError:
            used_bytes, map_size = _lmdb_used_bytes(env)
            _grow_map_if_needed(
                env,
                threshold=0,
                resize_factor=resize_factor,
                required_size=max(map_size + 1, used_bytes + 1),
            )


def _close_worker_env() -> None:
    """Close the multiprocessing worker's LMDB environment, if it is open."""
    global _WORKER_ENV, _WORKER_ENV_FINALIZER
    if _WORKER_ENV is not None:
        _WORKER_ENV.close()
        _WORKER_ENV = None
    _WORKER_ENV_FINALIZER = None


def _init_worker_env(db_path: str, map_size: int) -> None:
    """Initialize the per-process LMDB environment for a pool worker."""
    global _WORKER_ENV, _WORKER_ENV_FINALIZER

    if _WORKER_ENV is not None:
        _close_worker_env()

    _WORKER_ENV = lmdb.open(db_path, map_size=map_size, writemap=True, readonly=False)
    _WORKER_ENV_FINALIZER = multiprocessing_util.Finalize(
        _WORKER_ENV, _close_worker_env, exitpriority=10
    )


def _get_worker_env(db_path: str, map_size: int) -> lmdb.Environment:
    """Return the per-process LMDB environment, opening it if necessary."""
    if _WORKER_ENV is None:
        _init_worker_env(db_path, map_size)
    assert _WORKER_ENV is not None
    return _WORKER_ENV


def _unordered_thread_map(
    executor: ThreadPoolExecutor,
    worker: Callable[[T], tuple[str, T, str, R | None | Exception]],
    iterable: Iterable[T],
    max_pending: int,
):
    """Yield thread-pool results as they complete without submitting all items at once."""
    item_iterator = iter(iterable)
    pending = set()

    try:
        for _ in range(max_pending):
            pending.add(executor.submit(worker, next(item_iterator)))
    except StopIteration:
        pass

    while pending:
        done, pending = wait(pending, return_when=FIRST_COMPLETED)
        for future in done:
            yield future.result()
            try:
                pending.add(executor.submit(worker, next(item_iterator)))
            except StopIteration:
                pass


def _worker_process_item(
    item: T,
    func: Callable[[T], R],
    key_func: Callable[[T], str],
    env: Optional[lmdb.Environment] = None,
    db_path: Optional[str] = None,
    map_size: Optional[int] = None,
    repro: ReproType = "none",
    func_args: tuple = (),
    func_kwargs: dict = dict(),
    map_resize_threshold: float = 0.8,
    map_resize_factor: float = 2.0,
) -> tuple[str, T, str, R | None | Exception]:
    """
    Process an item and return a tuple with status information.

    Returns:
        (status, item, key, result_or_error)
        where status is one of: COMPLETED, SKIPPED, ERROR
    """
    # Determine which environment to use.  Multithreading passes a shared env;
    # multiprocessing workers use a per-process global initialized by the pool.
    if env is None:
        assert db_path is not None
        assert map_size is not None
        env = _get_worker_env(db_path, map_size)

    key = key_func(item)

    # Check if repro is a callable - if so, use it to determine processing
    # The LMDB tracker is ignored for the decision, but still used for marking
    if callable(repro):
        should_process = repro(item)
        if not should_process:
            return (SKIPPED, item, key, None)
    else:
        # repro is a string mode
        try:
            with env.begin() as txn:
                has_success = txn.get(key.encode()) is not None
                has_error = txn.get(f"error:{key}".encode()) is not None

                if repro == "none":
                    # Skip if already processed (success or error)
                    if has_success or has_error:
                        return (SKIPPED, item, key, None)
                elif repro == "errors":
                    # Only process items that have errors but no success (retry failures)
                    if not has_error or has_success:
                        return (SKIPPED, item, key, None)
                elif repro == "all":
                    # Process everything, ignore existing state
                    pass
                else:
                    raise ValueError(
                        f"repro must be 'none', 'errors', 'all', or a callable, got '{repro}'"
                    )
        except Exception:
            # If we can't read from the database, proceed with processing
            pass

    try:
        result = func(item, *func_args, **func_kwargs)

        def write_success(txn: lmdb.Transaction) -> None:
            txn.put(key.encode(), b"1")
            # Clear any existing error marker for this key
            txn.delete(f"error:{key}".encode())

        _write_with_dynamic_map(
            env, write_success, map_resize_threshold, map_resize_factor
        )
        return (COMPLETED, item, key, result)
    except Exception as e:
        error_data = {
            "timestamp": datetime.now().isoformat(),
            "error_type": type(e).__name__,
            "error_message": str(e),
            "traceback": traceback.format_exc(),
        }
        def write_error(txn: lmdb.Transaction) -> None:
            txn.put(f"error:{key}".encode(), pickle.dumps(error_data))
            # Clear any existing success marker for this key
            txn.delete(key.encode())

        _write_with_dynamic_map(env, write_error, map_resize_threshold, map_resize_factor)
        logging.error(e)
        return (ERROR, item, key, e)


class TrackedParallelIterator:
    """
    A parallel iterator that tracks processed items in LMDB.

    Supports multiprocessing, multithreading, and singlethreaded modes with
    configurable reprocessing behavior and an optional Rich progress bar.

    Args:
        iterable: The input items to process
        func: Function to apply to each item
        key_func: Function to generate a unique string key for each item. Defaults
            to str(item).
        db_path: Path to the LMDB database directory. Defaults to ".trapit".
        mode: 'multiprocessing', 'multithreading', or 'singlethreaded'
        workers: Number of parallel workers. Defaults to cpu_count - 1
        chunksize: For multiprocessing, number of items per chunk
        map_size: Initial LMDB map size in bytes
        map_resize_threshold: Grow the LMDB map when approximate usage reaches
            this fraction of the current map size. Defaults to 0.8.
        map_resize_factor: Multiplier used when growing the LMDB map. Defaults to 2.0.
        preserve_order: For multiprocessing and multithreading, yield results in input
            order. Defaults to False so progress can update as workers complete.
        worker_timeout: For multiprocessing, maximum seconds to wait for the next
            worker result before terminating the pool and raising TimeoutError.
            Defaults to 300 seconds. Requires chunksize=1 unless set to None.
        repro: Reprocessing mode - 'none', 'errors', 'all', or a callable
            - 'none': Skip items already processed (success or error)
            - 'errors': Only reprocess items that previously errored
            - 'all': Process all items, ignoring existing state
            - Callable[[T], bool]: A function that takes an item and returns True
              if it should be processed.
        func_args: Additional positional arguments to pass to func. Can be a single value,
            an iterable (tuple, list, etc.), or None. Non-iterable values (except strings/bytes)
            are wrapped in a tuple. (default: None)
        func_kwargs: Additional keyword arguments to pass to func (default: {})
        show_progress: Whether to show a Rich progress bar (default: True if TTY)

    Yields:
        tuple: (item, key, result) for each successfully processed item.

    Attributes:
        completed: Returns the number of successfully completed items.
        errors: Returns the number of items that resulted in errors.
        skipped: Returns the number of items that were skipped.
    """

    def __init__(
        self,
        iterable: Iterable[T],
        func: Callable[[T], R],
        key_func: Callable[[T], str] | None = None,
        db_path: str = ".trapit",
        mode: str = "multiprocessing",
        workers: Optional[int] = None,
        chunksize: int = 1,
        map_size: int = 1024 * 1024 * 1024,
        map_resize_threshold: float = 0.8,
        map_resize_factor: float = 2.0,
        preserve_order: bool = False,
        worker_timeout: Optional[float] = 300,
        repro: ReproType = "none",
        func_args: tuple | None = None,
        func_kwargs: dict | None = None,
        show_progress: Optional[bool] = None,
    ):
        if workers is None:
            workers = max(1, cpu_count() - 1)

        if key_func is None:
            key_func = str
        elif not callable(key_func):
            raise ValueError("key_func must be callable")

        if not callable(repro) and repro not in ("none", "errors", "all"):
            raise ValueError(
                f"repro must be 'none', 'errors', 'all', or a callable, got '{repro}'"
            )
        if worker_timeout is not None and chunksize != 1:
            raise ValueError("worker_timeout requires chunksize=1")
        if not 0 < map_resize_threshold < 1:
            raise ValueError("map_resize_threshold must be between 0 and 1")
        if map_resize_factor <= 1:
            raise ValueError("map_resize_factor must be greater than 1")
        self.iterable = iterable
        self.func = func
        self.key_func = key_func
        self.db_path = db_path
        self.mode = mode
        self.workers = workers
        self.chunksize = chunksize
        self.map_size = map_size
        self.map_resize_threshold = map_resize_threshold
        self.map_resize_factor = map_resize_factor
        self.preserve_order = preserve_order
        self.worker_timeout = worker_timeout
        self.repro = repro
        # Normalize func_args to a tuple
        # Accept: None -> (), scalar -> (scalar,), iterable -> tuple(iterable)
        if func_args is None:
            self.func_args = ()
        elif isinstance(func_args, Iterable) and not isinstance(
            func_args, (str, bytes)
        ):
            self.func_args = tuple(func_args)
        else:
            self.func_args = (func_args,)
        self.func_kwargs = func_kwargs if func_kwargs is not None else {}
        self._pool = None
        self._pool_terminated = False
        self._executor = None
        self._env = None  # Shared environment for multithreading
        # Counters for tracking progress
        self._completed_count = 0
        self._error_count = 0
        self._skipped_count = 0

        # Determine if progress bar should be shown
        self._show_progress = show_progress
        if self._show_progress is None:
            self._show_progress = sys.stdout.isatty()

        # Try to determine total from iterable length
        try:
            self._total: Optional[int] = len(iterable)  # type: ignore[arg-type]
        except TypeError:
            self._total = None

        self._progress = None

    def __enter__(self):
        # Reset counters when entering context
        self._pool_terminated = False
        self._completed_count = 0
        self._error_count = 0
        self._skipped_count = 0

        # Initialize progress bar if enabled
        if self._show_progress:
            self._progress = Progress(
                BarColumn(),
                TextColumn("[progress.description]{task.description}"),
                TextColumn("[progress.completed]{task.completed}/{task.total}"),
                TimeRemainingColumn(),
                console=Console(),
                expand=True,
            )
            self._progress.start()
            self._task_id = self._progress.add_task(
                "Processing",
                total=self._total,
                completed=0,
            )

        if self.mode == "multiprocessing":
            self._pool = Pool(
                self.workers,
                initializer=_init_worker_env,
                initargs=(self.db_path, self.map_size),
            )
            worker = partial(
                _worker_process_item,
                func=self.func,
                key_func=self.key_func,
                db_path=self.db_path,
                map_size=self.map_size,
                repro=self.repro,
                func_args=self.func_args,
                func_kwargs=self.func_kwargs,
                map_resize_threshold=self.map_resize_threshold,
                map_resize_factor=self.map_resize_factor,
            )
            map_method = (
                self._pool.imap if self.preserve_order else self._pool.imap_unordered
            )
            self._iterator = map_method(worker, self.iterable, chunksize=self.chunksize)
        elif self.mode in ("multithreading", "singlethreaded"):
            # Open environment once for all threads, or once for the current
            # process in singlethreaded mode.
            self._env = lmdb.open(
                self.db_path, map_size=self.map_size, writemap=True, readonly=False
            )
            worker = partial(
                _worker_process_item,
                func=self.func,
                key_func=self.key_func,
                env=self._env,
                repro=self.repro,
                func_args=self.func_args,
                func_kwargs=self.func_kwargs,
                map_resize_threshold=self.map_resize_threshold,
                map_resize_factor=self.map_resize_factor,
            )
            if self.mode == "multithreading":
                self._executor = ThreadPoolExecutor(max_workers=self.workers)
                if not self.preserve_order:
                    self._iterator = _unordered_thread_map(
                        self._executor, worker, self.iterable, self.workers
                    )
                else:
                    self._iterator = self._executor.map(worker, self.iterable)
            else:
                self._iterator = map(worker, self.iterable)
        else:
            raise ValueError(
                "mode must be 'multiprocessing', 'multithreading', or 'singlethreaded'"
            )
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._progress:
            self._progress.stop()

        if self.mode == "multiprocessing" and self._pool:
            if not self._pool_terminated:
                self._pool.close()
                self._pool.join()
        elif self.mode == "multithreading" and self._executor:
            self._executor.shutdown(wait=True)
        if self._env:
            self._env.close()
            self._env = None

    def __iter__(self):
        """
        Iterate over processed items.

        Yields:
            tuple: (item, key, result) for each successfully processed item.
            Skipped and errored items are not yielded but are counted.
        """
        while True:
            try:
                if self.mode == "multiprocessing" and self.worker_timeout is not None:
                    result = self._iterator.next(timeout=self.worker_timeout)
                else:
                    result = next(self._iterator)
            except StopIteration:
                break
            except MultiprocessingTimeoutError as exc:
                if self._pool is not None:
                    self._pool.terminate()
                    self._pool.join()
                    self._pool_terminated = True
                raise TimeoutError(
                    f"No multiprocessing worker result received within "
                    f"{self.worker_timeout} seconds; worker pool was terminated"
                ) from exc

            if result is None:
                continue
            status, item, key, data = result
            if status == COMPLETED:
                self._completed_count += 1
                if self._progress:
                    self._progress.update(self._task_id, advance=1)
                yield (item, key, data)
            elif status == SKIPPED:
                self._skipped_count += 1
                if self._progress:
                    self._progress.update(self._task_id, advance=1)
            elif status == ERROR:
                self._error_count += 1
                if self._progress:
                    self._progress.update(self._task_id, advance=1)

    @property
    def completed(self) -> int:
        """
        Return the number of items that have been successfully completed.

        Returns:
            int: Count of completed items
        """
        return self._completed_count

    @property
    def errors(self) -> int:
        """
        Return the number of items that resulted in errors.

        Returns:
            int: Count of errored items
        """
        return self._error_count

    @property
    def skipped(self) -> int:
        """
        Return the number of items that were skipped.

        Returns:
            int: Count of skipped items
        """
        return self._skipped_count

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

        def write_error(txn: lmdb.Transaction) -> None:
            txn.put(f"error:{key}".encode(), pickle.dumps(error_data))
            # Clear any existing success marker
            txn.delete(key.encode())

        # Use existing environment if available (multithreading mode)
        if self._env is not None:
            _write_with_dynamic_map(
                self._env,
                write_error,
                self.map_resize_threshold,
                self.map_resize_factor,
            )
        else:
            # Open a new environment for multiprocessing mode or external usage
            env = lmdb.open(
                self.db_path, map_size=self.map_size, writemap=True, readonly=False
            )
            try:
                _write_with_dynamic_map(
                    env,
                    write_error,
                    self.map_resize_threshold,
                    self.map_resize_factor,
                )
            finally:
                env.close()
