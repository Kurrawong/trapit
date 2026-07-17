"""
TRAPIT - Tracked Async/Parallel Iterator with Rich Progress Bar

Extended to include a Rich progress bar with ETA, only displayed if running in a TTY.
"""

import logging
import os
import queue
import sys
import threading
from collections import deque
from collections.abc import Iterable
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from functools import partial
from time import monotonic
from multiprocessing import Pool, TimeoutError as MultiprocessingTimeoutError, cpu_count
from multiprocessing import util as multiprocessing_util
from typing import Callable, Hashable, Optional, TypeVar, Union

import lmdb
from rich.progress import BarColumn, Console, Progress, TextColumn, TimeRemainingColumn

logger = logging.getLogger(__name__)

T = TypeVar("T")
R = TypeVar("R")
K = TypeVar("K", bound=Hashable)
C = TypeVar("C")

_ENV_PREFIX = "TRAPIT_"
_UNSET = object()


def _parse_bool(value: str) -> bool:
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ValueError("expected true/false, yes/no, on/off, or 1/0")


def _parse_optional_float(value: str) -> Optional[float]:
    if value.strip().lower() in {"none", "null"}:
        return None
    return float(value)


def _parse_optional_bool(value: str) -> Optional[bool]:
    if value.strip().lower() in {"none", "null", "auto"}:
        return None
    return _parse_bool(value)


def _config_value(
    name: str, supplied: object, default: C, parser: Callable[[str], C]
) -> C:
    """Resolve an explicit value, then a TRAPIT_ environment value, then default."""
    if supplied is not _UNSET:
        return supplied  # type: ignore[return-value]

    env_name = f"{_ENV_PREFIX}{name.upper()}"
    value = os.environ.get(env_name)
    if value is None:
        return default
    try:
        return parser(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Invalid value for {env_name}: {value!r} ({exc})") from exc


# Type alias for repro parameter: can be a string mode or a callable
ReproType = Union[str, Callable[[T], bool]]

# Status constants for worker results
SKIPPED = "__SKIPPED__"
ERROR = "__ERROR__"
COMPLETED = "__COMPLETED__"
SUCCESS_MARKER = b"0"
ERROR_MARKER = b"1"

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
        logger.info("Increased LMDB map size from %s to %s bytes", map_size, new_size)


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


def _key_bytes(key: str) -> bytes:
    """Return LMDB key bytes for a marker key."""
    if not isinstance(key, str):
        raise TypeError(f"key_func must return str, got {type(key).__name__}")
    return key.encode()


def _write_markers_with_dynamic_map(
    env: lmdb.Environment,
    markers: list[tuple[str, Optional[bytes]]],
    threshold: float,
    resize_factor: float,
) -> None:
    """Write success/error markers in one transaction."""

    # If the same key appears multiple times in a batch, only the final state
    # matters. Coalescing reduces LMDB puts/deletes for duplicate keys.
    latest_markers = dict(markers)

    def write_markers(txn: lmdb.Transaction) -> None:
        for key, error_payload in latest_markers.items():
            key_bytes = _key_bytes(key)
            if error_payload is None:
                txn.put(key_bytes, SUCCESS_MARKER)
            else:
                txn.put(key_bytes, error_payload)

    _write_with_dynamic_map(env, write_markers, threshold, resize_factor)


class _QueuedMarkerWriter:
    """Single LMDB writer thread that batches marker updates."""

    _STOP = object()

    def __init__(
        self,
        db_path: str,
        map_size: int,
        threshold: float,
        resize_factor: float,
        batch_size: int,
        flush_interval: float,
        env: Optional[lmdb.Environment] = None,
    ) -> None:
        self._db_path = db_path
        self._map_size = map_size
        self._threshold = threshold
        self._resize_factor = resize_factor
        self._batch_size = batch_size
        self._flush_interval = flush_interval
        self._env = env
        self._owns_env = env is None
        queue_size = max(batch_size * 4, 1)
        self._queue: queue.Queue[tuple[str, Optional[bytes]] | object] = queue.Queue(
            maxsize=queue_size
        )
        self._closed = False
        self._state_lock = threading.Lock()
        self._error: BaseException | None = None
        self._thread = threading.Thread(
            target=self._run,
            name="trapit-lmdb-writer",
            daemon=False,
        )
        self._thread.start()

    def enqueue(self, key: str, error_payload: Optional[bytes]) -> None:
        while True:
            self._raise_if_failed()
            with self._state_lock:
                if self._closed:
                    raise RuntimeError("queued marker writer is closed")
            try:
                self._queue.put((key, error_payload), timeout=0.1)
                return
            except queue.Full:
                continue

    def close(self) -> None:
        with self._state_lock:
            already_closed = self._closed
            self._closed = True
        if already_closed:
            self._raise_if_failed()
            return
        while True:
            self._raise_if_failed()
            try:
                self._queue.put(self._STOP, timeout=0.1)
                break
            except queue.Full:
                continue
        self._thread.join()
        self._raise_if_failed()

    def _raise_if_failed(self) -> None:
        if self._error is not None:
            raise RuntimeError("queued marker writer failed") from self._error

    def _run(self) -> None:
        env = self._env
        if env is None:
            env = lmdb.open(
                self._db_path,
                map_size=self._map_size,
                writemap=True,
                readonly=False,
            )
        batch: list[tuple[str, Optional[bytes]]] = []
        try:
            next_flush = monotonic() + self._flush_interval
            while True:
                timeout = max(0.0, next_flush - monotonic())
                try:
                    if self._flush_interval == 0 and not batch:
                        item = self._queue.get()
                    else:
                        item = self._queue.get(timeout=timeout)
                except queue.Empty:
                    if batch:
                        _write_markers_with_dynamic_map(
                            env, batch, self._threshold, self._resize_factor
                        )
                        batch.clear()
                    next_flush = monotonic() + self._flush_interval
                    continue

                if item is self._STOP:
                    if batch:
                        _write_markers_with_dynamic_map(
                            env, batch, self._threshold, self._resize_factor
                        )
                        batch.clear()
                    return

                batch.append(item)  # type: ignore[arg-type]
                if len(batch) >= self._batch_size:
                    _write_markers_with_dynamic_map(
                        env, batch, self._threshold, self._resize_factor
                    )
                    batch.clear()
                    next_flush = monotonic() + self._flush_interval
        except Exception as exc:
            self._error = exc
        finally:
            if self._owns_env and env is not None:
                env.close()


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
    worker: Callable[[T], tuple[str, T, str, R | None | Exception, Optional[bytes]]],
    iterable: Iterable[T],
    max_pending: int,
):
    """Yield thread-pool results as they complete without submitting all items at once."""
    item_iterator = iter(iterable)
    pending = []

    try:
        for _ in range(max_pending):
            pending.append(executor.submit(worker, next(item_iterator)))
    except StopIteration:
        pass

    while pending:
        done, _ = wait(pending, return_when=FIRST_COMPLETED)
        remaining = []
        for future in pending:
            if future in done:
                yield future.result()
                try:
                    remaining.append(executor.submit(worker, next(item_iterator)))
                except StopIteration:
                    pass
            else:
                remaining.append(future)
        pending = remaining


def _ordered_thread_map(
    executor: ThreadPoolExecutor,
    worker: Callable[[T], tuple[str, T, str, R | None | Exception, Optional[bytes]]],
    iterable: Iterable[T],
    max_pending: int,
):
    """Yield thread-pool results in input order without submitting all items."""
    item_iterator = iter(iterable)
    pending: deque = deque()

    try:
        for _ in range(max_pending):
            pending.append(executor.submit(worker, next(item_iterator)))
    except StopIteration:
        pass

    while pending:
        future = pending.popleft()
        yield future.result()
        try:
            pending.append(executor.submit(worker, next(item_iterator)))
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
    func_kwargs: Optional[dict] = None,
    map_resize_threshold: float = 0.8,
    map_resize_factor: float = 2.0,
    defer_writes: bool = False,
    persistent_tracking: bool = True,
) -> tuple[str, T, str, R | None | Exception, Optional[bytes]]:
    """
    Process an item and return a tuple with status information.

    Returns:
        (status, item, key, result_or_error)
        where status is one of: COMPLETED, SKIPPED, ERROR
    """
    # Determine which environment to use. Multithreading passes a shared env;
    # multiprocessing workers use a per-process global initialized by the pool.
    # No environment is opened when persistent tracking is disabled.
    if persistent_tracking and env is None:
        assert db_path is not None
        assert map_size is not None
        env = _get_worker_env(db_path, map_size)

    if func_kwargs is None:
        func_kwargs = {}

    key = key_func(item)
    key_bytes = _key_bytes(key)

    # Callable reprocessing still acts as an item filter without persistence.
    if callable(repro):
        should_process = repro(item)
        if not should_process:
            return (SKIPPED, item, key, None, None)
    elif persistent_tracking:
        # repro is a string mode
        assert env is not None
        try:
            with env.begin() as txn:
                marker = txn.get(key_bytes)
                has_success = marker == SUCCESS_MARKER
                has_error = marker == ERROR_MARKER
        except lmdb.Error as exc:
            # If we can't read from the database, proceed with processing.
            logger.warning("Failed to read tracker state for key %r: %s", key, exc)
        else:
            if repro == "none":
                # Skip if already processed (success or error)
                if has_success or has_error:
                    return (SKIPPED, item, key, None, None)
            elif repro == "errors":
                # Only process items currently marked as errors (retry failures)
                if not has_error or has_success:
                    return (SKIPPED, item, key, None, None)
            elif repro == "all":
                # Process everything, ignore existing state
                pass
            else:
                raise ValueError(
                    f"repro must be 'none', 'errors', 'all', or a callable, got '{repro}'"
                )

    try:
        result = func(item, *func_args, **func_kwargs)

        if persistent_tracking and not defer_writes:
            assert env is not None

            def write_success(txn: lmdb.Transaction) -> None:
                txn.put(key_bytes, SUCCESS_MARKER)

            _write_with_dynamic_map(
                env, write_success, map_resize_threshold, map_resize_factor
            )
        return (COMPLETED, item, key, result, None)
    except Exception as e:
        error_payload = ERROR_MARKER if persistent_tracking else None
        if persistent_tracking and not defer_writes:
            assert env is not None

            def write_error(txn: lmdb.Transaction) -> None:
                txn.put(key_bytes, ERROR_MARKER)

            _write_with_dynamic_map(
                env, write_error, map_resize_threshold, map_resize_factor
            )
        logger.exception("Error processing key %r", key)
        return (ERROR, item, key, e, error_payload)


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
        batch_writes: If True (default), workers defer success/error marker writes
            to a single queued LMDB writer thread that flushes batches periodically.
            This reduces write contention while still persisting progress during
            long runs. Pending writes are flushed when iteration exits.
        write_batch_size: Maximum number of status updates per LMDB transaction
            when batch_writes=True.
        write_flush_interval: Maximum seconds to keep a partial write batch in
            memory before flushing it when batch_writes=True.
        persistent_tracking: Whether to read and write persistent LMDB state.
            Disable this to avoid all database I/O when reprocessing is not needed.

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
        db_path: str | object = _UNSET,
        mode: str | object = _UNSET,
        workers: Optional[int] | object = _UNSET,
        chunksize: int | object = _UNSET,
        map_size: int | object = _UNSET,
        map_resize_threshold: float | object = _UNSET,
        map_resize_factor: float | object = _UNSET,
        preserve_order: bool | object = _UNSET,
        worker_timeout: Optional[float] | object = _UNSET,
        repro: ReproType | object = _UNSET,
        func_args: tuple | None = None,
        func_kwargs: dict | None = None,
        show_progress: Optional[bool] | object = _UNSET,
        batch_writes: bool | object = _UNSET,
        write_batch_size: int | object = _UNSET,
        write_flush_interval: float | object = _UNSET,
        persistent_tracking: bool | object = _UNSET,
    ):
        # Explicit constructor arguments take precedence over environment values.
        db_path = _config_value("db_path", db_path, ".trapit", str)
        mode = _config_value("mode", mode, "multiprocessing", str)
        workers = _config_value("workers", workers, None, int)
        chunksize = _config_value("chunksize", chunksize, 1, int)
        map_size = _config_value("map_size", map_size, 1024 * 1024 * 1024, int)
        map_resize_threshold = _config_value(
            "map_resize_threshold", map_resize_threshold, 0.8, float
        )
        map_resize_factor = _config_value(
            "map_resize_factor", map_resize_factor, 2.0, float
        )
        preserve_order = _config_value(
            "preserve_order", preserve_order, False, _parse_bool
        )
        default_worker_timeout: Optional[float] = 300.0
        worker_timeout = _config_value(
            "worker_timeout",
            worker_timeout,
            default_worker_timeout,
            _parse_optional_float,
        )
        repro = _config_value("repro", repro, "none", str)
        show_progress = _config_value(
            "show_progress", show_progress, None, _parse_optional_bool
        )
        batch_writes = _config_value("batch_writes", batch_writes, True, _parse_bool)
        write_batch_size = _config_value(
            "write_batch_size", write_batch_size, 1000, int
        )
        write_flush_interval = _config_value(
            "write_flush_interval", write_flush_interval, 0.5, float
        )
        persistent_tracking = _config_value(
            "persistent_tracking", persistent_tracking, True, _parse_bool
        )

        if workers is None:
            workers = max(1, cpu_count() - 1)

        if mode not in ("multiprocessing", "multithreading", "singlethreaded"):
            raise ValueError(
                "mode must be 'multiprocessing', 'multithreading', or 'singlethreaded'"
            )
        if workers < 1:
            raise ValueError("workers must be at least 1")
        if chunksize < 1:
            raise ValueError("chunksize must be at least 1")
        if map_size < 1:
            raise ValueError("map_size must be positive")

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
        if write_batch_size < 1:
            raise ValueError("write_batch_size must be at least 1")
        if write_flush_interval < 0:
            raise ValueError("write_flush_interval must be non-negative")
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
        self.batch_writes = batch_writes
        self.write_batch_size = write_batch_size
        self.write_flush_interval = write_flush_interval
        self.persistent_tracking = persistent_tracking
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
        self._iterator = None
        self._entered = False
        self._exhausted = False
        self._deferred_writes: list[tuple[str, Optional[bytes]]] = []
        self._batch_writer: Optional[_QueuedMarkerWriter] = None
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
        if self._entered:
            raise RuntimeError("TrackedParallelIterator cannot be re-entered")
        self._entered = True
        self._exhausted = False
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
                TextColumn(
                    "completed={task.fields[completed_count]} "
                    "errors={task.fields[error_count]} "
                    "skipped={task.fields[skipped_count]}"
                ),
                TimeRemainingColumn(),
                console=Console(),
                expand=True,
            )
            self._progress.start()
            self._task_id = self._progress.add_task(
                "Processing",
                total=self._total,
                completed=0,
                completed_count=self._completed_count,
                error_count=self._error_count,
                skipped_count=self._skipped_count,
            )

        if self.mode == "multiprocessing":
            if self.persistent_tracking:
                self._pool = Pool(
                    self.workers,
                    initializer=_init_worker_env,
                    initargs=(self.db_path, self.map_size),
                )
            else:
                self._pool = Pool(self.workers)
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
                defer_writes=self.batch_writes,
                persistent_tracking=self.persistent_tracking,
            )
            map_method = (
                self._pool.imap if self.preserve_order else self._pool.imap_unordered
            )
            self._iterator = map_method(worker, self.iterable, chunksize=self.chunksize)
        elif self.mode in ("multithreading", "singlethreaded"):
            # Open an environment once for tracked thread/single-process modes.
            if self.persistent_tracking:
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
                defer_writes=self.batch_writes,
                persistent_tracking=self.persistent_tracking,
            )
            if self.mode == "multithreading":
                self._executor = ThreadPoolExecutor(max_workers=self.workers)
                if not self.preserve_order:
                    self._iterator = _unordered_thread_map(
                        self._executor, worker, self.iterable, self.workers
                    )
                else:
                    self._iterator = _ordered_thread_map(
                        self._executor, worker, self.iterable, self.workers
                    )
            else:
                self._iterator = map(worker, self.iterable)

        if self.persistent_tracking and self.batch_writes:
            self._batch_writer = _QueuedMarkerWriter(
                db_path=self.db_path,
                map_size=self.map_size,
                threshold=self.map_resize_threshold,
                resize_factor=self.map_resize_factor,
                batch_size=self.write_batch_size,
                flush_interval=self.write_flush_interval,
                env=self._env,
            )
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        cleanup_error: BaseException | None = None
        try:
            self._flush_deferred_writes()
        except BaseException as exc:
            cleanup_error = exc
        finally:
            try:
                if self.mode == "multiprocessing" and self._pool:
                    if not self._pool_terminated:
                        if exc_type is not None or not self._exhausted:
                            self._pool.terminate()
                            self._pool_terminated = True
                        else:
                            self._pool.close()
                        self._pool.join()
                elif self.mode == "multithreading" and self._executor:
                    self._executor.shutdown(
                        wait=True,
                        cancel_futures=(exc_type is not None or not self._exhausted),
                    )
            finally:
                if self._env:
                    self._env.close()
                    self._env = None
                if self._progress:
                    self._progress.stop()
                    self._progress = None
                self._entered = False

        if cleanup_error is not None and exc_type is None:
            raise cleanup_error

    def __iter__(self):
        """
        Iterate over processed items.

        Yields:
            tuple: (item, key, result) for each successfully processed item.
            Skipped and errored items are not yielded but are counted.
        """
        if not self._entered or self._iterator is None:
            raise RuntimeError(
                "TrackedParallelIterator must be used as a context manager"
            )

        try:
            while True:
                try:
                    if (
                        self.mode == "multiprocessing"
                        and self.worker_timeout is not None
                    ):
                        result = self._iterator.next(timeout=self.worker_timeout)
                    else:
                        result = next(self._iterator)
                except StopIteration:
                    self._exhausted = True
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
                    logger.warning("Worker returned None; skipping result")
                    continue
                status, item, key, data, error_payload = result
                if status == COMPLETED:
                    self._completed_count += 1
                    if self.persistent_tracking and self.batch_writes:
                        self._queue_marker_write(key, None)
                    self._update_progress(advance=1)
                    yield (item, key, data)
                elif status == SKIPPED:
                    self._skipped_count += 1
                    self._update_progress(advance=1)
                elif status == ERROR:
                    self._error_count += 1
                    if (
                        self.persistent_tracking
                        and self.batch_writes
                        and error_payload is not None
                    ):
                        self._queue_marker_write(key, error_payload)
                    self._update_progress(advance=1)
        finally:
            self._flush_deferred_writes()

    def _update_progress(self, advance: int = 0) -> None:
        """Update Rich progress completion and status counters."""
        if self._progress:
            self._progress.update(
                self._task_id,
                advance=advance,
                completed_count=self._completed_count,
                error_count=self._error_count,
                skipped_count=self._skipped_count,
            )

    def _queue_marker_write(self, key: str, error_payload: Optional[bytes]) -> None:
        """Queue a success/error marker write or fall back to deferred writes."""
        if self._batch_writer is not None:
            self._batch_writer.enqueue(key, error_payload)
        else:
            self._deferred_writes.append((key, error_payload))

    def _flush_deferred_writes(self) -> None:
        """Flush queued/deferred success/error markers, if batch_writes is enabled."""
        if self._batch_writer is not None:
            writer = self._batch_writer
            writer.close()
            self._batch_writer = None

        if not self._deferred_writes:
            return

        markers = self._deferred_writes
        if self._env is not None:
            _write_markers_with_dynamic_map(
                self._env,
                markers,
                self.map_resize_threshold,
                self.map_resize_factor,
            )
        else:
            env = lmdb.open(
                self.db_path, map_size=self.map_size, writemap=True, readonly=False
            )
            try:
                _write_markers_with_dynamic_map(
                    env,
                    markers,
                    self.map_resize_threshold,
                    self.map_resize_factor,
                )
            finally:
                env.close()
        self._deferred_writes = []

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

    def log_error(self, key: str) -> None:
        """
        Mark an item as errored in the LMDB database.

        This is useful for logging errors that occur outside the worker
        (e.g., during post-processing). The stored value is always
        ERROR_MARKER.

        Args:
            key: The item key
        """
        if not self.persistent_tracking:
            raise RuntimeError("log_error requires persistent_tracking=True")

        self._flush_deferred_writes()

        key_bytes = _key_bytes(key)

        def write_error(txn: lmdb.Transaction) -> None:
            txn.put(key_bytes, ERROR_MARKER)

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
