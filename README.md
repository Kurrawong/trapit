# trapit

`trapit` is a **Tracked Reprocessable Async/Parallel Iterator** for Python. It applies a function to an iterable, optionally in parallel, while recording per-item success/error markers in an LMDB database so future runs can skip completed work or retry failed work.

It is useful for long-running batch jobs, crawlers, data pipelines, ETL tasks, enrichment jobs, and any workload where you want resumable processing with lightweight persistent state.

## Features

- **Persistent item tracking** with LMDB.
- **Resume support**: skip previously successful or failed items by default.
- **Retry failed items** with `repro="errors"`.
- **Force full reprocessing** with `repro="all"`.
- **Custom reprocessing decisions** with `Callable[[item], bool]`.
- **Three processing modes**:
  - `"multiprocessing"`
  - `"multithreading"`
  - `"singlethreaded"`
- **Unordered or ordered parallel result yielding** via `preserve_order`.
- **Worker timeout handling** for multiprocessing.
- **Status overwrite** when an item changes from error to success or success to error.
- **Rich progress bar** with ETA, enabled automatically in a TTY.
- **Processing counters** for completed, errored, and skipped items.

## Requirements

- Python `>=3.12`
- `lmdb`
- `rich`

## Installation

From GitHub:

```bash
pip install git+https://github.com/kurrawong/trapit.git
```

Install a specific tag or revision:

```bash
pip install git+https://github.com/kurrawong/trapit.git@v0.1.3
```

## Quick start

```python
from trapit import TrackedParallelIterator


def process_item(item: int) -> int:
    if item == 3:
        raise ValueError("example failure")
    return item * 2


items = [1, 2, 3, 4, 5]

with TrackedParallelIterator(
    iterable=items,
    func=process_item,
    mode="multithreading",
    workers=4,
) as pit:
    for item, key, result in pit:
        print(item, key, result)

print("completed", pit.completed)
print("errors", pit.errors)
print("skipped", pit.skipped)
```

Iteration yields only successful results as:

```python
(item, key, result)
```

Skipped and errored items are not yielded, but they are counted on the iterator.

## Core API

```python
TrackedParallelIterator(
    iterable,
    func,
    key_func=None,
    db_path=".trapit",
    mode="multiprocessing",
    workers=None,
    chunksize=1,
    map_size=1024 * 1024 * 1024,
    map_resize_threshold=0.8,
    map_resize_factor=2.0,
    preserve_order=False,
    worker_timeout=300,
    repro="none",
    func_args=None,
    func_kwargs=None,
    show_progress=None,
    batch_writes=True,
    write_batch_size=1000,
    write_flush_interval=0.5,
)
```

### Environment configuration

Optional configuration can also be set with `TRAPIT_`-prefixed environment
variables. Names are the uppercase parameter names, for example:

```bash
TRAPIT_DB_PATH=.trapit TRAPIT_MODE=multithreading TRAPIT_WORKERS=4 python app.py
```

Supported variables are `TRAPIT_DB_PATH`, `TRAPIT_MODE`, `TRAPIT_WORKERS`,
`TRAPIT_CHUNKSIZE`, `TRAPIT_MAP_SIZE`, `TRAPIT_MAP_RESIZE_THRESHOLD`,
`TRAPIT_MAP_RESIZE_FACTOR`, `TRAPIT_PRESERVE_ORDER`, `TRAPIT_WORKER_TIMEOUT`,
`TRAPIT_REPRO`, `TRAPIT_SHOW_PROGRESS`, `TRAPIT_BATCH_WRITES`,
`TRAPIT_WRITE_BATCH_SIZE`, and `TRAPIT_WRITE_FLUSH_INTERVAL`.

Boolean values accept `true`/`false`, `yes`/`no`, `on`/`off`, or `1`/`0`.
Use `none` for `TRAPIT_WORKER_TIMEOUT` to disable the timeout and `auto` for
`TRAPIT_SHOW_PROGRESS` to retain TTY detection. Explicit constructor arguments
take precedence over environment variables.

### Important parameters

| Parameter        | Default                   | Description                                                                           |
| ---------------- | ------------------------- | ------------------------------------------------------------------------------------- |
| `iterable`       | required                  | Items to process.                                                                     |
| `func`           | required                  | Callable applied to each item. Receives `item`, then `func_args`, then `func_kwargs`. |
| `key_func`       | `str`                     | Callable that returns a **string** key for each item.                                 |
| `db_path`        | `.trapit`                 | LMDB database directory used for tracking.                                            |
| `mode`           | `multiprocessing`         | One of `multiprocessing`, `multithreading`, or `singlethreaded`.                      |
| `workers`        | `max(1, cpu_count() - 1)` | Number of process/thread workers.                                                     |
| `chunksize`      | `1`                       | Multiprocessing chunk size. Must be `1` when `worker_timeout` is not `None`.          |
| `preserve_order` | `False`                   | Yield parallel results in input order when `True`.                                    |
| `worker_timeout` | `300`                     | Multiprocessing timeout in seconds for the next result. Use `None` to disable.        |
| `repro`          | `none`                    | Reprocessing mode. See below.                                                         |
| `show_progress`  | `None`                    | `None` auto-enables progress only when `sys.stdout.isatty()` is true.                 |
| `batch_writes`   | `True`                    | Defer marker writes to a queued writer thread.                                        |

## Tracking model

For each item, `key_func(item)` must return a `str`.

`trapit` stores compact LMDB markers:

- success marker: `key -> b"0"`
- error marker: `key -> b"1"`

Each item stores a single marker value at its key; later success/error updates overwrite the previous marker.

Exception details are logged through the named `trapit` logger, but only a compact error marker is stored in LMDB.

## Reprocessing modes

The `repro` parameter controls whether an item should be processed.

| Mode       | Behavior                                                                                                                                                                        |
| ---------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `"none"`   | Default. Skip items that already have either a success marker or an error marker.                                                                                               |
| `"errors"` | Process only items that currently have an error marker.                                                                                                                         |
| `"all"`    | Process every item, ignoring existing tracker state.                                                                                                                            |
| callable   | Call `repro(item)`. Process the item only if it returns `True`. Existing LMDB state is ignored for the decision, but new success/error state is still written after processing. |

Retry only failed items:

```python
with TrackedParallelIterator(
    items,
    process_item,
    key_func=item_key,
    repro="errors",
) as pit:
    for item, key, result in pit:
        print(key, result)
```

Reprocess everything:

```python
with TrackedParallelIterator(
    items,
    process_item,
    key_func=item_key,
    repro="all",
) as pit:
    list(pit)
```

Use a custom decision function:

```python
def should_process(item: dict) -> bool:
    return item.get("priority") == "high"


with TrackedParallelIterator(
    records,
    process_record,
    key_func=lambda record: record["id"],
    repro=should_process,
) as pit:
    for record, key, result in pit:
        ...
```

## Processing modes

### Single-threaded

Useful for debugging, deterministic local runs, or functions that should not run concurrently.

```python
with TrackedParallelIterator(
    items,
    process_item,
    mode="singlethreaded",
) as pit:
    for item, key, result in pit:
        ...
```

### Multithreading

Useful for I/O-bound work.

```python
with TrackedParallelIterator(
    items,
    process_item,
    mode="multithreading",
) as pit:
    for item, key, result in pit:
        ...
```

### Multiprocessing

Useful for CPU-bound work.

```python
with TrackedParallelIterator(
    items,
    process_item,
    mode="multiprocessing",
) as pit:
    for item, key, result in pit:
        ...
```

For multiprocessing, functions and items must be pickle-compatible on platforms that use process spawning.

## Result ordering

Parallel modes yield completed work as soon as it is available by default:

```python
with TrackedParallelIterator(items, process_item, preserve_order=False) as pit:
    ...  # results may be out of input order
```

Set `preserve_order=True` to yield successful results in input order:

```python
with TrackedParallelIterator(items, process_item, preserve_order=True) as pit:
    ...
```

## Multiprocessing timeouts

When `mode="multiprocessing"` and `worker_timeout` is not `None`, `trapit` waits at most that many
seconds for the next worker result. If the timeout expires, the pool is terminated and a
built-in `TimeoutError` is raised.

```python
with TrackedParallelIterator(
    items,
    process_item,
    mode="multiprocessing",
    worker_timeout=60,
) as pit:
    list(pit)
```

`worker_timeout` requires `chunksize=1`. Disable timeout handling with:

```python
TrackedParallelIterator(..., worker_timeout=None)
```

## Passing extra function arguments

`func_args` and `func_kwargs` are passed to `func` after the item.

```python
def process_with_config(item: int, multiplier: int, offset: int = 0) -> int:
    return item * multiplier + offset


with TrackedParallelIterator(
    [1, 2, 3],
    process_with_config,
    func_args=(10,),
    func_kwargs={"offset": 5},
    mode="singlethreaded",
) as pit:
    assert list(pit) == [(1, "1", 15), (2, "2", 25), (3, "3", 35)]
```

`func_args` normalization:

- `None` becomes `()`
- non-iterable scalars become `(value,)`
- strings and bytes are treated as scalar values
- other iterables become `tuple(value)`

## Progress bar

By default, progress is shown only when stdout is a TTY:

```python
TrackedParallelIterator(..., show_progress=None)
```

Force it on or off:

```python
TrackedParallelIterator(..., show_progress=True)
TrackedParallelIterator(..., show_progress=False)
```

The progress bar includes a bar, description, completed/total count, status counts
(`completed`, `errors`, `skipped`), and estimated time remaining.
If the iterable has no length, the total is unknown.

## Batch writes

By default, workers return marker updates to the main iterator, where a queued writer thread
batches them into fewer LMDB transactions. Set `batch_writes=False` to make each worker write
its own success/error marker immediately.

```python
with TrackedParallelIterator(
    items,
    process_item,
    mode="multiprocessing",
    batch_writes=True,
    write_batch_size=1000,
    write_flush_interval=0.5,
) as pit:
    for item, key, result in pit:
        ...
```

Pending writes are flushed when iteration exits, including early exits from the context manager.

## Dynamic LMDB map resizing

LMDB requires a configured map size. `trapit` starts with `map_size` and grows the map when
usage approaches `map_resize_threshold` or when LMDB raises `MapFullError`.

```python
TrackedParallelIterator(
    items,
    process_item,
    map_size=64 * 1024 * 1024,
    map_resize_threshold=0.8,
    map_resize_factor=2.0,
)
```

`map_resize_threshold` must be between `0` and `1`, and `map_resize_factor` must be greater than `1`.

## Logging external errors

Use `log_error(key)` to mark an item as errored outside the worker function, for example during downstream post-processing.

```python
with TrackedParallelIterator(
    items,
    process_item,
) as pit:
    try:
        for item, key, result in pit:
            post_process(result)
    except Exception:
        pit.log_error(key)
```

`log_error` writes `key -> ERROR_MARKER`, overwriting any existing success marker for `key`.

## Counters

Inside the context manager, the following read-only properties are updated during iteration:

```python
pit.completed  # successful items yielded
pit.errors     # items whose worker function raised
pit.skipped    # items skipped by repro/tracker state
```

Counters are reset when entering the context manager.

## Caveats

- `TrackedParallelIterator` must be used as a context manager.
- The same instance cannot be re-entered while already active.
- `key_func` must return `str`; other return types raise `TypeError`.
- Worker exceptions are swallowed by the iterator, logged, counted, and marked as errors. They are not yielded.
- Multiprocessing requires pickle-compatible functions, arguments, and items on spawn-based platforms.
- `worker_timeout` applies only to multiprocessing result retrieval.

## Development

Install dependencies with `uv`:

```bash
uv sync
```

Run tests:

```bash
uv run pytest
```

Run linting:

```bash
uv run ruff check .
```

Run type checking:

```bash
uv run mypy trapit
```
