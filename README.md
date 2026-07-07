# trapit

Tracked Reprocessable Async/Parallel Iterator - parallel processing with LMDB tracking.

## Installation

From GitHub (latest):

```bash
pip install git+https://github.com/kurrawong/trapit.git
```

From GitHub (specific version/tag):

```bash
uv pip install git+https://github.com/kurrawong/trapit.git@v0.0.1
```

## Usage

```python
from trapit import TrackedParallelIterator

def process_item(item: int) -> int:
    if item == 3:
        raise ValueError(f"Error on item {item}")
    return item * 2

def get_key(item: int) -> str:
    return f"item_{item}"

items = [1, 2, 3, 4, 5]

# Process items with tracking. If db_path is omitted, ".trapit" is used.
# If key_func is omitted, str(item) is used.
with TrackedParallelIterator(
    items,
    process_item,
    get_key,
    "./tracker_db",
    mode="multithreading",
    workers=4,
) as pit:
    for item, item_key, result in pit:
        print(f"Processed {item_key}: {result}")

    # Get processing statistics
    print(f"Completed: {pit.completed}")  # Number of successfully processed items
    print(f"Errors: {pit.errors}")        # Number of items that failed
    print(f"Skipped: {pit.skipped}")      # Number of items that were skipped

# Log errors that occur outside the worker
try:
    do_something_with(result)
except Exception as e:
    pit.log_error(item_key, e)

# Pass additional arguments to your processing function
def process_with_config(item: int, multiplier: int, offset: int = 0) -> int:
    return (item * multiplier) + offset

with TrackedParallelIterator(
    items,
    process_with_config,
    get_key,
    "./tracker_db",
    mode="multiprocessing",
    preserve_order=True,         # Optional: enforce input order
    worker_timeout=300,          # Kill stalled workers after 300s; default is 5s
    func_args=(3,),              # Pass multiplier=3 as positional arg
    func_kwargs={"offset": 10},  # Pass offset=10 as keyword arg
) as pit:
    for item, item_key, result in pit:
        print(f"Processed {item_key}: {result}")
```

## Reprocessing Modes

The `repro` parameter controls how items are reprocessed:

- **`"none"`** (default): Skip items that were already processed (success or error)
- **`"errors"`**: Only reprocess items that previously failed
- **`"all"`**: Process all items, ignoring existing state
- **`Callable[[T], bool]`**: A custom function that takes an item and returns `True` if it should be processed. When using a callable, the LMDB tracker is **ignored** for determining whether to process, but is still used to mark items as processed or in error after processing.

```python
# Retry only failed items
with TrackedParallelIterator(..., repro="errors") as pit:
    for item_key, result in pit:
        ...

# Reprocess everything from scratch
with TrackedParallelIterator(..., repro="all") as pit:
    for item_key, result in pit:
        ...

# Use a custom function to decide per-item
def should_reprocess(item: dict) -> bool:
    # Only reprocess high-priority items
    return item.get("priority") == "high"

with TrackedParallelIterator(..., repro=should_reprocess) as pit:
    for item_key, result in pit:
        ...
```

## Features

- **Processing Modes**: Supports multiprocessing, multithreading, and singlethreaded modes
- **Persistent Tracking**: Uses LMDB for fast, reliable tracking of processed items
- **Error Tracking**: Errors are logged with timestamps, error types, messages, and tracebacks
- **Resumable**: Can resume processing from where it left off
- **Status Cleanup**: When an item's status changes (error → success or vice versa), old markers are automatically cleaned up
- **Processing Statistics**: Track completed, errored, and skipped item counts with `completed`, `errors`, and `skipped` properties
- **3-tuple Yield**: Iteration now yields `(item, key, result)` for each successfully processed item
- **Rich Progress Bar**: Built-in progress bar with ETA, displayed when running in a TTY
- **Function Arguments**: Pass additional arguments to your processing function with `func_args` and `func_kwargs`
- **Default Tracking Path and Keys**: `db_path` defaults to `.trapit`, and `key_func` defaults to `str(item)`
- **Per-worker LMDB Environments**: In multiprocessing mode, each worker process opens LMDB once and closes it when the process exits
- **Responsive Multiprocessing Progress**: Multiprocessing is unordered by default so progress updates as workers complete; set `preserve_order=True` to yield in input order
- **Worker Timeout**: Set `worker_timeout` in multiprocessing mode to terminate the worker pool if no result arrives within the timeout
- **Dynamic LMDB Map Resizing**: The tracking database automatically grows when it gets close to the configured `map_size` limit. Tune with `map_resize_threshold` and `map_resize_factor`.

## Processing Modes

Use `mode="singlethreaded"` to process items sequentially while still using the same LMDB tracking, repro, error, and progress behavior:

```python
with TrackedParallelIterator(..., mode="singlethreaded") as pit:
    ...
```

## Multiprocessing Ordering and Timeouts

Multiprocessing mode uses unordered results by default for better progress reporting, especially on Windows:

```python
with TrackedParallelIterator(..., mode="multiprocessing") as pit:
    ...  # results may be yielded out of input order
```

To enforce input ordering:

```python
with TrackedParallelIterator(..., mode="multiprocessing", preserve_order=True) as pit:
    ...
```

To terminate stalled workers, set `worker_timeout` to the maximum number of seconds to wait for the next result. It defaults to 5 seconds. This requires `chunksize=1`; set `worker_timeout=None` to disable timeout handling:

```python
with TrackedParallelIterator(..., mode="multiprocessing", worker_timeout=300) as pit:
    ...
```

## Progress Bar

By default, a Rich progress bar is displayed when running in a terminal (TTY). You can control this behavior with the `show_progress` parameter:

```python
# Show progress bar (default when in TTY)
with TrackedParallelIterator(..., show_progress=True) as pit:
    ...

# Hide progress bar
with TrackedParallelIterator(..., show_progress=False) as pit:
    ...

# Auto-detect based on TTY (default behavior)
with TrackedParallelIterator(...) as pit:
    ...
```

The progress bar shows:

- A visual progress bar
- Task description
- Completed/total count
- Estimated time remaining

## Development

```bash
# Install dev dependencies
uv pip install -e ".[dev]"

# Run tests
pytest

# Run type checking
mypy tarpit
```
