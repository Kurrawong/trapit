# trapit

Tracked Reprocessable Async/Parallel Iterator - parallel processing with LMDB tracking.

## Installation

```bash
uv pip install trapit
```

Or with pip:

```bash
pip install trapit
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

# Process items with tracking
with TrackedParallelIterator(
    items,
    process_item,
    get_key,
    "./tracker_db",
    mode="multithreading",
    workers=4,
) as pit:
    for item_key, result in pit:
        print(f"Processed {item_key}: {result}")

# Log errors that occur outside the worker
try:
    do_something_with(result)
except Exception as e:
    pit.log_error(item_key, e)
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

- **Parallel Processing**: Supports both multiprocessing and multithreading modes
- **Persistent Tracking**: Uses LMDB for fast, reliable tracking of processed items
- **Error Tracking**: Errors are logged with timestamps, error types, messages, and tracebacks
- **Resumable**: Can resume processing from where it left off
- **Status Cleanup**: When an item's status changes (error → success or vice versa), old markers are automatically cleaned up

## Development

```bash
# Install dev dependencies
uv pip install -e ".[dev]"

# Run tests
pytest

# Run type checking
mypy tarpit
```
