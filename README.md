# redecorate

A drop-in distributed cache for Python functions using decorator syntax and Redis.

## Features

- Support for complex data types (dicts, lists, DataFrames, etc.).
- Configurable time-to-live for cache entries.
- Programmatically invalidate any function's cache.
- Memory cache size limits.
- Automatic serialization/deserialization.


## Installation

```bash
pip install redecorate
# or
uv add redecorate
```

## Usage

1. Create a `CacheConfig` object.
2. Call `redecorate` to create a `@cache` decorator.
3. Add the `@cache` decorator to any function to cache its results.

### Basic Caching

```python
from redecorate import redecorate, CacheConfig

# Configure the cache
config = CacheConfig(
    host="localhost",
    port=6379,
)

# Create the cache decorator
cache = redecorate(config)

# Use the decorator to cache function results
@cache(ttl=5)  # Cache for 5 seconds
def expensive_operation(x: int) -> int:
    return x * 2

# First call executes the function
result1 = expensive_operation(5)  # Returns 10

# Second call returns cached result
result2 = expensive_operation(5)  # Returns 10 from cache

# Different input executes function again
result3 = expensive_operation(7)  # Returns 14
```

### Caching Database Queries

```python
@cache(ttl=5)
def get_user_by_id(user_id: int) -> dict[str, str]:
    with psycopg.connect(
        host="localhost",
        port=5432,
        user="postgres",
        password="postgres",
        dbname="postgres"
    ) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, name, email
                FROM users
                WHERE id = %s
            """, (user_id,))
            result = cur.fetchone()
            return {
                "id": result[0],
                "name": result[1],
                "email": result[2]
            }

# Cache the result for user ID 1
user = get_user_by_id(1)
```

### Caching Complex Data Types

```python
from datetime import datetime

@cache(ttl=5)
def get_complex_data() -> dict[str, Any]:
    return {
        "string": "hello",
        "number": 42,
        "list": [1, 2, 3],
        "nested": {"a": 1, "b": [4, 5, 6], "c": {"x": "y"}},
        "date": datetime.now(),
    }
```

### Cache Invalidation

```python
@cache(ttl=30)
def get_data(key: str) -> dict[str, Any]:
    return {"key": key, "timestamp": time.time()}

# First call
result1 = get_data("test")

# Invalidate specific key
get_data.invalidate("test")

# Clear all cache entries
get_data.clear()
```

### Memory Caching with Redis Fallback

```python
config = CacheConfig(
    host="localhost",
    port=6379,
    db=0,
    prefix="myapp",
    storage_backend=KeyValueStorageBackend(
        MessagePackSerializer(),
        maxsize=100,  # Maximum number of items in memory cache
        ttl=5,        # Time-to-live for memory cache entries
    )
)

@cache()
def get_value(key: str) -> dict[str, Any]:
    return {"key": key, "timestamp": time.time()}
```

### Caching Pandas DataFrames

```python
import pandas as pd
import numpy as np

@cache(ttl=5)
def process_dataframe(size: int) -> pd.DataFrame:
    df = pd.DataFrame({
        "A": np.random.rand(size),
        "B": np.random.rand(size),
        "C": np.random.rand(size),
    })
    df["D"] = df["A"] + df["B"]
    df["E"] = df["C"] * 2
    return df

# Cache DataFrame operations
df = process_dataframe(1000)
```
