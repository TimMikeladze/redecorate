import os
import time
from collections.abc import Generator
from datetime import datetime
from typing import Any, Callable, TypeVar

import numpy as np
import pandas as pd
import psycopg
import pytest
import redis

from src.redecorate import (
    CacheConfig,
    KeyValueStorageBackend,
    MessagePackSerializer,
    redecorate,
)

T = TypeVar("T")
CachedFunction = Callable[..., T]


def setup_redis() -> bool:
    """Setup Redis connection and clear test database"""
    redis_client = redis.Redis(host="localhost", port=6379, db=0)
    try:
        redis_client.ping()
        # Clear all keys in the test database
        redis_client.flushdb()
    except redis.ConnectionError:
        pytest.skip("Redis server is not running")
    else:
        return True
    finally:
        redis_client.close()


@pytest.fixture(autouse=True)
def clean_redis() -> None:
    """Clean Redis before each test"""
    setup_redis()


@pytest.fixture
def pg_connection() -> Generator[psycopg.Connection[tuple[Any, ...]], None, None]:
    """Create a PostgreSQL connection for testing."""
    conn = psycopg.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", "postgres"),
        dbname=os.getenv("POSTGRES_DB", "postgres"),
    )

    # Create test table
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS test_table (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
        )
        # Insert test data
        cur.execute(
            """
            INSERT INTO test_table (name) VALUES
            ('Alice'),
            ('Bob'),
            ('Charlie')
            ON CONFLICT DO NOTHING
        """
        )
    conn.commit()

    yield conn

    # Cleanup
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS test_table")
    conn.commit()
    conn.close()


def test_basic_caching() -> None:
    # Setup Redis connection and cache config
    config = CacheConfig(
        host="localhost",
        port=6379,
        db=0,
        prefix="test",
        storage_backend=KeyValueStorageBackend(MessagePackSerializer()),
    )

    cache = redecorate(config)

    # Define a simple function to cache
    call_count = 0

    @cache(ttl=5)  # type: ignore[call-arg]
    def expensive_operation(x: int) -> int:
        nonlocal call_count
        call_count += 1
        return x * 2

    # First call should execute the function
    result1 = expensive_operation(5)
    assert result1 == 10
    assert call_count == 1

    # Second call should return cached result
    result2 = expensive_operation(5)
    assert result2 == 10
    assert call_count == 1  # Function wasn't called again

    # Different input should execute function again
    result3 = expensive_operation(7)
    assert result3 == 14
    assert call_count == 2

    # Clean up
    expensive_operation.clear()  # type: ignore[attr-defined]


def test_cache_expiration() -> None:
    config = CacheConfig(
        host="localhost",
        port=6379,
        db=0,
        prefix="test",
        storage_backend=KeyValueStorageBackend(MessagePackSerializer()),
    )

    cache = redecorate(config)

    call_count = 0

    @cache(ttl=1)  # type: ignore[call-arg]
    def get_timestamp() -> int:
        nonlocal call_count
        call_count += 1
        return int(time.time())

    # First call
    result1 = get_timestamp()
    assert call_count == 1

    # Immediate second call should return cached result
    result2 = get_timestamp()
    assert result2 == result1
    assert call_count == 1

    # Wait for cache to expire
    time.sleep(1.1)

    # Call after expiration should get fresh result
    result3 = get_timestamp()
    assert result3 > result1
    assert call_count == 2

    # Clean up
    get_timestamp.clear()  # type: ignore[attr-defined]


def test_complex_data_types() -> None:
    config = CacheConfig(
        host="localhost",
        port=6379,
        db=0,
        prefix="test",
        storage_backend=KeyValueStorageBackend(MessagePackSerializer()),
    )

    cache = redecorate(config)

    call_count = 0

    @cache(ttl=5)  # type: ignore[call-arg]
    def get_complex_data() -> dict[str, Any]:
        nonlocal call_count
        call_count += 1
        return {
            "string": "hello",
            "number": 42,
            "list": [1, 2, 3],
            "nested": {"a": 1, "b": [4, 5, 6], "c": {"x": "y"}},
            "date": datetime.now(),
        }

    # First call
    result1 = get_complex_data()
    assert call_count == 1

    # Verify data structure
    assert isinstance(result1["string"], str)
    assert isinstance(result1["number"], int)
    assert isinstance(result1["list"], list)
    assert isinstance(result1["nested"], dict)
    assert isinstance(result1["date"], datetime)

    # Second call should return cached result
    result2 = get_complex_data()
    assert result2 == result1
    assert call_count == 1

    # Clean up
    get_complex_data.clear()  # type: ignore[attr-defined]


def test_cache_invalidation() -> None:
    config = CacheConfig(
        host="localhost",
        port=6379,
        db=0,
        prefix="test",
        storage_backend=KeyValueStorageBackend(MessagePackSerializer()),
    )

    cache = redecorate(config)

    call_count = 0

    @cache(ttl=30)  # type: ignore[call-arg]
    def get_data(key: str) -> dict[str, Any]:
        nonlocal call_count
        call_count += 1
        return {"key": key, "count": call_count}

    # First call
    result1 = get_data("test")
    assert result1["count"] == 1
    assert call_count == 1

    # Invalidate the cache for this specific key
    get_data.invalidate("test")  # type: ignore[attr-defined]

    # Next call should recalculate
    result2 = get_data("test")
    assert result2["count"] == 2
    assert call_count == 2

    # Clear all cache entries
    get_data.clear()  # type: ignore[attr-defined]

    # Next call should recalculate again
    result3 = get_data("test")
    assert result3["count"] == 3
    assert call_count == 3


def test_memory_caching() -> None:
    """Test that memory caching works correctly."""
    config = CacheConfig(
        host="localhost",
        port=6379,
        db=0,
        prefix="test",
        storage_backend=KeyValueStorageBackend(
            MessagePackSerializer(),
            maxsize=10,  # Small cache size for testing
            ttl=5,
        ),
    )

    cache = redecorate(config)
    call_count = 0

    @cache()  # type: ignore[call-arg]
    def get_value(key: str) -> dict[str, Any]:
        nonlocal call_count
        call_count += 1
        return {"key": key, "timestamp": time.time()}

    # First call - should miss both caches
    result1 = get_value("test1")
    assert call_count == 1

    # Second call to same key - should hit memory cache
    result2 = get_value("test1")
    assert result2 == result1
    assert call_count == 1

    # Force clear memory cache by filling it with other values
    for i in range(15):  # More than maxsize=10
        get_value(f"fill{i}")

    # Call original key - should hit Redis but miss memory
    result3 = get_value("test1")
    assert result3 == result1  # Same data from Redis
    assert call_count == 16  # Only new keys caused function calls

    # Wait for TTL to expire
    time.sleep(6)

    # Call after expiration - should miss both caches
    result4 = get_value("test1")
    assert result4 != result1
    assert call_count == 17

    # Clean up
    get_value.clear()  # type: ignore[attr-defined]


def test_custom_cache_params() -> None:
    """Test that custom cache parameters are respected."""
    config = CacheConfig(
        host="localhost",
        port=6379,
        db=0,
        prefix="test",
        storage_backend=KeyValueStorageBackend(
            MessagePackSerializer(),
            maxsize=5,  # Small cache size for testing
            ttl=2,  # Short TTL for testing
        ),
    )

    cache = redecorate(config)
    call_count = 0

    @cache()  # type: ignore[call-arg]
    def get_value(key: str) -> dict[str, Any]:
        nonlocal call_count
        call_count += 1
        return {"key": key, "timestamp": time.time()}

    # Fill memory cache
    for i in range(7):  # More than maxsize
        get_value(f"key{i}")

    assert call_count == 7

    # This should hit Redis but miss memory (due to LRU eviction)
    result = get_value("key0")
    assert call_count == 7  # No increase because it hit Redis
    assert result["key"] == "key0"  # Verify the result

    # Wait for TTL to expire
    time.sleep(2.1)

    # This should miss both caches
    result = get_value("key0")
    assert call_count == 8
    assert result["key"] == "key0"  # Verify the result

    # Clean up
    get_value.clear()  # type: ignore[attr-defined]


def test_postgres_caching(pg_connection: psycopg.Connection[tuple[Any, ...]]) -> None:
    """Test caching PostgreSQL query results."""
    config = CacheConfig(
        host="localhost",
        port=6379,
        db=0,
        prefix="test",
        storage_backend=KeyValueStorageBackend(MessagePackSerializer()),
    )

    cache = redecorate(config)
    query_count = 0

    @cache(ttl=5)  # type: ignore[call-arg]
    def get_user_by_id(user_id: int) -> dict[str, Any]:
        nonlocal query_count
        query_count += 1

        with pg_connection.cursor() as cur:
            cur.execute("SELECT id, name, created_at FROM test_table WHERE id = %s", (user_id,))
            result = cur.fetchone()
            if result is None:
                return {}

            return {"id": result[0], "name": result[1], "created_at": result[2]}

    try:
        # First call - should query the database
        user1 = get_user_by_id(1)
        assert user1["name"] == "Alice"
        assert query_count == 1

        # Second call - should use cache
        user1_cached = get_user_by_id(1)
        assert user1_cached == user1
        assert query_count == 1  # Query count shouldn't increase

        # Different user - should query database again
        user2 = get_user_by_id(2)
        assert user2["name"] == "Bob"
        assert query_count == 2
    finally:
        # Clean up
        get_user_by_id.clear()  # type: ignore[attr-defined]
        pg_connection.commit()  # Ensure transaction is committed before cleanup


def test_pandas_dataframe_caching() -> None:
    """Test caching pandas DataFrame results."""
    config = CacheConfig(
        host="localhost",
        port=6379,
        db=0,
        prefix="test",
        storage_backend=KeyValueStorageBackend(MessagePackSerializer()),
    )

    cache = redecorate(config)
    computation_count = 0

    @cache(ttl=5)  # type: ignore[call-arg]
    def process_dataframe(size: int) -> pd.DataFrame:
        nonlocal computation_count
        computation_count += 1

        # Create a sample DataFrame with some calculations
        df = pd.DataFrame({
            "A": np.random.rand(size),
            "B": np.random.rand(size),
            "C": np.random.rand(size),
        })

        # Perform some calculations
        df["D"] = df["A"] + df["B"]
        df["E"] = df["C"] * 2

        return df

    # First call - should compute the DataFrame
    df1 = process_dataframe(1000)
    assert isinstance(df1, pd.DataFrame)
    assert len(df1) == 1000
    assert computation_count == 1

    # Second call with same parameters - should use cache
    df2 = process_dataframe(1000)
    assert computation_count == 1  # Count shouldn't increase
    pd.testing.assert_frame_equal(df1, df2)  # DataFrames should be identical

    # Different size parameter - should compute new DataFrame
    df3 = process_dataframe(500)
    assert len(df3) == 500
    assert computation_count == 2

    # Clean up
    process_dataframe.clear()  # type: ignore[attr-defined]
