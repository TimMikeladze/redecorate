import time
from datetime import datetime

import pytest
import redis

from src.redecorate.redis_cache import (
    CacheConfig,
    KeyValueStorageBackend,
    MessagePackSerializer,
    redecorate,
)


def setup_redis():
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
def clean_redis():
    """Clean Redis before each test"""
    setup_redis()


def test_basic_caching():
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

    @cache(ttl=5)
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
    expensive_operation.clear()


def test_cache_expiration():
    config = CacheConfig(
        host="localhost",
        port=6379,
        db=0,
        prefix="test",
        storage_backend=KeyValueStorageBackend(MessagePackSerializer()),
    )

    cache = redecorate(config)

    call_count = 0

    @cache(ttl=1)  # 1 second TTL
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
    get_timestamp.clear()


def test_complex_data_types():
    config = CacheConfig(
        host="localhost",
        port=6379,
        db=0,
        prefix="test",
        storage_backend=KeyValueStorageBackend(MessagePackSerializer()),
    )

    cache = redecorate(config)

    call_count = 0

    @cache(ttl=5)
    def get_complex_data() -> dict:
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
    get_complex_data.clear()


def test_cache_invalidation():
    config = CacheConfig(
        host="localhost",
        port=6379,
        db=0,
        prefix="test",
        storage_backend=KeyValueStorageBackend(MessagePackSerializer()),
    )

    cache = redecorate(config)

    call_count = 0

    @cache(ttl=30)
    def get_data(key: str) -> dict:
        nonlocal call_count
        call_count += 1
        return {"key": key, "count": call_count}

    # First call
    result1 = get_data("test")
    assert result1["count"] == 1
    assert call_count == 1

    # Invalidate the cache for this specific key
    get_data.invalidate("test")

    # Next call should recalculate
    result2 = get_data("test")
    assert result2["count"] == 2
    assert call_count == 2

    # Clear all cache entries
    get_data.clear()

    # Next call should recalculate again
    result3 = get_data("test")
    assert result3["count"] == 3
    assert call_count == 3
