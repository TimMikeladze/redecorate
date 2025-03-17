import json
import logging
import os
import threading
import time
import uuid
import zlib
from abc import ABC, abstractmethod
from collections.abc import Iterator
from dataclasses import asdict, is_dataclass
from datetime import datetime
from enum import Enum
from functools import lru_cache, wraps
from typing import (
    Any,
    Callable,
    Optional,
    Protocol,
    TypeVar,
    Union,
    cast,
)

import msgpack
import pandas as pd
import redis

logger = logging.getLogger("redecorate")

# Generic type for better type safety
T = TypeVar("T")
DataT = TypeVar("DataT")


class CacheStorageBackend(ABC):
    """Abstract base class for different cache storage backends with built-in memory caching"""

    def __init__(self, maxsize: int = 1000, ttl: Optional[int] = 3600):
        """
        Initialize cache backend with memory caching.

        Args:
            maxsize: Maximum size of the in-memory LRU cache (default 1000)
            ttl: Optional TTL for cache entries (default 1 hour)
        """
        self.ttl = ttl

        # Create instance-specific LRU cache
        @lru_cache(maxsize=maxsize)
        def _memory_cache(key: str) -> Any:
            return None

        self._memory_cache = _memory_cache
        self._memory_cache_set: dict[str, Any] = {}

    def get(self, redis_client: redis.Redis, key: str) -> Any:
        """Get value from cache, trying memory first then Redis."""
        # Try memory cache first
        mem_result = self._memory_cache(key)
        if mem_result is not None:
            return mem_result

        # On memory miss, try Redis
        redis_result = self._get_from_redis(redis_client, key)
        if redis_result is not None:
            # Update memory cache
            self._memory_cache_set[key] = redis_result
            # Update the LRU cache
            self._memory_cache.cache_clear()  # Clear existing cache
            for k, _ in self._memory_cache_set.items():
                self._memory_cache(k)  # Rebuild cache with updated values
            return redis_result

        return None

    def set(self, redis_client: redis.Redis, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        """Set value in both memory and Redis."""
        # Update memory cache
        self._memory_cache_set[key] = value
        self._memory_cache.cache_clear()  # Clear existing cache
        for k, _ in self._memory_cache_set.items():
            self._memory_cache(k)  # Rebuild cache with updated values

        # Set in Redis
        return self._set_in_redis(redis_client, key, value, ttl or self.ttl)

    def delete(self, redis_client: redis.Redis, *keys: str) -> bool:
        """Delete values from both memory and Redis."""
        # Clear from memory cache
        for key in keys:
            self._memory_cache_set.pop(key, None)
        self._memory_cache.cache_clear()  # Clear existing cache
        for k, _ in self._memory_cache_set.items():
            self._memory_cache(k)  # Rebuild cache with remaining values

        # Delete from Redis
        return self._delete_from_redis(redis_client, *keys)

    def clear_pattern(self, redis_client: redis.Redis, pattern: str, scan_batch_size: int = 1000) -> int:
        """Clear all matching keys from both memory and Redis."""
        # Clear memory cache completely since we can't easily pattern match
        self._memory_cache_set.clear()
        self._memory_cache.cache_clear()

        # Clear from Redis
        return self._clear_pattern_from_redis(redis_client, pattern, scan_batch_size)

    @abstractmethod
    def _get_from_redis(self, redis_client: redis.Redis, key: str) -> Any:
        """Retrieve a value from Redis"""
        pass

    @abstractmethod
    def _set_in_redis(self, redis_client: redis.Redis, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        """Store a value in Redis with optional TTL"""
        pass

    @abstractmethod
    def _delete_from_redis(self, redis_client: redis.Redis, *keys: str) -> bool:
        """Delete a value from Redis"""
        pass

    def _clear_pattern_from_redis(self, redis_client: redis.Redis, pattern: str, scan_batch_size: int = 1000) -> int:
        """Clear all keys matching a pattern from Redis using non-blocking SCAN"""
        deleted_count = 0
        scan_count = scan_batch_size

        cursor = 0
        while True:
            cursor, keys = redis_client.scan(cursor=cursor, match=pattern, count=scan_count)

            if keys:
                keys_list = cast(list[bytes], keys)
                deleted = cast(int, redis_client.delete(*keys_list))
                deleted_count += deleted

                if deleted > 0:
                    logger.debug(f"Deleted {deleted} keys (batch), total: {deleted_count}")

            if cursor == 0:
                break

        return deleted_count


class Serializer(Protocol):
    """Protocol defining serialization methods"""

    def serialize(self, obj: Any) -> Union[str, bytes]: ...
    def deserialize(self, data: Union[str, bytes]) -> Any: ...


class CompressedJSONSerializer:
    """JSON serializer with optional compression"""

    def __init__(self, compression_threshold: int = 1000) -> None:
        self.compression_threshold = compression_threshold

    def _convert_value(self, obj: Any) -> Any:
        """Helper method to convert values for JSON serialization."""
        if isinstance(obj, datetime):
            return obj.isoformat()
        elif isinstance(obj, Enum):
            return str(obj)
        elif is_dataclass(obj) and not isinstance(obj, type):
            obj_dict = asdict(obj)
            return {k: self._convert_value(v) for k, v in obj_dict.items()}
        elif isinstance(obj, dict):
            return {k: self._convert_value(v) for k, v in obj.items()}
        elif isinstance(obj, (list, tuple)):
            return [self._convert_value(v) for v in obj]
        return obj

    def serialize(self, obj: Any) -> str:
        converted_obj = self._convert_value(obj)
        json_str = json.dumps(converted_obj)

        if len(json_str.encode()) > self.compression_threshold:
            return "c:" + zlib.compress(json_str.encode()).hex()
        return "r:" + json_str

    def deserialize(self, data: str) -> Any:
        prefix, value = data[:2], data[2:]
        if prefix == "c:":
            return json.loads(zlib.decompress(bytes.fromhex(value)).decode())
        return json.loads(value)


class MessagePackSerializer:
    """MessagePack serializer with optional compression"""

    def __init__(self, compression_threshold: int = 1000):
        self.compression_threshold = compression_threshold

    def _convert_value(self, obj: Any) -> Any:
        """Helper method to convert values for MessagePack serialization."""
        if isinstance(obj, datetime):
            return {"__datetime__": True, "value": obj.isoformat()}
        elif isinstance(obj, Enum):
            return {
                "__enum__": True,
                "class": obj.__class__.__name__,
                "value": obj.value,
            }
        elif hasattr(obj, "__class__") and obj.__class__.__name__ == "DataFrame":
            return {
                "__dataframe__": True,
                "data": obj.to_dict(orient="split"),
            }
        elif is_dataclass(obj) and not isinstance(obj, type):
            return {k: self._convert_value(v) for k, v in asdict(obj).items()}
        elif isinstance(obj, dict):
            return {k: self._convert_value(v) for k, v in obj.items()}
        elif isinstance(obj, (list, tuple)):
            return [self._convert_value(v) for v in obj]
        return obj

    def serialize(self, obj: Any) -> Union[str, bytes]:
        converted_obj = self._convert_value(obj)
        packed = msgpack.packb(converted_obj)

        if len(packed) > self.compression_threshold:
            return b"c" + zlib.compress(packed)
        return b"r" + packed

    def deserialize(self, data: Union[str, bytes]) -> Any:
        if isinstance(data, str):
            data = data.encode("utf-8")
        prefix, value = data[:1], data[1:]
        if prefix == b"c":
            return self._process_obj(msgpack.unpackb(zlib.decompress(value)))
        return self._process_obj(msgpack.unpackb(value))

    def _process_obj(self, obj: Any) -> Any:
        """Process special objects during deserialization."""
        if isinstance(obj, dict):
            if obj.get("__datetime__"):
                return datetime.fromisoformat(obj["value"])
            elif obj.get("__enum__"):
                # You'd need more logic here to recreate the enum
                return obj["value"]
            elif obj.get("__dataframe__"):
                return pd.DataFrame(**obj["data"])
            return {k: self._process_obj(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._process_obj(v) for v in obj]
        return obj


class KeyValueStorageBackend(CacheStorageBackend):
    """Redis key-value storage backend using serialization with built-in memory caching"""

    def __init__(self, serializer: Serializer, maxsize: int = 1000, ttl: Optional[int] = 3600):
        super().__init__(maxsize=maxsize, ttl=ttl)
        self.serializer = serializer

    def _get_from_redis(self, redis_client: redis.Redis, key: str) -> Any:
        data = redis_client.get(key)
        if data is None:
            return None
        return self.serializer.deserialize(cast(bytes, data))

    def _set_in_redis(self, redis_client: redis.Redis, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        serialized = self.serializer.serialize(value)
        if ttl is not None:
            return bool(redis_client.setex(key, ttl, serialized))
        return bool(redis_client.set(key, serialized))

    def _delete_from_redis(self, redis_client: redis.Redis, *keys: str) -> bool:
        if not keys:
            return False
        keys_list: list[str] = list(keys)
        result = redis_client.delete(*keys_list)
        return bool(0 if result is None else result)


class HashStorageBackend(CacheStorageBackend):
    """Redis hash storage backend for flattened data structures with built-in memory caching"""

    def __init__(self, maxsize: int = 1000, ttl: Optional[int] = 3600):
        super().__init__(maxsize=maxsize, ttl=ttl)

    def _flatten_dict(self, obj: Any, prefix: str = "") -> dict[Union[str, bytes], Union[str, bytes, float, int]]:
        """Flatten a nested dictionary into a single level dict with key paths"""
        result: dict[Union[str, bytes], Union[str, bytes, float, int]] = {}

        if isinstance(obj, dict):
            for k, v in obj.items():
                key = f"{prefix}.{k}" if prefix else k
                if isinstance(v, (dict, list)) and not isinstance(v, (str, bytes)):
                    result.update(self._flatten_dict(v, key))
                else:
                    result[key] = json.dumps(v)
        elif isinstance(obj, list):
            for i, v in enumerate(obj):
                key = f"{prefix}[{i}]"
                if isinstance(v, (dict, list)) and not isinstance(v, (str, bytes)):
                    result.update(self._flatten_dict(v, key))
                else:
                    result[key] = json.dumps(v)
        else:
            result[prefix] = json.dumps(obj)

        return result

    def _handle_array_key(
        self,
        current: dict[str, Any],
        array_key: str,
        idx: int,
        value: str,
        is_last: bool,
    ) -> dict[str, Any]:
        """Handle array-type keys in the flattened dictionary."""
        if array_key not in current:
            current[array_key] = []
        while len(current[array_key]) <= idx:
            current[array_key].append({} if not is_last else None)
        if is_last:
            current[array_key][idx] = json.loads(value)
            return current
        return cast(dict[str, Any], current[array_key][idx])

    def _handle_regular_key(self, current: dict[str, Any], part: str, value: str, is_last: bool) -> dict[str, Any]:
        """Handle regular (non-array) keys in the flattened dictionary."""
        if is_last:
            current[part] = json.loads(value)
            return current
        if part not in current:
            current[part] = {}
        return cast(dict[str, Any], current[part])

    def _process_key_part(self, current: dict[str, Any], part: str, value: str, is_last: bool) -> dict[str, Any]:
        """Process a single part of the key path."""
        if "[" in part and part.endswith("]"):
            array_key, idx_str = part.split("[", 1)
            idx = int(idx_str[:-1])  # Remove the closing bracket and convert to int
            return self._handle_array_key(current, array_key, idx, value, is_last)
        return self._handle_regular_key(current, part, value, is_last)

    def _unflatten_dict(self, flat_dict: dict[str, str]) -> dict[str, Any]:
        """Convert a flattened dict back to a nested structure"""
        result: dict[str, Any] = {}

        for key, value in flat_dict.items():
            parts = key.split(".")
            current = result

            for i, part in enumerate(parts):
                is_last = i == len(parts) - 1
                current = self._process_key_part(current, part, value, is_last)

        return result

    def _get_from_redis(self, redis_client: redis.Redis, key: str) -> Any:
        """
        Retrieve a hash using cursor-based pagination to avoid blocking Redis
        for large hashes.
        """
        # Check if key exists first (to avoid unnecessary scanning)
        if not redis_client.exists(key):
            return None

        # Use HSCAN for cursor-based pagination
        hash_data = self._hscan_iter_to_dict(redis_client.hscan_iter(key))

        if not hash_data:
            return None

        # Convert from bytes to string
        str_hash = {
            k.decode("utf-8"): v.decode("utf-8")
            for k, v in hash_data.items()
            if isinstance(k, bytes) and isinstance(v, bytes)
        }

        return self._unflatten_dict(str_hash)

    def _hscan_iter_to_dict(self, hscan_iter: Iterator[tuple[bytes, bytes]]) -> dict:
        """
        Convert a Redis HSCAN iterator to a dictionary.
        This avoids loading the entire hash into memory at once.
        """
        result = {}
        for field, value in hscan_iter:
            result[field] = value
        return result

    def _set_in_redis(self, redis_client: redis.Redis, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        flattened = self._flatten_dict(value)

        # Create a pipeline for atomic operation
        pipe = redis_client.pipeline()
        pipe.delete(key)  # Clear existing hash first
        if flattened:
            pipe.hset(key, mapping=flattened)
        if ttl is not None:
            pipe.expire(key, ttl)

        pipe.execute()
        return True

    def _delete_from_redis(self, redis_client: redis.Redis, *keys: str) -> bool:
        if not keys:
            return False
        # Redis delete returns int
        keys_list: list[str] = list(keys)
        result = redis_client.delete(*keys_list)
        return bool(0 if result is None else result)


class KeyGenerator:
    """Handles cache key generation with namespace support and smart arg/kwarg handling."""

    def __init__(self, prefix: str = "cache", namespace: Optional[str] = None):
        self.prefix = prefix
        self.namespace = namespace

    def _fnv1a_32(self, data: bytes) -> int:
        """FNV-1a 32-bit hash implementation - faster than SHA256 for short strings."""
        FNV_PRIME = 0x01000193
        FNV_OFFSET = 0x811C9DC5

        hash_value = FNV_OFFSET
        for byte in data:
            hash_value ^= byte
            hash_value = (hash_value * FNV_PRIME) & 0xFFFFFFFF
        return hash_value

    def _serialize_arg(self, arg: Any) -> str:
        """Smartly serialize a single argument for key generation."""
        if isinstance(arg, (str, int, float, bool)):
            return str(arg)
        elif isinstance(arg, (list, tuple, set)):
            return f"[{','.join(self._serialize_arg(x) for x in arg)}]"
        elif isinstance(arg, dict):
            sorted_items = sorted(arg.items(), key=lambda x: x[0])
            return f"{{{','.join(f'{k}:{self._serialize_arg(v)}' for k, v in sorted_items)}}}"
        elif hasattr(arg, "__cache_key__"):
            return str(arg.__cache_key__())
        elif hasattr(arg, "id"):
            return str(arg.id)
        else:
            # For other objects, use their type and id
            return f"{arg.__class__.__name__}:{id(arg)}"

    def generate_key(self, func: Callable[..., T], args: tuple, kwargs: dict[str, Any]) -> str:
        """Generate a consistent, fast cache key with namespace support."""
        # Start with base components
        key_parts = [self.prefix]
        if self.namespace:
            key_parts.append(self.namespace)

        # Add function identifier (module:name)
        key_parts.append(f"{func.__module__}:{func.__name__}")

        # Build args and kwargs parts efficiently
        key_data = []
        if args:
            key_data.append("args:" + ",".join(self._serialize_arg(arg) for arg in args))
        if kwargs:
            sorted_kwargs = sorted(kwargs.items(), key=lambda x: x[0])
            key_data.append("kwargs:" + ",".join(f"{k}:{self._serialize_arg(v)}" for k, v in sorted_kwargs))

        # Generate hash only for variable parts (args and kwargs)
        if key_data:
            data_str = "|".join(key_data)
            hash_value = self._fnv1a_32(data_str.encode())
            key_parts.append(hex(hash_value)[2:])  # Remove '0x' prefix

        # Join all parts with ':' separator
        return ":".join(key_parts)


class RedisConnectionManager:
    """Manages Redis connections with optimized pooling and reuse."""

    def __init__(self, pool: redis.ConnectionPool):
        self.pool = pool
        self._local_client: Optional[redis.Redis] = None
        self._last_used: float = 0
        self._health_check_interval: float = 30.0  # seconds
        self._max_idle_time: float = 300.0  # 5 minutes

    def _is_connection_healthy(self, client: redis.Redis) -> bool:
        """Check if the connection is still alive and healthy."""
        try:
            return bool(client.ping())
        except (redis.ConnectionError, redis.TimeoutError):
            return False

    def _should_health_check(self) -> bool:
        """Determine if we should perform a health check."""
        current_time = time.time()
        return (current_time - self._last_used) > self._health_check_interval

    def get_client(self) -> redis.Redis:
        """Get a Redis client, reusing existing connection if healthy."""
        current_time = time.time()

        # If we have an existing client, check if it's still usable
        if self._local_client is not None and (
            (current_time - self._last_used) > self._max_idle_time
            or self._should_health_check()
            and not self._is_connection_healthy(self._local_client)
        ):
            self._local_client.close()
            self._local_client = None

        # Create new client if needed
        if self._local_client is None:
            self._local_client = redis.Redis(connection_pool=self.pool)

        self._last_used = current_time
        return self._local_client

    def close(self) -> None:
        """Close the current connection if it exists."""
        if self._local_client is not None:
            self._local_client.close()
            self._local_client = None


class CacheConfig:
    def __init__(
        self,
        host: str = "localhost",
        port: int = 6379,
        db: int = 0,
        prefix: str = "cache",
        namespace: Optional[str] = None,
        storage_backend: Optional[CacheStorageBackend] = None,
        raise_errors: bool = False,
        connection_pool: Optional[redis.ConnectionPool] = None,
        max_connections: int = 50,
        socket_timeout: int = 2,
        socket_connect_timeout: int = 1,
        socket_keepalive: bool = True,
        retry_on_timeout: bool = True,
        health_check_interval: int = 60,
        scan_batch_size: int = 1000,
        memory_cache_size: int = 1000,  # Default size for memory cache
        memory_cache_ttl: Optional[int] = 3600,  # Default TTL for memory cache (1 hour)
        **redis_kwargs: Any,
    ) -> None:
        self.host = host
        self.port = port
        self.db = db
        self.prefix = prefix
        self.namespace = namespace
        self.key_generator = KeyGenerator(prefix=prefix, namespace=namespace)

        # Default to MessagePack if not specified
        self.storage_backend = storage_backend or KeyValueStorageBackend(
            MessagePackSerializer(),
            maxsize=memory_cache_size,
            ttl=memory_cache_ttl,
        )

        self.raise_errors = raise_errors
        self.max_connections = max_connections
        self.socket_timeout = socket_timeout
        self.socket_connect_timeout = socket_connect_timeout
        self.socket_keepalive = socket_keepalive
        self.retry_on_timeout = retry_on_timeout
        self.health_check_interval = health_check_interval
        self.scan_batch_size = scan_batch_size
        self.connection_pool = connection_pool or redis.ConnectionPool(
            host=host,
            port=port,
            db=db,
            max_connections=max_connections,
            socket_timeout=socket_timeout,
            socket_connect_timeout=socket_connect_timeout,
            socket_keepalive=socket_keepalive,
            health_check_interval=health_check_interval,
            retry_on_timeout=retry_on_timeout,
            **redis_kwargs,
        )
        self.redis_kwargs = redis_kwargs


class CacheDecorator:
    """Handles the core caching logic and provides the decorator interface."""

    def __init__(self, redis_client: redis.Redis, config: CacheConfig) -> None:
        self.redis_client = redis_client
        self.config = config
        self.conn_manager = RedisConnectionManager(config.connection_pool)
        self.lock_manager = Lock(redis_client)

    def _get_client(self) -> redis.Redis:
        """Get an optimized Redis client for the current operation."""
        return self.conn_manager.get_client()

    def acquire_lock(self, key: str, ttl: int = 10) -> tuple[bool, Optional[str]]:
        """Acquire a distributed lock using Redis with owner identification."""
        lock_key = f"lock:{key}"
        return self.lock_manager.acquire(lock_key, ttl_ms=ttl * 1000)

    def release_lock(self, key: str, lock_identifier: str) -> bool:
        """Release a distributed lock if we still own it."""
        lock_key = f"lock:{key}"
        return self.lock_manager.release(lock_key, lock_identifier)

    def _handle_cache_miss(
        self,
        key: str,
        func: Callable[..., DataT],
        args: tuple,
        kwargs: dict,
        ttl: Optional[int],
        start_time: float,
    ) -> DataT:
        """Handle cache miss by computing and caching the value."""
        result: DataT = func(*args, **kwargs)

        if result is not None:
            client = self._get_client()
            self.config.storage_backend.set(client, key, result, ttl)
        return result

    def _retry_get_cached_value(
        self,
        key: str,
        miss_retries: int,
        miss_backoff_base: float,
        miss_backoff_max: float,
        start_time: float,
    ) -> Optional[DataT]:
        """Retry getting cached value with exponential backoff."""
        client = self._get_client()
        for miss_attempt in range(miss_retries):
            time.sleep(min(miss_backoff_base * (2**miss_attempt), miss_backoff_max))
            cached_result = self.config.storage_backend.get(client, key)
            if cached_result is not None:
                return cast(DataT, cached_result)
        return None

    def create_decorator(
        self,
        ttl: Optional[int] = None,
        miss_retries: int = 3,
        miss_backoff_base: float = 0.1,
        miss_backoff_max: float = 0.5,
        lock_ttl: int = 10,
    ) -> Callable[[Callable[..., DataT]], Callable[..., DataT]]:
        """Create a caching decorator with the specified parameters."""

        def decorator(func: Callable[..., DataT]) -> Callable[..., DataT]:
            @wraps(func)
            def wrapper(*args: Any, **kwargs: Any) -> DataT:
                key = self.config.key_generator.generate_key(func, args, kwargs)
                start_time = time.time()
                client = self._get_client()

                try:
                    # Try to get the cached value
                    cached_result = self.config.storage_backend.get(client, key)
                    if cached_result is not None:
                        return cast(DataT, cached_result)

                    # Cache miss - acquire lock to compute value
                    lock_success, lock_id = self.acquire_lock(key, lock_ttl)
                    if lock_success and lock_id:
                        try:
                            return self._handle_cache_miss(key, func, args, kwargs, ttl, start_time)
                        finally:
                            self.release_lock(key, lock_id)

                    # Someone else is computing the value, wait and retry
                    cached_result = self._retry_get_cached_value(
                        key,
                        miss_retries,
                        miss_backoff_base,
                        miss_backoff_max,
                        start_time,
                    )
                    if cached_result is not None:
                        return cast(DataT, cached_result)

                    # If we still don't have a value, compute it ourselves
                    return self._handle_cache_miss(key, func, args, kwargs, ttl, start_time)

                except Exception:
                    logger.exception("Cache error for %s", key)
                    if self.config.raise_errors:
                        raise
                    return func(*args, **kwargs)

            def invalidate(*args: Any, **kwargs: Any) -> bool:
                """Invalidate cache for specific arguments."""
                key = self.config.key_generator.generate_key(func, args, kwargs)
                client = self._get_client()
                return bool(self.config.storage_backend.delete(client, key))

            def clear() -> int:
                """Clear all cache entries for this function with namespace support."""
                pattern = (
                    f"{self.config.prefix}:{self.config.namespace}:*"
                    if self.config.namespace
                    else f"{self.config.prefix}:*"
                )
                client = self._get_client()
                return cast(int, self.config.storage_backend.clear_pattern(client, pattern))

            wrapper.invalidate = invalidate  # type: ignore[attr-defined]
            wrapper.clear = clear  # type: ignore[attr-defined]
            return wrapper

        return decorator

    def clear_by_pattern(self, pattern: str) -> int:
        """
        Clear cache entries by pattern using the optimized SCAN-based method.
        Provides a public interface for pattern-based cache clearing.
        """
        client = self._get_client()
        return cast(
            int,
            self.config.storage_backend.clear_pattern(client, pattern, scan_batch_size=self.config.scan_batch_size),
        )


class NoRedisInstanceError(ValueError):
    """Raised when no Redis instances are provided to Lock."""

    pass


class Lock:
    """
    Unified distributed locking implementation that supports both single and multi-instance Redis setups.
    Always includes lock owner identification for safety.
    """

    def __init__(
        self,
        redis_instances: Union[redis.Redis, list[redis.Redis]],
        retry_count: int = 3,
        retry_delay_ms: int = 200,
        clock_drift_factor: float = 0.01,
    ):
        """
        Initialize Lock with either a single Redis instance or multiple instances for Redlock.

        Args:
            redis_instances: Single Redis client or list of Redis clients for Redlock
            retry_count: Number of retries for lock acquisition
            retry_delay_ms: Delay between retries in milliseconds
            clock_drift_factor: Safety factor for clock drift (default 1%)
        """
        # Convert single instance to list for unified handling
        self.redis_instances = [redis_instances] if isinstance(redis_instances, redis.Redis) else redis_instances
        if not self.redis_instances:
            raise NoRedisInstanceError

        self.retry_count = retry_count
        self.retry_delay_ms = retry_delay_ms
        self.clock_drift_factor = clock_drift_factor

        # Generate process and thread identifiers for lock ownership
        self.process_id = str(os.getpid())
        self.thread_id = str(threading.get_ident())

        # Flag to indicate if we're in single-instance mode
        self.single_instance = len(self.redis_instances) == 1

    def _generate_lock_id(self) -> str:
        """Generate a unique lock identifier combining process, thread and random UUID."""
        random_id = str(uuid.uuid4())
        return f"{self.process_id}:{self.thread_id}:{random_id}"

    def _acquire_lock(self, redis_instance: redis.Redis, key: str, value: str, ttl_ms: int) -> bool:
        """
        Try to acquire lock on a single Redis instance with owner identification.

        Args:
            redis_instance: Redis client instance
            key: Lock key
            value: Lock value (owner identifier)
            ttl_ms: Time-to-live in milliseconds
        """
        try:
            return bool(redis_instance.set(key, value, nx=True, px=ttl_ms))
        except redis.RedisError:
            logger.warning("Failed to acquire lock on Redis instance", exc_info=True)
            return False

    def _release_lock(self, redis_instance: redis.Redis, key: str, value: str) -> bool:
        """
        Release lock on a single Redis instance using Lua script for atomic operation.
        Only releases if the lock is still owned by us.
        """
        release_script = """
        if redis.call("get", KEYS[1]) == ARGV[1] then
            return redis.call("del", KEYS[1])
        else
            return 0
        end
        """
        try:
            return bool(redis_instance.eval(release_script, 1, key, value))
        except redis.RedisError:
            logger.warning("Failed to release lock on Redis instance", exc_info=True)
            return False

    def acquire(self, key: str, ttl_ms: int = 10000) -> tuple[bool, Optional[str]]:
        """
        Try to acquire lock using either single-instance or Redlock algorithm.

        Args:
            key: Lock key
            ttl_ms: Time-to-live in milliseconds

        Returns:
            Tuple of (success, lock_identifier)
        """
        lock_identifier = self._generate_lock_id()

        # Fast path for single instance
        if self.single_instance:
            success = self._acquire_lock(self.redis_instances[0], key, lock_identifier, ttl_ms)
            return success, lock_identifier if success else None

        # Redlock algorithm for multiple instances
        drift_time = int(ttl_ms * self.clock_drift_factor) + 2

        for _ in range(self.retry_count):
            start_time = int(time.time() * 1000)
            n_success = 0

            # Try to acquire lock on all instances
            for redis_instance in self.redis_instances:
                if self._acquire_lock(redis_instance, key, lock_identifier, ttl_ms):
                    n_success += 1

            elapsed_time = int(time.time() * 1000) - start_time

            # Check if we acquired locks from majority of instances and have enough time left
            if n_success >= (len(self.redis_instances) // 2 + 1) and elapsed_time < ttl_ms - drift_time:
                return True, lock_identifier

            # If we failed to acquire the majority, release all locks
            self.release(key, lock_identifier)

            # Wait before retry
            time.sleep(self.retry_delay_ms / 1000.0)

        return False, None

    def release(self, key: str, lock_identifier: str) -> bool:
        """
        Release acquired locks from all Redis instances.
        Only releases if we still own the lock.

        Args:
            key: Lock key
            lock_identifier: The unique identifier returned from acquire()
        """
        if self.single_instance:
            return self._release_lock(self.redis_instances[0], key, lock_identifier)

        success = True
        for redis_instance in self.redis_instances:
            if not self._release_lock(redis_instance, key, lock_identifier):
                success = False
        return success

    def extend(self, key: str, lock_identifier: str, extend_ms: int) -> bool:
        """
        Extend the TTL of an existing lock if we still own it.

        Args:
            key: Lock key
            lock_identifier: The unique identifier returned from acquire()
            extend_ms: Additional time in milliseconds
        """
        extend_script = """
        if redis.call("get", KEYS[1]) == ARGV[1] then
            return redis.call("pexpire", KEYS[1], ARGV[2])
        else
            return 0
        end
        """

        if self.single_instance:
            try:
                return bool(self.redis_instances[0].eval(extend_script, 1, key, lock_identifier, extend_ms))
            except redis.RedisError:
                logger.warning("Failed to extend lock", exc_info=True)
                return False

        # For Redlock, we need majority agreement
        n_success = 0
        for redis_instance in self.redis_instances:
            try:
                if bool(redis_instance.eval(extend_script, 1, key, lock_identifier, extend_ms)):
                    n_success += 1
            except redis.RedisError:
                logger.warning("Failed to extend lock on Redis instance", exc_info=True)
                continue

        return n_success >= (len(self.redis_instances) // 2 + 1)


def redecorate(
    config: CacheConfig,
) -> Callable[[Optional[int]], Callable[[Callable[..., DataT]], Callable[..., DataT]]]:
    """
    Returns a configured cache decorator with pluggable storage backends.
    Includes backoff and retry mechanism for cache misses.
    """
    try:
        redis_client = redis.Redis(connection_pool=config.connection_pool)
        redis_client.ping()  # Test connection
        logger.info("Redis client configured successfully")
        cache_decorator = CacheDecorator(redis_client, config)
    except redis.RedisError:
        logger.exception("Failed to configure Redis client")
        if config.raise_errors:
            raise

        def no_cache(
            ttl: Optional[int] = None,
        ) -> Callable[[Callable[..., DataT]], Callable[..., DataT]]:
            return lambda func: func

        return no_cache
    else:
        return cache_decorator.create_decorator
