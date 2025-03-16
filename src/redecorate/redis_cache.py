import hashlib
import json
import logging
import time
import zlib
from abc import ABC, abstractmethod
from dataclasses import asdict, is_dataclass
from datetime import datetime
from enum import Enum
from functools import wraps
from typing import Any, Callable, Optional, Protocol, TypeVar

import msgpack
import redis

logger = logging.getLogger("redis-cache")

# Generic type for better type safety
T = TypeVar("T")


class CacheMetrics:
    def __init__(self):
        self.hits = 0
        self.misses = 0
        self.errors = 0
        self.latency_sum = 0
        self.operations = 0
        self._start_time = time.time()

    def record_operation(self, start_time: float, hit: bool, error: bool = False):
        self.operations += 1
        self.latency_sum += time.time() - start_time
        if error:
            self.errors += 1
        elif hit:
            self.hits += 1
        else:
            self.misses += 1

    @property
    def hit_rate(self) -> float:
        total = self.hits + self.misses
        return self.hits / total if total > 0 else 0

    @property
    def average_latency(self) -> float:
        return self.latency_sum / self.operations if self.operations > 0 else 0


class CacheStorageBackend(ABC):
    """Abstract base class for different cache storage backends"""

    @abstractmethod
    def get(self, redis_client: redis.Redis, key: str) -> Any:
        """Retrieve a value from cache"""
        pass

    @abstractmethod
    def set(self, redis_client: redis.Redis, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        """Store a value in cache with optional TTL"""
        pass

    @abstractmethod
    def delete(self, redis_client: redis.Redis, *keys: str) -> bool:
        """Delete a value from cache"""
        pass

    @abstractmethod
    def clear_pattern(self, redis_client: redis.Redis, pattern: str) -> int:
        """Clear all keys matching a pattern"""
        pass


class Serializer(Protocol):
    """Protocol defining serialization methods"""

    def serialize(self, obj: Any) -> Any: ...
    def deserialize(self, data: Any) -> Any: ...


class CompressedJSONSerializer:
    """JSON serializer with optional compression"""

    def __init__(self, compression_threshold: int = 1000):
        self.compression_threshold = compression_threshold

    def _convert_value(self, obj: Any) -> Any:
        """Helper method to convert values for JSON serialization."""
        if isinstance(obj, datetime):
            return obj.isoformat()
        elif isinstance(obj, Enum):
            return str(obj)
        elif is_dataclass(obj):
            return {k: self._convert_value(v) for k, v in asdict(obj).items()}
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
        elif is_dataclass(obj):
            return {k: self._convert_value(v) for k, v in asdict(obj).items()}
        elif isinstance(obj, dict):
            return {k: self._convert_value(v) for k, v in obj.items()}
        elif isinstance(obj, (list, tuple)):
            return [self._convert_value(v) for v in obj]
        return obj

    def serialize(self, obj: Any) -> bytes:
        converted_obj = self._convert_value(obj)
        packed = msgpack.packb(converted_obj)

        if len(packed) > self.compression_threshold:
            return b"c" + zlib.compress(packed)
        return b"r" + packed

    def deserialize(self, data: bytes) -> Any:
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
            return {k: self._process_obj(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._process_obj(v) for v in obj]
        return obj


class KeyValueStorageBackend(CacheStorageBackend):
    """Redis key-value storage backend using serialization"""

    def __init__(self, serializer: Serializer):
        self.serializer = serializer

    def get(self, redis_client: redis.Redis, key: str) -> Any:
        data = redis_client.get(key)
        if data is None:
            return None
        return self.serializer.deserialize(data)

    def set(self, redis_client: redis.Redis, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        serialized = self.serializer.serialize(value)
        if ttl is not None:
            return redis_client.setex(key, ttl, serialized)
        return redis_client.set(key, serialized)

    def delete(self, redis_client: redis.Redis, *keys: str) -> bool:
        if not keys:
            return False
        return bool(redis_client.delete(*keys))

    def clear_pattern(self, redis_client: redis.Redis, pattern: str) -> int:
        keys = redis_client.keys(pattern)
        if not keys:
            return 0
        return redis_client.delete(*keys)


class HashStorageBackend(CacheStorageBackend):
    """Redis hash storage backend for flattened data structures"""

    def __init__(self):
        pass

    def _flatten_dict(self, obj: Any, prefix: str = "") -> dict[str, str]:
        """Flatten a nested dictionary into a single level dict with key paths"""
        result = {}

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

    def _handle_array_key(self, current: dict, array_key: str, idx: int, value: str, is_last: bool) -> dict:
        """Handle array-type keys in the flattened dictionary."""
        if array_key not in current:
            current[array_key] = []
        while len(current[array_key]) <= idx:
            current[array_key].append({} if not is_last else None)
        if is_last:
            current[array_key][idx] = json.loads(value)
            return current
        return current[array_key][idx]

    def _handle_regular_key(self, current: dict, part: str, value: str, is_last: bool) -> dict:
        """Handle regular (non-array) keys in the flattened dictionary."""
        if is_last:
            current[part] = json.loads(value)
            return current
        if part not in current:
            current[part] = {}
        return current[part]

    def _process_key_part(self, current: dict, part: str, value: str, is_last: bool) -> dict:
        """Process a single part of the key path."""
        if "[" in part and part.endswith("]"):
            array_key, idx_str = part.split("[", 1)
            idx = int(idx_str[:-1])  # Remove the closing bracket and convert to int
            return self._handle_array_key(current, array_key, idx, value, is_last)
        return self._handle_regular_key(current, part, value, is_last)

    def _unflatten_dict(self, flat_dict: dict[str, str]) -> dict[str, Any]:
        """Convert a flattened dict back to a nested structure"""
        result = {}

        for key, value in flat_dict.items():
            parts = key.split(".")
            current = result

            for i, part in enumerate(parts):
                is_last = i == len(parts) - 1
                current = self._process_key_part(current, part, value, is_last)

        return result

    def get(self, redis_client: redis.Redis, key: str) -> Any:
        hash_data = redis_client.hgetall(key)
        if not hash_data:
            return None

        # Convert from bytes to string
        str_hash = {k.decode("utf-8"): v.decode("utf-8") for k, v in hash_data.items()}
        return self._unflatten_dict(str_hash)

    def set(self, redis_client: redis.Redis, key: str, value: Any, ttl: Optional[int] = None) -> bool:
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

    def delete(self, redis_client: redis.Redis, key: str) -> bool:
        return bool(redis_client.delete(key))

    def clear_pattern(self, redis_client: redis.Redis, pattern: str) -> int:
        keys = redis_client.keys(pattern)
        if not keys:
            return 0
        return redis_client.delete(*keys)


class KeyGenerator:
    """Handles cache key generation with namespace support and smart arg/kwarg handling."""

    def __init__(self, prefix: str = "cache", namespace: Optional[str] = None):
        self.prefix = prefix
        self.namespace = namespace

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
        """Generate a consistent, hashed cache key with namespace support."""
        # Start with base components
        key_parts = [self.prefix]
        if self.namespace:
            key_parts.append(self.namespace)

        # Add function identifier
        key_parts.append(f"{func.__module__}:{func.__name__}")

        # Add serialized positional args
        if args:
            args_str = ",".join(self._serialize_arg(arg) for arg in args)
            key_parts.append(f"args:{args_str}")

        # Add serialized keyword args (sorted for consistency)
        if kwargs:
            sorted_kwargs = sorted(kwargs.items(), key=lambda x: x[0])
            kwargs_str = ",".join(f"{k}:{self._serialize_arg(v)}" for k, v in sorted_kwargs)
            key_parts.append(f"kwargs:{kwargs_str}")

        # Join all parts and hash
        key_data = ":".join(key_parts)
        key_hash = hashlib.sha256(key_data.encode()).hexdigest()[:32]  # Use first 32 chars for shorter keys

        # Return final key
        return f"{':'.join(key_parts[:-1])}:{key_hash}"


class CacheConfig:
    def __init__(
        self,
        host: str = "localhost",
        port: int = 6379,
        db: int = 0,
        prefix: str = "cache",
        namespace: Optional[str] = None,
        storage_backend: CacheStorageBackend = None,
        raise_errors: bool = False,
        connection_pool: Optional[redis.ConnectionPool] = None,
        max_connections: int = 50,
        socket_timeout: int = 2,
        socket_connect_timeout: int = 1,
        socket_keepalive: bool = True,
        retry_on_timeout: bool = True,
        health_check_interval: int = 60,
        early_recompute_probability: float = 0.1,
        **redis_kwargs: dict[str, Any],
    ):
        self.host = host
        self.port = port
        self.db = db
        self.prefix = prefix
        self.namespace = namespace
        self.key_generator = KeyGenerator(prefix=prefix, namespace=namespace)
        # Default to MessagePack if not specified
        self.storage_backend = storage_backend or KeyValueStorageBackend(MessagePackSerializer())
        self.raise_errors = raise_errors
        self.max_connections = max_connections
        self.socket_timeout = socket_timeout
        self.socket_connect_timeout = socket_connect_timeout
        self.socket_keepalive = socket_keepalive
        self.retry_on_timeout = retry_on_timeout
        self.health_check_interval = health_check_interval
        self.early_recompute_probability = early_recompute_probability
        self.metrics = CacheMetrics()
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

    def __init__(self, redis_client: redis.Redis, config: CacheConfig):
        self.redis_client = redis_client
        self.config = config

    def acquire_lock(self, key: str, ttl: int = 10) -> bool:
        """Acquire a distributed lock using Redis."""
        lock_key = f"lock:{key}"
        return bool(self.redis_client.set(lock_key, "1", nx=True, ex=ttl))

    def release_lock(self, key: str) -> bool:
        """Release a distributed lock."""
        lock_key = f"lock:{key}"
        return bool(self.redis_client.delete(lock_key))

    def _handle_cache_miss(
        self,
        key: str,
        func: Callable[..., T],
        args: tuple,
        kwargs: dict,
        ttl: Optional[int],
        start_time: float,
    ) -> T:
        """Handle cache miss by computing and caching the value."""
        self.config.metrics.record_operation(start_time, hit=False)
        result = func(*args, **kwargs)

        if result is not None:
            self.config.storage_backend.set(self.redis_client, key, result, ttl)
        return result

    def _retry_get_cached_value(
        self,
        key: str,
        miss_retries: int,
        miss_backoff_base: float,
        miss_backoff_max: float,
        start_time: float,
    ) -> Optional[Any]:
        """Retry getting cached value with exponential backoff."""
        for miss_attempt in range(miss_retries):
            time.sleep(min(miss_backoff_base * (2**miss_attempt), miss_backoff_max))
            cached_result = self.config.storage_backend.get(self.redis_client, key)
            if cached_result is not None:
                self.config.metrics.record_operation(start_time, hit=True)
                return cached_result
        return None

    def create_decorator(
        self,
        ttl: Optional[int] = None,
        miss_retries: int = 3,
        miss_backoff_base: float = 0.1,
        miss_backoff_max: float = 0.5,
        lock_ttl: int = 10,
    ) -> Callable[[Callable[..., T]], Callable[..., T]]:
        """Create a caching decorator with the specified parameters."""

        def decorator(func: Callable[..., T]) -> Callable[..., T]:
            @wraps(func)
            def wrapper(*args: Any, **kwargs: Any) -> T:
                key = self.config.key_generator.generate_key(func, args, kwargs)
                start_time = time.time()

                try:
                    # Try to get the cached value
                    cached_result = self.config.storage_backend.get(self.redis_client, key)
                    if cached_result is not None:
                        self.config.metrics.record_operation(start_time, hit=True)
                        return cached_result

                    # Cache miss - acquire lock to compute value
                    if self.acquire_lock(key, lock_ttl):
                        try:
                            return self._handle_cache_miss(key, func, args, kwargs, ttl, start_time)
                        finally:
                            self.release_lock(key)

                    # Someone else is computing the value, wait and retry
                    cached_result = self._retry_get_cached_value(
                        key,
                        miss_retries,
                        miss_backoff_base,
                        miss_backoff_max,
                        start_time,
                    )
                    if cached_result is not None:
                        return cached_result

                    # If we still don't have a value, compute it ourselves
                    return self._handle_cache_miss(key, func, args, kwargs, ttl, start_time)

                except Exception:
                    self.config.metrics.record_operation(start_time, hit=False, error=True)
                    logger.exception("Cache error for %s", key)
                    if self.config.raise_errors:
                        raise
                    return func(*args, **kwargs)

            def invalidate(*args: Any, **kwargs: Any) -> bool:
                """Invalidate cache for specific arguments."""
                key = self.config.key_generator.generate_key(func, args, kwargs)
                return bool(self.config.storage_backend.delete(self.redis_client, key))

            def clear() -> int:
                """Clear all cache entries for this function with namespace support."""
                pattern = (
                    f"{self.config.prefix}:{self.config.namespace}:*"
                    if self.config.namespace
                    else f"{self.config.prefix}:*"
                )
                return self.config.storage_backend.clear_pattern(self.redis_client, pattern)

            wrapper.invalidate = invalidate
            wrapper.clear = clear
            return wrapper

        return decorator


def redecorate(
    config: CacheConfig,
) -> Callable[[Optional[int]], Callable[[Callable[..., T]], Callable[..., T]]]:
    """
    Returns a configured cache decorator with pluggable storage backends.
    Includes backoff and retry mechanism for cache misses.
    """
    try:
        redis_client = redis.Redis(connection_pool=config.connection_pool, **config.redis_kwargs)
        redis_client.ping()  # Test connection
        logger.info("Redis client configured successfully")
        cache_decorator = CacheDecorator(redis_client, config)
    except redis.RedisError:
        logger.exception("Failed to configure Redis client")
        if config.raise_errors:
            raise
        return lambda ttl=None: lambda func: func
    else:
        return cache_decorator.create_decorator


# Example usage
if __name__ == "__main__":
    # Configure cache with MessagePack backend
    msgpack_config = CacheConfig(
        host="localhost",
        port=6379,
        db=0,
        prefix="myapp:msgpack",
        storage_backend=KeyValueStorageBackend(MessagePackSerializer()),
        max_connections=20,
    )

    # Create cache decorator with backoff and retry for cache misses
    try:
        cache = redecorate(msgpack_config)

        # Function with MessagePack caching and miss retry
        @cache(ttl=3600, miss_retries=3, miss_backoff_base=0.1, miss_backoff_max=0.5)
        def get_user_data(user_id: int) -> dict:
            """Simulate fetching user data from database"""
            print("Fetching user data from database...")
            return {
                "id": user_id,
                "name": "John Doe",
                "last_login": datetime.now(),
                "metadata": {
                    "visits": 10,
                    "preferences": {"theme": "dark", "notifications": True},
                },
            }

        # Test the implementation
        print("===== MessagePack Storage with Backoff and Retry =====")
        # First call - caches the result
        user_data = get_user_data(123)
        print("First call:", user_data)
        # Second call - returns cached result
        cached_data = get_user_data(123)
        print("Second call (cached):", cached_data)

    except redis.RedisError as e:
        print(f"Redis error: {e}")
        print("Please make sure Redis is running")
