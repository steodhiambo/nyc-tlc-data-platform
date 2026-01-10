"""
Caching Layer Implementation for NYC TLC Data Platform

This module implements a Redis-based caching layer to improve performance
of frequently accessed data.
"""

import redis
import json
import pickle
from typing import Any, Optional, Union
from datetime import timedelta
import logging
import os
from functools import wraps

logger = logging.getLogger(__name__)


class RedisCache:
    """Redis-based caching layer for the NYC TLC Data Platform"""
    
    def __init__(self, host: str = None, port: int = None, db: int = 0, password: str = None):
        """
        Initialize Redis cache
        
        Args:
            host: Redis host (defaults to REDIS_HOST env var or localhost)
            port: Redis port (defaults to REDIS_PORT env var or 6379)
            db: Redis database number
            password: Redis password (defaults to REDIS_PASSWORD env var)
        """
        self.host = host or os.getenv('REDIS_HOST', 'localhost')
        self.port = port or int(os.getenv('REDIS_PORT', 6379))
        self.db = db
        self.password = password or os.getenv('REDIS_PASSWORD')
        
        # Connect to Redis
        try:
            self.redis_client = redis.Redis(
                host=self.host,
                port=self.port,
                db=self.db,
                password=self.password,
                decode_responses=False,  # Keep as bytes for pickling
                socket_connect_timeout=5,
                socket_timeout=5,
                retry_on_timeout=True
            )
            # Test connection
            self.redis_client.ping()
            logger.info(f"Connected to Redis at {self.host}:{self.port}")
        except redis.ConnectionError as e:
            logger.error(f"Failed to connect to Redis: {e}")
            raise
    
    def set(self, key: str, value: Any, expiration: Union[int, timedelta] = None) -> bool:
        """
        Set a value in the cache
        
        Args:
            key: Cache key
            value: Value to cache
            expiration: Expiration time (seconds or timedelta)
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Serialize the value using pickle for complex objects
            serialized_value = pickle.dumps(value)
            
            # Set with expiration if provided
            if expiration:
                result = self.redis_client.setex(key, expiration, serialized_value)
            else:
                result = self.redis_client.set(key, serialized_value)
            
            if result:
                logger.debug(f"Set cache key: {key}")
                return True
            else:
                logger.warning(f"Failed to set cache key: {key}")
                return False
        except Exception as e:
            logger.error(f"Error setting cache key {key}: {e}")
            return False
    
    def get(self, key: str, default: Any = None) -> Any:
        """
        Get a value from the cache
        
        Args:
            key: Cache key
            default: Default value if key not found
            
        Returns:
            Cached value or default
        """
        try:
            value = self.redis_client.get(key)
            if value is not None:
                # Deserialize the value
                deserialized_value = pickle.loads(value)
                logger.debug(f"Retrieved cache key: {key}")
                return deserialized_value
            else:
                logger.debug(f"Cache miss for key: {key}")
                return default
        except Exception as e:
            logger.error(f"Error getting cache key {key}: {e}")
            return default
    
    def delete(self, key: str) -> bool:
        """
        Delete a key from the cache
        
        Args:
            key: Cache key to delete
            
        Returns:
            True if deleted, False if key didn't exist
        """
        try:
            result = self.redis_client.delete(key)
            if result > 0:
                logger.debug(f"Deleted cache key: {key}")
                return True
            else:
                logger.debug(f"Cache key not found for deletion: {key}")
                return False
        except Exception as e:
            logger.error(f"Error deleting cache key {key}: {e}")
            return False
    
    def exists(self, key: str) -> bool:
        """
        Check if a key exists in the cache
        
        Args:
            key: Cache key to check
            
        Returns:
            True if key exists, False otherwise
        """
        try:
            return bool(self.redis_client.exists(key))
        except Exception as e:
            logger.error(f"Error checking existence of cache key {key}: {e}")
            return False
    
    def expire(self, key: str, expiration: Union[int, timedelta]) -> bool:
        """
        Set expiration for a cache key
        
        Args:
            key: Cache key
            expiration: Expiration time (seconds or timedelta)
            
        Returns:
            True if successful, False otherwise
        """
        try:
            result = self.redis_client.expire(key, expiration)
            if result:
                logger.debug(f"Set expiration for cache key: {key}")
                return True
            else:
                logger.warning(f"Failed to set expiration for cache key: {key}")
                return False
        except Exception as e:
            logger.error(f"Error setting expiration for cache key {key}: {e}")
            return False
    
    def flush_all(self) -> bool:
        """Flush all cache entries"""
        try:
            self.redis_client.flushall()
            logger.info("Flushed all cache entries")
            return True
        except Exception as e:
            logger.error(f"Error flushing cache: {e}")
            return False
    
    def get_keys(self, pattern: str = "*") -> list:
        """
        Get all keys matching a pattern
        
        Args:
            pattern: Key pattern to match (Redis glob-style)
            
        Returns:
            List of matching keys
        """
        try:
            keys = self.redis_client.keys(pattern)
            return [key.decode('utf-8') if isinstance(key, bytes) else key for key in keys]
        except Exception as e:
            logger.error(f"Error getting keys with pattern {pattern}: {e}")
            return []


def cache_key_generator(*args, **kwargs) -> str:
    """
    Generate a cache key from function arguments
    
    Args:
        *args: Function positional arguments
        **kwargs: Function keyword arguments
        
    Returns:
        Generated cache key
    """
    # Create a string representation of args and kwargs
    key_parts = [str(arg) for arg in args]
    key_parts.extend([f"{k}={v}" for k, v in sorted(kwargs.items())])
    key_string = ":".join(key_parts)
    
    # Hash the string to create a consistent key
    import hashlib
    hashed_key = hashlib.md5(key_string.encode()).hexdigest()
    
    return f"cache:{hashed_key}"


def cached(expiration: Union[int, timedelta] = 3600, cache_instance: RedisCache = None):
    """
    Decorator to cache function results in Redis
    
    Args:
        expiration: Cache expiration time (seconds or timedelta)
        cache_instance: RedisCache instance (will create one if not provided)
    """
    def decorator(func):
        nonlocal cache_instance
        if cache_instance is None:
            cache_instance = RedisCache()
        
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Generate cache key from function name and arguments
            cache_key = f"func:{func.__module__}:{func.__name__}:{cache_key_generator(*args, **kwargs)}"
            
            # Try to get result from cache
            cached_result = cache_instance.get(cache_key)
            if cached_result is not None:
                logger.debug(f"Cache hit for function {func.__name__}")
                return cached_result
            
            # Execute function and cache result
            result = func(*args, **kwargs)
            
            # Store in cache
            cache_success = cache_instance.set(cache_key, result, expiration)
            if cache_success:
                logger.debug(f"Cached result for function {func.__name__}")
            else:
                logger.warning(f"Failed to cache result for function {func.__name__}")
            
            return result
        
        # Add cache invalidation method to the wrapper
        def invalidate(*args, **kwargs):
            cache_key = f"func:{func.__module__}:{func.__name__}:{cache_key_generator(*args, **kwargs)}"
            return cache_instance.delete(cache_key)
        
        wrapper.invalidate_cache = invalidate
        wrapper.cache_instance = cache_instance
        
        return wrapper
    
    return decorator


class QueryResultCache:
    """Specialized cache for query results"""
    
    def __init__(self, cache_instance: RedisCache = None):
        self.cache = cache_instance or RedisCache()
    
    def cache_query_result(self, query_hash: str, result: Any, expiration: Union[int, timedelta] = 3600):
        """
        Cache a query result
        
        Args:
            query_hash: Unique hash of the query
            result: Query result to cache
            expiration: Cache expiration time
        """
        cache_key = f"query_result:{query_hash}"
        return self.cache.set(cache_key, result, expiration)
    
    def get_cached_query_result(self, query_hash: str, default: Any = None):
        """
        Get a cached query result
        
        Args:
            query_hash: Unique hash of the query
            default: Default value if not found
            
        Returns:
            Cached query result or default
        """
        cache_key = f"query_result:{query_hash}"
        return self.cache.get(cache_key, default)
    
    def invalidate_query_result(self, query_hash: str):
        """
        Invalidate a cached query result
        
        Args:
            query_hash: Unique hash of the query to invalidate
        """
        cache_key = f"query_result:{query_hash}"
        return self.cache.delete(cache_key)


class DatasetCache:
    """Cache for dataset operations"""
    
    def __init__(self, cache_instance: RedisCache = None):
        self.cache = cache_instance or RedisCache()
    
    def cache_dataset(self, dataset_name: str, data: Any, expiration: Union[int, timedelta] = 7200):
        """
        Cache a dataset
        
        Args:
            dataset_name: Name of the dataset
            data: Dataset to cache
            expiration: Cache expiration time
        """
        cache_key = f"dataset:{dataset_name}"
        return self.cache.set(cache_key, data, expiration)
    
    def get_cached_dataset(self, dataset_name: str, default: Any = None):
        """
        Get a cached dataset
        
        Args:
            dataset_name: Name of the dataset
            default: Default value if not found
            
        Returns:
            Cached dataset or default
        """
        cache_key = f"dataset:{dataset_name}"
        return self.cache.get(cache_key, default)
    
    def invalidate_dataset(self, dataset_name: str):
        """
        Invalidate a cached dataset
        
        Args:
            dataset_name: Name of the dataset to invalidate
        """
        cache_key = f"dataset:{dataset_name}"
        return self.cache.delete(cache_key)
    
    def cache_dataframe_metadata(self, dataset_name: str, metadata: dict, expiration: Union[int, timedelta] = 3600):
        """
        Cache metadata about a dataset (size, schema, etc.)
        
        Args:
            dataset_name: Name of the dataset
            metadata: Metadata to cache
            expiration: Cache expiration time
        """
        cache_key = f"dataset_metadata:{dataset_name}"
        return self.cache.set(cache_key, metadata, expiration)
    
    def get_cached_metadata(self, dataset_name: str, default: dict = None):
        """
        Get cached metadata for a dataset
        
        Args:
            dataset_name: Name of the dataset
            default: Default value if not found
            
        Returns:
            Cached metadata or default
        """
        cache_key = f"dataset_metadata:{dataset_name}"
        return self.cache.get(cache_key, default)