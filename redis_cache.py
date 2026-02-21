"""
Redis缓存适配器 - 兼容现有的 global_cache 接口
"""

import logging
import time
from typing import Any, Optional, Dict, List
from functools import wraps

from config import Config
from redis_manager import redis_manager

logger = logging.getLogger("GroupCheckInBot.RedisCache")


class RedisCacheAdapter:
    """
    Redis缓存适配器 - 提供与 global_cache 相同的接口
    支持降级到内存缓存
    """

    def __init__(self):
        self._enabled = Config.REDIS_ENABLED
        self._fallback_to_memory = Config.REDIS_FALLBACK_TO_MEMORY

        # 内存缓存（降级用）
        self._memory_cache = {}
        self._memory_ttl = {}

        # 统计
        self._stats = {
            "redis_hits": 0,
            "redis_misses": 0,
            "memory_hits": 0,
            "memory_misses": 0,
            "redis_sets": 0,
            "memory_sets": 0,
            "redis_errors": 0,
        }

    async def initialize(self):
        """初始化缓存"""
        if self._enabled:
            await redis_manager.initialize()
            logger.info(
                f"✅ Redis缓存适配器初始化完成 (启用={self._enabled}, 降级={self._fallback_to_memory})"
            )
        else:
            logger.info("ℹ️ Redis缓存已禁用，使用内存缓存")

    async def close(self):
        """关闭缓存"""
        if self._enabled:
            await redis_manager.close()

    def _is_redis_available(self) -> bool:
        """检查Redis是否可用"""
        if not self._enabled:
            return False
        return redis_manager._is_available()

    # ========== 内存缓存操作（降级用） ==========

    def _memory_get(self, key: str) -> Any:
        """从内存缓存获取"""
        if key in self._memory_ttl and time.time() < self._memory_ttl[key]:
            self._stats["memory_hits"] += 1
            return self._memory_cache.get(key)
        else:
            self._stats["memory_misses"] += 1
            self._memory_cache.pop(key, None)
            self._memory_ttl.pop(key, None)
            return None

    def _memory_set(self, key: str, value: Any, ttl: int = 300):
        """设置内存缓存"""
        self._memory_cache[key] = value
        self._memory_ttl[key] = time.time() + ttl
        self._stats["memory_sets"] += 1

    def _memory_delete(self, key: str):
        """删除内存缓存"""
        self._memory_cache.pop(key, None)
        self._memory_ttl.pop(key, None)

    def _memory_clear_expired(self):
        """清理过期内存缓存"""
        current_time = time.time()
        expired = [k for k, t in self._memory_ttl.items() if current_time >= t]
        for k in expired:
            self._memory_cache.pop(k, None)
            self._memory_ttl.pop(k, None)
        return len(expired)

    # ========== 统一的缓存接口 ==========

    async def get(self, key: str, default: Any = None) -> Any:
        """获取缓存值"""
        # 先尝试Redis
        if self._is_redis_available():
            try:
                value = await redis_manager.get(key, None)
                if value is not None:
                    self._stats["redis_hits"] += 1
                    return value
                self._stats["redis_misses"] += 1
            except Exception as e:
                logger.warning(f"Redis get失败，降级到内存: {e}")
                self._stats["redis_errors"] += 1

        # Redis不可用或未命中，尝试内存缓存
        if self._fallback_to_memory:
            value = self._memory_get(key)
            if value is not None:
                return value

        return default

    async def set(self, key: str, value: Any, ttl: int = None) -> bool:
        """设置缓存值"""
        if ttl is None:
            ttl = Config.REDIS_DEFAULT_TTL

        success = False

        # 尝试Redis
        if self._is_redis_available():
            try:
                redis_success = await redis_manager.set(key, value, ttl)
                if redis_success:
                    self._stats["redis_sets"] += 1
                    success = True
            except Exception as e:
                logger.warning(f"Redis set失败: {e}")
                self._stats["redis_errors"] += 1

        # 总是设置内存缓存（作为备份）
        if self._fallback_to_memory:
            self._memory_set(key, value, ttl)
            success = True

        return success

    async def delete(self, *keys: str) -> int:
        """删除缓存"""
        deleted = 0

        # 删除Redis缓存
        if self._is_redis_available():
            try:
                redis_deleted = await redis_manager.delete(*keys)
                deleted += redis_deleted
            except Exception as e:
                logger.warning(f"Redis delete失败: {e}")
                self._stats["redis_errors"] += 1

        # 删除内存缓存
        if self._fallback_to_memory:
            for key in keys:
                self._memory_delete(key)
                deleted += 1

        return deleted

    async def exists(self, key: str) -> bool:
        """检查键是否存在"""
        # 先检查Redis
        if self._is_redis_available():
            try:
                if await redis_manager.exists(key):
                    return True
            except Exception as e:
                logger.warning(f"Redis exists失败: {e}")
                self._stats["redis_errors"] += 1

        # 检查内存缓存
        if self._fallback_to_memory:
            if key in self._memory_ttl and time.time() < self._memory_ttl[key]:
                return True

        return False

    async def expire(self, key: str, ttl: int) -> bool:
        """设置过期时间"""
        success = False

        if self._is_redis_available():
            try:
                if await redis_manager.expire(key, ttl):
                    success = True
            except Exception as e:
                logger.warning(f"Redis expire失败: {e}")
                self._stats["redis_errors"] += 1

        if self._fallback_to_memory and key in self._memory_ttl:
            self._memory_ttl[key] = time.time() + ttl
            success = True

        return success

    async def ttl(self, key: str) -> int:
        """获取剩余过期时间"""
        if self._is_redis_available():
            try:
                return await redis_manager.ttl(key)
            except Exception as e:
                logger.warning(f"Redis ttl失败: {e}")
                self._stats["redis_errors"] += 1

        if self._fallback_to_memory and key in self._memory_ttl:
            return int(self._memory_ttl[key] - time.time())

        return -2

    async def mget(self, keys: List[str]) -> Dict[str, Any]:
        """批量获取"""
        result = {}

        # 尝试Redis
        if self._is_redis_available():
            try:
                redis_result = await redis_manager.mget(keys)
                result.update(redis_result)
                self._stats["redis_hits"] += len(
                    [v for v in redis_result.values() if v is not None]
                )
                self._stats["redis_misses"] += len(
                    [k for k in keys if k not in redis_result]
                )
            except Exception as e:
                logger.warning(f"Redis mget失败: {e}")
                self._stats["redis_errors"] += 1

        # 补全内存缓存
        if self._fallback_to_memory:
            for key in keys:
                if key not in result:
                    value = self._memory_get(key)
                    if value is not None:
                        result[key] = value

        return result

    async def mset(self, mapping: Dict[str, Any], ttl: int = None) -> bool:
        """批量设置"""
        success = False

        if self._is_redis_available():
            try:
                if await redis_manager.mset(mapping, ttl):
                    self._stats["redis_sets"] += len(mapping)
                    success = True
            except Exception as e:
                logger.warning(f"Redis mset失败: {e}")
                self._stats["redis_errors"] += 1

        if self._fallback_to_memory:
            for key, value in mapping.items():
                self._memory_set(key, value, ttl)
            success = True

        return success

    async def clear_expired(self):
        """清理过期缓存"""
        # Redis自动过期，不需要手动清理

        # 清理内存缓存
        if self._fallback_to_memory:
            expired = self._memory_clear_expired()
            if expired > 0:
                logger.debug(f"内存缓存清理: {expired}个过期")

    async def clear_all(self):
        """清空所有缓存"""
        # 清空Redis
        if self._is_redis_available():
            try:
                await redis_manager.flush_all()
            except Exception as e:
                logger.warning(f"Redis清空失败: {e}")

        # 清空内存缓存
        if self._fallback_to_memory:
            self._memory_cache.clear()
            self._memory_ttl.clear()

        logger.info("所有缓存已清空")

    async def get_stats(self) -> Dict[str, Any]:
        """获取缓存统计"""
        stats = self._stats.copy()

        # 计算命中率
        total_redis = stats["redis_hits"] + stats["redis_misses"]
        stats["redis_hit_rate"] = (
            stats["redis_hits"] / total_redis if total_redis > 0 else 0
        )

        total_memory = stats["memory_hits"] + stats["memory_misses"]
        stats["memory_hit_rate"] = (
            stats["memory_hits"] / total_memory if total_memory > 0 else 0
        )

        # Redis状态
        if self._is_redis_available():
            try:
                redis_stats = await redis_manager.get_stats()
                stats.update(
                    {
                        "redis_version": redis_stats.get("redis_version"),
                        "redis_memory": redis_stats.get("used_memory_human"),
                        "redis_clients": redis_stats.get("connected_clients"),
                        "redis_info": redis_stats,
                    }
                )
            except Exception as e:
                stats["redis_error"] = str(e)

        # 内存缓存大小
        stats["memory_cache_size"] = len(self._memory_cache)

        return stats


# 装饰器：支持Redis的缓存装饰器
def redis_cache(key_prefix: str, ttl: int = None, key_builder=None):
    """
    Redis缓存装饰器

    Args:
        key_prefix: 缓存键前缀
        ttl: 过期时间（秒）
        key_builder: 自定义键构建函数
    """

    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            from performance import global_cache as cache

            # 构建缓存键
            if key_builder:
                cache_key = key_builder(*args, **kwargs)
            else:
                # 默认键构建方式
                args_str = ":".join(str(a) for a in args if a is not None)
                kwargs_str = ":".join(f"{k}={v}" for k, v in sorted(kwargs.items()))
                cache_key = f"{key_prefix}:{args_str}:{kwargs_str}".strip(":")

            # 尝试从缓存获取
            cached_value = await cache.get(cache_key)
            if cached_value is not None:
                return cached_value

            # 执行原函数
            result = await func(*args, **kwargs)

            # 存入缓存
            if result is not None:
                await cache.set(cache_key, result, ttl)

            return result

        return wrapper

    return decorator


# 全局缓存实例（兼容现有代码）
redis_cache_adapter = RedisCacheAdapter()
