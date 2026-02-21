"""
Redis管理器 - 提供分布式缓存支持
"""

import json
import pickle
import logging
import asyncio
from typing import Any, Optional, Dict, List, Union
from datetime import timedelta
import redis.asyncio as redis
from redis.asyncio import Redis
from redis.exceptions import RedisError, ConnectionError, TimeoutError

from config import Config

logger = logging.getLogger("GroupCheckInBot.RedisManager")


class RedisManager:
    """Redis缓存管理器"""

    def __init__(self, redis_url: str = None):
        self.redis_url = redis_url or self._get_redis_url()
        self.client: Optional[Redis] = None
        self._initialized = False
        self._pubsub = None
        self._health_check_task = None

        # 缓存统计
        self._stats = {
            "hits": 0,
            "misses": 0,
            "sets": 0,
            "deletes": 0,
        }

        # 默认过期时间（秒）
        self.default_ttl = 300  # 5分钟

        # 键前缀（用于隔离不同环境）
        self.key_prefix = (
            Config.REDIS_KEY_PREFIX if hasattr(Config, "REDIS_KEY_PREFIX") else "bot:"
        )

    def _get_redis_url(self) -> str:
        """获取Redis连接URL"""
        # 优先从环境变量获取
        import os

        redis_url = os.getenv("REDIS_URL", "")
        if redis_url:
            return redis_url

        # 从配置构建
        host = getattr(Config, "REDIS_HOST", "localhost")
        port = getattr(Config, "REDIS_PORT", 6379)
        db = getattr(Config, "REDIS_DB", 0)
        password = getattr(Config, "REDIS_PASSWORD", None)

        if password:
            return f"redis://:{password}@{host}:{port}/{db}"
        else:
            return f"redis://{host}:{port}/{db}"

    def _make_key(self, key: str) -> str:
        """生成带前缀的键"""
        return f"{self.key_prefix}{key}"

    async def initialize(self):
        """初始化Redis连接"""
        if self._initialized:
            return

        max_retries = 5
        for attempt in range(max_retries):
            try:
                logger.info(f"🔄 连接Redis (尝试 {attempt + 1}/{max_retries})...")

                # 创建连接池
                pool = redis.ConnectionPool.from_url(
                    self.redis_url,
                    max_connections=10,
                    decode_responses=False,  # 不自动解码，支持二进制数据
                )

                self.client = redis.Redis(
                    connection_pool=pool,
                    socket_connect_timeout=5,
                    socket_keepalive=True,
                    retry_on_timeout=True,
                )

                # 测试连接
                await self.client.ping()

                logger.info("✅ Redis连接成功")
                self._initialized = True

                # 启动健康检查
                self._health_check_task = asyncio.create_task(self._health_check_loop())

                return

            except Exception as e:
                logger.warning(f"❌ Redis连接失败 (尝试 {attempt + 1}): {e}")
                if attempt == max_retries - 1:
                    logger.error("Redis连接失败，将降级使用内存缓存")
                    self._initialized = False
                    return
                await asyncio.sleep(2**attempt)

    async def close(self):
        """关闭Redis连接"""
        if self._health_check_task:
            self._health_check_task.cancel()
            try:
                await self._health_check_task
            except asyncio.CancelledError:
                pass

        if self.client:
            await self.client.close()
            logger.info("Redis连接已关闭")

    async def _health_check_loop(self):
        """健康检查循环"""
        while True:
            try:
                await asyncio.sleep(30)
                if self.client:
                    await self.client.ping()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Redis健康检查失败: {e}")
                # 尝试重连
                await self._reconnect()

    async def _reconnect(self):
        """重新连接Redis"""
        logger.info("🔄 尝试重新连接Redis...")
        try:
            await self.close()
            await self.initialize()
        except Exception as e:
            logger.error(f"Redis重连失败: {e}")

    def _is_available(self) -> bool:
        """检查Redis是否可用"""
        return self._initialized and self.client is not None

    # ========== 基础缓存操作 ==========

    async def get(self, key: str, default: Any = None) -> Any:
        """获取缓存值"""
        if not self._is_available():
            self._stats["misses"] += 1
            return default

        try:
            full_key = self._make_key(key)
            data = await self.client.get(full_key)

            if data is None:
                self._stats["misses"] += 1
                return default

            # 尝试反序列化
            try:
                value = pickle.loads(data)
            except:
                # 如果不是pickle格式，尝试JSON
                try:
                    value = json.loads(data)
                except:
                    value = data.decode() if isinstance(data, bytes) else data

            self._stats["hits"] += 1
            return value

        except (RedisError, ConnectionError, TimeoutError) as e:
            logger.warning(f"Redis get失败 ({key}): {e}")
            self._stats["misses"] += 1
            return default

    async def set(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        """设置缓存值"""
        if not self._is_available():
            return False

        try:
            full_key = self._make_key(key)
            ttl = ttl or self.default_ttl

            # 序列化
            try:
                data = pickle.dumps(value)
            except:
                try:
                    data = json.dumps(value).encode()
                except:
                    logger.error(f"无法序列化值: {type(value)}")
                    return False

            await self.client.setex(full_key, ttl, data)
            self._stats["sets"] += 1
            return True

        except Exception as e:
            logger.warning(f"Redis set失败 ({key}): {e}")
            return False

    async def delete(self, *keys: str) -> int:
        """删除缓存"""
        if not self._is_available():
            return 0

        try:
            full_keys = [self._make_key(k) for k in keys]
            result = await self.client.delete(*full_keys)
            self._stats["deletes"] += result
            return result
        except Exception as e:
            logger.warning(f"Redis delete失败: {e}")
            return 0

    async def exists(self, key: str) -> bool:
        """检查键是否存在"""
        if not self._is_available():
            return False

        try:
            full_key = self._make_key(key)
            return await self.client.exists(full_key) > 0
        except Exception as e:
            logger.warning(f"Redis exists失败: {e}")
            return False

    async def expire(self, key: str, ttl: int) -> bool:
        """设置过期时间"""
        if not self._is_available():
            return False

        try:
            full_key = self._make_key(key)
            return await self.client.expire(full_key, ttl)
        except Exception as e:
            logger.warning(f"Redis expire失败: {e}")
            return False

    async def ttl(self, key: str) -> int:
        """获取剩余过期时间"""
        if not self._is_available():
            return -2

        try:
            full_key = self._make_key(key)
            return await self.client.ttl(full_key)
        except Exception as e:
            logger.warning(f"Redis ttl失败: {e}")
            return -2

    # ========== 批量操作 ==========

    async def mget(self, keys: List[str]) -> Dict[str, Any]:
        """批量获取"""
        if not self._is_available():
            return {}

        try:
            full_keys = [self._make_key(k) for k in keys]
            values = await self.client.mget(full_keys)

            result = {}
            for i, key in enumerate(keys):
                if values[i]:
                    try:
                        result[key] = pickle.loads(values[i])
                    except:
                        result[key] = values[i]
                    self._stats["hits"] += 1
                else:
                    self._stats["misses"] += 1

            return result

        except Exception as e:
            logger.warning(f"Redis mget失败: {e}")
            return {}

    async def mset(self, mapping: Dict[str, Any], ttl: Optional[int] = None) -> bool:
        """批量设置"""
        if not self._is_available():
            return False

        try:
            ttl = ttl or self.default_ttl
            pipe = self.client.pipeline()

            for key, value in mapping.items():
                full_key = self._make_key(key)
                try:
                    data = pickle.dumps(value)
                except:
                    data = json.dumps(value).encode()
                pipe.setex(full_key, ttl, data)

            await pipe.execute()
            self._stats["sets"] += len(mapping)
            return True

        except Exception as e:
            logger.warning(f"Redis mset失败: {e}")
            return False

    # ========== 哈希表操作 ==========

    async def hget(self, key: str, field: str) -> Any:
        """获取哈希字段"""
        if not self._is_available():
            return None

        try:
            full_key = self._make_key(key)
            data = await self.client.hget(full_key, field)

            if data is None:
                self._stats["misses"] += 1
                return None

            try:
                value = pickle.loads(data)
            except:
                value = data.decode() if isinstance(data, bytes) else data

            self._stats["hits"] += 1
            return value

        except Exception as e:
            logger.warning(f"Redis hget失败: {e}")
            return None

    async def hset(self, key: str, field: str, value: Any) -> bool:
        """设置哈希字段"""
        if not self._is_available():
            return False

        try:
            full_key = self._make_key(key)
            try:
                data = pickle.dumps(value)
            except:
                data = json.dumps(value).encode()

            await self.client.hset(full_key, field, data)
            self._stats["sets"] += 1
            return True

        except Exception as e:
            logger.warning(f"Redis hset失败: {e}")
            return False

    async def hgetall(self, key: str) -> Dict[str, Any]:
        """获取整个哈希表"""
        if not self._is_available():
            return {}

        try:
            full_key = self._make_key(key)
            data = await self.client.hgetall(full_key)

            result = {}
            for field, value in data.items():
                field = field.decode() if isinstance(field, bytes) else field
                try:
                    result[field] = pickle.loads(value)
                except:
                    result[field] = (
                        value.decode() if isinstance(value, bytes) else value
                    )

            self._stats["hits"] += len(result)
            return result

        except Exception as e:
            logger.warning(f"Redis hgetall失败: {e}")
            return {}

    # ========== 列表操作 ==========

    async def lpush(self, key: str, *values) -> int:
        """从左侧推入列表"""
        if not self._is_available():
            return 0

        try:
            full_key = self._make_key(key)
            serialized = [pickle.dumps(v) for v in values]
            return await self.client.lpush(full_key, *serialized)
        except Exception as e:
            logger.warning(f"Redis lpush失败: {e}")
            return 0

    async def rpop(self, key: str) -> Any:
        """从右侧弹出列表"""
        if not self._is_available():
            return None

        try:
            full_key = self._make_key(key)
            data = await self.client.rpop(full_key)
            if data:
                return pickle.loads(data)
            return None
        except Exception as e:
            logger.warning(f"Redis rpop失败: {e}")
            return None

    async def lrange(self, key: str, start: int, end: int) -> List[Any]:
        """获取列表范围"""
        if not self._is_available():
            return []

        try:
            full_key = self._make_key(key)
            data = await self.client.lrange(full_key, start, end)
            return [pickle.loads(d) for d in data]
        except Exception as e:
            logger.warning(f"Redis lrange失败: {e}")
            return []

    # ========== 集合操作 ==========

    async def sadd(self, key: str, *members) -> int:
        """添加集合成员"""
        if not self._is_available():
            return 0

        try:
            full_key = self._make_key(key)
            serialized = [pickle.dumps(m) for m in members]
            return await self.client.sadd(full_key, *serialized)
        except Exception as e:
            logger.warning(f"Redis sadd失败: {e}")
            return 0

    async def smembers(self, key: str) -> set:
        """获取所有集合成员"""
        if not self._is_available():
            return set()

        try:
            full_key = self._make_key(key)
            members = await self.client.smembers(full_key)
            return {pickle.loads(m) for m in members}
        except Exception as e:
            logger.warning(f"Redis smembers失败: {e}")
            return set()

    async def sismember(self, key: str, member: Any) -> bool:
        """检查是否是集合成员"""
        if not self._is_available():
            return False

        try:
            full_key = self._make_key(key)
            serialized = pickle.dumps(member)
            return await self.client.sismember(full_key, serialized)
        except Exception as e:
            logger.warning(f"Redis sismember失败: {e}")
            return False

    # ========== 有序集合操作 ==========

    async def zadd(self, key: str, mapping: Dict[Any, float]) -> int:
        """添加有序集合成员"""
        if not self._is_available():
            return 0

        try:
            full_key = self._make_key(key)
            # 序列化成员
            serialized = {pickle.dumps(m): score for m, score in mapping.items()}
            return await self.client.zadd(full_key, serialized)
        except Exception as e:
            logger.warning(f"Redis zadd失败: {e}")
            return 0

    async def zrange(
        self, key: str, start: int, end: int, withscores: bool = False
    ) -> List:
        """获取有序集合范围"""
        if not self._is_available():
            return []

        try:
            full_key = self._make_key(key)
            result = await self.client.zrange(
                full_key, start, end, withscores=withscores
            )

            if withscores:
                return [(pickle.loads(m), s) for m, s in result]
            else:
                return [pickle.loads(m) for m in result]

        except Exception as e:
            logger.warning(f"Redis zrange失败: {e}")
            return []

    # ========== 发布订阅 ==========

    async def publish(self, channel: str, message: Any) -> int:
        """发布消息"""
        if not self._is_available():
            return 0

        try:
            full_channel = self._make_key(channel)
            data = pickle.dumps(message)
            return await self.client.publish(full_channel, data)
        except Exception as e:
            logger.warning(f"Redis publish失败: {e}")
            return 0

    async def subscribe(self, channel: str, callback):
        """订阅频道"""
        if not self._is_available():
            return

        try:
            if not self._pubsub:
                self._pubsub = self.client.pubsub()

            full_channel = self._make_key(channel)
            await self._pubsub.subscribe(full_channel)

            asyncio.create_task(self._pubsub_listener(callback))

        except Exception as e:
            logger.warning(f"Redis subscribe失败: {e}")

    async def _pubsub_listener(self, callback):
        """发布订阅监听器"""
        try:
            async for message in self._pubsub.listen():
                if message["type"] == "message":
                    try:
                        data = pickle.loads(message["data"])
                        await callback(data)
                    except Exception as e:
                        logger.error(f"处理订阅消息失败: {e}")
        except Exception as e:
            logger.error(f"发布订阅监听器异常: {e}")

    # ========== 统计和监控 ==========

    async def get_stats(self) -> Dict[str, Any]:
        """获取缓存统计"""
        stats = self._stats.copy()

        if self._is_available():
            try:
                info = await self.client.info()
                stats.update(
                    {
                        "redis_version": info.get("redis_version"),
                        "used_memory_human": info.get("used_memory_human"),
                        "connected_clients": info.get("connected_clients"),
                        "total_commands_processed": info.get(
                            "total_commands_processed"
                        ),
                        "keyspace_hits": info.get("keyspace_hits", 0),
                        "keyspace_misses": info.get("keyspace_misses", 0),
                    }
                )

                # 计算命中率
                total = stats["keyspace_hits"] + stats["keyspace_misses"]
                if total > 0:
                    stats["redis_hit_rate"] = stats["keyspace_hits"] / total

            except Exception as e:
                logger.warning(f"获取Redis信息失败: {e}")

        # 计算本地命中率
        total = stats["hits"] + stats["misses"]
        stats["local_hit_rate"] = stats["hits"] / total if total > 0 else 0
        stats["available"] = self._is_available()

        return stats

    async def flush_all(self, pattern: Optional[str] = None):
        """清空缓存"""
        if not self._is_available():
            return 0

        try:
            if pattern:
                # 按模式删除
                full_pattern = self._make_key(pattern)
                keys = await self.client.keys(full_pattern)
                if keys:
                    return await self.client.delete(*keys)
                return 0
            else:
                # 清空当前数据库
                return await self.client.flushdb()

        except Exception as e:
            logger.warning(f"Redis flush失败: {e}")
            return 0


# 全局Redis实例
redis_manager = RedisManager()
