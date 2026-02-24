"""
性能优化模块 - 完整保留所有性能监控和优化功能
"""

import time
import asyncio
import logging
import gc
import os
import psutil
from typing import Dict, Any, Callable, Optional, List
from functools import wraps
from dataclasses import dataclass, field
from datetime import datetime, timedelta

logger = logging.getLogger("GroupCheckInBot.Performance")


@dataclass
class PerformanceMetrics:
    """性能指标"""

    count: int = 0
    total_time: float = 0
    avg_time: float = 0
    max_time: float = 0
    min_time: float = float("inf")
    last_updated: float = 0


class PerformanceMonitor:
    """性能监控器 - 完整版"""

    def __init__(self):
        self.metrics: Dict[str, PerformanceMetrics] = {}
        self.slow_operations: List[Dict] = []
        self.start_time = time.time()
        self.slow_threshold = 1.0  # 1秒

    def track(self, operation_name: str):
        """性能跟踪装饰器"""

        def decorator(func):
            @wraps(func)
            async def async_wrapper(*args, **kwargs):
                start = time.time()
                try:
                    return await func(*args, **kwargs)
                finally:
                    self._record(operation_name, time.time() - start)

            @wraps(func)
            def sync_wrapper(*args, **kwargs):
                start = time.time()
                try:
                    return func(*args, **kwargs)
                finally:
                    self._record(operation_name, time.time() - start)

            return async_wrapper if asyncio.iscoroutinefunction(func) else sync_wrapper

        return decorator

    def _record(self, name: str, duration: float):
        """记录指标"""
        if name not in self.metrics:
            self.metrics[name] = PerformanceMetrics()

        m = self.metrics[name]
        m.count += 1
        m.total_time += duration
        m.avg_time = m.total_time / m.count
        m.max_time = max(m.max_time, duration)
        m.min_time = min(m.min_time, duration)
        m.last_updated = time.time()

        if duration > self.slow_threshold:
            self.slow_operations.append(
                {
                    "name": name,
                    "duration": duration,
                    "time": datetime.now().isoformat(),
                }
            )
            if len(self.slow_operations) > 100:
                self.slow_operations = self.slow_operations[-100:]

            logger.warning(f"⏱️ 慢操作: {name} 耗时 {duration:.3f}秒")

    def get_report(self) -> Dict[str, Any]:
        """获取性能报告"""
        uptime = time.time() - self.start_time

        # 内存使用
        try:
            process = psutil.Process()
            memory_mb = process.memory_info().rss / 1024 / 1024
            memory_percent = process.memory_percent()
        except:
            memory_mb = 0
            memory_percent = 0

        return {
            "uptime": uptime,
            "uptime_str": self._format_duration(int(uptime)),
            "memory_mb": round(memory_mb, 1),
            "memory_percent": round(memory_percent, 1),
            "slow_operations": len(self.slow_operations),
            "total_operations": sum(m.count for m in self.metrics.values()),
            "metrics": {
                name: {
                    "count": m.count,
                    "avg": round(m.avg_time, 3),
                    "max": round(m.max_time, 3),
                    "min": round(m.min_time, 3) if m.min_time != float("inf") else 0,
                }
                for name, m in self.metrics.items()
            },
            "recent_slow": self.slow_operations[-10:],
        }

    def _format_duration(self, seconds: int) -> str:
        """格式化时长"""
        h = seconds // 3600
        m = (seconds % 3600) // 60
        s = seconds % 60
        parts = []
        if h:
            parts.append(f"{h}小时")
        if m:
            parts.append(f"{m}分")
        if s:
            parts.append(f"{s}秒")
        return "".join(parts) or "0秒"


class RetryManager:
    """重试管理器 - 完整版"""

    def __init__(self, max_retries: int = 3, base_delay: float = 1.0):
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.retry_counts: Dict[str, int] = {}

    def with_retry(self, operation_name: str = "unknown"):
        """重试装饰器"""

        def decorator(func):
            @wraps(func)
            async def async_wrapper(*args, **kwargs):
                last_error = None
                for attempt in range(self.max_retries + 1):
                    try:
                        return await func(*args, **kwargs)
                    except Exception as e:
                        last_error = e
                        if attempt == self.max_retries:
                            break

                        delay = self.base_delay * (2**attempt)
                        logger.warning(
                            f"🔄 重试 {operation_name} ({attempt+1}/{self.max_retries}): {e}"
                        )
                        await asyncio.sleep(delay)

                logger.error(f"❌ {operation_name} 重试失败: {last_error}")
                raise last_error

            return async_wrapper

        return decorator

    def reset_count(self, key: str):
        """重置重试计数"""
        self.retry_counts.pop(key, None)


class GlobalCache:
    """全局缓存管理器 - 完整版"""

    def __init__(self, default_ttl: int = 300, max_size: int = 1000):
        self._cache: Dict[str, Any] = {}
        self._ttl: Dict[str, float] = {}
        self._access: List[str] = []  # LRU
        self.default_ttl = default_ttl
        self.max_size = max_size
        self.hits = 0
        self.misses = 0

    def get(self, key: str) -> Any:
        """获取缓存"""
        now = time.time()

        if key in self._ttl and now < self._ttl[key]:
            self.hits += 1
            # 更新LRU
            if key in self._access:
                self._access.remove(key)
            self._access.append(key)
            return self._cache.get(key)
        else:
            self.misses += 1
            self._cache.pop(key, None)
            self._ttl.pop(key, None)
            if key in self._access:
                self._access.remove(key)
            return None

    def set(self, key: str, value: Any, ttl: int = None):
        """设置缓存"""
        if ttl is None:
            ttl = self.default_ttl

        # 清理空间
        if len(self._cache) >= self.max_size:
            self._cleanup_lru()

        self._cache[key] = value
        self._ttl[key] = time.time() + ttl
        if key in self._access:
            self._access.remove(key)
        self._access.append(key)

    def delete(self, key: str):
        """删除缓存"""
        self._cache.pop(key, None)
        self._ttl.pop(key, None)
        if key in self._access:
            self._access.remove(key)

    def clear(self):
        """清空缓存"""
        self._cache.clear()
        self._ttl.clear()
        self._access.clear()
        logger.info("缓存已清空")

    def clear_expired(self):
        """清理过期缓存"""
        now = time.time()
        expired = [k for k, t in self._ttl.items() if now >= t]
        for key in expired:
            self.delete(key)
        if expired:
            logger.debug(f"清理 {len(expired)} 个过期缓存")

    def _cleanup_lru(self):
        """LRU清理"""
        remove_count = len(self._cache) - int(self.max_size * 0.7)
        if remove_count > 0 and self._access:
            to_remove = self._access[:remove_count]
            for key in to_remove:
                self.delete(key)
            logger.debug(f"LRU清理 {len(to_remove)} 个缓存")

    def get_stats(self) -> Dict[str, Any]:
        """获取统计"""
        total = self.hits + self.misses
        return {
            "size": len(self._cache),
            "max_size": self.max_size,
            "hits": self.hits,
            "misses": self.misses,
            "hit_rate": round(self.hits / total, 3) if total else 0,
        }


class TaskManager:
    """任务管理器 - 完整版"""

    def __init__(self):
        self._tasks: Dict[str, asyncio.Task] = {}
        self._task_counter = 0

    def create(self, coro, name: str = None) -> asyncio.Task:
        """创建任务"""
        if not name:
            self._task_counter += 1
            name = f"task_{self._task_counter}"

        task = asyncio.create_task(coro, name=name)
        self._tasks[name] = task
        task.add_done_callback(lambda t: self._tasks.pop(name, None))
        return task

    async def cancel(self, name: str):
        """取消任务"""
        task = self._tasks.get(name)
        if task and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    async def cancel_all(self):
        """取消所有任务"""
        for name in list(self._tasks.keys()):
            await self.cancel(name)

    def get_count(self) -> int:
        """获取任务数"""
        return len(self._tasks)

    def get_active(self) -> List[str]:
        """获取活跃任务"""
        return [n for n, t in self._tasks.items() if not t.done()]

    def cleanup_finished(self):
        """清理已完成任务"""
        finished = [n for n, t in self._tasks.items() if t.done()]
        for n in finished:
            self._tasks.pop(n, None)


class MessageDeduplicate:
    """消息去重管理器 - 完整版"""

    def __init__(self, ttl: int = 60):
        self._messages: Dict[str, float] = {}
        self.ttl = ttl

    def is_duplicate(self, msg_id: str) -> bool:
        """检查是否重复"""
        now = time.time()

        # 清理过期
        expired = [k for k, t in self._messages.items() if now - t > self.ttl]
        for k in expired:
            self._messages.pop(k, None)

        if msg_id in self._messages:
            return True

        self._messages[msg_id] = now
        return False

    def clear(self):
        """清空"""
        self._messages.clear()


class EnhancedPerformanceOptimizer:
    """增强版性能优化器 - 完整版"""

    def __init__(self):
        self.cleanup_interval = 300
        self.last_cleanup = time.time()
        self.is_render = self._detect_render()
        self.render_memory_limit = 400  # MB

    def _detect_render(self) -> bool:
        """检测Render环境"""
        return "RENDER" in os.environ or "RENDER_EXTERNAL_URL" in os.environ

    async def memory_cleanup(self):
        """内存清理"""
        if self.is_render:
            await self._render_cleanup()
        else:
            await self._regular_cleanup()

    async def _render_cleanup(self):
        """Render环境清理"""
        try:
            process = psutil.Process()
            memory_mb = process.memory_info().rss / 1024 / 1024
            logger.debug(f"Render内存: {memory_mb:.1f}MB")

            if memory_mb > self.render_memory_limit:
                logger.warning(f"内存过高 {memory_mb:.1f}MB，执行清理")

                # 清理缓存
                global_cache.clear_expired()

                # GC
                collected = gc.collect()
                logger.info(f"清理完成, GC回收 {collected} 对象")

        except Exception as e:
            logger.error(f"Render清理失败: {e}")

    async def _regular_cleanup(self):
        """常规环境清理"""
        now = time.time()
        if now - self.last_cleanup < self.cleanup_interval:
            return

        logger.debug("执行周期清理...")

        # 并行清理
        tasks = [
            global_cache.clear_expired(),
            task_manager.cleanup_finished(),
        ]
        await asyncio.gather(*tasks, return_exceptions=True)

        collected = gc.collect()
        if collected:
            logger.info(f"GC回收 {collected} 对象")

        self.last_cleanup = now

    def memory_usage_ok(self) -> bool:
        """检查内存使用"""
        try:
            process = psutil.Process()
            if self.is_render:
                memory_mb = process.memory_info().rss / 1024 / 1024
                return memory_mb < self.render_memory_limit
            else:
                return process.memory_percent() < 80
        except:
            return True

    def get_memory_info(self) -> Dict:
        """获取内存信息"""
        try:
            process = psutil.Process()
            memory_mb = process.memory_info().rss / 1024 / 1024
            memory_percent = process.memory_percent()
            return {
                "memory_mb": round(memory_mb, 1),
                "memory_percent": round(memory_percent, 1),
                "is_render": self.is_render,
                "limit_mb": self.render_memory_limit if self.is_render else None,
                "status": "healthy" if self.memory_usage_ok() else "warning",
            }
        except Exception as e:
            return {"error": str(e)}


# 全局实例
performance_monitor = PerformanceMonitor()
retry_manager = RetryManager()
global_cache = GlobalCache()
task_manager = TaskManager()
message_deduplicate = MessageDeduplicate()
performance_optimizer = EnhancedPerformanceOptimizer()


# 装饰器
def track_performance(name: str):
    """性能跟踪"""
    return performance_monitor.track(name)


def with_retry(name: str = "unknown", max_retries: int = 3):
    """重试"""
    rm = RetryManager(max_retries=max_retries)
    return rm.with_retry(name)


def message_deduplicate_decorator(ttl: int = 60):
    """消息去重"""
    dedup = MessageDeduplicate(ttl=ttl)

    def decorator(func):
        @wraps(func)
        async def wrapper(msg, *args, **kwargs):
            msg_id = f"{msg.chat.id}_{msg.message_id}"
            if dedup.is_duplicate(msg_id):
                logger.debug(f"跳过重复消息: {msg_id}")
                return
            return await func(msg, *args, **kwargs)

        return wrapper

    return decorator


def handle_database_errors(func):
    """数据库错误处理"""

    @wraps(func)
    async def wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except Exception as e:
            logger.error(f"数据库错误 {func.__name__}: {e}")
            raise

    return wrapper


def handle_telegram_errors(func):
    """Telegram错误处理"""

    @wraps(func)
    async def wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except Exception as e:
            logger.error(f"Telegram错误 {func.__name__}: {e}")
            raise

    return wrapper
