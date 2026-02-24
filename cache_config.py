"""
缓存配置 - 统一缓存管理
"""

from aiocache import Cache
from aiocache.serializers import JsonSerializer
import os

# 根据环境选择缓存后端
REDIS_URL = os.environ.get("REDIS_URL")
if REDIS_URL:
    CACHE_TYPE = Cache.REDIS
    REDIS_ENDPOINT = REDIS_URL
else:
    CACHE_TYPE = Cache.MEMORY

SERIALIZER = JsonSerializer()

# 缓存TTL配置（秒）
TTL = {
    "user": 30,  # 用户数据
    "group": 300,  # 群组配置
    "activity": 600,  # 活动配置
    "shift": 300,  # 班次判定
    "count": 30,  # 活动计数
    "reset": 86400,  # 重置标记（24小时）
    "rank": 60,  # 排行榜
    "stats": 300,  # 统计数据
    "fine": 600,  # 罚款配置
    "medium": 300,  # 中等时长缓存（5分钟）
    "short": 30,  # 短时缓存（30秒）
    "long": 3600,  # 长时缓存（1小时）
    "very_long": 86400,  # 超长缓存（24小时）
}


class CacheKeys:
    """统一的缓存键生成器"""

    @staticmethod
    def user(chat_id: int, user_id: int) -> str:
        return f"user:{chat_id}:{user_id}"

    @staticmethod
    def group(chat_id: int) -> str:
        return f"group:{chat_id}"

    @staticmethod
    def activity(name: str = None) -> str:
        return f"activity:{name}" if name else "activity:all"

    @staticmethod
    def shift(chat_id: int, hour: int, minute_slot: int, checkin_type: str) -> str:
        return f"shift:{chat_id}:{hour}:{minute_slot}:{checkin_type}"

    @staticmethod
    def count(chat_id: int, user_id: int, activity: str, shift: str, date: str) -> str:
        return f"count:{chat_id}:{user_id}:{activity}:{shift}:{date}"

    @staticmethod
    def reset(chat_id: int, mode: str, date: str) -> str:
        return f"reset:{mode}:{chat_id}:{date}"

    @staticmethod
    def state(chat_id: int, user_id: int, shift: str) -> str:
        return f"state:{chat_id}:{user_id}:{shift}"

    @staticmethod
    def rank(chat_id: int, date: str, shift: str = None) -> str:
        base = f"rank:{chat_id}:{date}"
        return f"{base}:{shift}" if shift else base

    @staticmethod
    def monthly(chat_id: int, year: int, month: int) -> str:
        return f"monthly:{chat_id}:{year}:{month}"

    @staticmethod
    def fine(activity: str = None) -> str:
        return f"fine:{activity}" if activity else "fine:all"

    @staticmethod
    def push() -> str:
        return "push:settings"
