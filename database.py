"""
数据库层 - 完整保留所有功能
"""

import logging
import asyncio
import time
import json
from datetime import datetime, timedelta, date
from typing import Dict, Optional, List, Any, Union
import asyncpg
from asyncpg.pool import Pool

from config import Config, beijing_tz
from cache_config import CacheKeys, TTL
from aiocache import cached

logger = logging.getLogger("GroupCheckInBot.Database")


class Database:
    """PostgreSQL数据库管理器 - 完整版"""

    def __init__(self, database_url: str = None):
        self.database_url = database_url or Config.DATABASE_URL
        self.pool: Optional[Pool] = None
        self._initialized = False
        self._cache = {}
        self._cache_ttl = {}

        # 重连相关
        self._last_connection_check = 0
        self._connection_check_interval = 30
        self._reconnect_attempts = 0
        self._max_reconnect_attempts = 5
        self._reconnect_base_delay = 1.0

        self._maintenance_running = False
        self._maintenance_task = None

        self._cache_max_size = 1000
        self._cache_access_order = []

    # ========== 初始化 ==========
    async def initialize(self):
        """初始化数据库"""
        if self._initialized:
            return

        max_retries = 5
        for attempt in range(max_retries):
            try:
                logger.info(f"连接PostgreSQL (尝试 {attempt + 1}/{max_retries})")
                await self._initialize_impl()
                logger.info("✅ 数据库初始化完成")
                self._initialized = True

                # 启动维护任务
                await self.start_connection_maintenance()
                return
            except Exception as e:
                logger.warning(f"初始化失败 {attempt + 1}: {e}")
                if attempt == max_retries - 1:
                    logger.error("初始化失败，尝试强制重建")
                    await self._force_recreate_tables()
                    self._initialized = True
                await asyncio.sleep(2**attempt)

    async def _initialize_impl(self):
        """实际初始化"""
        self.pool = await asyncpg.create_pool(
            self.database_url,
            min_size=Config.DB_MIN_CONNECTIONS,
            max_size=Config.DB_MAX_CONNECTIONS,
            max_inactive_connection_lifetime=Config.DB_POOL_RECYCLE,
            command_timeout=Config.DB_CONNECTION_TIMEOUT,
            timeout=60,
        )

        await self._create_tables()
        await self._create_indexes()
        await self._init_default_data()

    async def _force_recreate_tables(self):
        """强制重建表"""
        logger.warning("🔄 强制重建表...")
        async with self.pool.acquire() as conn:
            tables = [
                "monthly_statistics",
                "activity_user_limits",
                "push_settings",
                "work_fine_configs",
                "fine_configs",
                "activity_configs",
                "work_records",
                "user_activities",
                "users",
                "groups",
                "shift_states",
                "daily_statistics",
                "reset_logs",
            ]
            for table in tables:
                await conn.execute(f"DROP TABLE IF EXISTS {table} CASCADE")
            await self._create_tables()
            await self._create_indexes()
            await self._init_default_data()
        logger.info("🎉 表重建完成")

    async def _create_tables(self):
        """创建所有表"""
        async with self.pool.acquire() as conn:
            # groups表
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS groups (
                    chat_id BIGINT PRIMARY KEY,
                    channel_id BIGINT,
                    notification_group_id BIGINT,
                    extra_work_group BIGINT,
                    reset_hour INTEGER DEFAULT 0,
                    reset_minute INTEGER DEFAULT 0,
                    soft_reset_hour INTEGER DEFAULT 0,
                    soft_reset_minute INTEGER DEFAULT 0,
                    work_start_time TEXT DEFAULT '09:00',
                    work_end_time TEXT DEFAULT '18:00',
                    dual_mode BOOLEAN DEFAULT FALSE,
                    dual_day_start TEXT,
                    dual_day_end TEXT,
                    shift_grace_before INTEGER DEFAULT 120,
                    shift_grace_after INTEGER DEFAULT 360,
                    workend_grace_before INTEGER DEFAULT 120,
                    workend_grace_after INTEGER DEFAULT 360,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """
            )

            # users表
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS users (
                    id SERIAL PRIMARY KEY,
                    chat_id BIGINT NOT NULL,
                    user_id BIGINT NOT NULL,
                    nickname TEXT,
                    current_activity TEXT,
                    activity_start_time TIMESTAMPTZ,
                    shift TEXT DEFAULT 'day',
                    checkin_message_id BIGINT,
                    total_accumulated_time INTEGER DEFAULT 0,
                    total_activity_count INTEGER DEFAULT 0,
                    total_fines INTEGER DEFAULT 0,
                    overtime_count INTEGER DEFAULT 0,
                    total_overtime_time INTEGER DEFAULT 0,
                    last_updated DATE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(chat_id, user_id)
                )
            """
            )

            # user_activities表
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS user_activities (
                    id SERIAL PRIMARY KEY,
                    chat_id BIGINT,
                    user_id BIGINT,
                    activity_date DATE,
                    activity_name TEXT,
                    activity_count INTEGER DEFAULT 0,
                    accumulated_time INTEGER DEFAULT 0,
                    shift TEXT DEFAULT 'day',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(chat_id, user_id, activity_date, activity_name, shift)
                )
            """
            )

            # work_records表
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS work_records (
                    id SERIAL PRIMARY KEY,
                    chat_id BIGINT,
                    user_id BIGINT,
                    record_date DATE,
                    checkin_type TEXT,
                    checkin_time TEXT,
                    status TEXT,
                    time_diff_minutes REAL,
                    fine_amount INTEGER DEFAULT 0,
                    shift TEXT DEFAULT 'day',
                    shift_detail TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(chat_id, user_id, record_date, checkin_type, shift)
                )
            """
            )

            # shift_states表
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS shift_states (
                    chat_id BIGINT NOT NULL,
                    user_id BIGINT NOT NULL,
                    shift TEXT NOT NULL,
                    record_date DATE NOT NULL,
                    shift_start_time TIMESTAMPTZ NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (chat_id, user_id, shift)
                )
            """
            )

            # activity_configs表
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS activity_configs (
                    activity_name TEXT PRIMARY KEY,
                    max_times INTEGER,
                    time_limit INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """
            )

            # fine_configs表
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS fine_configs (
                    id SERIAL PRIMARY KEY,
                    activity_name TEXT,
                    time_segment TEXT,
                    fine_amount INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(activity_name, time_segment)
                )
            """
            )

            # work_fine_configs表
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS work_fine_configs (
                    id SERIAL PRIMARY KEY,
                    checkin_type TEXT,
                    time_segment TEXT,
                    fine_amount INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(checkin_type, time_segment)
                )
            """
            )

            # push_settings表
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS push_settings (
                    setting_key TEXT PRIMARY KEY,
                    setting_value INTEGER DEFAULT 1,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """
            )

            # monthly_statistics表
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS monthly_statistics (
                    id SERIAL PRIMARY KEY,
                    chat_id BIGINT,
                    user_id BIGINT,
                    statistic_date DATE,
                    activity_name TEXT,
                    activity_count INTEGER DEFAULT 0,
                    accumulated_time INTEGER DEFAULT 0,
                    shift TEXT DEFAULT 'day',
                    work_days INTEGER DEFAULT 0,
                    work_hours INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(chat_id, user_id, statistic_date, activity_name, shift)
                )
            """
            )

            # activity_user_limits表
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS activity_user_limits (
                    activity_name TEXT PRIMARY KEY,
                    max_users INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """
            )

            # daily_statistics表
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS daily_statistics (
                    id SERIAL PRIMARY KEY,
                    chat_id BIGINT,
                    user_id BIGINT,
                    record_date DATE,
                    activity_name TEXT,
                    activity_count INTEGER DEFAULT 0,
                    accumulated_time INTEGER DEFAULT 0,
                    fine_amount INTEGER DEFAULT 0,
                    overtime_count INTEGER DEFAULT 0,
                    overtime_time INTEGER DEFAULT 0,
                    work_days INTEGER DEFAULT 0,
                    work_hours INTEGER DEFAULT 0,
                    shift TEXT DEFAULT 'day',
                    is_soft_reset BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(chat_id, user_id, record_date, activity_name, is_soft_reset, shift)
                )
            """
            )

            # reset_logs表
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS reset_logs (
                    id SERIAL PRIMARY KEY,
                    chat_id BIGINT NOT NULL,
                    mode VARCHAR(10) NOT NULL,
                    reset_date DATE NOT NULL,
                    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                    UNIQUE(chat_id, mode, reset_date)
                )
            """
            )

    async def _create_indexes(self):
        """创建索引"""
        async with self.pool.acquire() as conn:
            indexes = [
                "CREATE INDEX IF NOT EXISTS idx_users_primary ON users (chat_id, user_id)",
                "CREATE INDEX IF NOT EXISTS idx_users_activity ON users (chat_id, current_activity) WHERE current_activity IS NOT NULL",
                "CREATE INDEX IF NOT EXISTS idx_users_message ON users (chat_id, checkin_message_id) WHERE checkin_message_id IS NOT NULL",
                "CREATE INDEX IF NOT EXISTS idx_activities_main ON user_activities (chat_id, user_id, activity_date, shift)",
                "CREATE INDEX IF NOT EXISTS idx_activities_cleanup ON user_activities (chat_id, created_at)",
                "CREATE INDEX IF NOT EXISTS idx_work_main ON work_records (chat_id, user_id, record_date, shift)",
                "CREATE INDEX IF NOT EXISTS idx_work_night ON work_records (chat_id, user_id, shift, created_at)",
                "CREATE INDEX IF NOT EXISTS idx_work_cleanup ON work_records (chat_id, created_at)",
                "CREATE INDEX IF NOT EXISTS idx_daily_main ON daily_statistics (chat_id, record_date, user_id, shift)",
                "CREATE INDEX IF NOT EXISTS idx_monthly_main ON monthly_statistics (chat_id, statistic_date, user_id, shift)",
                "CREATE INDEX IF NOT EXISTS idx_groups_config ON groups (chat_id, dual_mode, reset_hour, reset_minute)",
                "CREATE INDEX IF NOT EXISTS idx_fine_lookup ON fine_configs (activity_name, time_segment)",
                "CREATE INDEX IF NOT EXISTS idx_shift_state ON shift_states (chat_id, shift, shift_start_time)",
            ]
            for idx in indexes:
                await conn.execute(idx)

    async def _init_default_data(self):
        """初始化默认数据"""
        async with self.pool.acquire() as conn:
            # 活动配置
            for activity, limits in Config.DEFAULT_ACTIVITY_LIMITS.items():
                await conn.execute(
                    """
                    INSERT INTO activity_configs (activity_name, max_times, time_limit)
                    VALUES ($1, $2, $3) ON CONFLICT (activity_name) DO NOTHING
                """,
                    activity,
                    limits["max_times"],
                    limits["time_limit"],
                )

            # 罚款配置
            for activity, fines in Config.DEFAULT_FINE_RATES.items():
                for segment, amount in fines.items():
                    await conn.execute(
                        """
                        INSERT INTO fine_configs (activity_name, time_segment, fine_amount)
                        VALUES ($1, $2, $3) ON CONFLICT (activity_name, time_segment) DO NOTHING
                    """,
                        activity,
                        segment,
                        amount,
                    )

            # 推送设置
            for key, value in Config.AUTO_EXPORT_SETTINGS.items():
                await conn.execute(
                    """
                    INSERT INTO push_settings (setting_key, setting_value)
                    VALUES ($1, $2) ON CONFLICT (setting_key) DO NOTHING
                """,
                    key,
                    1 if value else 0,
                )

    # ========== 连接管理 ==========
    async def start_connection_maintenance(self):
        """启动连接维护"""
        if self._maintenance_running:
            return
        self._maintenance_running = True
        self._maintenance_task = asyncio.create_task(self._maintenance_loop())
        logger.info("✅ 连接维护已启动")

    async def stop_connection_maintenance(self):
        """停止连接维护"""
        self._maintenance_running = False
        if self._maintenance_task:
            self._maintenance_task.cancel()
            try:
                await self._maintenance_task
            except asyncio.CancelledError:
                pass

    async def _maintenance_loop(self):
        """维护循环"""
        while self._maintenance_running:
            try:
                await asyncio.sleep(60)
                await self._ensure_healthy_connection()
                await self.cleanup_cache()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"维护异常: {e}")

    async def _ensure_healthy_connection(self) -> bool:
        """确保连接健康"""
        now = time.time()
        if now - self._last_connection_check < self._connection_check_interval:
            return True

        try:
            healthy = await self.health_check()
            if not healthy:
                logger.warning("连接不健康，尝试重连")
                await self._reconnect()
            self._last_connection_check = now
            return True
        except Exception as e:
            logger.error(f"连接检查失败: {e}")
            return False

    async def _reconnect(self):
        """重连"""
        self._reconnect_attempts += 1
        if self._reconnect_attempts > self._max_reconnect_attempts:
            raise ConnectionError("重连失败")

        delay = self._reconnect_base_delay * (2 ** (self._reconnect_attempts - 1))
        logger.info(f"{delay}秒后重连...")
        await asyncio.sleep(delay)

        if self.pool:
            await self.pool.close()

        self.pool = None
        self._initialized = False
        await self._initialize_impl()
        self._reconnect_attempts = 0
        logger.info("✅ 重连成功")

    async def execute_with_retry(
        self,
        name: str,
        query: str,
        *args,
        fetch: bool = False,
        fetchrow: bool = False,
        fetchval: bool = False,
        max_retries: int = 2,
        timeout: int = 30,
    ):
        """带重试的执行"""
        if not await self._ensure_healthy_connection():
            raise ConnectionError("连接不健康")

        if sum([fetch, fetchrow, fetchval]) > 1:
            raise ValueError("只能指定一种查询类型")

        for attempt in range(max_retries + 1):
            try:
                async with self.pool.acquire() as conn:
                    await conn.execute(f"SET statement_timeout = {timeout * 1000}")

                    if fetch:
                        return await conn.fetch(query, *args)
                    elif fetchrow:
                        return await conn.fetchrow(query, *args)
                    elif fetchval:
                        return await conn.fetchval(query, *args)
                    else:
                        return await conn.execute(query, *args)

            except (
                asyncpg.PostgresConnectionError,
                asyncpg.ConnectionDoesNotExistError,
            ) as e:
                if attempt == max_retries:
                    raise
                await asyncio.sleep(1 * (2**attempt))
                await self._reconnect()
            except Exception as e:
                if attempt == max_retries:
                    raise
                await asyncio.sleep(1)

    # ========== 时区 ==========
    def get_beijing_time(self) -> datetime:
        """获取北京时间"""
        return datetime.now(beijing_tz)

    def get_beijing_date(self) -> date:
        """获取北京日期"""
        return self.get_beijing_time().date()

    # ========== 缓存管理 ==========
    def _get_cached(self, key: str):
        """获取缓存"""
        if key in self._cache_ttl and time.time() < self._cache_ttl[key]:
            if key in self._cache_access_order:
                self._cache_access_order.remove(key)
                self._cache_access_order.append(key)
            return self._cache.get(key)
        else:
            self._cache.pop(key, None)
            self._cache_ttl.pop(key, None)
            if key in self._cache_access_order:
                self._cache_access_order.remove(key)
            return None

    def _set_cached(self, key: str, value: Any, ttl: int = 60):
        """设置缓存"""
        self._cache[key] = value
        self._cache_ttl[key] = time.time() + ttl
        if key in self._cache_access_order:
            self._cache_access_order.remove(key)
        self._cache_access_order.append(key)
        self._maybe_cleanup_cache()

    def _maybe_cleanup_cache(self):
        """清理缓存"""
        if len(self._cache) <= self._cache_max_size:
            return

        # LRU清理
        excess = len(self._cache) - int(self._cache_max_size * 0.7)
        if excess > 0 and self._cache_access_order:
            keys = self._cache_access_order[:excess]
            for key in keys:
                self._cache.pop(key, None)
                self._cache_ttl.pop(key, None)
            self._cache_access_order = self._cache_access_order[excess:]

    async def cleanup_cache(self):
        """清理过期缓存"""
        now = time.time()
        expired = [k for k, t in self._cache_ttl.items() if now >= t]
        for key in expired:
            self._cache.pop(key, None)
            self._cache_ttl.pop(key, None)
            if key in self._cache_access_order:
                self._cache_access_order.remove(key)

    async def force_refresh_activity_cache(self):
        """强制刷新活动缓存"""
        for key in ["activity_limits", "push_settings", "fine_rates"]:
            self._cache.pop(key, None)
            self._cache_ttl.pop(key, None)

    # ========== 群组操作 ==========
    @cached(
        ttl=TTL["group"], key_builder=lambda f, self, chat_id: CacheKeys.group(chat_id)
    )
    async def get_group(self, chat_id: int) -> Optional[Dict]:
        """获取群组配置"""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM groups WHERE chat_id = $1", chat_id
            )
            return dict(row) if row else None

    async def init_group(self, chat_id: int):
        """初始化群组"""
        await self.execute_with_retry(
            "init_group",
            "INSERT INTO groups (chat_id) VALUES ($1) ON CONFLICT (chat_id) DO NOTHING",
            chat_id,
        )

    async def update_group_channel(self, chat_id: int, channel_id: int):
        """更新频道"""
        await self.execute_with_retry(
            "update_channel",
            "UPDATE groups SET channel_id = $1, updated_at = NOW() WHERE chat_id = $2",
            channel_id,
            chat_id,
        )

    async def update_group_notification(self, chat_id: int, group_id: int):
        """更新通知群组"""
        await self.execute_with_retry(
            "update_notification",
            "UPDATE groups SET notification_group_id = $1, updated_at = NOW() WHERE chat_id = $2",
            group_id,
            chat_id,
        )

    async def update_group_extra_work(self, chat_id: int, group_id: int):
        """更新额外工作群组"""
        await self.execute_with_retry(
            "update_extra_work",
            "UPDATE groups SET extra_work_group = $1, updated_at = NOW() WHERE chat_id = $2",
            group_id,
            chat_id,
        )

    async def get_extra_work_group(self, chat_id: int) -> Optional[int]:
        """获取额外工作群组"""
        group = await self.get_group(chat_id)
        return group.get("extra_work_group") if group else None

    async def clear_extra_work_group(self, chat_id: int):
        """清除额外工作群组"""
        await self.execute_with_retry(
            "clear_extra_work",
            "UPDATE groups SET extra_work_group = NULL, updated_at = NOW() WHERE chat_id = $1",
            chat_id,
        )

    async def update_group_reset_time(self, chat_id: int, hour: int, minute: int):
        """更新重置时间"""
        await self.execute_with_retry(
            "update_reset_time",
            "UPDATE groups SET reset_hour = $1, reset_minute = $2, updated_at = NOW() WHERE chat_id = $3",
            hour,
            minute,
            chat_id,
        )

    async def update_group_soft_reset_time(self, chat_id: int, hour: int, minute: int):
        """更新软重置时间"""
        await self.execute_with_retry(
            "update_soft_reset",
            "UPDATE groups SET soft_reset_hour = $1, soft_reset_minute = $2, updated_at = NOW() WHERE chat_id = $3",
            hour,
            minute,
            chat_id,
        )

    async def get_group_soft_reset_time(self, chat_id: int) -> tuple[int, int]:
        """获取软重置时间"""
        group = await self.get_group(chat_id)
        return (
            group.get("soft_reset_hour", 0) if group else 0,
            group.get("soft_reset_minute", 0) if group else 0,
        )

    async def update_group_work_time(self, chat_id: int, start: str, end: str):
        """更新工作时间"""
        await self.execute_with_retry(
            "update_work_time",
            "UPDATE groups SET work_start_time = $1, work_end_time = $2, updated_at = NOW() WHERE chat_id = $3",
            start,
            end,
            chat_id,
        )

    async def get_group_work_time(self, chat_id: int) -> Dict[str, str]:
        """获取工作时间"""
        group = await self.get_group(chat_id)
        if group and group.get("work_start_time") and group.get("work_end_time"):
            return {
                "work_start": group["work_start_time"],
                "work_end": group["work_end_time"],
            }
        return Config.DEFAULT_WORK_HOURS.copy()

    async def has_work_hours_enabled(self, chat_id: int) -> bool:
        """检查是否启用上下班"""
        work = await self.get_group_work_time(chat_id)
        return work["work_start"] != "09:00" or work["work_end"] != "18:00"

    async def update_group_dual_mode(
        self, chat_id: int, enabled: bool, start: str = None, end: str = None
    ):
        """更新双班模式"""
        await self.execute_with_retry(
            "update_dual_mode",
            "UPDATE groups SET dual_mode = $1, dual_day_start = $2, dual_day_end = $3, updated_at = NOW() WHERE chat_id = $4",
            enabled,
            start if enabled else None,
            end if enabled else None,
            chat_id,
        )

    async def update_shift_grace(self, chat_id: int, before: int, after: int):
        """更新时间宽容窗口"""
        await self.execute_with_retry(
            "update_grace",
            "UPDATE groups SET shift_grace_before = $1, shift_grace_after = $2, updated_at = NOW() WHERE chat_id = $3",
            before,
            after,
            chat_id,
        )

    async def update_workend_grace(self, chat_id: int, before: int, after: int):
        """更新下班专用窗口"""
        await self.execute_with_retry(
            "update_workend_grace",
            "UPDATE groups SET workend_grace_before = $1, workend_grace_after = $2, updated_at = NOW() WHERE chat_id = $3",
            before,
            after,
            chat_id,
        )

    async def get_all_groups(self) -> List[int]:
        """获取所有群组"""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("SELECT chat_id FROM groups")
            return [r["chat_id"] for r in rows]

    async def is_dual_mode_enabled(self, chat_id: int) -> bool:
        """检查双班模式"""
        group = await self.get_group(chat_id)
        return bool(group and group.get("dual_mode", False))

    async def get_shift_config(self, chat_id: int) -> Dict:
        """获取班次配置"""
        group = await self.get_group(chat_id)
        work = await self.get_group_work_time(chat_id)
        has_work = await self.has_work_hours_enabled(chat_id)

        if has_work:
            day_start = work["work_start"]
            day_end = work["work_end"]
        elif group and group.get("dual_mode"):
            day_start = group.get("dual_day_start", "09:00")
            day_end = group.get("dual_day_end", "21:00")
        else:
            day_start = "09:00"
            day_end = "21:00"

        return {
            "dual_mode": bool(group and group.get("dual_mode", False)),
            "day_start": day_start,
            "day_end": day_end,
            "grace_before": group.get("shift_grace_before", 120) if group else 120,
            "grace_after": group.get("shift_grace_after", 360) if group else 360,
            "workend_grace_before": (
                group.get("workend_grace_before", 120) if group else 120
            ),
            "workend_grace_after": (
                group.get("workend_grace_after", 360) if group else 360
            ),
        }

    async def get_business_date_range(
        self, chat_id: int, dt: datetime = None
    ) -> Dict[str, date]:
        """获取业务日期范围"""
        if dt is None:
            dt = self.get_beijing_time()
        today = await self.get_business_date(chat_id, dt)
        return {
            "business_today": today,
            "business_yesterday": today - timedelta(days=1),
            "business_day_before": today - timedelta(days=2),
            "natural_today": dt.date(),
        }

    # ========== 用户操作 ==========
    @cached(
        ttl=TTL["user"],
        key_builder=lambda f, self, chat_id, user_id: CacheKeys.user(chat_id, user_id),
    )
    async def get_user(self, chat_id: int, user_id: int) -> Optional[Dict]:
        """获取用户"""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT user_id, nickname, current_activity, activity_start_time,
                       shift, checkin_message_id, total_accumulated_time,
                       total_activity_count, total_fines, overtime_count,
                       total_overtime_time, last_updated
                FROM users WHERE chat_id = $1 AND user_id = $2
            """,
                chat_id,
                user_id,
            )
            return dict(row) if row else None

    async def init_user(self, chat_id: int, user_id: int, nickname: str = None):
        """初始化用户"""
        today = await self.get_business_date(chat_id)
        await self.execute_with_retry(
            "init_user",
            """
            INSERT INTO users (chat_id, user_id, nickname, last_updated)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (chat_id, user_id) DO UPDATE SET
                nickname = COALESCE($3, users.nickname),
                last_updated = $4,
                updated_at = NOW()
            """,
            chat_id,
            user_id,
            nickname,
            today,
        )

    async def update_user_activity(
        self,
        chat_id: int,
        user_id: int,
        activity: str,
        start_time: datetime,
        nickname: str = None,
        shift: str = "day",
    ):
        """更新用户活动"""
        if nickname:
            await self.execute_with_retry(
                "update_user_activity",
                """
                UPDATE users SET current_activity = $1, activity_start_time = $2,
                    nickname = $3, shift = $4, updated_at = NOW()
                WHERE chat_id = $5 AND user_id = $6
                """,
                activity,
                start_time,
                nickname,
                shift,
                chat_id,
                user_id,
            )
        else:
            await self.execute_with_retry(
                "update_user_activity",
                """
                UPDATE users SET current_activity = $1, activity_start_time = $2,
                    shift = $3, updated_at = NOW()
                WHERE chat_id = $4 AND user_id = $5
                """,
                activity,
                start_time,
                shift,
                chat_id,
                user_id,
            )

    async def update_checkin_message(self, chat_id: int, user_id: int, message_id: int):
        """更新打卡消息ID"""
        await self.execute_with_retry(
            "update_message",
            "UPDATE users SET checkin_message_id = $1, updated_at = NOW() WHERE chat_id = $2 AND user_id = $3",
            message_id,
            chat_id,
            user_id,
        )

    async def get_checkin_message(self, chat_id: int, user_id: int) -> Optional[int]:
        """获取打卡消息ID"""
        user = await self.get_user(chat_id, user_id)
        return user.get("checkin_message_id") if user else None

    async def clear_checkin_message(self, chat_id: int, user_id: int):
        """清除打卡消息ID"""
        await self.execute_with_retry(
            "clear_message",
            "UPDATE users SET checkin_message_id = NULL, updated_at = NOW() WHERE chat_id = $1 AND user_id = $2",
            chat_id,
            user_id,
        )

    async def update_user_last_updated(
        self, chat_id: int, user_id: int, update_date: date
    ):
        """更新最后更新时间"""
        await self.execute_with_retry(
            "update_last_updated",
            "UPDATE users SET last_updated = $1 WHERE chat_id = $2 AND user_id = $3",
            update_date,
            chat_id,
            user_id,
        )

    async def get_group_members(self, chat_id: int) -> List[Dict]:
        """获取群组成员"""
        today = await self.get_business_date(chat_id)
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT user_id, nickname, current_activity, activity_start_time,
                       total_accumulated_time, total_activity_count, total_fines,
                       overtime_count, total_overtime_time
                FROM users WHERE chat_id = $1 AND last_updated = $2
            """,
                chat_id,
                today,
            )
            return [dict(r) for r in rows]

    # ========== 活动计数 ==========
    @cached(
        ttl=TTL["count"],
        key_builder=lambda f, self, chat_id, user_id, activity, shift, date: CacheKeys.count(
            chat_id, user_id, activity, shift or "all", str(date)
        ),
    )
    async def get_activity_count(
        self,
        chat_id: int,
        user_id: int,
        activity: str,
        shift: Optional[str] = None,
        query_date: Optional[date] = None,
    ) -> int:
        """获取活动次数"""
        if query_date is None:
            query_date = await self.get_business_date(chat_id)
            if shift == "night" and self.get_beijing_time().hour < 12:
                query_date -= timedelta(days=1)

        query = """
            SELECT activity_count FROM user_activities
            WHERE chat_id = $1 AND user_id = $2 AND activity_date = $3 AND activity_name = $4
        """
        params = [chat_id, user_id, query_date, activity]

        if shift:
            final_shift = (
                "night" if shift in ("night", "night_last", "night_tonight") else shift
            )
            query += " AND shift = $5"
            params.append(final_shift)

        async with self.pool.acquire() as conn:
            count = await conn.fetchval(query, *params)
            return count or 0

    async def get_user_activities(
        self, chat_id: int, user_id: int, target_date: Optional[date] = None
    ) -> Dict[str, Dict]:
        """获取用户所有活动"""
        if target_date is None:
            target_date = await self.get_business_date(chat_id)

        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT activity_name, activity_count, accumulated_time, shift
                FROM user_activities
                WHERE chat_id = $1 AND user_id = $2 AND activity_date = $3
            """,
                chat_id,
                user_id,
                target_date,
            )

            result = {}
            for r in rows:
                shift = r["shift"] or "day"
                if shift not in result:
                    result[shift] = {}
                result[shift][r["activity_name"]] = {
                    "count": r["activity_count"],
                    "time": r["accumulated_time"],
                }
            return result

    # ========== 活动完成 ==========
    async def complete_activity(
        self,
        chat_id: int,
        user_id: int,
        activity: str,
        elapsed: int,
        fine: int = 0,
        is_overtime: bool = False,
        shift: str = "day",
        forced_date: Optional[date] = None,
    ):
        """完成活动 - 同步所有表"""
        target_date = forced_date or await self.get_business_date(chat_id)
        month_start = target_date.replace(day=1)
        now = self.get_beijing_time()
        overtime_seconds = 0

        if is_overtime:
            limit = await self.get_activity_time_limit(activity)
            overtime_seconds = max(0, elapsed - limit * 60)

        async with self.pool.acquire() as conn:
            async with conn.transaction():
                # user_activities
                await conn.execute(
                    """
                    INSERT INTO user_activities (chat_id, user_id, activity_date, activity_name,
                                                 activity_count, accumulated_time, shift)
                    VALUES ($1, $2, $3, $4, 1, $5, $6)
                    ON CONFLICT (chat_id, user_id, activity_date, activity_name, shift)
                    DO UPDATE SET
                        activity_count = user_activities.activity_count + 1,
                        accumulated_time = user_activities.accumulated_time + $5,
                        updated_at = NOW()
                """,
                    chat_id,
                    user_id,
                    target_date,
                    activity,
                    elapsed,
                    shift,
                )

                # daily_statistics
                await conn.execute(
                    """
                    INSERT INTO daily_statistics (chat_id, user_id, record_date, activity_name,
                                                  activity_count, accumulated_time, shift, is_soft_reset)
                    VALUES ($1, $2, $3, $4, 1, $5, $6, FALSE)
                    ON CONFLICT (chat_id, user_id, record_date, activity_name, is_soft_reset, shift)
                    DO UPDATE SET
                        activity_count = daily_statistics.activity_count + 1,
                        accumulated_time = daily_statistics.accumulated_time + $5,
                        updated_at = NOW()
                """,
                    chat_id,
                    user_id,
                    target_date,
                    activity,
                    elapsed,
                    shift,
                )

                # monthly_statistics
                await conn.execute(
                    """
                    INSERT INTO monthly_statistics (chat_id, user_id, statistic_date, activity_name,
                                                    activity_count, accumulated_time, shift)
                    VALUES ($1, $2, $3, $4, 1, $5, $6)
                    ON CONFLICT (chat_id, user_id, statistic_date, activity_name, shift)
                    DO UPDATE SET
                        activity_count = monthly_statistics.activity_count + 1,
                        accumulated_time = monthly_statistics.accumulated_time + $5,
                        updated_at = NOW()
                """,
                    chat_id,
                    user_id,
                    month_start,
                    activity,
                    elapsed,
                    shift,
                )

                if fine > 0:
                    await conn.execute(
                        """
                        INSERT INTO daily_statistics (chat_id, user_id, record_date, activity_name, accumulated_time, shift, is_soft_reset)
                        VALUES ($1, $2, $3, 'total_fines', $4, $5, FALSE)
                        ON CONFLICT (chat_id, user_id, record_date, activity_name, is_soft_reset, shift)
                        DO UPDATE SET accumulated_time = daily_statistics.accumulated_time + $4
                    """,
                        chat_id,
                        user_id,
                        target_date,
                        fine,
                        shift,
                    )

                    await conn.execute(
                        """
                        INSERT INTO monthly_statistics (chat_id, user_id, statistic_date, activity_name, accumulated_time, shift)
                        VALUES ($1, $2, $3, 'total_fines', $4, $5)
                        ON CONFLICT (chat_id, user_id, statistic_date, activity_name, shift)
                        DO UPDATE SET accumulated_time = monthly_statistics.accumulated_time + $4
                    """,
                        chat_id,
                        user_id,
                        month_start,
                        fine,
                        shift,
                    )

                if is_overtime and overtime_seconds > 0:
                    await conn.execute(
                        """
                        INSERT INTO daily_statistics (chat_id, user_id, record_date, activity_name, activity_count, shift, is_soft_reset)
                        VALUES ($1, $2, $3, 'overtime_count', 1, $4, FALSE)
                        ON CONFLICT (chat_id, user_id, record_date, activity_name, is_soft_reset, shift)
                        DO UPDATE SET activity_count = daily_statistics.activity_count + 1
                    """,
                        chat_id,
                        user_id,
                        target_date,
                        shift,
                    )

                    await conn.execute(
                        """
                        INSERT INTO daily_statistics (chat_id, user_id, record_date, activity_name, accumulated_time, shift, is_soft_reset)
                        VALUES ($1, $2, $3, 'overtime_time', $4, $5, FALSE)
                        ON CONFLICT (chat_id, user_id, record_date, activity_name, is_soft_reset, shift)
                        DO UPDATE SET accumulated_time = daily_statistics.accumulated_time + $4
                    """,
                        chat_id,
                        user_id,
                        target_date,
                        overtime_seconds,
                        shift,
                    )

                # users表
                fields = [
                    "total_accumulated_time = total_accumulated_time + $1",
                    "total_activity_count = total_activity_count + 1",
                    "current_activity = NULL",
                    "activity_start_time = NULL",
                    "checkin_message_id = NULL",
                    "last_updated = $2",
                ]
                params = [elapsed, target_date]

                if fine > 0:
                    fields.append(f"total_fines = total_fines + ${len(params)+1}")
                    params.append(fine)

                if is_overtime:
                    fields.append("overtime_count = overtime_count + 1")
                    fields.append(
                        f"total_overtime_time = total_overtime_time + ${len(params)+1}"
                    )
                    params.append(overtime_seconds)

                fields.append("updated_at = NOW()")
                params.extend([chat_id, user_id])

                await conn.execute(
                    f"""
                    UPDATE users SET {', '.join(fields)}
                    WHERE chat_id = ${len(params)-1} AND user_id = ${len(params)}
                """,
                    *params,
                )

    async def complete_all_activities_before_reset(
        self, chat_id: int, reset_time: datetime
    ) -> Dict[str, Any]:
        """重置前完成所有活动"""
        async with self.pool.acquire() as conn:
            active = await conn.fetch(
                """
                SELECT user_id, nickname, current_activity, activity_start_time, shift
                FROM users WHERE chat_id = $1 AND current_activity IS NOT NULL
            """,
                chat_id,
            )

            result = {"completed_count": len(active), "total_fines": 0, "details": []}

            for user in active:
                try:
                    start = datetime.fromisoformat(str(user["activity_start_time"]))
                    elapsed = int((reset_time - start).total_seconds())

                    limit = await self.get_activity_time_limit(user["current_activity"])
                    is_overtime = elapsed > limit * 60
                    overtime_sec = max(0, elapsed - limit * 60)

                    fine = 0
                    if is_overtime:
                        fine = await self.calculate_fine(
                            user["current_activity"], overtime_sec / 60
                        )

                    await self.complete_activity(
                        chat_id,
                        user["user_id"],
                        user["current_activity"],
                        elapsed,
                        fine,
                        is_overtime,
                        user["shift"],
                        forced_date=reset_time.date() - timedelta(days=1),
                    )

                    result["total_fines"] += fine
                    result["details"].append(
                        {
                            "user_id": user["user_id"],
                            "nickname": user["nickname"],
                            "activity": user["current_activity"],
                            "elapsed": elapsed,
                            "fine": fine,
                            "is_overtime": is_overtime,
                        }
                    )
                except Exception as e:
                    logger.error(f"完成活动失败: {e}")

            return result

    # ========== 工作记录 ==========
    async def add_work_record(
        self,
        chat_id: int,
        user_id: int,
        record_date: date,
        checkin_type: str,
        checkin_time: str,
        status: str,
        time_diff: float,
        fine: int = 0,
        shift: str = "day",
        shift_detail: Optional[str] = None,
    ):
        """添加上下班记录"""
        business_date = await self.get_business_date(chat_id)
        month_start = business_date.replace(day=1)

        async with self.pool.acquire() as conn:
            async with conn.transaction():
                # work_records
                await conn.execute(
                    """
                    INSERT INTO work_records
                    (chat_id, user_id, record_date, checkin_type, checkin_time,
                     status, time_diff_minutes, fine_amount, shift, shift_detail)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                    ON CONFLICT (chat_id, user_id, record_date, checkin_type, shift)
                    DO UPDATE SET
                        checkin_time = EXCLUDED.checkin_time,
                        status = EXCLUDED.status,
                        time_diff_minutes = EXCLUDED.time_diff_minutes,
                        fine_amount = EXCLUDED.fine_amount,
                        shift_detail = EXCLUDED.shift_detail,
                        created_at = NOW()
                """,
                    chat_id,
                    user_id,
                    record_date,
                    checkin_type,
                    checkin_time,
                    status,
                    time_diff,
                    fine,
                    shift,
                    shift_detail,
                )

                if fine > 0:
                    activity_name = f"{checkin_type}_fines"
                    await conn.execute(
                        """
                        INSERT INTO daily_statistics (chat_id, user_id, record_date, activity_name,
                                                      accumulated_time, shift, is_soft_reset)
                        VALUES ($1, $2, $3, $4, $5, $6, FALSE)
                        ON CONFLICT (chat_id, user_id, record_date, activity_name, is_soft_reset, shift)
                        DO UPDATE SET accumulated_time = daily_statistics.accumulated_time + $5
                    """,
                        chat_id,
                        user_id,
                        business_date,
                        activity_name,
                        fine,
                        shift,
                    )

                    await conn.execute(
                        """
                        INSERT INTO monthly_statistics (chat_id, user_id, statistic_date, activity_name,
                                                        accumulated_time, shift)
                        VALUES ($1, $2, $3, $4, $5, $6)
                        ON CONFLICT (chat_id, user_id, statistic_date, activity_name, shift)
                        DO UPDATE SET accumulated_time = monthly_statistics.accumulated_time + $5
                    """,
                        chat_id,
                        user_id,
                        month_start,
                        activity_name,
                        fine,
                        shift,
                    )

                    await conn.execute(
                        """
                        UPDATE users SET total_fines = total_fines + $1
                        WHERE chat_id = $2 AND user_id = $3
                    """,
                        fine,
                        chat_id,
                        user_id,
                    )

                if checkin_type == "work_end":
                    # 计算工作时长
                    start = await conn.fetchrow(
                        """
                        SELECT checkin_time FROM work_records
                        WHERE chat_id = $1 AND user_id = $2 AND record_date = $3
                          AND checkin_type = 'work_start' AND shift = $4
                    """,
                        chat_id,
                        user_id,
                        record_date,
                        shift,
                    )

                    if start:
                        try:
                            start_dt = datetime.strptime(start["checkin_time"], "%H:%M")
                            end_dt = datetime.strptime(checkin_time, "%H:%M")
                            if end_dt < start_dt:
                                end_dt += timedelta(days=1)
                            work_seconds = int((end_dt - start_dt).total_seconds())

                            await conn.execute(
                                """
                                INSERT INTO daily_statistics (chat_id, user_id, record_date, activity_name,
                                                              accumulated_time, shift, is_soft_reset)
                                VALUES ($1, $2, $3, 'work_hours', $4, $5, FALSE)
                                ON CONFLICT (chat_id, user_id, record_date, activity_name, is_soft_reset, shift)
                                DO UPDATE SET accumulated_time = daily_statistics.accumulated_time + $4
                            """,
                                chat_id,
                                user_id,
                                business_date,
                                work_seconds,
                                shift,
                            )

                            await conn.execute(
                                """
                                INSERT INTO monthly_statistics (chat_id, user_id, statistic_date, activity_name,
                                                                accumulated_time, shift)
                                VALUES ($1, $2, $3, 'work_hours', $4, $5)
                                ON CONFLICT (chat_id, user_id, statistic_date, activity_name, shift)
                                DO UPDATE SET accumulated_time = monthly_statistics.accumulated_time + $4
                            """,
                                chat_id,
                                user_id,
                                month_start,
                                work_seconds,
                                shift,
                            )

                            await conn.execute(
                                """
                                INSERT INTO daily_statistics (chat_id, user_id, record_date, activity_name,
                                                              activity_count, shift, is_soft_reset)
                                VALUES ($1, $2, $3, 'work_days', 1, $4, FALSE)
                                ON CONFLICT (chat_id, user_id, record_date, activity_name, is_soft_reset, shift)
                                DO UPDATE SET activity_count = daily_statistics.activity_count + 1
                            """,
                                chat_id,
                                user_id,
                                business_date,
                                shift,
                            )

                            await conn.execute(
                                """
                                INSERT INTO monthly_statistics (chat_id, user_id, statistic_date, activity_name,
                                                                activity_count, shift)
                                VALUES ($1, $2, $3, 'work_days', 1, $4)
                                ON CONFLICT (chat_id, user_id, statistic_date, activity_name, shift)
                                DO UPDATE SET activity_count = monthly_statistics.activity_count + 1
                            """,
                                chat_id,
                                user_id,
                                month_start,
                                shift,
                            )
                        except Exception as e:
                            logger.error(f"计算工作时长失败: {e}")

    async def get_work_records(
        self,
        chat_id: int,
        user_id: int,
        shift: Optional[str] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> Dict[str, List]:
        """获取工作记录"""
        if start_date is None:
            start_date = await self.get_business_date(chat_id)
        if end_date is None:
            end_date = start_date

        query = """
            SELECT checkin_type, checkin_time, status, time_diff_minutes,
                   fine_amount, shift, shift_detail, created_at, record_date
            FROM work_records
            WHERE chat_id = $1 AND user_id = $2
              AND record_date >= $3 AND record_date <= $4
        """
        params = [chat_id, user_id, start_date, end_date]

        if shift:
            shift_value = "night" if shift in ("night", "夜班") else "day"
            query += " AND shift = $5"
            params.append(shift_value)

        query += " ORDER BY created_at DESC"

        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, *params)
            result = {}
            for r in rows:
                ct = r["checkin_type"]
                if ct not in result:
                    result[ct] = []
                result[ct].append(dict(r))
            return result

    async def get_today_work_records(
        self, chat_id: int, user_id: int
    ) -> Dict[str, Dict]:
        """获取今天的工作记录"""
        group = await self.get_group(chat_id)
        reset_hour = group.get("reset_hour", 0) if group else 0
        reset_minute = group.get("reset_minute", 0) if group else 0

        now = self.get_beijing_time()
        reset_today = now.replace(hour=reset_hour, minute=reset_minute, second=0)

        if now < reset_today:
            period_start = reset_today - timedelta(days=1)
        else:
            period_start = reset_today

        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT * FROM work_records
                WHERE chat_id = $1 AND user_id = $2
                  AND record_date >= $3 AND record_date <= $4
                ORDER BY record_date DESC, checkin_type
            """,
                chat_id,
                user_id,
                period_start.date(),
                now.date(),
            )

            records = {}
            for r in rows:
                if r["checkin_type"] not in records:
                    records[r["checkin_type"]] = dict(r)
            return records

    # ========== 班次状态 ==========
    async def set_shift_state(
        self, chat_id: int, user_id: int, shift: str, record_date: date
    ) -> bool:
        """设置班次状态"""
        try:
            await self.execute_with_retry(
                "set_shift",
                """
                INSERT INTO shift_states (chat_id, user_id, shift, record_date, shift_start_time)
                VALUES ($1, $2, $3, $4, NOW())
                ON CONFLICT (chat_id, user_id, shift)
                DO UPDATE SET
                    record_date = EXCLUDED.record_date,
                    shift_start_time = NOW(),
                    updated_at = NOW()
                """,
                chat_id,
                user_id,
                shift,
                record_date,
            )
            return True
        except Exception as e:
            logger.error(f"设置班次状态失败: {e}")
            return False

    async def clear_shift_state(self, chat_id: int, user_id: int, shift: str) -> bool:
        """清除班次状态"""
        try:
            await self.execute_with_retry(
                "clear_shift",
                "DELETE FROM shift_states WHERE chat_id = $1 AND user_id = $2 AND shift = $3",
                chat_id,
                user_id,
                shift,
            )
            return True
        except Exception as e:
            logger.error(f"清除班次状态失败: {e}")
            return False

    async def get_shift_state(
        self, chat_id: int, user_id: int, shift: str
    ) -> Optional[Dict]:
        """获取班次状态"""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT * FROM shift_states
                WHERE chat_id = $1 AND user_id = $2 AND shift = $3
            """,
                chat_id,
                user_id,
                shift,
            )
            return dict(row) if row else None

    async def get_active_shift(self, chat_id: int, user_id: int) -> Optional[Dict]:
        """获取活跃班次"""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT shift, record_date, shift_start_time
                FROM shift_states
                WHERE chat_id = $1 AND user_id = $2
                ORDER BY shift_start_time DESC LIMIT 1
            """,
                chat_id,
                user_id,
            )
            return dict(row) if row else None

    async def count_active_users(self, chat_id: int, shift: str) -> int:
        """统计活跃用户"""
        async with self.pool.acquire() as conn:
            return (
                await conn.fetchval(
                    """
                SELECT COUNT(*) FROM shift_states
                WHERE chat_id = $1 AND shift = $2
            """,
                    chat_id,
                    shift,
                )
                or 0
            )

    async def cleanup_expired_shifts(self, hours: int = 16) -> int:
        """清理过期班次"""
        cutoff = self.get_beijing_time() - timedelta(hours=hours)
        async with self.pool.acquire() as conn:
            result = await conn.execute(
                "DELETE FROM shift_states WHERE shift_start_time < $1", cutoff
            )
            return self._parse_count(result)

    async def get_user_current_shift(
        self, chat_id: int, user_id: int
    ) -> Optional[Dict]:
        """获取用户当前班次（基于work_records）"""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT shift, record_date, created_at as shift_start_time
                FROM work_records
                WHERE chat_id = $1 AND user_id = $2 AND checkin_type = 'work_start'
                  AND NOT EXISTS (
                      SELECT 1 FROM work_records w2
                      WHERE w2.chat_id = work_records.chat_id
                        AND w2.user_id = work_records.user_id
                        AND w2.shift = work_records.shift
                        AND w2.record_date = work_records.record_date
                        AND w2.checkin_type = 'work_end'
                  )
                ORDER BY created_at DESC LIMIT 1
            """,
                chat_id,
                user_id,
            )
            return dict(row) if row else None

    # ========== 活动配置 ==========
    @cached(ttl=TTL["activity"], key_builder=lambda f, self: CacheKeys.activity())
    async def get_activity_configs(self) -> Dict:
        """获取所有活动配置"""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("SELECT * FROM activity_configs")
            return {
                r["activity_name"]: {
                    "max_times": r["max_times"],
                    "time_limit": r["time_limit"],
                }
                for r in rows
            }

    async def get_activity_time_limit(self, activity: str) -> int:
        """获取活动时间限制"""
        configs = await self.get_activity_configs()
        return configs.get(activity, {}).get("time_limit", 0)

    async def get_activity_max_times(self, activity: str) -> int:
        """获取活动最大次数"""
        configs = await self.get_activity_configs()
        return configs.get(activity, {}).get("max_times", 0)

    async def activity_exists(self, activity: str) -> bool:
        """检查活动是否存在"""
        configs = await self.get_activity_configs()
        return activity in configs

    async def update_activity(self, activity: str, max_times: int, time_limit: int):
        """更新活动"""
        await self.execute_with_retry(
            "update_activity",
            """
            INSERT INTO activity_configs (activity_name, max_times, time_limit)
            VALUES ($1, $2, $3)
            ON CONFLICT (activity_name) DO UPDATE SET
                max_times = EXCLUDED.max_times,
                time_limit = EXCLUDED.time_limit,
                created_at = NOW()
            """,
            activity,
            max_times,
            time_limit,
        )

    async def delete_activity(self, activity: str):
        """删除活动"""
        await self.execute_with_retry(
            "delete_activity",
            "DELETE FROM activity_configs WHERE activity_name = $1",
            activity,
        )

    # ========== 活动人数限制 ==========
    async def set_activity_user_limit(self, activity: str, max_users: int):
        """设置活动人数限制"""
        await self.execute_with_retry(
            "set_user_limit",
            """
            INSERT INTO activity_user_limits (activity_name, max_users)
            VALUES ($1, $2)
            ON CONFLICT (activity_name) DO UPDATE SET
                max_users = EXCLUDED.max_users,
                updated_at = NOW()
            """,
            activity,
            max_users,
        )

    async def get_activity_user_limit(self, activity: str) -> int:
        """获取活动人数限制"""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT max_users FROM activity_user_limits WHERE activity_name = $1",
                activity,
            )
            return row["max_users"] if row else 0

    async def get_current_activity_users(self, chat_id: int, activity: str) -> int:
        """获取当前进行活动的人数"""
        async with self.pool.acquire() as conn:
            return (
                await conn.fetchval(
                    "SELECT COUNT(*) FROM users WHERE chat_id = $1 AND current_activity = $2",
                    chat_id,
                    activity,
                )
                or 0
            )

    async def get_all_activity_limits(self) -> Dict[str, int]:
        """获取所有活动人数限制"""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT activity_name, max_users FROM activity_user_limits"
            )
            return {r["activity_name"]: r["max_users"] for r in rows}

    async def remove_activity_user_limit(self, activity: str):
        """移除活动人数限制"""
        await self.execute_with_retry(
            "remove_user_limit",
            "DELETE FROM activity_user_limits WHERE activity_name = $1",
            activity,
        )

    # ========== 罚款配置 ==========
    @cached(
        ttl=TTL["fine"],
        key_builder=lambda f, self, activity=None: CacheKeys.fine(activity),
    )
    async def get_fine_rates(self, activity: str = None) -> Dict:
        """获取罚款费率"""
        if activity:
            async with self.pool.acquire() as conn:
                rows = await conn.fetch(
                    """
                    SELECT time_segment, fine_amount FROM fine_configs
                    WHERE activity_name = $1
                """,
                    activity,
                )
                return {r["time_segment"]: r["fine_amount"] for r in rows}
        else:
            async with self.pool.acquire() as conn:
                rows = await conn.fetch("SELECT * FROM fine_configs")
                result = {}
                for r in rows:
                    if r["activity_name"] not in result:
                        result[r["activity_name"]] = {}
                    result[r["activity_name"]][r["time_segment"]] = r["fine_amount"]
                return result

    async def update_fine(self, activity: str, segment: str, amount: int):
        """更新罚款"""
        await self.execute_with_retry(
            "update_fine",
            """
            INSERT INTO fine_configs (activity_name, time_segment, fine_amount)
            VALUES ($1, $2, $3)
            ON CONFLICT (activity_name, time_segment) DO UPDATE SET
                fine_amount = EXCLUDED.fine_amount,
                created_at = NOW()
            """,
            activity,
            segment,
            amount,
        )

    async def calculate_fine(self, activity: str, overtime_minutes: float) -> int:
        """计算罚款"""
        rates = await self.get_fine_rates(activity)
        if not rates:
            return 0

        segments = []
        for k in rates.keys():
            try:
                v = int(str(k).replace("min", ""))
                segments.append(v)
            except:
                pass
        segments.sort()

        for s in segments:
            if overtime_minutes <= s:
                return rates.get(str(s), rates.get(f"{s}min", 0))

        if segments:
            last = segments[-1]
            return rates.get(str(last), rates.get(f"{last}min", 0))
        return 0

    # ========== 上下班罚款 ==========
    async def get_work_fine_rates(self, checkin_type: str = None) -> Dict:
        """获取上下班罚款"""
        if checkin_type:
            async with self.pool.acquire() as conn:
                rows = await conn.fetch(
                    """
                    SELECT time_segment, fine_amount FROM work_fine_configs
                    WHERE checkin_type = $1
                """,
                    checkin_type,
                )
                return {r["time_segment"]: r["fine_amount"] for r in rows}
        else:
            async with self.pool.acquire() as conn:
                rows = await conn.fetch("SELECT * FROM work_fine_configs")
                result = {}
                for r in rows:
                    if r["checkin_type"] not in result:
                        result[r["checkin_type"]] = {}
                    result[r["checkin_type"]][r["time_segment"]] = r["fine_amount"]
                return result

    async def update_work_fine(self, checkin_type: str, segment: str, amount: int):
        """更新上下班罚款"""
        await self.execute_with_retry(
            "update_work_fine",
            """
            INSERT INTO work_fine_configs (checkin_type, time_segment, fine_amount)
            VALUES ($1, $2, $3)
            ON CONFLICT (checkin_type, time_segment) DO UPDATE SET
                fine_amount = EXCLUDED.fine_amount,
                created_at = NOW()
            """,
            checkin_type,
            segment,
            amount,
        )

    async def clear_work_fine(self, checkin_type: str):
        """清空上下班罚款"""
        await self.execute_with_retry(
            "clear_work_fine",
            "DELETE FROM work_fine_configs WHERE checkin_type = $1",
            checkin_type,
        )

    # ========== 推送设置 ==========
    @cached(ttl=TTL["medium"], key_builder=lambda f, self: "push_settings")
    async def get_push_settings(self) -> Dict:
        """获取推送设置"""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("SELECT * FROM push_settings")
            return {r["setting_key"]: bool(r["setting_value"]) for r in rows}

    async def update_push_setting(self, key: str, value: bool):
        """更新推送设置"""
        await self.execute_with_retry(
            "update_push",
            """
            INSERT INTO push_settings (setting_key, setting_value)
            VALUES ($1, $2) ON CONFLICT (setting_key) DO UPDATE SET
                setting_value = EXCLUDED.setting_value,
                created_at = NOW()
            """,
            key,
            1 if value else 0,
        )

    # ========== 统计 ==========
    async def get_group_stats(
        self, chat_id: int, target_date: Optional[date] = None
    ) -> List[Dict]:
        """获取群组统计"""
        if target_date is None:
            target_date = await self.get_business_date(chat_id)

        async with self.pool.acquire() as conn:
            # 获取所有用户
            users = await conn.fetch(
                "SELECT user_id, nickname FROM users WHERE chat_id = $1", chat_id
            )

            # 获取有记录的用户统计
            rows = await conn.fetch(
                """
                SELECT 
                    ds.user_id,
                    ds.shift,
                    ds.is_soft_reset,
                    u.nickname,
                    SUM(CASE WHEN ds.activity_name NOT IN (
                        'work_days','work_hours','work_fines',
                        'work_start_fines','work_end_fines',
                        'overtime_count','overtime_time','total_fines'
                    ) THEN ds.accumulated_time ELSE 0 END) AS total_time,
                    SUM(CASE WHEN ds.activity_name NOT IN (
                        'work_days','work_hours','work_fines',
                        'work_start_fines','work_end_fines',
                        'overtime_count','overtime_time','total_fines'
                    ) THEN ds.activity_count ELSE 0 END) AS total_count,
                    SUM(CASE WHEN ds.activity_name IN (
                        'total_fines','work_fines','work_start_fines','work_end_fines'
                    ) THEN ds.accumulated_time ELSE 0 END) AS total_fines,
                    jsonb_object_agg(
                        ds.activity_name,
                        jsonb_build_object('count', ds.activity_count, 'time', ds.accumulated_time)
                    ) FILTER (WHERE ds.activity_name NOT IN (
                        'work_days','work_hours','work_fines',
                        'work_start_fines','work_end_fines',
                        'overtime_count','overtime_time','total_fines'
                    )) AS activities
                FROM daily_statistics ds
                LEFT JOIN users u ON ds.chat_id = u.chat_id AND ds.user_id = u.user_id
                WHERE ds.chat_id = $1 AND ds.record_date = $2
                GROUP BY ds.user_id, ds.shift, ds.is_soft_reset, u.nickname
            """,
                chat_id,
                target_date,
            )

            # 获取工作统计
            work_rows = await conn.fetch(
                """
                SELECT user_id, shift,
                       COUNT(CASE WHEN checkin_type = 'work_start' THEN 1 END) AS work_start_count,
                       COUNT(CASE WHEN checkin_type = 'work_end' THEN 1 END) AS work_end_count,
                       SUM(CASE WHEN checkin_type = 'work_start' THEN fine_amount ELSE 0 END) AS work_start_fines,
                       SUM(CASE WHEN checkin_type = 'work_end' THEN fine_amount ELSE 0 END) AS work_end_fines
                FROM work_records
                WHERE chat_id = $1 AND record_date = $2
                GROUP BY user_id, shift
            """,
                chat_id,
                target_date,
            )

            work_map = {}
            for w in work_rows:
                work_map[f"{w['user_id']}:{w['shift']}"] = w

            result = []
            shift_config = await self.get_shift_config(chat_id)
            has_dual = shift_config.get("dual_mode", False)

            # 为每个用户构建数据
            user_map = {u["user_id"]: u for u in users}
            row_map = {(r["user_id"], r["shift"]): r for r in rows}

            for user_id, user in user_map.items():
                if has_dual:
                    shifts = ["day", "night"]
                else:
                    shifts = ["day"]

                for shift in shifts:
                    key = (user_id, shift)
                    if key in row_map:
                        data = dict(row_map[key])
                        data["nickname"] = user["nickname"] or f"用户{user_id}"

                        wk = work_map.get(f"{user_id}:{shift}")
                        if wk:
                            data["work_start_count"] = wk["work_start_count"]
                            data["work_end_count"] = wk["work_end_count"]
                            data["work_start_fines"] = wk["work_start_fines"]
                            data["work_end_fines"] = wk["work_end_fines"]
                        else:
                            data["work_start_count"] = 0
                            data["work_end_count"] = 0
                            data["work_start_fines"] = 0
                            data["work_end_fines"] = 0

                        if isinstance(data.get("activities"), str):
                            try:
                                data["activities"] = json.loads(data["activities"])
                            except:
                                data["activities"] = {}

                        result.append(data)
                    else:
                        result.append(
                            {
                                "user_id": user_id,
                                "nickname": user["nickname"] or f"用户{user_id}",
                                "shift": shift,
                                "is_soft_reset": False,
                                "total_time": 0,
                                "total_count": 0,
                                "total_fines": 0,
                                "work_start_count": 0,
                                "work_end_count": 0,
                                "work_start_fines": 0,
                                "work_end_fines": 0,
                                "activities": {},
                            }
                        )

            result.sort(key=lambda x: (x["user_id"], x["shift"]))
            return result

    # ========== 月度统计 ==========
    async def get_monthly_statistics(
        self, chat_id: int, year: int = None, month: int = None
    ) -> List[Dict]:
        """获取月度统计"""
        if year is None or month is None:
            today = self.get_beijing_time()
            year = today.year
            month = today.month

        start = date(year, month, 1)
        if month == 12:
            end = date(year + 1, 1, 1)
        else:
            end = date(year, month + 1, 1)

        async with self.pool.acquire() as conn:
            users = await conn.fetch(
                """
                SELECT DISTINCT user_id FROM user_activities
                WHERE chat_id = $1 AND activity_date >= $2 AND activity_date < $3
                UNION
                SELECT DISTINCT user_id FROM work_records
                WHERE chat_id = $1 AND record_date >= $2 AND record_date < $3
            """,
                chat_id,
                start,
                end,
            )

            result = []
            for u in users:
                user_id = u["user_id"]
                user_info = await conn.fetchrow(
                    "SELECT nickname FROM users WHERE chat_id = $1 AND user_id = $2",
                    chat_id,
                    user_id,
                )
                nickname = user_info["nickname"] if user_info else f"用户{user_id}"

                # 活动统计
                acts = await conn.fetch(
                    """
                    SELECT activity_name, SUM(activity_count) as total_count,
                           SUM(accumulated_time) as total_time
                    FROM user_activities
                    WHERE chat_id = $1 AND user_id = $2
                      AND activity_date >= $3 AND activity_date < $4
                    GROUP BY activity_name
                """,
                    chat_id,
                    user_id,
                    start,
                    end,
                )

                activities = {}
                total_time = 0
                total_count = 0
                for a in acts:
                    activities[a["activity_name"]] = {
                        "count": a["total_count"],
                        "time": a["total_time"],
                    }
                    total_time += a["total_time"]
                    total_count += a["total_count"]

                # 罚款
                fines = (
                    await conn.fetchval(
                        """
                    SELECT SUM(accumulated_time)
                    FROM daily_statistics
                    WHERE chat_id = $1 AND user_id = $2
                      AND record_date >= $3 AND record_date < $4
                      AND activity_name IN ('total_fines', 'work_fines', 'work_start_fines', 'work_end_fines')
                """,
                        chat_id,
                        user_id,
                        start,
                        end,
                    )
                    or 0
                )

                # 工作统计
                work = await conn.fetchrow(
                    """
                    SELECT 
                        SUM(CASE WHEN checkin_type = 'work_start' THEN 1 ELSE 0 END) as work_start_count,
                        SUM(CASE WHEN checkin_type = 'work_end' THEN 1 ELSE 0 END) as work_end_count,
                        SUM(CASE WHEN checkin_type = 'work_start' THEN fine_amount ELSE 0 END) as work_start_fines,
                        SUM(CASE WHEN checkin_type = 'work_end' THEN fine_amount ELSE 0 END) as work_end_fines
                    FROM work_records
                    WHERE chat_id = $1 AND user_id = $2
                      AND record_date >= $3 AND record_date < $4
                """,
                    chat_id,
                    user_id,
                    start,
                    end,
                )

                # 工作时长
                work_hours = (
                    await conn.fetchval(
                        """
                    SELECT SUM(accumulated_time)
                    FROM daily_statistics
                    WHERE chat_id = $1 AND user_id = $2
                      AND record_date >= $3 AND record_date < $4
                      AND activity_name = 'work_hours'
                """,
                        chat_id,
                        user_id,
                        start,
                        end,
                    )
                    or 0
                )

                work_days = (
                    await conn.fetchval(
                        """
                    SELECT SUM(activity_count)
                    FROM daily_statistics
                    WHERE chat_id = $1 AND user_id = $2
                      AND record_date >= $3 AND record_date < $4
                      AND activity_name = 'work_days'
                """,
                        chat_id,
                        user_id,
                        start,
                        end,
                    )
                    or 0
                )

                # 迟到早退
                late = (
                    await conn.fetchval(
                        """
                    SELECT COUNT(*) FROM work_records
                    WHERE chat_id = $1 AND user_id = $2
                      AND record_date >= $3 AND record_date < $4
                      AND checkin_type = 'work_start' AND time_diff_minutes > 0
                """,
                        chat_id,
                        user_id,
                        start,
                        end,
                    )
                    or 0
                )

                early = (
                    await conn.fetchval(
                        """
                    SELECT COUNT(*) FROM work_records
                    WHERE chat_id = $1 AND user_id = $2
                      AND record_date >= $3 AND record_date < $4
                      AND checkin_type = 'work_end' AND time_diff_minutes < 0
                """,
                        chat_id,
                        user_id,
                        start,
                        end,
                    )
                    or 0
                )

                # 超时
                overtime = (
                    await conn.fetchrow(
                        """
                    SELECT 
                        SUM(CASE WHEN activity_name = 'overtime_count' THEN activity_count ELSE 0 END) as overtime_count,
                        SUM(CASE WHEN activity_name = 'overtime_time' THEN accumulated_time ELSE 0 END) as overtime_time
                    FROM daily_statistics
                    WHERE chat_id = $1 AND user_id = $2
                      AND record_date >= $3 AND record_date < $4
                """,
                        chat_id,
                        user_id,
                        start,
                        end,
                    )
                    or {"overtime_count": 0, "overtime_time": 0}
                )

                result.append(
                    {
                        "user_id": user_id,
                        "nickname": nickname,
                        "total_time": total_time,
                        "total_count": total_count,
                        "total_fines": fines,
                        "overtime_count": overtime["overtime_count"] or 0,
                        "overtime_time": overtime["overtime_time"] or 0,
                        "work_days": work_days,
                        "work_hours": work_hours,
                        "work_start_count": work["work_start_count"] if work else 0,
                        "work_end_count": work["work_end_count"] if work else 0,
                        "work_start_fines": work["work_start_fines"] if work else 0,
                        "work_end_fines": work["work_end_fines"] if work else 0,
                        "late_count": late,
                        "early_count": early,
                        "activities": activities,
                    }
                )

            return result

    async def get_monthly_work_stats(
        self, chat_id: int, year: int = None, month: int = None
    ) -> List[Dict]:
        """获取月度工作统计"""
        if year is None or month is None:
            today = self.get_beijing_time()
            year = today.year
            month = today.month

        start = date(year, month, 1)
        if month == 12:
            end = date(year + 1, 1, 1)
        else:
            end = date(year, month + 1, 1)

        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT 
                    wr.user_id,
                    u.nickname,
                    COUNT(CASE WHEN wr.checkin_type = 'work_start' THEN 1 END) as work_start_count,
                    COUNT(CASE WHEN wr.checkin_type = 'work_end' THEN 1 END) as work_end_count,
                    SUM(CASE WHEN wr.checkin_type = 'work_start' THEN wr.fine_amount ELSE 0 END) as work_start_fines,
                    SUM(CASE WHEN wr.checkin_type = 'work_end' THEN wr.fine_amount ELSE 0 END) as work_end_fines
                FROM work_records wr
                JOIN users u ON wr.chat_id = u.chat_id AND wr.user_id = u.user_id
                WHERE wr.chat_id = $1 AND wr.record_date >= $2 AND wr.record_date < $3
                GROUP BY wr.user_id, u.nickname
            """,
                chat_id,
                start,
                end,
            )
            return [dict(r) for r in rows]

    async def get_monthly_ranking(
        self, chat_id: int, year: int = None, month: int = None
    ) -> Dict[str, List]:
        """获取月度排行榜"""
        if year is None or month is None:
            today = self.get_beijing_time()
            year = today.year
            month = today.month

        stat_date = date(year, month, 1)
        limits = await self.get_activity_configs()
        result = {}

        async with self.pool.acquire() as conn:
            for activity in limits.keys():
                rows = await conn.fetch(
                    """
                    SELECT 
                        ms.user_id,
                        u.nickname,
                        ms.accumulated_time as total_time,
                        ms.activity_count as total_count
                    FROM monthly_statistics ms
                    JOIN users u ON ms.chat_id = u.chat_id AND ms.user_id = u.user_id
                    WHERE ms.chat_id = $1 AND ms.activity_name = $2
                      AND ms.statistic_date = $3
                    ORDER BY ms.accumulated_time DESC
                    LIMIT 10
                """,
                    chat_id,
                    activity,
                    stat_date,
                )
                result[activity] = [dict(r) for r in rows]

        return result

    # ========== 业务日期 ==========
    async def get_business_date(
        self,
        chat_id: int,
        current_dt: Optional[datetime] = None,
        shift: Optional[str] = None,
        checkin_type: Optional[str] = None,
        shift_detail: Optional[str] = None,
        record_date: Optional[date] = None,
    ) -> date:
        """获取业务日期"""
        if current_dt is None:
            current_dt = self.get_beijing_time()

        today = current_dt.date()

        if record_date is not None:
            if shift == "night" and checkin_type == "work_end":
                return record_date + timedelta(days=1)
            return record_date

        if shift_detail == "night_last":
            return today - timedelta(days=1)
        elif shift_detail in ("night_tonight", "day"):
            return today

        group = await self.get_group(chat_id)
        reset_hour = group.get("reset_hour", 0) if group else 0
        reset_minute = group.get("reset_minute", 0) if group else 0

        reset_today = current_dt.replace(hour=reset_hour, minute=reset_minute, second=0)

        if current_dt < reset_today:
            return (current_dt - timedelta(days=1)).date()
        return today

    # ========== 重置 ==========
    async def reset_user_daily(
        self, chat_id: int, user_id: int, target_date: Optional[date] = None
    ) -> bool:
        """重置用户每日数据"""
        try:
            if target_date is None:
                target_date = await self.get_business_date(chat_id)

            user = await self.get_user(chat_id, user_id)

            async with self.pool.acquire() as conn:
                async with conn.transaction():
                    # 删除记录
                    await conn.execute(
                        "DELETE FROM daily_statistics WHERE chat_id = $1 AND user_id = $2 AND record_date = $3",
                        chat_id,
                        user_id,
                        target_date,
                    )
                    await conn.execute(
                        "DELETE FROM user_activities WHERE chat_id = $1 AND user_id = $2 AND activity_date = $3",
                        chat_id,
                        user_id,
                        target_date,
                    )
                    await conn.execute(
                        "DELETE FROM work_records WHERE chat_id = $1 AND user_id = $2 AND record_date = $3",
                        chat_id,
                        user_id,
                        target_date,
                    )

                    # 重置用户
                    await conn.execute(
                        """
                        UPDATE users SET
                            total_accumulated_time = 0,
                            total_activity_count = 0,
                            total_fines = 0,
                            total_overtime_time = 0,
                            overtime_count = 0,
                            current_activity = NULL,
                            activity_start_time = NULL,
                            checkin_message_id = NULL,
                            last_updated = $3,
                            updated_at = NOW()
                        WHERE chat_id = $1 AND user_id = $2
                    """,
                        chat_id,
                        user_id,
                        target_date,
                    )

            return True
        except Exception as e:
            logger.error(f"重置用户失败: {e}")
            return False

    async def reset_user_soft(self, chat_id: int, user_id: int) -> bool:
        """软重置用户"""
        try:
            today = await self.get_business_date(chat_id)

            async with self.pool.acquire() as conn:
                async with conn.transaction():
                    # 删除当日活动记录
                    await conn.execute(
                        "DELETE FROM user_activities WHERE chat_id = $1 AND user_id = $2 AND activity_date = $3",
                        chat_id,
                        user_id,
                        today,
                    )

                    # 重置用户字段
                    await conn.execute(
                        """
                        UPDATE users SET
                            total_accumulated_time = 0,
                            total_activity_count = 0,
                            total_fines = 0,
                            total_overtime_time = 0,
                            overtime_count = 0,
                            current_activity = NULL,
                            activity_start_time = NULL,
                            checkin_message_id = NULL,
                            last_updated = $3,
                            updated_at = NOW()
                        WHERE chat_id = $1 AND user_id = $2
                    """,
                        chat_id,
                        user_id,
                        today,
                    )

            return True
        except Exception as e:
            logger.error(f"软重置失败: {e}")
            return False

    async def reset_group(self, chat_id: int, target_date: Optional[date] = None):
        """重置群组"""
        if target_date is None:
            target_date = await self.get_business_date(chat_id)

        next_day = target_date + timedelta(days=1)

        async with self.pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(
                    "DELETE FROM user_activities WHERE chat_id = $1 AND activity_date = $2",
                    chat_id,
                    target_date,
                )
                await conn.execute(
                    "DELETE FROM user_activities WHERE chat_id = $1 AND activity_date = $2",
                    chat_id,
                    next_day,
                )
                await conn.execute(
                    """
                    UPDATE users SET
                        total_accumulated_time = 0,
                        total_activity_count = 0,
                        total_fines = 0,
                        last_updated = $2
                    WHERE chat_id = $1
                """,
                    chat_id,
                    target_date,
                )

    # ========== 数据清理 ==========
    async def cleanup_old_data(self, days: int = 30) -> int:
        """清理旧数据"""
        cutoff = (self.get_beijing_time() - timedelta(days=days)).date()
        total = 0

        async with self.pool.acquire() as conn:
            result = await conn.execute(
                "DELETE FROM user_activities WHERE activity_date < $1", cutoff
            )
            total += self._parse_count(result)

            result = await conn.execute(
                "DELETE FROM work_records WHERE record_date < $1", cutoff
            )
            total += self._parse_count(result)

        return total

    async def cleanup_monthly(self, days: int = 90) -> int:
        """清理月度数据"""
        cutoff = (self.get_beijing_time() - timedelta(days=days)).date().replace(day=1)
        async with self.pool.acquire() as conn:
            result = await conn.execute(
                "DELETE FROM monthly_statistics WHERE statistic_date < $1", cutoff
            )
            return self._parse_count(result)

    async def cleanup_specific_month(self, year: int, month: int) -> int:
        """清理指定月份"""
        target = date(year, month, 1)
        async with self.pool.acquire() as conn:
            result = await conn.execute(
                "DELETE FROM monthly_statistics WHERE statistic_date = $1", target
            )
            return self._parse_count(result)

    async def cleanup_inactive_users(self, days: int = 30) -> int:
        """清理未活动用户"""
        cutoff = (self.get_beijing_time() - timedelta(days=days)).date()
        async with self.pool.acquire() as conn:
            result = await conn.execute(
                "DELETE FROM users WHERE last_updated < $1", cutoff
            )
            return self._parse_count(result)

    # ========== 健康检查 ==========
    async def health_check(self) -> bool:
        """健康检查"""
        try:
            async with self.pool.acquire() as conn:
                return await conn.fetchval("SELECT 1") == 1
        except:
            return False

    async def close(self):
        """关闭连接"""
        if self.pool:
            await self.pool.close()

    # ========== 工具 ==========
    def _parse_count(self, result: str) -> int:
        """解析SQL结果"""
        if not result or not isinstance(result, str):
            return 0
        try:
            parts = result.split()
            if len(parts) >= 2 and parts[0] in ("DELETE", "UPDATE", "INSERT"):
                return int(parts[-1])
        except:
            pass
        return 0

    @staticmethod
    def format_seconds(seconds: int) -> str:
        """格式化秒数"""
        if not seconds:
            return "0秒"
        h = seconds // 3600
        m = (seconds % 3600) // 60
        s = seconds % 60
        if h:
            return f"{h}小时{m}分{s}秒"
        elif m:
            return f"{m}分{s}秒"
        return f"{s}秒"


# 全局实例
db = Database()
