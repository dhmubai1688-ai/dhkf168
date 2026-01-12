# main.py - 完整异步重构优化版本（纯PostgreSQL）
import asyncio
import json
import os
import csv
import sys
import time
import gc
import aiofiles
import logging
import psutil
import traceback
from io import StringIO
from datetime import datetime, timedelta, date
from collections import defaultdict
from functools import wraps
from typing import Dict, Any, Optional, List, Tuple

from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import (
    ReplyKeyboardMarkup,
    KeyboardButton,
    ReplyKeyboardRemove,
    FSInputFile,
)
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiohttp import web

from config import Config, beijing_tz
from database import PostgreSQLDatabase as AsyncDatabase
from heartbeat import heartbeat_manager
from aiogram import types

from contextlib import suppress
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton


# 性能监控工具
from performance import (
    performance_monitor,
    task_manager,
    retry_manager,
    global_cache,
    track_performance,
    with_retry,
    message_deduplicate,
)

# 日志配置优化
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("bot.log", encoding="utf-8", mode="a"),
    ],
)
logger = logging.getLogger("GroupCheckInBot")

# 禁用过于详细的日志
logging.getLogger("aiohttp").setLevel(logging.WARNING)
logging.getLogger("asyncio").setLevel(logging.WARNING)

# 🧱 防重入全局表，防止重复点击导致多次回座
active_back_processing: dict[str, bool] = {}

# 初始化优化数据库
db = AsyncDatabase()


# 记录程序启动的时间
start_time = time.time()

# 初始化bot
bot = Bot(token=Config.TOKEN)
dp = Dispatcher(storage=MemoryStorage())


# ==================== 优化的并发安全机制 ====================
class UserLockManager:
    """优化的用户锁管理器 - 防止内存泄漏"""

    def __init__(self):
        self._locks = {}
        self._access_times = {}
        self._cleanup_interval = 3600  # 1小时清理一次
        self._last_cleanup = time.time()
        self._lock = asyncio.Lock()  # 保护内部数据结构

    def get_lock(self, chat_id: int, uid: int) -> asyncio.Lock:
        """获取用户级锁 - 优化版本"""
        key = f"{chat_id}-{uid}"

        # 记录访问时间
        self._access_times[key] = time.time()

        # 检查是否需要清理
        self._maybe_cleanup()

        # 返回或创建锁
        if key not in self._locks:
            self._locks[key] = asyncio.Lock()

        return self._locks[key]

    def _maybe_cleanup(self):
        """按需清理过期锁"""
        current_time = time.time()
        if current_time - self._last_cleanup < self._cleanup_interval:
            return

        # 执行清理
        self._last_cleanup = current_time
        self._cleanup_old_locks()

    def _cleanup_old_locks(self):
        """清理长时间未使用的锁"""
        now = time.time()
        max_age = 86400  # 24小时

        old_keys = [
            key
            for key, last_used in self._access_times.items()
            if now - last_used > max_age
        ]

        for key in old_keys:
            self._locks.pop(key, None)
            self._access_times.pop(key, None)

        if old_keys:
            logger.info(f"🧹 用户锁清理: 移除了 {len(old_keys)} 个过期锁")

    async def force_cleanup(self):
        """强制立即清理（用于内存紧张时）"""
        async with self._lock:
            old_count = len(self._locks)
            self._cleanup_old_locks()
            new_count = len(self._locks)
            logger.info(f"🚨 强制用户锁清理: {old_count} -> {new_count}")

    def get_stats(self) -> Dict[str, Any]:
        """获取锁管理器统计"""
        return {
            "active_locks": len(self._locks),
            "tracked_users": len(self._access_times),
            "last_cleanup": self._last_cleanup,
        }

    async def cancel_all_timers(self):
        """取消所有定时器 - 添加这个缺失的方法"""
        keys = list(self._timers.keys())
        cancelled_count = 0

        for key in keys:
            try:
                await self.cancel_timer(key)
                cancelled_count += 1
            except Exception as e:
                logger.error(f"取消定时器 {key} 失败: {e}")

        logger.info(f"✅ 已取消所有定时器: {cancelled_count}/{len(keys)} 个")
        return cancelled_count


# 全局用户锁管理器实例
user_lock_manager = UserLockManager()


class ActivityTimerManager:
    """活动定时器管理器 - 防止内存泄漏"""

    def __init__(self):
        self._timers = {}
        self._cleanup_interval = 300
        self._last_cleanup = time.time()

    async def start_timer(self, chat_id: int, uid: int, act: str, limit: int):
        """启动活动定时器"""
        key = f"{chat_id}-{uid}"
        await self.cancel_timer(key)

        timer_task = await task_manager.create_task(
            self._activity_timer_wrapper(chat_id, uid, act, limit), name=f"timer_{key}"
        )
        self._timers[key] = timer_task
        logger.debug(f"⏰ 启动定时器: {key} - {act}")

    async def _activity_timer_wrapper(
        self, chat_id: int, uid: int, act: str, limit: int
    ):
        """定时器包装器，确保异常处理"""
        try:
            await activity_timer(chat_id, uid, act, limit)
        except Exception as e:
            logger.error(f"定时器异常 {chat_id}-{uid}: {e}")

    async def cancel_timer(self, key: str):
        """取消定时器"""
        if key in self._timers:
            task = self._timers[key]
            if not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
            del self._timers[key]

    async def cleanup_finished_timers(self):
        """清理已完成定时器"""
        if time.time() - self._last_cleanup < self._cleanup_interval:
            return

        finished_keys = [key for key, task in self._timers.items() if task.done()]
        for key in finished_keys:
            del self._timers[key]

        if finished_keys:
            logger.info(f"🧹 定时器清理: 移除了 {len(finished_keys)} 个已完成定时器")

        self._last_cleanup = time.time()

    def get_stats(self):
        return {"active_timers": len(self._timers)}


timer_manager = ActivityTimerManager()


# ==================== 性能优化类 ====================
class EnhancedPerformanceOptimizer:
    """增强版性能优化器"""

    def __init__(self):
        self.last_cleanup = time.time()
        self.cleanup_interval = 300

    async def memory_cleanup(self):
        """智能内存清理"""
        try:
            current_time = time.time()
            if current_time - self.last_cleanup < self.cleanup_interval:
                return

            # 并行清理任务
            cleanup_tasks = [
                task_manager.cleanup_tasks(),
                global_cache.clear_expired(),
                db.cleanup_cache(),
            ]

            await asyncio.gather(*cleanup_tasks, return_exceptions=True)

            # 强制GC
            collected = gc.collect()
            logger.info(f"🧹 内存清理完成 - 回收对象: {collected}")

            self.last_cleanup = current_time
        except Exception as e:
            logger.error(f"❌ 内存清理失败: {e}")

    def memory_usage_ok(self) -> bool:
        """检查内存使用是否正常"""
        return task_manager.memory_usage_ok()

    def cleanup_user_locks(self):
        """清理长时间未使用的用户锁"""
        global user_locks
        user_locks.clear()


# 初始化优化器
performance_optimizer = EnhancedPerformanceOptimizer()


# ==================== 优化装饰器和工具类 ====================
def admin_required(func):
    """管理员权限检查装饰器 - 优化版本"""

    @wraps(func)
    async def wrapper(message: types.Message, *args, **kwargs):
        if not await is_admin(message.from_user.id):
            await message.answer(
                Config.MESSAGES["no_permission"],
                reply_markup=await get_main_keyboard(
                    message.chat.id, await is_admin(message.from_user.id)
                ),
            )
            return
        return await func(message, *args, **kwargs)

    return wrapper


def rate_limit(rate: int = 1, per: int = 1):
    """速率限制装饰器 - 优化版本"""

    def decorator(func):
        calls = []

        @wraps(func)
        async def wrapper(*args, **kwargs):
            now = time.time()
            # 清理过期记录
            calls[:] = [call for call in calls if now - call < per]

            if len(calls) >= rate:
                if args and isinstance(args[0], types.Message):
                    await args[0].answer("⏳ 操作过于频繁，请稍后再试")
                return

            calls.append(now)
            return await func(*args, **kwargs)

        return wrapper

    return decorator


class OptimizedUserContext:
    """优化版用户上下文管理器"""

    def __init__(self, chat_id: int, uid: int):
        self.chat_id = chat_id
        self.uid = uid

    async def __aenter__(self):
        await db.init_group(self.chat_id)
        await db.init_user(self.chat_id, self.uid)
        return await db.get_user_cached(self.chat_id, self.uid)

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass


class MessageFormatter:
    """消息格式化工具类 - 优化版本"""

    @staticmethod
    def format_time(seconds: int):
        """格式化时间显示 - 包含秒级精度"""
        if seconds is None:
            return "0秒"

        m, s = divmod(seconds, 60)
        h, m = divmod(m, 60)

        if h > 0:
            return f"{h}小时{m}分{s}秒"
        elif m > 0:
            return f"{m}分{s}秒"
        else:
            return f"{s}秒"

    @staticmethod
    def format_time_for_csv(seconds: int):
        """为 CSV 导出格式化时间显示 - 包含秒级精度"""
        if seconds is None:
            return "0分0秒"

        hours = seconds // 3600
        minutes = (seconds % 3600) // 60
        secs = seconds % 60

        if hours > 0:
            return f"{hours}时{minutes}分{secs}秒"
        else:
            return f"{minutes}分{secs}秒"

    @staticmethod
    def format_minutes_to_hms(minutes: float):
        """将分钟数格式化为小时:分钟:秒的字符串 - 修复精度问题"""
        if minutes is None:
            return "0小时0分0秒"

        total_seconds = int(minutes * 60)
        hours = total_seconds // 3600
        minutes_remaining = (total_seconds % 3600) // 60
        seconds_remaining = total_seconds % 60

        if hours > 0:
            return f"{hours}小时{minutes_remaining}分{seconds_remaining}秒"
        elif minutes_remaining > 0:
            return f"{minutes_remaining}分{seconds_remaining}秒"
        else:
            return f"{seconds_remaining}秒"

    @staticmethod
    def format_user_link(user_id: int, user_name: str):
        """格式化用户链接"""
        if not user_name:
            user_name = f"用户{user_id}"
        clean_name = (
            str(user_name)
            .replace("<", "")
            .replace(">", "")
            .replace("&", "")
            .replace('"', "")
        )
        return f'<a href="tg://user?id={user_id}">{clean_name}</a>'

    @staticmethod
    def create_dashed_line():
        """创建短虚线分割线"""
        return MessageFormatter.format_copyable_text("--------------------------")

    @staticmethod
    def format_copyable_text(text: str):
        """格式化可复制文本"""
        return f"<code>{text}</code>"

    @staticmethod
    def format_activity_message(
        user_id: int,
        user_name: str,
        activity: str,
        time_str: str,
        count: int,
        max_times: int,
        time_limit: int,
    ):
        """格式化打卡消息"""
        first_line = f"👤 用户：{MessageFormatter.format_user_link(user_id, user_name)}"

        message = (
            f"{first_line}\n"
            f"✅ 打卡成功：{MessageFormatter.format_copyable_text(activity)} - {MessageFormatter.format_copyable_text(time_str)}\n"
            f"⚠️ 注意：这是您第 {MessageFormatter.format_copyable_text(str(count))} 次{MessageFormatter.format_copyable_text(activity)}（今日上限：{MessageFormatter.format_copyable_text(str(max_times))}次）\n"
            f"⏰ 本次活动时间限制：{MessageFormatter.format_copyable_text(str(time_limit))} 分钟"
        )

        if count >= max_times:
            message += f"\n🚨 警告：本次结束后，您今日的{MessageFormatter.format_copyable_text(activity)}次数将达到上限，请留意！"

        message += f"\n💡提示：活动完成后请及时点击'✅ 回座'按钮"

        return message

    @staticmethod
    def format_back_message(
        user_id: int,
        user_name: str,
        activity: str,
        time_str: str,
        elapsed_time: str,
        total_activity_time: str,
        total_time: str,
        activity_counts: dict,
        total_count: int,
        is_overtime: bool = False,
        overtime_seconds: int = 0,
        fine_amount: int = 0,
    ):
        """格式化回座消息"""
        first_line = f"👤 用户：{MessageFormatter.format_user_link(user_id, user_name)}"

        message = (
            f"{first_line}\n"
            f"✅ {MessageFormatter.format_copyable_text(time_str)} 回座打卡成功\n"
            f"📝 活动：{MessageFormatter.format_copyable_text(activity)}\n"
            f"⏰ 本次活动耗时：{MessageFormatter.format_copyable_text(elapsed_time)}\n"
            f"📈 今日累计{MessageFormatter.format_copyable_text(activity)}时间：{MessageFormatter.format_copyable_text(total_activity_time)}\n"
            f"📊 今日总计时：{MessageFormatter.format_copyable_text(total_time)}\n"
        )

        if is_overtime:
            overtime_time = MessageFormatter.format_time(int(overtime_seconds))
            message += f"⚠️ 警告：您本次的活动已超时！\n🚨 超时时间：{MessageFormatter.format_copyable_text(overtime_time)}\n"
            if fine_amount > 0:
                message += f"💸 罚款：{MessageFormatter.format_copyable_text(str(fine_amount))} 元\n"

        dashed_line = MessageFormatter.create_dashed_line()
        message += f"{dashed_line}\n"

        for act, count in activity_counts.items():
            if count > 0:
                message += f"🔹 本日{MessageFormatter.format_copyable_text(act)}次数：{MessageFormatter.format_copyable_text(str(count))} 次\n"

        message += f"\n📊 今日总活动次数：{MessageFormatter.format_copyable_text(str(total_count))} 次"

        return message


class NotificationService:
    """统一推送服务 - 优化版本"""

    @staticmethod
    async def send_notification(
        chat_id: int, text: str, notification_type: str = "all"
    ):
        """发送通知到绑定的频道和群组"""
        sent = False
        push_settings = await db.get_push_settings()

        logger.info(f"🔔 开始推送通知，群组: {chat_id}, 设置: {push_settings}")

        # 获取群组数据
        group_data = await db.get_group_cached(chat_id)
        logger.info(f"🔔 群组数据: {group_data}")

        # 发送到频道
        if (
            push_settings.get("enable_channel_push")
            and group_data
            and group_data.get("channel_id")
        ):
            try:
                await bot.send_message(
                    group_data["channel_id"], text, parse_mode="HTML"
                )
                sent = True
                logger.info(f"✅ 已发送到频道: {group_data['channel_id']}")
            except Exception as e:
                logger.error(f"❌ 发送到频道失败: {e}")

        # 发送到通知群组
        if (
            push_settings.get("enable_group_push")
            and group_data
            and group_data.get("notification_group_id")
        ):
            try:
                await bot.send_message(
                    group_data["notification_group_id"], text, parse_mode="HTML"
                )
                sent = True
                logger.info(
                    f"✅ 已发送到通知群组: {group_data['notification_group_id']}"
                )
            except Exception as e:
                logger.error(f"❌ 发送到通知群组失败: {e}")

        # 管理员兜底推送
        if not sent and push_settings.get("enable_admin_push"):
            for admin_id in Config.ADMINS:
                try:
                    await bot.send_message(admin_id, text, parse_mode="HTML")
                    logger.info(f"✅ 已发送给管理员: {admin_id}")
                except Exception as e:
                    logger.error(f"❌ 发送给管理员失败: {e}")

        return sent

    @staticmethod
    async def send_document(chat_id: int, document: FSInputFile, caption: str = ""):
        """发送文档到绑定的频道和群组"""
        sent = False
        push_settings = await db.get_push_settings()
        group_data = await db.get_group_cached(chat_id)

        # 发送到频道
        if (
            push_settings.get("enable_channel_push")
            and group_data
            and group_data.get("channel_id")
        ):
            try:
                await bot.send_document(
                    group_data["channel_id"],
                    document,
                    caption=caption,
                    parse_mode="HTML",
                )
                sent = True
                logger.info(f"✅ 已发送文档到频道: {group_data['channel_id']}")
            except Exception as e:
                logger.error(f"❌ 发送文档到频道失败: {e}")

        # 发送到通知群组
        if (
            push_settings.get("enable_group_push")
            and group_data
            and group_data.get("notification_group_id")
        ):
            try:
                await bot.send_document(
                    group_data["notification_group_id"],
                    document,
                    caption=caption,
                    parse_mode="HTML",
                )
                sent = True
                logger.info(
                    f"✅ 已发送文档到通知群组: {group_data['notification_group_id']}"
                )
            except Exception as e:
                logger.error(f"❌ 发送文档到通知群组失败: {e}")

        # 管理员兜底推送
        if not sent and push_settings.get("enable_admin_push"):
            for admin_id in Config.ADMINS:
                try:
                    await bot.send_document(
                        admin_id, document, caption=caption, parse_mode="HTML"
                    )
                    logger.info(f"✅ 已发送文档给管理员: {admin_id}")
                except Exception as e:
                    logger.error(f"❌ 发送文档给管理员失败: {e}")

        return sent


# ==================== 并发安全机制优化 ====================
user_locks = defaultdict(lambda: asyncio.Lock())


def get_user_lock(chat_id: int, uid: int) -> asyncio.Lock:
    """获取用户级锁 - 优化版本（防内存泄漏）"""
    return user_lock_manager.get_lock(chat_id, uid)  # ✅ 使用新的管理器


# ==================== 状态机类 ====================
class AdminStates(StatesGroup):
    waiting_for_channel_id = State()
    waiting_for_group_id = State()


# ==================== 工具函数优化 ====================
def get_beijing_time():
    """获取北京时间"""
    return datetime.now(beijing_tz)


async def is_admin(uid):
    """检查用户是否为管理员"""
    return uid in Config.ADMINS


async def calculate_work_fine(checkin_type: str, late_minutes: float) -> int:
    """根据分钟阈值动态计算上下班罚款金额"""
    work_fine_rates = await db.get_work_fine_rates_for_type(checkin_type)
    if not work_fine_rates:
        return 0

    # 转换键为整数并排序
    thresholds = sorted([int(k) for k in work_fine_rates.keys() if str(k).isdigit()])
    late_minutes_abs = abs(late_minutes)

    applicable_fine = 0
    for threshold in thresholds:
        if late_minutes_abs >= threshold:
            applicable_fine = work_fine_rates[str(threshold)]
        else:
            break

    return applicable_fine


async def reset_daily_data_if_needed(chat_id: int, uid: int):
    """
    🎯 精确版每日数据重置 - 基于管理员设定的重置时间点
    逻辑：如果用户最后更新时间在上个重置周期之前，就重置数据
    """
    from datetime import date, datetime, timedelta

    try:
        now = get_beijing_time()

        # 获取群组自定义重置时间
        group_info = await db.get_group_cached(chat_id)
        if not group_info:
            # 如果群组不存在，先初始化
            await db.init_group(chat_id)
            group_info = await db.get_group_cached(chat_id)

        reset_hour = group_info.get("reset_hour", Config.DAILY_RESET_HOUR)
        reset_minute = group_info.get("reset_minute", Config.DAILY_RESET_MINUTE)

        # 计算当前重置周期开始时间
        reset_time_today = now.replace(
            hour=reset_hour, minute=reset_minute, second=0, microsecond=0
        )

        if now < reset_time_today:
            # 当前时间还没到今天的重置点 → 当前周期起点是昨天的重置时间
            current_period_start = reset_time_today - timedelta(days=1)
        else:
            # 已经过了今天的重置点 → 当前周期起点为今天的重置时间
            current_period_start = reset_time_today

        # 获取用户数据
        user_data = await db.get_user_cached(chat_id, uid)
        if not user_data:
            # 用户不存在，初始化用户
            await db.init_user(chat_id, uid, "用户")
            return

        last_updated_str = user_data.get("last_updated")
        if not last_updated_str:
            # 如果没有最后更新时间，重置数据
            logger.info(f"🔄 初始化用户数据: {chat_id}-{uid} (无最后更新时间)")
            await db.reset_user_daily_data(chat_id, uid, now.date())
            await db.update_user_last_updated(chat_id, uid, now.date())
            return

        # 解析最后更新时间
        last_updated = None
        if isinstance(last_updated_str, str):
            try:
                # 尝试ISO格式解析
                last_updated = datetime.fromisoformat(
                    str(last_updated_str).replace("Z", "+00:00")
                )
            except ValueError:
                try:
                    # 尝试日期格式解析
                    last_updated = datetime.strptime(str(last_updated_str), "%Y-%m-%d")
                except ValueError:
                    # 其他格式，直接使用今天日期
                    last_updated = now
        elif isinstance(last_updated_str, datetime):
            last_updated = last_updated_str
        elif isinstance(last_updated_str, date):
            last_updated = datetime.combine(last_updated_str, datetime.min.time())
        else:
            # 未知类型，使用今天日期
            last_updated = now

        # 🎯 关键逻辑：比较最后更新时间是否在当前重置周期之前
        if last_updated.date() < current_period_start.date():
            logger.info(
                f"🔄 重置用户数据: {chat_id}-{uid}\n"
                f"   最后活动时间: {last_updated.date()}\n"
                f"   当前周期开始: {current_period_start.date()}\n"
                f"   重置时间设置: {reset_hour:02d}:{reset_minute:02d}\n"
                f"   当前北京时问: {now.strftime('%Y-%m-%d %H:%M:%S')}"
            )

            # 执行重置
            await db.reset_user_daily_data(chat_id, uid, current_period_start.date())
            # 更新最后更新时间到当前周期
            await db.update_user_last_updated(chat_id, uid, now.date())

        else:
            logger.debug(
                f"✅ 无需重置: {chat_id}-{uid}\n"
                f"   最后活动: {last_updated.date()}\n"
                f"   周期开始: {current_period_start.date()}"
            )

    except Exception as e:
        logger.error(f"❌ 重置检查失败 {chat_id}-{uid}: {e}")
        # 出错时安全初始化用户
        try:
            await db.init_user(chat_id, uid, "用户")
            await db.update_user_last_updated(chat_id, uid, datetime.now().date())
        except Exception as init_error:
            logger.error(f"❌ 用户初始化也失败: {init_error}")


async def check_activity_limit(chat_id: int, uid: int, act: str):
    """检查活动次数是否达到上限"""
    await db.init_group(chat_id)
    await db.init_user(chat_id, uid)

    current_count = await db.get_user_activity_count(chat_id, uid, act)
    max_times = await db.get_activity_max_times(act)

    return current_count < max_times, current_count, max_times


async def has_active_activity(chat_id: int, uid: int):
    """检查用户是否有活动正在进行"""
    await db.init_group(chat_id)
    await db.init_user(chat_id, uid)
    user_data = await db.get_user_cached(chat_id, uid)
    return user_data["current_activity"] is not None, user_data["current_activity"]


async def has_work_hours_enabled(chat_id: int) -> bool:
    """检查是否启用了上下班功能"""
    return await db.has_work_hours_enabled(chat_id)


async def has_clocked_in_today(chat_id: int, uid: int, checkin_type: str) -> bool:
    """检查用户今天是否打过指定的上下班卡"""
    return await db.has_work_record_today(chat_id, uid, checkin_type)


async def can_perform_activities(chat_id: int, uid: int) -> tuple[bool, str]:
    """快速检查是否可以执行活动"""
    if not await db.has_work_hours_enabled(chat_id):
        return True, ""

    today_records = await db.get_today_work_records(chat_id, uid)

    if "work_start" not in today_records:
        return False, "❌ 请先打上班卡！"

    if "work_end" in today_records:
        return False, "❌ 已下班，无法进行活动！"

    return True, ""


async def calculate_fine(activity: str, overtime_minutes: float) -> int:
    """计算罚款金额 - 分段罚款（修复字符串键问题）"""
    fine_rates = await db.get_fine_rates_for_activity(activity)
    if not fine_rates:
        return 0

    # 修复：正确处理字符串键（如 '30min'）
    segments = []
    for time_key in fine_rates.keys():
        try:
            # 处理 '30min' 格式的键
            if isinstance(time_key, str) and "min" in time_key.lower():
                # 提取数字部分
                time_value = int(time_key.lower().replace("min", "").strip())
            else:
                time_value = int(time_key)
            segments.append(time_value)
        except (ValueError, TypeError) as e:
            logger.warning(f"⚠️ 无法解析罚款时间段键 '{time_key}': {e}")
            continue

    if not segments:
        return 0

    segments.sort()

    applicable_fine = 0
    for segment in segments:
        if overtime_minutes <= segment:
            # 使用原始键获取罚款金额
            original_key = str(segment)
            if original_key not in fine_rates:
                # 尝试 '30min' 格式
                original_key = f"{segment}min"
            applicable_fine = fine_rates.get(original_key, 0)
            break

    if applicable_fine == 0 and segments:
        # 使用最大的时间段
        max_segment = segments[-1]
        original_key = str(max_segment)
        if original_key not in fine_rates:
            original_key = f"{max_segment}min"
        applicable_fine = fine_rates.get(original_key, 0)

    logger.debug(
        f"💰 罚款计算: 活动={activity}, 超时={overtime_minutes:.1f}分钟, 罚款={applicable_fine}元"
    )
    return applicable_fine


# ==================== 回复键盘 ====================
async def get_main_keyboard(chat_id: int = None, show_admin=False):
    """获取主回复键盘 - 确保使用最新活动配置"""
    try:
        # 🆕 强制刷新活动配置缓存
        if "activity_limits" in db._cache:
            del db._cache["activity_limits"]
        if "activity_limits" in db._cache_ttl:
            del db._cache_ttl["activity_limits"]

        activity_limits = await db.get_activity_limits_cached()
        logger.info(f"🔄 键盘生成 - 活动数量: {len(activity_limits)}")
    except Exception as e:
        logger.error(f"❌ 获取活动配置失败: {e}")
        activity_limits = await db.get_activity_limits_cached()

    dynamic_buttons = []
    current_row = []

    for act in activity_limits.keys():
        current_row.append(KeyboardButton(text=act))
        if len(current_row) >= 3:
            dynamic_buttons.append(current_row)
            current_row = []

    # 添加上下班按钮（如果启用）
    if chat_id and await has_work_hours_enabled(chat_id):
        current_row.append(KeyboardButton(text="🟢 上班"))
        current_row.append(KeyboardButton(text="🔴 下班"))
        if len(current_row) >= 3:
            dynamic_buttons.append(current_row)
            current_row = []

    if current_row:
        dynamic_buttons.append(current_row)

    fixed_buttons = []
    fixed_buttons.append([KeyboardButton(text="✅ 回座")])

    bottom_buttons = []
    if show_admin:
        bottom_buttons.append(
            [
                KeyboardButton(text="👑 管理员面板"),
                KeyboardButton(text="📊 我的记录"),
                KeyboardButton(text="🏆 排行榜"),
            ]
        )
    else:
        bottom_buttons.append(
            [KeyboardButton(text="📊 我的记录"), KeyboardButton(text="🏆 排行榜")]
        )

    keyboard = dynamic_buttons + fixed_buttons + bottom_buttons

    return ReplyKeyboardMarkup(
        keyboard=keyboard,
        resize_keyboard=True,
        one_time_keyboard=False,
        input_field_placeholder="请选择操作或输入活动名称...",
    )


def get_admin_keyboard():
    """管理员专用键盘"""
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="👑 管理员面板"), KeyboardButton(text="📤 导出数据")],
            [KeyboardButton(text="🔙 返回主菜单")],
        ],
        resize_keyboard=True,
    )


# ==================== 活动定时提醒优化 ====================
async def activity_timer(chat_id: int, uid: int, act: str, limit: int):
    """活动定时提醒任务 - 纯业务逻辑版"""
    try:
        # ✅ 直接执行内部逻辑，不管理任务创建
        await _activity_timer_inner(chat_id, uid, act, limit)

    except asyncio.CancelledError:
        logger.info(f"定时器 {chat_id}-{uid} 被取消")
    except Exception as e:
        logger.error(f"定时器错误: {e}")


async def _activity_timer_inner(chat_id: int, uid: int, act: str, limit: int):
    """定时器内部逻辑 - 原有的 activity_timer 内容移动到这里"""
    one_minute_warning_sent = False
    timeout_immediate_sent = False
    timeout_5min_sent = False
    last_reminder_minute = 0

    while True:
        user_lock = get_user_lock(chat_id, uid)
        async with user_lock:
            user_data = await db.get_user_cached(chat_id, uid)
            if not user_data or user_data["current_activity"] != act:
                break

            start_time = datetime.fromisoformat(user_data["activity_start_time"])
            elapsed = (get_beijing_time() - start_time).total_seconds()
            remaining = limit * 60 - elapsed

            nickname = user_data.get("nickname", str(uid))

        # 1分钟前警告
        if 0 < remaining <= 60 and not one_minute_warning_sent:
            warning_msg = (
                f"⏳ <b>即将超时警告</b>\n"
                f"👤 用户：{MessageFormatter.format_user_link(uid, nickname)}\n"
                f"🕓 您本次 {MessageFormatter.format_copyable_text(act)} 还有 <code>1</code> 分钟即将超时！\n"
                f"💡 请及时回座，避免超时罚款"
            )
            # 创建回座按钮
            back_keyboard = InlineKeyboardMarkup(
                inline_keyboard=[
                    [
                        InlineKeyboardButton(
                            text="👉 点击✅立即回座 👈",
                            callback_data=f"quick_back:{chat_id}:{uid}",
                        )
                    ]
                ]
            )
            await bot.send_message(
                chat_id, warning_msg, parse_mode="HTML", reply_markup=back_keyboard
            )
            one_minute_warning_sent = True

        # 超时提醒
        if remaining <= 0:
            overtime_minutes = int(-remaining // 60)

            if overtime_minutes == 0 and not timeout_immediate_sent:
                timeout_msg = (
                    f"⚠️ <b>超时警告</b>\n"
                    f"👤 用户：{MessageFormatter.format_user_link(uid, nickname)}\n"
                    f"❌ 您的 {MessageFormatter.format_copyable_text(act)} 已经<code>超时</code>！\n"
                    f"🏃‍♂️ 请立即回座，避免产生更多罚款！"
                )
                # 创建回座按钮
                back_keyboard = InlineKeyboardMarkup(
                    inline_keyboard=[
                        [
                            InlineKeyboardButton(
                                text="👉 点击✅立即回座 👈",
                                callback_data=f"quick_back:{chat_id}:{uid}",
                            )
                        ]
                    ]
                )

                await bot.send_message(
                    chat_id, timeout_msg, parse_mode="HTML", reply_markup=back_keyboard
                )
                timeout_immediate_sent = True
                last_reminder_minute = 0

            elif overtime_minutes == 5 and not timeout_5min_sent:
                timeout_msg = (
                    f"🔔 <b>超时警告</b>\n"
                    f"👤 用户：{MessageFormatter.format_user_link(uid, nickname)}\n"
                    f"❌ 您的 {MessageFormatter.format_copyable_text(act)} 已经超时 <code>5</code> 分钟！\n"
                    f"😤 请立即回座，避免罚款增加！"
                )
                # 创建回座按钮
                back_keyboard = InlineKeyboardMarkup(
                    inline_keyboard=[
                        [
                            InlineKeyboardButton(
                                text="👉 点击✅立即回座 👈",
                                callback_data=f"quick_back:{chat_id}:{uid}",
                            )
                        ]
                    ]
                )
                await bot.send_message(
                    chat_id, timeout_msg, parse_mode="HTML", reply_markup=back_keyboard
                )
                timeout_5min_sent = True
                last_reminder_minute = 5

            elif (
                overtime_minutes >= 10
                and overtime_minutes % 10 == 0
                and overtime_minutes > last_reminder_minute
            ):
                timeout_msg = (
                    f"🚨 <b>超时警告</b>\n"
                    f"👤 用户：{MessageFormatter.format_user_link(uid, nickname)}\n"
                    f"❌ 您的 {MessageFormatter.format_copyable_text(act)} 已经超时 <code>{overtime_minutes}</code> 分钟！\n"
                    f"💢 请立即回座！"
                )
                # 创建回座按钮
                back_keyboard = InlineKeyboardMarkup(
                    inline_keyboard=[
                        [
                            InlineKeyboardButton(
                                text="👉 点击✅立即回座 👈",
                                callback_data=f"quick_back:{chat_id}:{uid}",
                            )
                        ]
                    ]
                )
                await bot.send_message(
                    chat_id, timeout_msg, parse_mode="HTML", reply_markup=back_keyboard
                )
                last_reminder_minute = overtime_minutes

        # 检查超时强制回座
        user_lock = get_user_lock(chat_id, uid)
        async with user_lock:
            user_data = await db.get_user_cached(chat_id, uid)
            if user_data and user_data["current_activity"] == act:

                if remaining <= -120 * 60:
                    overtime_minutes = 120
                    overtime_seconds = 120 * 60

                    fine_amount = await calculate_fine(act, overtime_minutes)

                    elapsed = (
                        get_beijing_time()
                        - datetime.fromisoformat(user_data["activity_start_time"])
                    ).total_seconds()

                    await db.complete_user_activity(
                        chat_id, uid, act, int(elapsed), fine_amount, True
                    )

                    auto_back_msg = (
                        f"🛑 <b>自动安全回座</b>\n"
                        f"👤 用户：{MessageFormatter.format_user_link(uid, nickname)}\n"
                        f"📝 活动：<code>{act}</code>\n"
                        f"⚠️ 由于超时超过2小时，系统已自动为您回座\n"
                        f"⏰ 超时时长：<code>120</code> 分钟\n"
                        f"💰 本次罚款：<code>{fine_amount}</code> 元\n"
                        f"💢 请检查是否忘记回座！"
                    )
                    await bot.send_message(chat_id, auto_back_msg, parse_mode="HTML")

                    try:
                        chat_title = str(chat_id)
                        try:
                            chat_info = await bot.get_chat(chat_id)
                            chat_title = chat_info.title or chat_title
                        except Exception:
                            pass

                        notif_text = (
                            f"🚨 <b>自动回座超时通知</b>\n"
                            f"🏢 群组：<code>{chat_title}</code>\n"
                            f"{MessageFormatter.create_dashed_line()}\n"
                            f"👤 用户：{MessageFormatter.format_user_link(uid, nickname)}\n"
                            f"📝 活动：<code>{act}</code>\n"
                            f"⏰ 回座时间：<code>{get_beijing_time().strftime('%m/%d %H:%M:%S')}</code>\n"
                            f"⏱️ 超时时长：<code>120</code> 分钟\n"
                            f"💰 本次罚款：<code>{fine_amount}</code> 元\n"
                            f"🔔 类型：系统自动回座（超时2小时强制）"
                        )
                        # 🆕 添加推送通知
                        sent = await NotificationService.send_notification(
                            chat_id, notif_text
                        )
                        if not sent:
                            logger.warning(
                                f"⚠️ 2小时自动回座通知发送失败，尝试管理员兜底。"
                            )
                            for admin_id in Config.ADMINS:
                                with suppress(Exception):
                                    await bot.send_message(
                                        admin_id, notif_text, parse_mode="HTML"
                                    )

                    except Exception as e:
                        logger.error(f"发送自动回座通知失败: {e}")

                    await timer_manager.cancel_timer(f"{chat_id}-{uid}")
                    break

        await asyncio.sleep(30)


# ==================== 核心打卡功能优化 ====================
async def _start_activity_locked(
    message: types.Message, act: str, chat_id: int, uid: int
):
    """线程安全的打卡逻辑 - 优化版本"""
    name = message.from_user.full_name
    now = get_beijing_time()

    if not await db.activity_exists(act):
        await message.answer(
            f"❌ 活动 '{act}' 不存在，请使用下方按钮选择活动",
            reply_markup=await get_main_keyboard(
                chat_id=chat_id, show_admin=await is_admin(uid)
            ),
        )
        return

    # 🆕 检查活动人数限制
    user_limit = await db.get_activity_user_limit(act)
    if user_limit > 0:
        current_users = await db.get_current_activity_users(chat_id, act)
        if current_users >= user_limit:
            await message.answer(
                f"❌ 打卡失败~ 活动 '<code>{act}</code>' 人数已满！\n\n"
                f"📊 当前状态：\n"
                f"• 限制人数：<code>{user_limit}</code> 人\n"
                f"• 当前进行：<code>{current_users}</code> 人\n"
                f"• 剩余名额：<code>0</code> 人\n\n"
                f"💡 请等待其他用户回座后再打卡进行此活动",
                reply_markup=await get_main_keyboard(
                    chat_id=chat_id, show_admin=await is_admin(uid)
                ),
                parse_mode="HTML",
            )
            return

    can_perform, reason = await can_perform_activities(chat_id, uid)
    if not can_perform:
        await message.answer(
            reason,
            reply_markup=await get_main_keyboard(
                chat_id=chat_id, show_admin=await is_admin(uid)
            ),
            parse_mode="HTML",
        )
        return

    has_active, current_act = await has_active_activity(chat_id, uid)
    if has_active:
        await message.answer(
            Config.MESSAGES["has_activity"].format(current_act),
            reply_markup=await get_main_keyboard(
                chat_id=chat_id, show_admin=await is_admin(uid)
            ),
        )
        return

    # 先重置数据（如果需要）
    await reset_daily_data_if_needed(chat_id, uid)

    can_start, current_count, max_times = await check_activity_limit(chat_id, uid, act)

    if not can_start:
        await message.answer(
            Config.MESSAGES["max_times_reached"].format(act, max_times),
            reply_markup=await get_main_keyboard(
                chat_id=chat_id, show_admin=await is_admin(uid)
            ),
        )
        return

    await db.update_user_activity(chat_id, uid, act, str(now), name)

    key = f"{chat_id}-{uid}"

    time_limit = await db.get_activity_time_limit(act)

    await timer_manager.start_timer(chat_id, uid, act, time_limit)

    await message.answer(
        MessageFormatter.format_activity_message(
            uid,
            name,
            act,
            now.strftime("%m/%d %H:%M:%S"),
            current_count + 1,
            max_times,
            time_limit,
        ),
        reply_markup=await get_main_keyboard(
            chat_id=chat_id, show_admin=await is_admin(uid)
        ),
        parse_mode="HTML",
    )


async def start_activity(message: types.Message, act: str):
    """优化的开始活动"""
    chat_id = message.chat.id
    uid = message.from_user.id

    logger.info(
        f"🔄 [start_activity] 开始处理活动: {act} - 用户 {uid} - 群组 {chat_id}"
    )

    user_lock = get_user_lock(chat_id, uid)
    async with user_lock:
        # 快速检查
        if not await db.activity_exists(act):
            await message.answer(f"❌ 活动 '{act}' 不存在")
            return

        # 检查活动限制
        can_perform, reason = await can_perform_activities(chat_id, uid)
        if not can_perform:
            await message.answer(reason)
            return

        # 开始活动
        await _start_activity_locked(message, act, chat_id, uid)




# ==================== 用户功能优化 ====================
async def show_history(message: types.Message):
    """显示用户历史记录 - 优化版本"""
    chat_id = message.chat.id
    uid = message.from_user.id

    async with OptimizedUserContext(chat_id, uid) as user:
        first_line = (
            f"👤 用户：{MessageFormatter.format_user_link(uid, user['nickname'])}"
        )
        text = f"{first_line}\n📊 今日记录：\n\n"

        has_records = False
        activity_limits = await db.get_activity_limits_cached()
        user_activities = await db.get_user_all_activities(chat_id, uid)

        for act in activity_limits.keys():
            activity_info = user_activities.get(act, {})
            total_time = activity_info.get("time", 0)
            count = activity_info.get("count", 0)
            max_times = activity_limits[act]["max_times"]
            if total_time > 0 or count > 0:
                status = "✅" if count < max_times else "❌"
                time_str = MessageFormatter.format_time(int(total_time))
                text += f"• <code>{act}</code>：<code>{time_str}</code>，次数：<code>{count}</code>/<code>{max_times}</code> {status}\n"
                has_records = True

        total_time_all = user.get("total_accumulated_time", 0)
        total_count_all = user.get("total_activity_count", 0)
        total_fine = user.get("total_fines", 0)
        overtime_count = user.get("overtime_count", 0)
        total_overtime = user.get("total_overtime_time", 0)

        text += f"\n📈 今日总统计：\n"
        text += f"• 总累计时间：<code>{MessageFormatter.format_time(int(total_time_all))}</code>\n"
        text += f"• 总活动次数：<code>{total_count_all}</code> 次\n"
        if overtime_count > 0:
            text += f"• 超时次数：<code>{overtime_count}</code> 次\n"
            text += f"• 总超时时间：<code>{MessageFormatter.format_time(int(total_overtime))}</code>\n"
        if total_fine > 0:
            text += f"• 累计罚款：<code>{total_fine}</code> 元"

        if not has_records and total_count_all == 0:
            text += "暂无记录，请先进行打卡活动"

        await message.answer(
            text,
            reply_markup=await get_main_keyboard(
                chat_id=chat_id, show_admin=await is_admin(uid)
            ),
            parse_mode="HTML",
        )


async def show_rank(message: types.Message):
    """显示排行榜（完整功能版）"""
    chat_id = message.chat.id
    uid = message.from_user.id

    await db.init_group(chat_id)
    activity_limits = await db.get_activity_limits_cached()

    if not activity_limits:
        await message.answer("⚠️ 当前没有配置任何活动，无法生成排行榜。")
        return

    rank_text = "🏆 今日活动排行榜\n\n"
    today = db.get_beijing_date()
    found_any_data = False

    async with db.pool.acquire() as conn:
        for act in activity_limits.keys():
            # 🎯 修复：保留次数统计的完整查询
            rows = await conn.fetch(
                """
                WITH ranked_activities AS (
                    -- 已完成的活动
                    SELECT 
                        ua.user_id,
                        COALESCE(u.nickname, '用户' || ua.user_id::text) as nickname,
                        ua.accumulated_time as total_time,
                        ua.activity_count,
                        'completed' as status,
                        NULL as activity_start_time,
                        ua.accumulated_time as sort_key  -- 按累计时间排序
                    FROM user_activities ua
                    LEFT JOIN users u ON ua.chat_id = u.chat_id AND ua.user_id = u.user_id
                    WHERE ua.chat_id = $1 
                      AND ua.activity_date = $2 
                      AND ua.activity_name = $3
                      AND ua.accumulated_time > 0
                    
                    UNION
                    
                    -- 进行中的活动
                    SELECT 
                        u.user_id,
                        COALESCE(u.nickname, '用户' || u.user_id::text) as nickname,
                        0 as total_time,
                        0 as activity_count,
                        'active' as status,
                        u.activity_start_time,
                        -- 🎯 关键优化：进行中活动按持续时间排序
                        EXTRACT(epoch FROM (CURRENT_TIMESTAMP - u.activity_start_time::timestamp)) as sort_key
                    FROM users u
                    WHERE u.chat_id = $1 
                      AND u.current_activity = $3
                )
                SELECT * FROM ranked_activities 
                ORDER BY sort_key DESC
                LIMIT 5
                """,
                chat_id,
                today,
                act,
            )

            if rows:
                found_any_data = True
                rank_text += f"📈 <code>{act}</code>：\n"

                for i, row in enumerate(rows, 1):
                    user_id = row["user_id"]
                    name = row["nickname"]
                    time_sec = row["total_time"] or 0
                    status = row["status"]
                    activity_count = row["activity_count"] or 0  # 🎯 修复：获取次数

                    if status == "completed" and time_sec > 0:
                        time_str = MessageFormatter.format_time(int(time_sec))
                        # 🎯 修复：显示次数统计
                        rank_text += f"  <code>{i}.</code> 🟢 {MessageFormatter.format_user_link(user_id, name)} - {time_str} ({activity_count}次)\n"
                    elif status == "active":
                        # 计算进行中活动的持续时间
                        duration_info = ""
                        if row["activity_start_time"]:
                            try:
                                start_time = datetime.fromisoformat(
                                    row["activity_start_time"]
                                )
                                now = get_beijing_time()
                                elapsed_seconds = int(
                                    (now - start_time).total_seconds()
                                )
                                duration_info = f" ({MessageFormatter.format_time(elapsed_seconds)})"
                            except Exception:
                                duration_info = ""
                        # 🎯 修复：进行中也显示次数（如果有的话）
                        count_info = (
                            f" ({activity_count}次)" if activity_count > 0 else ""
                        )
                        rank_text += f"  <code>{i}.</code> 🟡 {MessageFormatter.format_user_link(user_id, name)} - 进行中{duration_info}{count_info}\n"

                rank_text += "\n"

    if not found_any_data:
        rank_text = (
            "🏆 今日活动排行榜\n\n"
            "📊 今日还没有活动记录\n"
            "💪 开始第一个活动吧！\n\n"
            "💡 提示：开始活动后会立即显示在这里"
        )

    await message.answer(
        rank_text,
        reply_markup=await get_main_keyboard(chat_id, await is_admin(uid)),
        parse_mode="HTML",
    )


# ==================== 回座功能优化 ====================


async def _process_back_locked(message: types.Message, chat_id: int, uid: int):
    """线程安全的回座逻辑（防重入 + 超时 + 日志优化）"""
    start_time = time.time()
    key = f"{chat_id}:{uid}"

    # 🚧 防重入检测
    if active_back_processing.get(key):
        await message.answer("⚠️ 您的回座请求正在处理中，请稍候。")
        logger.warning(f"⏳ 阻止重复回座: chat_id={chat_id}, uid={uid}")
        return
    active_back_processing[key] = True

    try:
        logger.info(f"🔧 开始回座处理: chat_id={chat_id}, uid={uid}")

        # ✅ 整体超时保护（防止Supabase或网络阻塞）
        async def core_process():
            now = get_beijing_time()

            async with OptimizedUserContext(chat_id, uid) as user_data:
                if not user_data.get("current_activity"):
                    await message.answer(
                        Config.MESSAGES["no_activity"],
                        reply_markup=await get_main_keyboard(
                            chat_id=chat_id, show_admin=await is_admin(uid)
                        ),
                    )
                    return

                act = user_data["current_activity"]
                start_time_dt = datetime.fromisoformat(user_data["activity_start_time"])
                elapsed = (now - start_time_dt).total_seconds()

                # ✅ 带超时的数据库操作
                try:
                    time_limit_minutes = await asyncio.wait_for(
                        db.get_activity_time_limit(act), timeout=8
                    )
                except asyncio.TimeoutError:
                    logger.warning(f"⏰ 获取活动时长超时: {act}")
                    time_limit_minutes = Config.DEFAULT_ACTIVITY_LIMIT_MINUTES

                time_limit_seconds = time_limit_minutes * 60
                is_overtime = elapsed > time_limit_seconds
                overtime_seconds = max(0, int(elapsed - time_limit_seconds))
                overtime_minutes = overtime_seconds / 60

                fine_amount = 0
                if is_overtime and overtime_seconds > 0:
                    try:
                        fine_amount = await asyncio.wait_for(
                            calculate_fine(act, overtime_minutes),
                            timeout=5,
                        )
                    except asyncio.TimeoutError:
                        logger.warning(f"💸 计算罚款超时: act={act}")
                    except Exception as e:
                        logger.error(f"❌ 计算罚款失败: {e}")
                        fine_amount = 0  # 计算失败时不罚款

                # 记录活动计数前后变化
                try:
                    before_count = await asyncio.wait_for(
                        db.get_user_activity_count(chat_id, uid, act), timeout=8
                    )
                    logger.info(f"🔍 [回座前] 用户{uid} 活动{act} 计数: {before_count}")
                except Exception as e:
                    logger.warning(f"计数查询失败: {e}")
                    before_count = 0

                # ✅ 安全更新活动状态
                await asyncio.wait_for(
                    db.complete_user_activity(
                        chat_id, uid, act, int(elapsed), fine_amount, is_overtime
                    ),
                    timeout=10,
                )

                after_count = await db.get_user_activity_count(chat_id, uid, act)
                logger.info(f"🔍 [回座后] 用户{uid} 活动{act} 新计数: {after_count}")

            # 🔄 取消旧计时任务 - 确保这里没有遗漏
            try:
                await timer_manager.cancel_timer(f"{chat_id}-{uid}")
                logger.info(f"✅ 已取消定时器: {chat_id}-{uid}")
            except Exception as e:
                logger.warning(f"⚠️ 取消定时器失败: {e}")

            # ✅ 读取用户最新数据 - 添加更多错误处理
            try:
                user_data = await asyncio.wait_for(
                    db.get_user_cached(chat_id, uid), timeout=10
                )
                if not user_data:
                    logger.error(f"❌ 无法获取用户数据: {chat_id}:{uid}")
                    await message.answer("❌ 获取用户数据失败，请稍后重试。")
                    return
            except asyncio.TimeoutError:
                logger.error(f"⏰ 获取用户数据超时: {chat_id}:{uid}")
                await message.answer("❌ 数据获取超时，请稍后重试。")
                return
            except Exception as e:
                logger.error(f"❌ 获取用户数据失败: {e}")
                await message.answer("❌ 数据获取失败，请稍后重试。")
                return

            try:
                user_activities = await asyncio.wait_for(
                    db.get_user_all_activities(chat_id, uid), timeout=10
                )
            except Exception as e:
                logger.warning(f"⚠️ 获取用户活动数据失败: {e}")
                user_activities = {}

            activity_counts = {a: i.get("count", 0) for a, i in user_activities.items()}

            # 生成回座信息 - 添加更多空值保护
            try:
                await message.answer(
                    MessageFormatter.format_back_message(
                        user_id=uid,
                        user_name=user_data.get("nickname", "未知用户"),
                        activity=act,
                        time_str=now.strftime("%m/%d %H:%M:%S"),
                        elapsed_time=MessageFormatter.format_time(int(elapsed)),
                        total_activity_time=MessageFormatter.format_time(
                            int(user_activities.get(act, {}).get("time", 0))
                        ),
                        total_time=MessageFormatter.format_time(
                            int(user_data.get("total_accumulated_time", 0))
                        ),
                        activity_counts=activity_counts,
                        total_count=user_data.get("total_activity_count", 0),
                        is_overtime=is_overtime,
                        overtime_seconds=overtime_seconds,
                        fine_amount=fine_amount,
                    ),
                    reply_markup=await get_main_keyboard(
                        chat_id=chat_id, show_admin=await is_admin(uid)
                    ),
                    parse_mode="HTML",
                )
            except Exception as e:
                logger.error(f"❌ 发送回座消息失败: {e}")
                # 发送简化版消息
                await message.answer(
                    f"✅ 回座成功！\n"
                    f"活动: {act}\n"
                    f"时长: {MessageFormatter.format_time(int(elapsed))}\n"
                    f"{'⚠️ 已超时' if is_overtime else '✅ 按时完成'}",
                    reply_markup=await get_main_keyboard(
                        chat_id=chat_id, show_admin=await is_admin(uid)
                    ),
                )

            # ✅ 超时通知推送（容错）
            if is_overtime and fine_amount > 0:
                try:
                    chat_title = str(chat_id)
                    try:
                        chat_info = await bot.get_chat(chat_id)
                        chat_title = chat_info.title or chat_title
                    except Exception as e:
                        logger.warning(f"无法获取群组信息: {e}")

                    notif_text = (
                        f"🚨 <b>超时回座通知</b>\n"
                        f"🏢 群组：<code>{chat_title}</code>\n"
                        f"{MessageFormatter.create_dashed_line()}\n"
                        f"👤 用户：{MessageFormatter.format_user_link(uid, user_data.get('nickname', '未知用户'))}\n"
                        f"📝 活动：<code>{act}</code>\n"
                        f"⏰ 回座时间：<code>{now.strftime('%m/%d %H:%M:%S')}</code>\n"
                        f"⏱️ 超时：<code>{MessageFormatter.format_time(int(overtime_seconds))}</code>\n"
                        f"💰 罚款：<code>{fine_amount}</code> 元"
                    )
                    await asyncio.wait_for(
                        NotificationService.send_notification(chat_id, notif_text),
                        timeout=8,
                    )
                except Exception as e:
                    logger.error(f"⚠️ 超时通知推送异常: {e}")

        # 整体逻辑超时保护（防止单协程死锁）
        await asyncio.wait_for(core_process(), timeout=60)

    except asyncio.TimeoutError:
        logger.error(f"⏰ 回座逻辑整体超时: chat_id={chat_id}, uid={uid}")
        await message.answer("⚠️ 回座操作超时，请稍后重试。")

    except Exception as e:
        logger.error(f"💥 回座处理异常: {e}", exc_info=True)
        try:
            await message.answer("❌ 回座失败，请稍后重试。")
        except Exception:
            pass

    finally:
        # ✅ 释放防重入锁 - 确保这里没有遗漏
        active_back_processing.pop(key, None)
        duration = round(time.time() - start_time, 2)
        logger.info(f"✅ 回座结束 chat_id={chat_id}, uid={uid}，耗时 {duration}s")


async def process_back(message: types.Message):
    """回座打卡 - 优化版本"""
    chat_id = message.chat.id
    uid = message.from_user.id

    user_lock = get_user_lock(chat_id, uid)
    async with user_lock:
        await _process_back_locked(message, chat_id, uid)


# ==================== 管理员按钮处理优化 ====================


async def export_data(message: types.Message):
    """导出数据 - 优化版本"""
    if not await is_admin(message.from_user.id):
        await message.answer(
            Config.MESSAGES["no_permission"],
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=False
            ),
        )
        return

    chat_id = message.chat.id
    await message.answer("⏳ 正在导出数据...")
    try:
        await export_and_push_csv(chat_id)
        await message.answer("✅ 数据导出完成！")
    except Exception as e:
        await message.answer(f"❌ 导出失败：{e}")


# ==================== 从月度表获取统计数据 ====================


async def get_group_stats_from_monthly(chat_id: int, target_date: date) -> List[Dict]:
    """从月度统计表获取群组统计数据（用于重置后导出）"""
    try:
        # 获取目标日期对应的月份
        month_start = target_date.replace(day=1)

        logger.info(
            f"🔍 从月度表查询数据: 群组{chat_id}, 日期{target_date}, 月份{month_start}"
        )

        # 从月度表获取数据
        monthly_stats = await db.get_monthly_statistics(
            chat_id, month_start.year, month_start.month
        )

        if not monthly_stats:
            logger.warning(f"⚠️ 月度表中没有找到 {month_start} 的数据")
            return []

        result = []
        for stat in monthly_stats:
            # 🆕 调试日志：检查工作相关字段
            logger.debug(
                f"📊 用户 {stat['user_id']} 工作数据: "
                f"工作天数={stat.get('work_days', 0)}, "
                f"工作时长={stat.get('work_hours', 0)}秒"
            )

            user_data = {
                "user_id": stat["user_id"],
                "nickname": stat.get("nickname", f"用户{stat['user_id']}"),
                "total_accumulated_time": stat.get("total_accumulated_time", 0),
                "total_activity_count": stat.get("total_activity_count", 0),
                "total_fines": stat.get("total_fines", 0),
                "overtime_count": stat.get("overtime_count", 0),
                "total_overtime_time": stat.get("total_overtime_time", 0),
                "work_days": stat.get("work_days", 0),  # 🆕 新增工作天数
                "work_hours": stat.get("work_hours", 0),  # 🆕 新增工作时长
                "activities": stat.get("activities", {}),
            }

            result.append(user_data)

        logger.info(
            f"✅ 从月度表成功获取 {target_date} 的数据，共 {len(result)} 个用户"
        )
        return result

    except Exception as e:
        logger.error(f"❌ 从月度表获取数据失败: {e}")
        return []


# ==================== CSV导出推送功能优化 ====================
async def optimized_monthly_export(chat_id: int, year: int, month: int):
    """优化版月度数据导出 - 修复字段映射"""
    try:
        # 获取活动配置
        activity_limits = await db.get_activity_limits_cached()
        activity_names = list(activity_limits.keys())

        csv_buffer = StringIO()
        writer = csv.writer(csv_buffer)

        # 构建表头
        headers = ["用户ID", "用户昵称"]

        # 为每个活动添加次数和时长的列
        for act in activity_names:
            headers.extend([f"{act}次数", f"{act}总时长"])

        # 添加总计列
        headers.extend(
            [
                "活动次数总计",
                "活动用时总计",
                "罚款总金额",
                "超时次数",
                "总超时时间",
                "工作天数",
                "工作时长",
            ]
        )

        writer.writerow(headers)

        # 使用现有的月度统计方法
        monthly_stats = await db.get_monthly_statistics(chat_id, year, month)

        if not monthly_stats:
            return None

        # 处理每个用户的数据
        for user_stat in monthly_stats:
            row = [user_stat["user_id"], user_stat.get("nickname", "未知用户")]

            # 确保活动数据完整
            for act in activity_names:
                activity_info = user_stat.get("activities", {}).get(act, {})
                count = activity_info.get("count", 0)
                time_seconds = activity_info.get("time", 0)
                time_formatted = db.format_time_for_csv(time_seconds)

                row.append(count)
                row.append(time_formatted)

            # 使用正确的字段名映射
            row.extend(
                [
                    user_stat.get("total_activity_count", 0),  # 活动次数总计
                    db.format_time_for_csv(
                        user_stat.get("total_accumulated_time", 0)
                    ),  # 活动用时总计
                    user_stat.get("total_fines", 0),  # 罚款总金额
                    user_stat.get("overtime_count", 0),  # 超时次数
                    db.format_time_for_csv(
                        user_stat.get("total_overtime_time", 0)
                    ),  # 总超时时间
                    user_stat.get("work_days", 0),  # 工作天数
                    db.format_time_for_csv(user_stat.get("work_hours", 0)),  # 工作时长
                ]
            )

            writer.writerow(row)

        return csv_buffer.getvalue()

    except Exception as e:
        logger.error(f"❌ 月度导出优化版失败: {e}")
        return None


async def export_and_push_csv(
    chat_id: int,
    to_admin_if_no_group: bool = True,
    file_name: str = None,
    target_date=None,
):
    """导出群组数据为 CSV 并推送 - 支持从月度表恢复数据"""
    await db.init_group(chat_id)

    # 规范 target_date
    if target_date is not None and hasattr(target_date, "date"):
        target_date = target_date.date()

    if not file_name:
        if target_date is not None:
            date_str = target_date.strftime("%Y%m%d")
        else:
            date_str = get_beijing_time().strftime("%Y%m%d_%H%M%S")
        file_name = f"group_{chat_id}_statistics_{date_str}.csv"

    # 🆕 关键修复：检查是否是重置后的导出（目标日期是昨天）
    now = get_beijing_time()
    is_reset_export = False
    if target_date and target_date == (now - timedelta(days=1)).date():
        is_reset_export = True
        logger.info(f"🔄 检测到重置后导出，将从月度表恢复 {target_date} 的数据")

    csv_buffer = StringIO()
    writer = csv.writer(csv_buffer)

    activity_limits = await db.get_activity_limits_cached()
    headers = ["用户ID", "用户昵称"]
    for act in activity_limits.keys():
        headers.extend([f"{act}次数", f"{act}总时长"])
    headers.extend(
        ["活动次数总计", "活动用时总计", "罚款总金额", "超时次数", "总超时时间"]
    )
    writer.writerow(headers)

    has_data = False

    if is_reset_export:
        # 🆕 重置后导出：从月度表获取数据
        group_stats = await get_group_stats_from_monthly(chat_id, target_date)
    else:
        # 正常导出：从日常表获取数据
        group_stats = await db.get_group_statistics(chat_id, target_date)

    # 后续代码保持不变...
    for user_data in group_stats:
        total_count = user_data.get("total_activity_count", 0)
        total_time = user_data.get("total_accumulated_time", 0)
        if total_count > 0 or (total_time and total_time > 0):
            has_data = True

        row = [user_data["user_id"], user_data.get("nickname", "未知用户")]
        for act in activity_limits.keys():
            activity_info = user_data.get("activities", {}).get(act, {})
            count = activity_info.get("count", 0)
            total_seconds = int(activity_info.get("time", 0))
            time_str = MessageFormatter.format_time_for_csv(total_seconds)
            row.append(count)
            row.append(time_str)

        total_seconds_all = int(user_data.get("total_accumulated_time", 0) or 0)
        total_time_str = MessageFormatter.format_time_for_csv(total_seconds_all)

        overtime_seconds = int(user_data.get("total_overtime_time", 0) or 0)
        overtime_str = MessageFormatter.format_time_for_csv(overtime_seconds)

        row.extend(
            [
                total_count,
                total_time_str,
                user_data.get("total_fines", 0),
                user_data.get("overtime_count", 0),
                overtime_str,
            ]
        )
        writer.writerow(row)

    if not has_data:
        await bot.send_message(chat_id, "⚠️ 当前群组没有数据需要导出")
        return

    csv_content = csv_buffer.getvalue()
    csv_buffer.close()

    temp_file = f"temp_{file_name}"
    try:
        async with aiofiles.open(temp_file, "w", encoding="utf-8-sig") as f:
            await f.write(csv_content)

        chat_title = str(chat_id)
        try:
            chat_info = await bot.get_chat(chat_id)
            chat_title = chat_info.title or chat_title
        except:
            pass

        caption = (
            f"📊 群组：<b>{chat_title}</b>\n"
            f"📅 统计日期：<code>{(target_date.strftime('%Y-%m-%d') if target_date else get_beijing_time().strftime('%Y-%m-%d'))}</code>\n"
            f"⏰ 导出时间：<code>{get_beijing_time().strftime('%Y-%m-%d %H:%M:%S')}</code>"
        )

        # 先把文件发回到当前 chat（可选）
        try:
            csv_input_file = FSInputFile(temp_file, filename=file_name)
            await bot.send_document(
                chat_id, csv_input_file, caption=caption, parse_mode="HTML"
            )
        except Exception as e:
            logger.warning(f"发送到当前聊天失败: {e}")

        # 使用统一的 NotificationService 推送到绑定的频道/群组/管理员
        await NotificationService.send_document(
            chat_id, FSInputFile(temp_file, filename=file_name), caption=caption
        )

        logger.info(f"✅ 数据导出并推送完成: {file_name}")

    except Exception as e:
        logger.error(f"❌ 导出过程出错: {e}")
        await bot.send_message(chat_id, f"❌ 导出失败：{e}")
    finally:
        try:
            if os.path.exists(temp_file):
                os.remove(temp_file)
        except:
            pass


async def export_monthly_csv(
    chat_id: int,
    year: int = None,
    month: int = None,
    to_admin_if_no_group: bool = True,
    file_name: str = None,
):
    """导出月度数据为 CSV 并推送 - 优化版本"""
    if year is None or month is None:
        today = get_beijing_time()
        year = today.year
        month = today.month

    if not file_name:
        file_name = f"group_{chat_id}_monthly_{year:04d}{month:02d}.csv"

    # 使用优化版导出
    csv_content = await optimized_monthly_export(chat_id, year, month)

    if not csv_content:
        await bot.send_message(chat_id, f"⚠️ {year}年{month}月没有数据需要导出")
        return

    temp_file = f"temp_{file_name}"
    try:
        async with aiofiles.open(temp_file, "w", encoding="utf-8-sig") as f:
            await f.write(csv_content)

        chat_title = str(chat_id)
        try:
            chat_info = await bot.get_chat(chat_id)
            chat_title = chat_info.title or chat_title
        except:
            pass

        caption = (
            f"📊 月度数据导出\n"
            f"🏢 群组：<code>{chat_title}</code>\n"
            f"📅 统计月份：<code>{year}年{month}月</code>\n"
            f"⏰ 导出时间：<code>{get_beijing_time().strftime('%Y-%m-%d %H:%M:%S')}</code>\n"
            f"{MessageFormatter.create_dashed_line()}\n"
            f"💾 包含每个用户的月度活动统计"
        )

        try:
            csv_input_file = FSInputFile(temp_file, filename=file_name)
            await bot.send_document(
                chat_id, csv_input_file, caption=caption, parse_mode="HTML"
            )
        except Exception as e:
            logger.error(f"❌ 发送到当前聊天失败: {e}")

        await NotificationService.send_document(
            chat_id, FSInputFile(temp_file, filename=file_name), caption
        )

        logger.info(f"✅ 月度数据导出并推送完成: {file_name}")

    except Exception as e:
        logger.error(f"❌ 月度导出过程出错: {e}")
        await bot.send_message(chat_id, f"❌ 月度导出失败：{e}")
    finally:
        try:
            if os.path.exists(temp_file):
                os.remove(temp_file)
        except:
            pass


async def generate_monthly_report(chat_id: int, year: int = None, month: int = None):
    """生成月度报告 - 基于新的月度统计表"""
    if year is None or month is None:
        today = get_beijing_time()
        year = today.year
        month = today.month

    # 🆕 使用新的月度统计方法（基于 monthly_statistics 表）
    monthly_stats = await db.get_monthly_statistics(chat_id, year, month)
    work_stats = await db.get_monthly_work_statistics(chat_id, year, month)
    activity_ranking = await db.get_monthly_activity_ranking(chat_id, year, month)

    if not monthly_stats and not work_stats:
        return None

    chat_title = str(chat_id)
    try:
        chat_info = await bot.get_chat(chat_id)
        chat_title = chat_info.title or chat_title
    except:
        pass

    # 生成报告文本
    report = (
        f"📊 <b>{year}年{month}月打卡统计报告</b>\n"
        f"🏢 群组：<code>{chat_title}</code>\n"
        f"📅 生成时间：<code>{get_beijing_time().strftime('%Y-%m-%d %H:%M:%S')}</code>\n"
        f"{MessageFormatter.create_dashed_line()}\n"
    )

    # 总体统计
    total_users = len(monthly_stats)
    total_activity_time = sum(stat.get("total_time", 0) for stat in monthly_stats)
    total_activity_count = sum(stat.get("total_count", 0) for stat in monthly_stats)
    total_fines = sum(stat.get("total_fines", 0) for stat in monthly_stats)

    # 🆕 新增：工作天数和工作时长统计
    total_work_days = sum(stat.get("work_days", 0) for stat in monthly_stats)
    total_work_hours = sum(stat.get("work_hours", 0) for stat in monthly_stats)

    report += (
        f"👥 <b>总体统计</b>\n"
        f"• 活跃用户：<code>{total_users}</code> 人\n"
        f"• 总活动时长：<code>{MessageFormatter.format_time(int(total_activity_time))}</code>\n"
        f"• 总活动次数：<code>{total_activity_count}</code> 次\n"
        f"• 总工作天数：<code>{total_work_days}</code> 天\n"
        f"• 总工作时长：<code>{MessageFormatter.format_time(int(total_work_hours))}</code>\n"
        f"• 总罚款金额：<code>{total_fines}</code> 元\n\n"
    )

    # 上下班统计
    total_work_start = sum(stat.get("work_start_count", 0) for stat in work_stats)
    total_work_end = sum(stat.get("work_end_count", 0) for stat in work_stats)
    total_work_fines = sum(
        stat.get("work_start_fines", 0) + stat.get("work_end_fines", 0)
        for stat in work_stats
    )

    if total_work_start > 0 or total_work_end > 0:
        report += (
            f"🕒 <b>上下班统计</b>\n"
            f"• 上班打卡：<code>{total_work_start}</code> 次\n"
            f"• 下班打卡：<code>{total_work_end}</code> 次\n"
            f"• 上下班罚款：<code>{total_work_fines}</code> 元\n\n"
        )

    # 🆕 新增：个人工作统计排行
    if monthly_stats:
        report += f"👤 <b>个人工作统计</b>\n"

        # 按工作时长排行
        work_hours_ranking = sorted(
            [stat for stat in monthly_stats if stat.get("work_hours", 0) > 0],
            key=lambda x: x.get("work_hours", 0),
            reverse=True,
        )[:5]

        for i, stat in enumerate(work_hours_ranking, 1):
            work_hours_str = MessageFormatter.format_time(
                int(stat.get("work_hours", 0))
            )
            work_days = stat.get("work_days", 0)
            nickname = stat.get("nickname", f"用户{stat.get('user_id')}")
            report += (
                f"  <code>{i}.</code> {nickname} - {work_hours_str} ({work_days}天)\n"
            )
        report += "\n"

    # 活动排行榜
    report += f"🏆 <b>月度活动排行榜</b>\n"
    has_activity_data = False

    for activity, ranking in activity_ranking.items():
        if ranking:
            has_activity_data = True
            report += f"📈 <code>{activity}</code>：\n"
            for i, user in enumerate(ranking[:3], 1):
                time_str = MessageFormatter.format_time(int(user.get("total_time", 0)))
                count = user.get("total_count", 0)
                nickname = user.get("nickname", "未知用户")
                report += f"  <code>{i}.</code> {nickname} - {time_str} ({count}次)\n"
            report += "\n"

    if not has_activity_data:
        report += "暂无活动数据\n\n"

    # 🆕 新增：月度总结
    report += f"📈 <b>月度总结</b>\n"

    if total_activity_count > 0:
        avg_activity_time = (
            total_activity_time / total_activity_count
            if total_activity_count > 0
            else 0
        )
        report += f"• 平均每次活动时长：<code>{MessageFormatter.format_time(int(avg_activity_time))}</code>\n"

    if total_work_days > 0:
        avg_work_hours_per_day = (
            total_work_hours / total_work_days if total_work_days > 0 else 0
        )
        report += f"• 平均每日工作时长：<code>{MessageFormatter.format_time(int(avg_work_hours_per_day))}</code>\n"

    if total_users > 0:
        avg_activity_per_user = (
            total_activity_count / total_users if total_users > 0 else 0
        )
        report += f"• 人均活动次数：<code>{avg_activity_per_user:.1f}</code> 次\n"

        avg_work_days_per_user = total_work_days / total_users if total_users > 0 else 0
        report += f"• 人均工作天数：<code>{avg_work_days_per_user:.1f}</code> 天\n"

    # 🆕 新增：数据来源说明
    report += f"\n{MessageFormatter.create_dashed_line()}\n"
    report += f"💡 <i>注：本报告基于月度统计表生成，不受日常重置操作影响</i>"

    return report


# ==================== 系统维护功能优化 ====================
async def export_data_before_reset(chat_id: int):
    """在重置前自动导出CSV数据 - 优化版本"""
    try:
        # 先检查是否有数据需要导出
        group_stats = await db.get_group_statistics(chat_id)
        has_data = False

        if group_stats:
            for user_data in group_stats:
                total_count = user_data.get("total_activity_count", 0)
                total_time = user_data.get("total_accumulated_time", 0)
                if total_count > 0 or total_time > 0:
                    has_data = True
                    break

        if not has_data:
            logger.info(f"⚠️ 群组 {chat_id} 没有数据需要导出，跳过自动导出")
            return

        date_str = get_beijing_time().strftime("%Y%m%d")
        file_name = f"group_{chat_id}_statistics_{date_str}.csv"
        today_date = get_beijing_time().date()
        await export_and_push_csv(
            chat_id,
            to_admin_if_no_group=True,
            file_name=file_name,
            target_date=today_date,
        )
        logger.info(f"✅ 群组 {chat_id} 的每日数据已自动导出并推送")
    except Exception as e:
        logger.error(f"❌ 自动导出数据失败：{e}")


# ==================== 自动导出与每日重置任务（最终整合版） ====================


async def auto_daily_export_task():
    """
    每日重置前自动导出群组数据（重置前 1 分钟导出）
    """
    while True:
        now = get_beijing_time()
        logger.info(f"🕒 自动导出任务运行中，当前时间: {now}")

        try:
            # 获取群组列表
            all_groups = await asyncio.wait_for(db.get_all_groups(), timeout=15)
            if not all_groups:
                logger.warning("⚠️ 未获取到任何群组，10秒后重试。")
                await asyncio.sleep(10)
                continue
        except asyncio.TimeoutError:
            logger.error("⏰ 数据库查询超时（get_all_groups），将在30秒后重试。")
            await asyncio.sleep(30)
            continue
        except Exception as e:
            logger.error(f"❌ 获取群组列表失败: {e}")
            await asyncio.sleep(30)
            continue

        export_executed = False

        for chat_id in all_groups:
            try:
                group_data = await asyncio.wait_for(
                    db.get_group_cached(chat_id), timeout=10
                )
                if not group_data:
                    continue

                reset_hour = group_data.get("reset_hour", Config.DAILY_RESET_HOUR)
                reset_minute = group_data.get("reset_minute", Config.DAILY_RESET_MINUTE)

                # 计算目标时间（重置前1分钟）
                target_time = (reset_hour * 60 + reset_minute - 1) % (24 * 60)
                now_minutes = now.hour * 60 + now.minute

                if now_minutes == target_time:
                    logger.info(f"📤 到达重置前导出时间，导出群组 {chat_id} 数据中...")

                    file_name = (
                        f"group_{chat_id}_pre_reset_{now.strftime('%Y%m%d')}.csv"
                    )
                    await asyncio.wait_for(
                        export_and_push_csv(
                            chat_id, to_admin_if_no_group=True, file_name=file_name
                        ),
                        timeout=30,
                    )

                    logger.info(f"✅ 群组 {chat_id} 导出成功（重置前）")
                    export_executed = True

            except asyncio.TimeoutError:
                logger.warning(f"⏰ 群组 {chat_id} 导出或查询超时，跳过此群。")
            except Exception as e:
                logger.error(f"❌ 自动导出失败，群组 {chat_id}: {e}")

        # 导出完成后稍长休眠，未导出则快速循环
        sleep_time = 120 if export_executed else 60
        logger.info(f"🕐 导出循环结束，休眠 {sleep_time}s ...")
        await asyncio.sleep(sleep_time)



last_reset_record = {} 

async def daily_reset_task():
    """
    每日自动重置任务 - 终极稳定版
    """
    while True:
        now = get_beijing_time()
        # 将当前日期转为字符串，用于标记
        today_str = now.strftime("%Y-%m-%d") 
        
        try:
            all_groups = await asyncio.wait_for(db.get_all_groups(), timeout=15)
        except Exception as e:
            logger.error(f"❌ 获取群组列表失败: {e}")
            await asyncio.sleep(60)
            continue

        for chat_id in all_groups:
            try:
                # 检查此群组今天是否已经重置过了
                if last_reset_record.get(chat_id) == today_str:
                    continue

                group_data = await db.get_group_cached(chat_id)
                if not group_data: continue

                reset_hour = group_data.get("reset_hour", Config.DAILY_RESET_HOUR)
                reset_minute = group_data.get("reset_minute", Config.DAILY_RESET_MINUTE)

                # 判断时间
                if now.hour == reset_hour and now.minute == reset_minute:
                    # 💡 标记已重置，防止同一分钟重复执行
                    last_reset_record[chat_id] = today_str 
                    
                    logger.info(f"⏰ 到达重置时间，正在重置群组 {chat_id}...")

                    # 这里的计算逻辑使用你提供的“业务日期”方案，非常棒
                    business_today = await db.get_business_date(chat_id)
                    reset_target_date = business_today - timedelta(days=1)

                    # 💡 优化：对于成员很多的群组，使用 asyncio.gather 并行重置，提高效率
                    group_members = await db.get_group_members(chat_id)
                    
                    # 定义一个内部重置函数，方便加锁执行
                    async def reset_single_user(uid):
                        user_lock = get_user_lock(chat_id, uid)
                        async with user_lock:
                            await db.reset_user_daily_data(chat_id, uid, reset_target_date)

                    # 执行重置（建议控制并发数，防止数据库压力过大）
                    tasks = [reset_single_user(u["user_id"]) for u in group_members]
                    await asyncio.gather(*tasks)

                    logger.info(f"✅ 群组 {chat_id} 重置完成，目标周期: {reset_target_date}")
                    asyncio.create_task(delayed_export(chat_id, 30))

            except Exception as e:
                logger.error(f"❌ 群组 {chat_id} 处理出错: {e}")

        # 保持 60 秒检查一次
        await asyncio.sleep(60)

async def delayed_export(chat_id: int, delay_minutes: int = 30):
    """
    在每日重置后延迟导出昨日数据 - 修复版
    """
    try:
        logger.info(f"⏳ 群组 {chat_id} 将在 {delay_minutes} 分钟后导出昨日数据...")
        # 延迟执行
        await asyncio.sleep(delay_minutes * 60)

        # 🆕 关键修复：明确获取昨天的日期
        yesterday_dt = get_beijing_time() - timedelta(days=1)
        yesterday_date = yesterday_dt.date()

        # 生成文件名（用昨日日期）
        file_name = f"group_{chat_id}_statistics_{yesterday_dt.strftime('%Y%m%d')}.csv"

        # ✅ 关键修改：传入 target_date=yesterday_date
        await export_and_push_csv(
            chat_id,
            to_admin_if_no_group=True,
            file_name=file_name,
            target_date=yesterday_date,  # 明确传递昨天日期
        )

        logger.info(f"✅ 群组 {chat_id} 昨日({yesterday_date}) 数据导出并推送完成")

    except asyncio.TimeoutError:
        logger.warning(f"⏰ 群组 {chat_id} 延迟导出超时")
    except Exception as e:
        logger.error(f"❌ 群组 {chat_id} 延迟导出昨日数据失败: {e}", exc_info=True)


# ==================== 活动状态恢复功能 ====================
async def restore_activity_timers():
    """启动时恢复所有进行中的活动定时器"""
    logger.info("🔄 恢复进行中的活动定时器...")

    try:
        # 获取所有有进行中活动的用户
        conn = await db.get_connection()
        try:
            rows = await conn.fetch(
                "SELECT chat_id, user_id, current_activity, activity_start_time, nickname FROM users WHERE current_activity IS NOT NULL AND activity_start_time IS NOT NULL"
            )
        finally:
            await db.release_connection(conn)

        restored_count = 0
        expired_count = 0

        for row in rows:
            chat_id = row["chat_id"]
            user_id = row["user_id"]
            activity = row["current_activity"]
            start_time_str = row["activity_start_time"]
            nickname = row["nickname"] or str(user_id)

            try:
                # 计算已过去的时间
                start_time = datetime.fromisoformat(start_time_str)
                now = get_beijing_time()
                elapsed = (now - start_time).total_seconds()

                # 获取活动时间限制
                time_limit = await db.get_activity_time_limit(activity)
                time_limit_seconds = time_limit * 60
                remaining_time = time_limit_seconds - elapsed

                if remaining_time > 60:  # 剩余时间大于1分钟才恢复
                    # 还有剩余时间，恢复定时器
                    await timer_manager.start_timer(
                        chat_id, user_id, activity, time_limit
                    )  # 🆕 直接调用

                    logger.info(
                        f"✅ 恢复定时器: 用户{user_id}({nickname}) 活动{activity} 剩余{remaining_time/60:.1f}分钟"
                    )
                    restored_count += 1

                else:
                    # 剩余时间不足或已超时，自动结束活动
                    await handle_expired_activity(
                        chat_id, user_id, activity, start_time, nickname
                    )
                    expired_count += 1

            except Exception as e:
                logger.error(f"❌ 恢复用户{user_id}活动失败: {e}")

        logger.info(
            f"📊 定时器恢复完成: {restored_count}个活动已恢复, {expired_count}个活动已自动结束"
        )

    except Exception as e:
        logger.error(f"❌ 恢复活动定时器失败: {e}")


async def handle_expired_activity(
    chat_id: int, user_id: int, activity: str, start_time: datetime, nickname: str
):
    """处理已过期的活动"""
    try:
        now = get_beijing_time()
        elapsed = (now - start_time).total_seconds()

        # 计算超时和罚款
        time_limit_seconds = await db.get_activity_time_limit(activity) * 60
        overtime_seconds = max(0, int(elapsed - time_limit_seconds))
        overtime_minutes = overtime_seconds / 60

        fine_amount = 0
        if overtime_seconds > 0:
            fine_amount = await calculate_fine(activity, overtime_minutes)

        # 自动完成活动
        await db.complete_user_activity(
            chat_id, user_id, activity, int(elapsed), fine_amount, True
        )

        # 发送超时通知
        timeout_msg = (
            f"🔄 <b>系统恢复通知</b>\n"
            f"👤 用户：{MessageFormatter.format_user_link(user_id, nickname)}\n"
            f"📝 检测到未结束的活动：<code>{activity}</code>\n"
            f"⚠️ 由于服务重启，您的活动已自动结束\n"
            f"⏱️ 活动总时长：<code>{MessageFormatter.format_time(int(elapsed))}</code>"
        )

        if overtime_seconds > 0:
            timeout_msg += f"\n⏰ 超时时长：<code>{MessageFormatter.format_time(int(overtime_seconds))}</code>"
            if fine_amount > 0:
                timeout_msg += f"\n💰 超时罚款：<code>{fine_amount}</code> 元"

        await bot.send_message(chat_id, timeout_msg, parse_mode="HTML")
        logger.info(
            f"✅ 自动结束过期活动: 用户{user_id}({nickname}) 活动{activity} 时长{elapsed:.0f}秒"
        )

    except Exception as e:
        logger.error(f"❌ 处理过期活动失败 用户{user_id}: {e}")


# ==================== 月度报告任务优化 ====================
async def process_monthly_export_for_group(chat_id: int, year: int, month: int):
    """处理单个群组的月度导出 - 优化版本"""
    try:
        # 1. 生成CSV数据（使用优化版）
        csv_content = await optimized_monthly_export(chat_id, year, month)

        if not csv_content:
            logger.info(f"⚠️ 群组 {chat_id} 没有 {year}年{month}月的数据")
            return

        # 2. 保存临时文件
        file_name = f"monthly_report_{chat_id}_{year:04d}{month:02d}.csv"
        temp_file = f"temp_{file_name}"

        try:
            async with aiofiles.open(temp_file, "w", encoding="utf-8-sig") as f:
                await f.write(csv_content)

            # 3. 推送文件
            chat_title = await get_chat_title(chat_id)
            caption = (
                f"📊 月度打卡统计报告\n"
                f"🏢 群组：<code>{chat_title}</code>\n"
                f"📅 统计月份：<code>{year}年{month}月</code>\n"
                f"⏰ 生成时间：<code>{get_beijing_time().strftime('%Y-%m-%d %H:%M:%S')}</code>"
            )

            # 使用推送服务发送
            await NotificationService.send_document(
                chat_id, FSInputFile(temp_file, filename=file_name), caption
            )

            logger.info(f"✅ 群组 {chat_id} 月度报告推送完成")

        finally:
            # 清理临时文件
            try:
                if os.path.exists(temp_file):
                    os.remove(temp_file)
            except:
                pass

    except Exception as e:
        logger.error(f"❌ 处理群组 {chat_id} 月度导出失败: {e}")


async def efficient_monthly_export_task():
    """高效的月度数据导出任务 - 优化版本"""
    while True:
        now = get_beijing_time()

        # 每月1号上午10点执行（避开高峰期）
        if now.day == 1 and now.hour == 10 and now.minute == 0:
            last_month = now.month - 1 if now.month > 1 else 12
            last_year = now.year if now.month > 1 else now.year - 1

            logger.info(f"📊 开始执行月度数据导出: {last_year}年{last_month}月")

            all_groups = await db.get_all_groups()

            for chat_id in all_groups:
                try:
                    if not performance_optimizer.memory_usage_ok():
                        logger.warning(f"⚠️ 内存使用较高，跳过群组 {chat_id} 的月度导出")
                        continue

                    # 生成并推送月度报告
                    await process_monthly_export_for_group(
                        chat_id, last_year, last_month
                    )

                    # 每组处理完后休息一下，避免资源紧张
                    await asyncio.sleep(10)

                except Exception as e:
                    logger.error(f"❌ 群组 {chat_id} 月度导出失败: {e}")

            # 执行数据清理
            try:
                await db.manage_monthly_data()
                logger.info("✅ 月度数据管理完成")
            except Exception as e:
                logger.error(f"❌ 月度数据管理失败: {e}")

            # 等待24小时避免重复执行
            await asyncio.sleep(24 * 60 * 60)
        else:
            await asyncio.sleep(60)  # 每分钟检查一次


async def monthly_report_task():
    """月度报告推送任务 - 优化版本"""
    while True:
        now = get_beijing_time()
        logger.info(f"📅 月度报告任务检查，当前时间: {now}")

        # 每月1号上午9点推送上月报告
        if now.day == 1 and now.hour == 9 and now.minute == 0:
            last_month = now.month - 1 if now.month > 1 else 12
            last_year = now.year if now.month > 1 else now.year - 1

            logger.info(f"📊 开始生成 {last_year}年{last_month}月月度报告...")

            all_groups = await db.get_all_groups()
            for chat_id in all_groups:
                try:
                    # 生成月度报告
                    report = await generate_monthly_report(
                        chat_id, last_year, last_month
                    )
                    if report:
                        # 发送报告
                        await bot.send_message(chat_id, report, parse_mode="HTML")
                        logger.info(
                            f"✅ 已发送 {last_year}年{last_month}月报告到群组 {chat_id}"
                        )

                        # 导出CSV文件
                        await export_monthly_csv(chat_id, last_year, last_month)
                        logger.info(
                            f"✅ 已导出 {last_year}年{last_month}月数据到群组 {chat_id}"
                        )
                    else:
                        logger.info(
                            f"⚠️ 群组 {chat_id} 没有 {last_year}年{last_month}月的数据"
                        )

                except Exception as e:
                    logger.error(f"❌ 群组 {chat_id} 月度报告生成失败: {e}")

            # 等待24小时，避免重复执行
            await asyncio.sleep(24 * 60 * 60)
        else:
            # 每分钟检查一次
            await asyncio.sleep(60)


# ==================== 内存清理任务优化 ====================
async def memory_cleanup_task():
    """定期内存清理任务 - 安全且优化版"""
    while True:
        try:
            await asyncio.sleep(Config.CLEANUP_INTERVAL)

            # 1️⃣ 用户锁清理
            await user_lock_manager.force_cleanup()

            # 2️⃣ 内存优化
            await performance_optimizer.memory_cleanup()

            # 3️⃣ 数据库安全清理
            success = await db.safe_cleanup_old_data(30)
            # 🆕 添加定时器清理
            await timer_manager.cleanup_finished_timers()
            if not success:
                logger.warning("⚠️ 数据库清理未执行，但不影响主要功能")

            logger.debug("🧹 定期内存清理任务完成")

        except Exception as e:
            logger.error(f"❌ 内存清理任务失败: {e}")
            await asyncio.sleep(300)


async def health_monitoring_task():
    """健康监控任务 - 优化版本"""
    while True:
        try:
            # 检查内存使用
            if not performance_optimizer.memory_usage_ok():
                logger.warning("⚠️ 内存使用过高，执行紧急清理")
                await performance_optimizer.memory_cleanup()

            # 检查任务数量
            timer_stats = timer_manager.get_stats()
            if timer_stats["active_timers"] > 1000:
                logger.warning(f"⚠️ 活动任务数量过多: {timer_stats['active_timers']}")
                await performance_optimizer.memory_cleanup()

            await asyncio.sleep(60)
        except Exception as e:
            logger.error(f"❌ 健康监控任务失败: {e}")
            await asyncio.sleep(60)


# ==================== 辅助函数优化 ====================
async def get_chat_title(chat_id: int) -> str:
    """获取群组标题 - 优化版本"""
    try:
        chat_info = await bot.get_chat(chat_id)
        return chat_info.title or str(chat_id)
    except Exception:
        return str(chat_id)


# ==================== Render检查接口优化 ====================
async def enhanced_health_check(request):
    """增强版健康检查接口 - 包含心跳状态"""
    try:
        # 检查数据库连接
        db_stats = await db.get_database_stats()

        # 检查心跳状态
        heartbeat_status = heartbeat_manager.get_status()

        # 检查内存使用
        memory_ok = performance_optimizer.memory_usage_ok()

        lock_stats = user_lock_manager.get_stats()

        # 🆕 添加定时器状态
        timer_stats = timer_manager.get_stats()

        # 获取基本状态
        status = "healthy" if memory_ok else "degraded"

        return web.json_response(
            {
                "status": status,
                "timestamp": get_beijing_time().isoformat(),
                "bot_status": "running",
                "memory_ok": memory_ok,
                "database": db_stats,
                "heartbeat": heartbeat_status,
                "user_locks": lock_stats,
                "activity_timers": timer_stats,
                "active_tasks": timer_manager.get_stats()["active_timers"],
                "system": {
                    "python_version": sys.version,
                    "platform": sys.platform,
                    "uptime": (
                        time.time() - start_time if "start_time" in globals() else 0
                    ),
                },
            }
        )
    except Exception as e:
        logger.error(f"❌ 健康检查失败: {e}")
        return web.json_response(
            {
                "status": "unhealthy",
                "error": str(e),
                "timestamp": get_beijing_time().isoformat(),
            },
            status=500,
        )


async def start_web_server():
    """启动轻量HTTP健康检测服务 - 修复端口绑定版本"""
    try:
        app = web.Application()

        # 添加多个健康检查端点
        app.router.add_get("/", enhanced_health_check)
        app.router.add_get("/health", enhanced_health_check)
        app.router.add_get("/status", enhanced_health_check)
        app.router.add_get("/ping", lambda request: web.Response(text="pong"))
        app.router.add_get("/metrics", metrics_endpoint)
        app.router.add_get("/detailed-status", detailed_status_check)

        runner = web.AppRunner(app)
        await runner.setup()

        # 修复：使用 Render 提供的 PORT 环境变量
        port = int(os.environ.get("PORT", Config.WEB_SERVER_CONFIG["PORT"]))
        host = "0.0.0.0"  # 必须绑定到 0.0.0.0

        site = web.TCPSite(runner, host, port)
        await site.start()
        logger.info(f"🌐 Web server started on {host}:{port}")

        # 返回站点信息以便后续管理
        return site
    except Exception as e:
        logger.error(f"❌ Web server failed: {e}")
        raise


async def get_active_users_count() -> int:
    """获取活跃用户数量（今日有活动的用户）"""
    try:
        today = datetime.now(beijing_tz).date()
        conn = await db.get_connection()
        try:
            result = await conn.fetchval(
                "SELECT COUNT(DISTINCT user_id) FROM users WHERE last_updated = $1",
                today,
            )
            return result or 0
        finally:
            await db.release_connection(conn)
    except Exception as e:
        logger.error(f"获取活跃用户数失败: {e}")
        return 0


async def metrics_endpoint(request):
    """Prometheus格式指标端点"""
    try:
        # 获取活跃用户数（需要先定义 active_users）
        active_users_count = await get_active_users_count()

        # 获取内存使用（字节）
        memory_bytes = psutil.Process().memory_info().rss

        # 获取数据库连接数
        db_connections = 0
        if db.pool:
            try:
                # asyncpg 连接池统计
                db_connections = db.pool.get_size()
            except Exception as e:
                logger.warning(f"获取数据库连接数失败: {e}")

        # 获取其他性能指标
        timer_stats = timer_manager.get_stats()
        task_count = timer_stats["active_timers"]
        cache_stats = global_cache.get_stats()

        # Prometheus格式指标
        metrics = [
            "# HELP bot_active_users 活跃用户数量",
            "# TYPE bot_active_users gauge",
            f"bot_active_users {active_users_count}",
            "# HELP bot_memory_usage_bytes 内存使用量（字节）",
            "# TYPE bot_memory_usage_bytes gauge",
            f"bot_memory_usage_bytes {memory_bytes}",
            "# HELP bot_db_connections 数据库连接数",
            "# TYPE bot_db_connections gauge",
            f"bot_db_connections {db_connections}",
            "# HELP bot_active_tasks 活跃任务数量",
            "# TYPE bot_active_tasks gauge",
            f"bot_active_tasks {task_count}",
            "# HELP bot_cache_hits 缓存命中次数",
            "# TYPE bot_cache_hits counter",
            f"bot_cache_hits {cache_stats['hits']}",
            "# HELP bot_cache_misses 缓存未命中次数",
            "# TYPE bot_cache_misses counter",
            f"bot_cache_misses {cache_stats['misses']}",
            "# HELP bot_uptime_seconds 运行时间（秒）",
            "# TYPE bot_uptime_seconds gauge",
            f"bot_uptime_seconds {int(time.time() - start_time)}",
        ]

        return web.Response(text="\n".join(metrics), content_type="text/plain")

    except Exception as e:
        logger.error(f"❌ 指标端点错误: {e}")
        return web.Response(text=f"error: {e}", status=500)


async def detailed_status_check(request):
    """详细状态检查端点"""
    try:
        # 收集各种状态信息
        status_info = {
            "status": "healthy",
            "timestamp": get_beijing_time().isoformat(),
            "bot": {
                "active_tasks": timer_manager.get_stats()["active_timers"],
                "user_locks_count": len(user_locks),
                "memory_usage_ok": performance_optimizer.memory_usage_ok(),
            },
            "database": await db.get_database_stats(),
            "heartbeat": heartbeat_manager.get_status(),
            "system": {
                "python_version": sys.version,
                "platform": sys.platform,
                "current_time": get_beijing_time().isoformat(),
            },
        }

        return web.json_response(status_info)
    except Exception as e:
        return web.json_response({"error": str(e)}, status=500)


# ==================== 启动流程优化 ====================
async def on_startup():
    """启动时执行 - 优化版本"""
    logger.info("🤖 机器人启动中...")
    await bot.delete_webhook(drop_pending_updates=True)
    # 初始化异步数据库
    await db.initialize()
    logger.info("✅ Webhook 已删除，使用 polling 模式")


async def on_shutdown():
    """关闭时执行 - 优化版本"""
    logger.info("🛑 机器人正在关闭...")

    await timer_manager.cancel_all_timers()

    logger.info("✅ 清理完成")


def check_environment():
    """检查环境配置 - 优化版本"""
    if not Config.TOKEN:
        logger.error("❌ BOT_TOKEN 未设置")
        return False
    return True


# ==================== Webhook 设置函数 ====================
async def setup_webhook():
    """配置Webhook - 带洪水控制保护"""
    if not Config.should_use_webhook():
        # 明确使用Polling模式，清理Webhook
        try:
            await bot.delete_webhook(drop_pending_updates=True)
            logger.info("✅ 已删除Webhook，使用Polling模式")
        except Exception as e:
            logger.warning(f"⚠️ 删除Webhook失败: {e}")
        return False

    if not Config.WEBHOOK_URL:
        logger.error("❌ Webhook模式已启用，但WEBHOOK_URL未设置，将使用Polling模式")
        return False

    try:
        # 修复URL格式
        base_url = Config.WEBHOOK_URL.rstrip("/")
        webhook_url = f"{base_url}/webhook"

        # 先检查当前Webhook状态，避免不必要的设置
        current_webhook = await bot.get_webhook_info()

        if current_webhook.url == webhook_url:
            logger.info(f"✅ Webhook已正确设置: {webhook_url}")
            return True

        logger.info(f"🔗 设置Webhook: {webhook_url}")

        # 先删除旧Webhook
        await bot.delete_webhook(drop_pending_updates=True)
        await asyncio.sleep(2)  # 等待2秒避免洪水限制

        # 设置新Webhook
        await bot.set_webhook(
            url=webhook_url,
            drop_pending_updates=True,
            allowed_updates=["message", "callback_query"],
        )

        # 验证设置
        await asyncio.sleep(1)
        new_webhook = await bot.get_webhook_info()

        if new_webhook.url == webhook_url:
            logger.info(f"✅ Webhook设置成功: {webhook_url}")
            logger.info(f"📊 待处理更新: {new_webhook.pending_update_count}")
            return True
        else:
            logger.error(f"❌ Webhook设置验证失败")
            return False

    except Exception as e:
        logger.error(f"❌ Webhook设置失败: {e}")

        # 如果是洪水限制，等待后重试一次
        if "Flood control" in str(e) or "Too Many Requests" in str(e):
            logger.warning("⚠️ 遇到洪水限制，等待10秒后重试...")
            await asyncio.sleep(10)

            try:
                await bot.delete_webhook(drop_pending_updates=True)
                await asyncio.sleep(2)
                await bot.set_webhook(url=webhook_url, drop_pending_updates=True)
                logger.info("✅ 重试Webhook设置成功")
                return True
            except Exception as retry_error:
                logger.error(f"❌ Webhook重试失败: {retry_error}")

        return False


async def optimized_on_startup():
    """优化版启动流程 - 修复洪水控制问题"""
    logger.info("🤖 机器人启动中...")

    max_retries = 2  # 减少重试次数
    for attempt in range(max_retries):
        try:
            # 并行执行启动任务（除了Webhook）
            startup_tasks = [
                db.initialize(),
                preload_frequent_data(),
                heartbeat_manager.initialize(),
            ]

            results = await asyncio.gather(*startup_tasks, return_exceptions=True)

            # 检查是否有失败的任务
            failed_tasks = [r for r in results if isinstance(r, Exception)]
            if failed_tasks:
                raise Exception(f"启动任务失败: {failed_tasks}")

            # 设置Webhook（如果启用）- 单独处理以避免影响其他启动任务
            webhook_success = await setup_webhook()

            if Config.should_use_webhook() and not webhook_success:
                logger.warning("⚠️ Webhook设置失败，应用将在Polling模式下运行")
                # 更新配置以使用Polling
                Config.BOT_MODE = "polling"
                # 确保删除Webhook
                try:
                    await bot.delete_webhook(drop_pending_updates=True)
                except:
                    pass

            logger.info("✅ 优化启动完成")
            return

        except Exception as e:
            logger.warning(f"⚠️ 启动第 {attempt + 1} 次失败: {e}")
            if attempt == max_retries - 1:
                logger.error(f"❌ 启动重试{max_retries}次后失败")
                raise
            await asyncio.sleep(2**attempt)


async def optimized_on_shutdown():
    """优化版关闭流程"""
    logger.info("🛑 机器人正在关闭...")

    try:
        # 并行清理任务
        cleanup_tasks = [
            performance_optimizer.memory_cleanup(),
            db.cleanup_cache(),
            heartbeat_manager.stop(),  # 停止心跳管理器
        ]

        # 取消所有活动任务
        await timer_manager.cancel_all_timers()
        await asyncio.gather(*cleanup_tasks, return_exceptions=True)

        logger.info("✅ 优化清理完成")
    except Exception as e:
        logger.error(f"❌ 关闭过程中出错: {e}")


# ========== 主启动函数优化 ==========

logger = logging.getLogger("GroupCheckInBot")


# =======================
# Render 保活 HTTP 服务
# =======================
async def health_check(request):
    return web.json_response({"status": "ok", "timestamp": time.time()})


async def start_health_server():
    """Render 保活端口监听"""
    app = web.Application()
    app.router.add_get("/health", health_check)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host="0.0.0.0", port=Config.WEB_SERVER_CONFIG["PORT"])
    await site.start()
    logger.info(
        f"🌐 Health check server running on port {Config.WEB_SERVER_CONFIG['PORT']}"
    )


# =======================
# 主程序启动逻辑
# =======================
async def optimized_main():
    """优化版主启动函数 - Render 修复版本"""
    if not check_environment():
        sys.exit(1)

    try:
        await optimized_on_startup()

        # 🚀 Render 需要一个端口监听 —— 启动保活服务
        asyncio.create_task(start_health_server())

        # 启动后台任务
        critical_tasks = [
            asyncio.create_task(memory_cleanup_task()),
            asyncio.create_task(health_monitoring_task()),
            asyncio.create_task(heartbeat_manager.start_heartbeat_loop()),
        ]

        normal_tasks = [
            asyncio.create_task(auto_daily_export_task()),
            asyncio.create_task(daily_reset_task()),
            asyncio.create_task(efficient_monthly_export_task()),
            asyncio.create_task(monthly_report_task()),
        ]

        all_tasks = critical_tasks + normal_tasks
        logger.info(f"✅ 所有后台任务已启动: {len(all_tasks)} 个任务")

        # 智能模式选择
        if Config.should_use_webhook():
            logger.info("🚀 使用 Webhook 模式运行")
            # Webhook 模式：Render 端口会持续监听
            while True:
                await asyncio.sleep(3600)
        else:
            logger.info("🚀 使用 Polling 模式运行")
            await dp.start_polling(bot, skip_updates=True)

    except Exception as e:
        logger.error(f"❌ 启动过程中出错: {e}")
        raise
    finally:
        await optimized_on_shutdown()


# ==================== Webhook 路由处理 ====================


async def webhook_handler(request: web.Request):
    """处理Telegram Webhook请求"""
    try:
        # 验证请求来源（可选但推荐）
        # 您可以添加Token验证来确保请求来自Telegram

        update_data = await request.json()
        update = types.Update(**update_data)

        # 使用Dispatcher处理更新
        await dp.feed_update(bot, update)

        return web.Response(status=200, text="OK")

    except Exception as e:
        logger.error(f"❌ Webhook处理错误: {e}")
        return web.Response(status=500, text="Internal Server Error")


async def start_webhook_server():
    """启动Webhook服务器"""
    try:
        # 设置Webhook
        webhook_url = f"{Config.WEBHOOK_URL}/webhook"

        logger.info(f"🔗 设置Webhook: {webhook_url}")
        await bot.set_webhook(
            url=webhook_url,
            drop_pending_updates=True,
            allowed_updates=["message", "callback_query", "chat_member"],
        )

        # 验证Webhook设置
        webhook_info = await bot.get_webhook_info()
        logger.info(f"📊 Webhook信息: {webhook_info.url}")
        logger.info(f"📊 待处理更新: {webhook_info.pending_update_count}")

        # 创建aiohttp应用
        app = web.Application()

        # 添加路由
        app.router.add_post("/webhook", webhook_handler)
        app.router.add_get("/health", enhanced_health_check)
        app.router.add_get("/", enhanced_health_check)
        app.router.add_get("/status", enhanced_health_check)
        app.router.add_get("/ping", lambda request: web.Response(text="pong"))

        # 启动服务器
        runner = web.AppRunner(app)
        await runner.setup()

        port = int(os.environ.get("PORT", Config.WEB_SERVER_CONFIG["PORT"]))
        site = web.TCPSite(runner, "0.0.0.0", port)
        await site.start()

        logger.info(f"🌐 Webhook服务器已在端口 {port} 启动")
        logger.info("✅ Webhook模式已就绪，等待Telegram请求...")

        return runner

    except Exception as e:
        logger.error(f"❌ Webhook服务器启动失败: {e}")
        # 尝试删除Webhook并回退到Polling
        try:
            await bot.delete_webhook(drop_pending_updates=True)
            logger.info("🔄 已删除Webhook，将使用Polling模式")
        except:
            pass
        raise


async def webhook_main():
    """Webhook模式主函数"""
    logger.info("🚀 启动Webhook模式...")

    try:
        await optimized_on_startup()

        # 启动Webhook服务器
        webhook_runner = await start_webhook_server()

        # 启动后台任务
        background_tasks = [
            asyncio.create_task(memory_cleanup_task()),
            asyncio.create_task(health_monitoring_task()),
            asyncio.create_task(heartbeat_manager.start_heartbeat_loop()),
            asyncio.create_task(daily_reset_task()),
            asyncio.create_task(auto_daily_export_task()),
            asyncio.create_task(efficient_monthly_export_task()),
        ]

        logger.info(f"✅ 后台任务已启动: {len(background_tasks)} 个任务")

        # 保持服务器运行
        try:
            while True:
                await asyncio.sleep(3600)  # 每小时检查一次

                # 可选：定期检查Webhook状态
                try:
                    webhook_info = await bot.get_webhook_info()
                    if webhook_info.pending_update_count > 100:
                        logger.warning(
                            f"⚠️ 待处理更新较多: {webhook_info.pending_update_count}"
                        )
                except Exception as e:
                    logger.warning(f"⚠️ 检查Webhook状态失败: {e}")

        except asyncio.CancelledError:
            logger.info("🛑 Webhook服务器被取消")
        except Exception as e:
            logger.error(f"❌ Webhook服务器运行错误: {e}")
            raise

    except Exception as e:
        logger.error(f"❌ Webhook模式启动失败: {e}")
        raise

    finally:
        # 清理资源
        try:
            if "webhook_runner" in locals():
                await webhook_runner.cleanup()
        except Exception as e:
            logger.warning(f"⚠️ 清理Webhook运行器失败: {e}")

        await optimized_on_shutdown()


async def polling_main():
    """Polling模式主函数"""
    logger.info("🚀 启动Polling模式...")

    await optimized_on_startup()

    # 启动后台任务
    background_tasks = [
        asyncio.create_task(memory_cleanup_task()),
        asyncio.create_task(health_monitoring_task()),
        asyncio.create_task(heartbeat_manager.start_heartbeat_loop()),
        asyncio.create_task(daily_reset_task()),
        asyncio.create_task(auto_daily_export_task()),
        asyncio.create_task(efficient_monthly_export_task()),
    ]

    logger.info(f"✅ 后台任务已启动: {len(background_tasks)} 个任务")
    logger.info("🔄 开始轮询消息...")

    try:
        await dp.start_polling(bot, skip_updates=True)
    except Exception as e:
        logger.error(f"❌ Polling模式运行错误: {e}")
        raise



# 修改主函数以支持两种模式
from handlers import * 
async def main():
    """主启动函数 - 简化版本避免重复启动"""
    if not check_environment():
        logger.error("❌ 环境检查失败")
        sys.exit(1)

    # 立即设置Polling模式，避免Webhook问题
    Config.BOT_MODE = "polling"  # 强制使用Polling模式

    try:
        await db.initialize()
        logger.info("✅ 数据库初始化完成")

        # 🆕 初始化心跳服务
        try:
            await heartbeat_manager.initialize()
            logger.info("✅ 心跳管理器初始化完成")
        except Exception as e:
            logger.warning(f"⚠️ 初始化心跳管理器失败: {e}")

        # 使用简化的启动
        await simple_on_startup()

        # 直接使用Polling模式
        logger.info("🚀 使用 Polling 模式运行")

        # 启动必要的后台任务
        essential_tasks = [
            asyncio.create_task(memory_cleanup_task()),
            asyncio.create_task(heartbeat_manager.start_heartbeat_loop()),
        ]

        logger.info(f"✅ 基础后台任务已启动: {len(essential_tasks)} 个任务")

        # 启动轮询
        await dp.start_polling(bot, skip_updates=True)

    except KeyboardInterrupt:
        logger.info("👋 收到中断信号，正在关闭...")
    except Exception as e:
        logger.error(f"💥 主程序异常: {e}")
        raise
    finally:
        # 清理资源
        try:
            await db.close()
            logger.info("✅ 数据库连接已关闭")
        except Exception as e:
            logger.error(f"❌ 关闭数据库连接失败: {e}")
        try:
            await bot.session.close()
            logger.info("✅ 已安全关闭 aiohttp ClientSession（bot.session）")
        except Exception as e:
            logger.warning(f"⚠️ 关闭 bot.session 失败: {e}")
        try:
            await heartbeat_manager.stop()
            logger.info("✅ 心跳管理器已关闭")
        except Exception as e:
            logger.warning(f"⚠️ 关闭心跳管理器失败: {e}")

        logger.info("🎉 程序安全退出")


# ==================== 修复缺失的函数 ====================
async def simple_on_startup():
    """简化版启动流程 - 修复版本"""
    logger.info("🔧 执行简化启动...")

    # 删除Webhook，确保使用Polling模式
    try:
        await bot.delete_webhook(drop_pending_updates=True)
        logger.info("✅ 已确认使用Polling模式")
    except Exception as e:
        logger.warning(f"⚠️ 删除Webhook失败: {e}")

    # 预加载必要数据
    try:
        await preload_frequent_data()
        logger.info("✅ 数据预加载完成")
    except Exception as e:
        logger.warning(f"⚠️ 数据预加载失败: {e}")

    # 恢复活动定时器
    try:
        await restore_activity_timers()
    except Exception as e:
        logger.error(f"❌ 恢复定时器失败: {e}")


async def preload_frequent_data():
    """预加载常用数据"""
    try:
        # 并行预加载
        preload_tasks = [
            db.get_activity_limits_cached(),
            db.get_push_settings(),
            db.get_fine_rates(),
        ]

        await asyncio.gather(*preload_tasks)
        logger.info("✅ 常用数据预加载完成")
    except Exception as e:
        logger.warning(f"⚠️ 预加载数据失败: {e}")


# 使用render就注释，其他服务器再打开
# if __name__ == "__main__":
#     try:
#         asyncio.run(main())
#     except KeyboardInterrupt:
#         logger.info("👋 机器人已手动停止")
#     except Exception as e:
#         logger.error(f"💥 机器人异常退出: {e}")
#         sys.exit(1)
