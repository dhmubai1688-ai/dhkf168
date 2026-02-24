"""
工具函数 - 完整保留所有工具类
"""

import os
import time
import asyncio
import logging
import gc
from datetime import datetime, timedelta, date
from typing import Dict, Any, List, Optional, Tuple, Callable
from functools import wraps
from aiogram import types

from config import beijing_tz

logger = logging.getLogger("GroupCheckInBot.Utils")


class MessageFormatter:
    """消息格式化工具 - 完整版"""

    @staticmethod
    def format_time(seconds: int) -> str:
        """格式化时间"""
        if not seconds:
            return "0秒"
        h = seconds // 3600
        m = (seconds % 3600) // 60
        s = seconds % 60
        if h:
            return f"{h}小时{m}分{s}秒"
        if m:
            return f"{m}分{s}秒"
        return f"{s}秒"

    @staticmethod
    def format_time_csv(seconds: int) -> str:
        """CSV时间格式化"""
        if not seconds:
            return "0分0秒"
        h = seconds // 3600
        m = (seconds % 3600) // 60
        s = seconds % 60
        if h:
            return f"{h}时{m}分{s}秒"
        return f"{m}分{s}秒"

    @staticmethod
    def format_user_link(user_id: int, name: str) -> str:
        """格式化用户链接"""
        clean = (
            str(name)
            .replace("<", "")
            .replace(">", "")
            .replace("&", "")
            .replace('"', "")
        )
        return f'<a href="tg://user?id={user_id}">{clean}</a>'

    @staticmethod
    def create_dashed_line() -> str:
        """创建分割线"""
        return "━━━━━━━━━━━━━━━━"

    @staticmethod
    def format_code(text: str) -> str:
        """格式化代码文本"""
        return f"<code>{text}</code>"

    @staticmethod
    def format_activity_message(
        user_id: int,
        name: str,
        activity: str,
        time_str: str,
        count: int,
        max_times: int,
        time_limit: int,
        shift: str = None,
    ) -> str:
        """格式化活动消息"""
        first = f"👤 用户：{MessageFormatter.format_user_link(user_id, name)}"
        line = MessageFormatter.create_dashed_line()

        msg = f"{first}\n"
        msg += f"✅ 打卡成功：{MessageFormatter.format_code(activity)} - {MessageFormatter.format_code(time_str)}\n"

        if shift:
            shift_text = "白班" if shift == "day" else "夜班"
            msg += f"📊 班次：{MessageFormatter.format_code(shift_text)}\n"

        msg += (
            f"▫️ 本次活动类型：{MessageFormatter.format_code(activity)}\n"
            f"⏰ 单次时长限制：{MessageFormatter.format_code(str(time_limit))}分钟\n"
            f"📈 今日{MessageFormatter.format_code(activity)}次数：第 {MessageFormatter.format_code(str(count))} 次（上限 {MessageFormatter.format_code(str(max_times))} 次）\n"
        )

        if count >= max_times:
            msg += f"🚨 警告：本次结束后，您今日的{MessageFormatter.format_code(activity)}次数将达到上限，请留意！\n"

        msg += f"{line}\n"
        msg += "💡 操作提示\n活动结束后请及时点击 👉【✅ 回座】👈按钮。"

        return msg

    @staticmethod
    def format_back_message(
        user_id: int,
        name: str,
        activity: str,
        time_str: str,
        elapsed: str,
        total_activity: str,
        total_time: str,
        counts: dict,
        total_count: int,
        is_overtime: bool = False,
        overtime_sec: int = 0,
        fine: int = 0,
    ) -> str:
        """格式化回座消息"""
        first = f"👤 用户：{MessageFormatter.format_user_link(user_id, name)}"
        line = MessageFormatter.create_dashed_line()
        today_count = counts.get(activity, 0)

        msg = (
            f"{first}\n"
            f"✅ 回座打卡：{MessageFormatter.format_code(time_str)}\n"
            f"{line}\n"
            f"📍 活动记录\n"
            f"▫️ 活动类型：{MessageFormatter.format_code(activity)}\n"
            f"▫️ 本次耗时：{MessageFormatter.format_code(elapsed)} ⏰\n"
            f"▫️ 累计时长：{MessageFormatter.format_code(total_activity)}\n"
            f"▫️ 今日次数：{MessageFormatter.format_code(str(today_count))}次\n"
        )

        if is_overtime:
            overtime_str = MessageFormatter.format_time(overtime_sec)
            msg += f"\n⚠️ 超时提醒\n"
            msg += f"▫️ 超时时长：{MessageFormatter.format_code(overtime_str)} 🚨\n"
            if fine:
                msg += f"▫️ 扣除绩效：{MessageFormatter.format_code(str(fine))} 分 💸\n"

        msg += f"{line}\n"
        msg += f"📊 今日总计\n"
        msg += f"▫️ 活动详情\n"

        for act, cnt in counts.items():
            if cnt:
                msg += f"   ➤ {MessageFormatter.format_code(act)}：{MessageFormatter.format_code(str(cnt))} 次 📝\n"

        msg += f"▫️ 总活动次数：{MessageFormatter.format_code(str(total_count))}次\n"
        msg += f"▫️ 总活动时长：{MessageFormatter.format_code(total_time)}"

        return msg

    @staticmethod
    def format_duration(seconds: int) -> str:
        """格式化时长"""
        if not seconds:
            return "0分钟"
        h = seconds // 3600
        m = (seconds % 3600) // 60
        s = seconds % 60
        parts = []
        if h:
            parts.append(f"{h}小时")
        if m:
            parts.append(f"{m}分钟")
        if s:
            parts.append(f"{s}秒")
        return "".join(parts)


class UserLockManager:
    """用户锁管理器 - 完整版"""

    def __init__(self):
        self._locks: Dict[str, asyncio.Lock] = {}
        self._access: Dict[str, float] = {}
        self._cleanup_interval = 3600
        self._last_cleanup = time.time()
        self._max_locks = 5000

    def get_lock(self, chat_id: int, user_id: int) -> asyncio.Lock:
        """获取用户锁"""
        key = f"{chat_id}-{user_id}"

        # 检查清理
        if len(self._locks) >= self._max_locks:
            self._emergency_cleanup()
        else:
            self._maybe_cleanup()

        # 记录访问
        self._access[key] = time.time()

        # 创建或返回锁
        if key not in self._locks:
            self._locks[key] = asyncio.Lock()

        return self._locks[key]

    def _maybe_cleanup(self):
        """按需清理"""
        now = time.time()
        if now - self._last_cleanup < self._cleanup_interval:
            return
        self._last_cleanup = now
        self._cleanup_old()

    def _cleanup_old(self, max_age: int = 86400):
        """清理旧锁"""
        now = time.time()
        old = [k for k, t in self._access.items() if now - t > max_age]
        for key in old:
            self._locks.pop(key, None)
            self._access.pop(key, None)
        if old:
            logger.info(f"清理 {len(old)} 个旧锁")

    def _emergency_cleanup(self):
        """紧急清理"""
        now = time.time()
        # 清理1小时未用的
        old = [k for k, t in self._access.items() if now - t > 3600]
        for key in old:
            self._locks.pop(key, None)
            self._access.pop(key, None)

        # 如果还不够，清理最旧的20%
        if len(self._locks) >= self._max_locks:
            sorted_keys = sorted(self._access.items(), key=lambda x: x[1])
            remove = max(100, len(sorted_keys) // 5)
            for key, _ in sorted_keys[:remove]:
                self._locks.pop(key, None)
                self._access.pop(key, None)

        logger.warning(f"紧急清理完成，当前锁数: {len(self._locks)}")

    def get_stats(self) -> Dict:
        """获取统计"""
        return {
            "locks": len(self._locks),
            "users": len(self._access),
            "last_cleanup": self._last_cleanup,
        }


class HeartbeatManager:
    """心跳管理器 - 完整版"""

    def __init__(self):
        self._last_heartbeat = time.time()
        self._is_running = False
        self._task: Optional[asyncio.Task] = None
        self._listeners: List[Callable] = []

    async def start(self):
        """启动"""
        self._is_running = True
        self._task = asyncio.create_task(self._run())
        logger.info("心跳管理器已启动")

    async def stop(self):
        """停止"""
        self._is_running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("心跳管理器已停止")

    async def _run(self):
        """运行"""
        while self._is_running:
            try:
                self._last_heartbeat = time.time()

                # 通知监听器
                for listener in self._listeners:
                    try:
                        await listener()
                    except Exception as e:
                        logger.error(f"心跳监听器错误: {e}")

                await asyncio.sleep(60)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"心跳错误: {e}")
                await asyncio.sleep(10)

    def add_listener(self, callback: Callable):
        """添加监听器"""
        self._listeners.append(callback)

    def get_status(self) -> Dict:
        """获取状态"""
        ago = time.time() - self._last_heartbeat
        return {
            "is_running": self._is_running,
            "last_heartbeat": self._last_heartbeat,
            "seconds_ago": round(ago, 1),
            "status": "healthy" if ago < 120 else "unhealthy",
        }


class ShiftStateManager:
    """班次状态管理器 - 完整版"""

    def __init__(self, db):
        self.db = db
        self._check_interval = 300
        self._is_running = False
        self._task: Optional[asyncio.Task] = None

    async def start(self):
        """启动"""
        self._is_running = True
        self._task = asyncio.create_task(self._run())
        logger.info("班次状态管理器已启动")

    async def stop(self):
        """停止"""
        self._is_running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("班次状态管理器已停止")

    async def _run(self):
        """运行"""
        while self._is_running:
            try:
                await asyncio.sleep(self._check_interval)
                cleaned = await self.db.cleanup_expired_shifts(16)
                if cleaned:
                    logger.info(f"清理 {cleaned} 个过期班次状态")
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"清理错误: {e}")
                await asyncio.sleep(60)


def get_beijing_time() -> datetime:
    """获取北京时间"""
    return datetime.now(beijing_tz)


def calculate_cross_day_time_diff(
    current: datetime,
    expected_time: str,
    checkin_type: str,
    record_date: Optional[date] = None,
) -> Tuple[float, int, datetime]:
    """计算跨天时间差"""
    try:
        h, m = map(int, expected_time.split(":"))

        if record_date is None:
            logger.error("缺少record_date参数")
            record_date = current.date()

        expected = datetime.combine(
            record_date, datetime.strptime(expected_time, "%H:%M").time()
        )
        expected = expected.replace(tzinfo=current.tzinfo)

        diff_sec = int((current - expected).total_seconds())
        diff_min = diff_sec / 60

        return diff_min, diff_sec, expected
    except Exception as e:
        logger.error(f"时间计算出错: {e}")
        return 0.0, 0, current


def rate_limit(rate: int = 1, per: int = 1):
    """速率限制装饰器"""
    calls: List[float] = []

    def decorator(func):
        @wraps(func)
        async def wrapper(msg: types.Message, *args, **kwargs):
            nonlocal calls
            now = time.time()
            calls = [c for c in calls if now - c < per]

            if len(calls) >= rate:
                await msg.answer("⏳ 操作过于频繁，请稍后再试")
                return

            calls.append(now)
            return await func(msg, *args, **kwargs)

        return wrapper

    return decorator


async def send_reset_notification(chat_id: int, result: Dict, reset_time: datetime):
    """发送重置通知"""
    from notification import notification

    completed = result.get("completed_count", 0)
    fines = result.get("total_fines", 0)
    details = result.get("details", [])

    if not completed:
        text = (
            f"🔄 系统重置完成\n"
            f"⏰ {reset_time.strftime('%m/%d %H:%M')}\n"
            f"✅ 没有进行中的活动"
        )
    else:
        text = (
            f"🔄 系统重置完成\n"
            f"⏰ {reset_time.strftime('%m/%d %H:%M')}\n"
            f"📊 结束活动: {completed} 个\n"
            f"💰 总罚款: {fines} 分\n"
        )

        if details:
            text += "\n📋 详情:\n"
            for d in details[:5]:
                name = d.get("nickname", f"用户{d['user_id']}")
                fine = f" (罚款 {d['fine']})" if d.get("fine") else ""
                text += f"• {name}: {d['activity']}{fine}\n"
            if len(details) > 5:
                text += f"... 还有 {len(details)-5} 个"

    await notification.send_with_push_settings(chat_id, text)


def init_notification_service(bot_manager=None, bot=None, db=None):
    """初始化通知服务"""
    from notification import notification

    notification.init(bot_manager, bot, db)
    logger.info(
        f"通知服务初始化: bot_manager={bot_manager is not None}, bot={bot is not None}"
    )
    return notification


# 全局实例
user_lock_manager = UserLockManager()
heartbeat_manager = HeartbeatManager()
shift_state_manager = None


def init_shift_state_manager(db_instance):
    """初始化班次状态管理器"""
    global shift_state_manager
    shift_state_manager = ShiftStateManager(db_instance)
    return shift_state_manager
