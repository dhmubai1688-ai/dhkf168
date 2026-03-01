import os
import time
import asyncio
import logging
import gc
import psutil
import weakref
from functools import wraps
from datetime import datetime, timedelta, date
from typing import Dict, Any, List, Optional, Tuple, Union, Callable

from aiogram import types
from datetime import time as dt_time

from config import Config, beijing_tz
from database import db
from performance import global_cache, task_manager

logger = logging.getLogger("GroupCheckInBot")


class MessageFormatter:
    """消息格式化工具类 - 优化版"""

    @staticmethod
    def format_time(seconds: Optional[int]) -> str:
        """格式化时间显示"""
        if seconds is None or seconds <= 0:
            return "0秒"

        m, s = divmod(int(seconds), 60)
        h, m = divmod(m, 60)

        if h > 0:
            return f"{h}小时{m}分{s}秒"
        elif m > 0:
            return f"{m}分{s}秒"
        else:
            return f"{s}秒"

    @staticmethod
    def format_time_for_csv(seconds: Optional[int]) -> str:
        """为CSV导出格式化时间显示"""
        if seconds is None or seconds <= 0:
            return "0分0秒"

        seconds = int(seconds)
        hours = seconds // 3600
        minutes = (seconds % 3600) // 60
        secs = seconds % 60

        if hours > 0:
            return f"{hours}时{minutes}分{secs}秒"
        else:
            return f"{minutes}分{secs}秒"

    @staticmethod
    def format_user_link(user_id: int, user_name: Optional[str]) -> str:
        """格式化用户链接 - 安全版"""
        if not user_name:
            user_name = f"用户{user_id}"
        
        # 一次性替换所有危险字符
        import re
        clean_name = re.sub(r'[<>&"]', '', str(user_name))
        
        return f'<a href="tg://user?id={user_id}">{clean_name}</a>'

    @staticmethod
    def create_dashed_line(length: int = 26) -> str:
        """创建虚线分割线"""
        return MessageFormatter.format_copyable_text("─" * length)

    @staticmethod
    def format_copyable_text(text: str) -> str:
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
        shift: Optional[str] = None,
    ) -> str:
        """格式化打卡消息"""
        first_line = f"👤 用户：{MessageFormatter.format_user_link(user_id, user_name)}"
        dashed_line = MessageFormatter.create_dashed_line()

        message = [
            first_line,
            f"✅ 打卡成功：{MessageFormatter.format_copyable_text(activity)} - {MessageFormatter.format_copyable_text(time_str)}",
        ]

        if shift:
            shift_text = "A班" if shift == "day" else "B班"
            message.append(f"📊 班次：{MessageFormatter.format_copyable_text(shift_text)}")

        message.extend([
            f"▫️ 本次活动类型：{MessageFormatter.format_copyable_text(activity)}",
            f"⏰ 单次时长限制：{MessageFormatter.format_copyable_text(str(time_limit))}分钟",
            f"📈 今日{MessageFormatter.format_copyable_text(activity)}次数：第 {MessageFormatter.format_copyable_text(str(count))} 次（上限 {MessageFormatter.format_copyable_text(str(max_times))} 次）",
        ])

        if count >= max_times:
            message.append(f"🚨 警告：本次结束后，您今日的{MessageFormatter.format_copyable_text(activity)}次数将达到上限，请留意！")

        message.extend([
            dashed_line,
            "💡 操作提示",
            "活动结束后请及时点击 👉【✅ 回座】👈按钮。"
        ])

        return "\n".join(message)

    @staticmethod
    def format_back_message(
        user_id: int,
        user_name: str,
        activity: str,
        time_str: str,
        elapsed_time: str,
        total_activity_time: str,
        total_time: str,
        activity_counts: Dict[str, int],
        total_count: int,
        is_overtime: bool = False,
        overtime_seconds: int = 0,
        fine_amount: int = 0,
    ) -> str:
        """格式化回座消息 - 优化版"""
        first_line = f"👤 用户：{MessageFormatter.format_user_link(user_id, user_name)}"
        dashed_line = MessageFormatter.create_dashed_line()

        today_count = activity_counts.get(activity, 0)

        message = [
            first_line,
            f"✅ 回座打卡：{MessageFormatter.format_copyable_text(time_str)}",
            dashed_line,
            "📍 活动记录",
            f"▫️ 活动类型：{MessageFormatter.format_copyable_text(activity)}",
            f"▫️ 本次耗时：{MessageFormatter.format_copyable_text(elapsed_time)} ⏰",
            f"▫️ 累计时长：{MessageFormatter.format_copyable_text(total_activity_time)}",
            f"▫️ 今日次数：{MessageFormatter.format_copyable_text(str(today_count))}次",
        ]

        if is_overtime:
            overtime_time = MessageFormatter.format_time(int(overtime_seconds))
            message.extend([
                "",
                "⚠️ 超时提醒",
                f"▫️ 超时时长：{MessageFormatter.format_copyable_text(overtime_time)} 🚨",
            ])
            if fine_amount > 0:
                message.append(f"▫️ 扣除绩效：{MessageFormatter.format_copyable_text(str(fine_amount))} 分 💸")

        message.extend([
            dashed_line,
            "📊 今日总计",
            "▫️ 活动详情",
        ])

        # 只显示有次数的活动
        active_activities = [(act, cnt) for act, cnt in activity_counts.items() if cnt > 0]
        for act, cnt in active_activities:
            message.append(f"   ➤ {MessageFormatter.format_copyable_text(act)}：{MessageFormatter.format_copyable_text(str(cnt))} 次 📝")

        message.extend([
            f"▫️ 总活动次数：{MessageFormatter.format_copyable_text(str(total_count))}次",
            f"▫️ 总活动时长：{MessageFormatter.format_copyable_text(total_time)}",
        ])

        return "\n".join(message)

    @staticmethod
    def format_duration(seconds: int) -> str:
        """格式化持续时间"""
        seconds = int(seconds)
        if seconds <= 0:
            return "0分钟"

        h = seconds // 3600
        m = (seconds % 3600) // 60
        s = seconds % 60

        parts = []
        if h > 0:
            parts.append(f"{h}小时")
        if m > 0:
            parts.append(f"{m}分钟")
        if s > 0 and h == 0:  # 只有不到1小时才显示秒
            parts.append(f"{s}秒")

        return "".join(parts) if parts else "0分钟"


class NotificationService:
    """统一推送服务 - 优化版"""

    def __init__(self, bot_manager=None):
        self.bot_manager = bot_manager
        self.bot = None
        self._last_notification_time: Dict[str, float] = {}
        self._rate_limit_window = 60
        self._max_retries = 3
        self._stats = {
            "sent": 0,
            "failed": 0,
            "rate_limited": 0,
        }

    async def send_notification(
        self, chat_id: int, text: str, notification_type: str = "all"
    ) -> bool:
        """发送通知到绑定的频道和群组"""
        if not self.bot_manager and not self.bot:
            logger.warning("❌ NotificationService: bot_manager 和 bot 都未初始化")
            return False

        # 速率限制检查
        notification_key = f"{chat_id}:{hash(text)}"
        current_time = time.time()
        
        if notification_key in self._last_notification_time:
            time_since_last = current_time - self._last_notification_time[notification_key]
            if time_since_last < self._rate_limit_window:
                self._stats["rate_limited"] += 1
                logger.debug(f"⏱️ 跳过重复通知: {notification_key}")
                return True

        sent = False
        try:
            push_settings = await db.get_push_settings()
            group_data = await db.get_group_cached(chat_id)

            if self.bot_manager and hasattr(self.bot_manager, "send_message_with_retry"):
                sent = await self._send_with_bot_manager(
                    chat_id, text, group_data, push_settings
                )
            elif self.bot:
                sent = await self._send_with_bot(
                    chat_id, text, group_data, push_settings
                )

            if sent:
                self._last_notification_time[notification_key] = current_time
                self._stats["sent"] += 1
            else:
                self._stats["failed"] += 1

        except Exception as e:
            self._stats["failed"] += 1
            logger.error(f"❌ 发送通知失败: {e}")

        return sent

    async def _send_with_bot_manager(
        self, chat_id: int, text: str, group_data: Dict, push_settings: Dict
    ) -> bool:
        """使用 bot_manager 发送通知"""
        sent = False

        # 发送到频道
        if (push_settings.get("enable_channel_push") and 
            group_data and group_data.get("channel_id")):
            try:
                if await self.bot_manager.send_message_with_retry(
                    group_data["channel_id"], text, parse_mode="HTML"
                ):
                    sent = True
                    logger.info(f"✅ 已发送到频道: {group_data['channel_id']}")
            except Exception as e:
                logger.error(f"❌ 发送到频道失败: {e}")

        # 发送到通知群组
        if (push_settings.get("enable_group_push") and 
            group_data and group_data.get("notification_group_id")):
            try:
                if await self.bot_manager.send_message_with_retry(
                    group_data["notification_group_id"], text, parse_mode="HTML"
                ):
                    sent = True
                    logger.info(f"✅ 已发送到通知群组: {group_data['notification_group_id']}")
            except Exception as e:
                logger.error(f"❌ 发送到通知群组失败: {e}")

        # 发送给管理员
        if not sent and push_settings.get("enable_admin_push"):
            for admin_id in Config.ADMINS:
                try:
                    if await self.bot_manager.send_message_with_retry(
                        admin_id, text, parse_mode="HTML"
                    ):
                        logger.info(f"✅ 已发送给管理员: {admin_id}")
                        sent = True
                        break
                except Exception as e:
                    logger.error(f"❌ 发送给管理员失败: {e}")

        return sent

    async def _send_with_bot(
        self, chat_id: int, text: str, group_data: Dict, push_settings: Dict
    ) -> bool:
        """直接使用 bot 实例发送通知"""
        sent = False

        if (push_settings.get("enable_channel_push") and 
            group_data and group_data.get("channel_id")):
            try:
                await self.bot.send_message(
                    group_data["channel_id"], text, parse_mode="HTML"
                )
                sent = True
                logger.info(f"✅ 已发送到频道: {group_data['channel_id']}")
            except Exception as e:
                logger.error(f"❌ 发送到频道失败: {e}")

        if (push_settings.get("enable_group_push") and 
            group_data and group_data.get("notification_group_id")):
            try:
                await self.bot.send_message(
                    group_data["notification_group_id"], text, parse_mode="HTML"
                )
                sent = True
                logger.info(f"✅ 已发送到通知群组: {group_data['notification_group_id']}")
            except Exception as e:
                logger.error(f"❌ 发送到通知群组失败: {e}")

        if not sent and push_settings.get("enable_admin_push"):
            for admin_id in Config.ADMINS:
                try:
                    await self.bot.send_message(admin_id, text, parse_mode="HTML")
                    logger.info(f"✅ 已发送给管理员: {admin_id}")
                    sent = True
                    break
                except Exception as e:
                    logger.error(f"❌ 发送给管理员失败: {e}")

        return sent

    async def send_document(self, chat_id: int, document, caption: str = "") -> bool:
        """发送文档到绑定的频道和群组"""
        if not self.bot_manager and not self.bot:
            logger.warning("❌ NotificationService: bot_manager 和 bot 都未初始化")
            return False

        sent = False
        try:
            push_settings = await db.get_push_settings()
            group_data = await db.get_group_cached(chat_id)

            if self.bot_manager and hasattr(self.bot_manager, "send_document_with_retry"):
                sent = await self._send_document_with_bot_manager(
                    chat_id, document, caption, group_data, push_settings
                )
            elif self.bot:
                sent = await self._send_document_with_bot(
                    chat_id, document, caption, group_data, push_settings
                )

            if sent:
                self._stats["sent"] += 1
            else:
                self._stats["failed"] += 1

        except Exception as e:
            self._stats["failed"] += 1
            logger.error(f"❌ 发送文档失败: {e}")

        return sent

    async def _send_document_with_bot_manager(
        self, chat_id: int, document, caption: str, group_data: Dict, push_settings: Dict
    ) -> bool:
        """使用 bot_manager 发送文档"""
        sent = False

        if (push_settings.get("enable_channel_push") and 
            group_data and group_data.get("channel_id")):
            try:
                if await self.bot_manager.send_document_with_retry(
                    group_data["channel_id"], document, caption=caption, parse_mode="HTML"
                ):
                    sent = True
                    logger.info(f"✅ 已发送文档到频道: {group_data['channel_id']}")
            except Exception as e:
                logger.error(f"❌ 发送文档到频道失败: {e}")

        if (push_settings.get("enable_group_push") and 
            group_data and group_data.get("notification_group_id")):
            try:
                if await self.bot_manager.send_document_with_retry(
                    group_data["notification_group_id"], 
                    document, caption=caption, parse_mode="HTML"
                ):
                    sent = True
                    logger.info(f"✅ 已发送文档到通知群组: {group_data['notification_group_id']}")
            except Exception as e:
                logger.error(f"❌ 发送文档到通知群组失败: {e}")

        if not sent and push_settings.get("enable_admin_push"):
            for admin_id in Config.ADMINS:
                try:
                    if await self.bot_manager.send_document_with_retry(
                        admin_id, document, caption=caption, parse_mode="HTML"
                    ):
                        logger.info(f"✅ 已发送文档给管理员: {admin_id}")
                        sent = True
                        break
                except Exception as e:
                    logger.error(f"❌ 发送文档给管理员失败: {e}")

        return sent

    async def _send_document_with_bot(
        self, chat_id: int, document, caption: str, group_data: Dict, push_settings: Dict
    ) -> bool:
        """直接使用 bot 实例发送文档"""
        sent = False

        if (push_settings.get("enable_channel_push") and 
            group_data and group_data.get("channel_id")):
            try:
                await self.bot.send_document(
                    group_data["channel_id"], document, caption=caption, parse_mode="HTML"
                )
                sent = True
                logger.info(f"✅ 已发送文档到频道: {group_data['channel_id']}")
            except Exception as e:
                logger.error(f"❌ 发送文档到频道失败: {e}")

        if (push_settings.get("enable_group_push") and 
            group_data and group_data.get("notification_group_id")):
            try:
                await self.bot.send_document(
                    group_data["notification_group_id"], 
                    document, caption=caption, parse_mode="HTML"
                )
                sent = True
                logger.info(f"✅ 已发送文档到通知群组: {group_data['notification_group_id']}")
            except Exception as e:
                logger.error(f"❌ 发送文档到通知群组失败: {e}")

        if not sent and push_settings.get("enable_admin_push"):
            for admin_id in Config.ADMINS:
                try:
                    await self.bot.send_document(
                        admin_id, document, caption=caption, parse_mode="HTML"
                    )
                    logger.info(f"✅ 已发送文档给管理员: {admin_id}")
                    sent = True
                    break
                except Exception as e:
                    logger.error(f"❌ 发送文档给管理员失败: {e}")

        return sent

    def get_stats(self) -> Dict[str, Any]:
        """获取推送统计"""
        return self._stats.copy()


class UserLockManager:
    """用户锁管理器 - 优化版"""

    def __init__(self):
        self._locks: Dict[str, asyncio.Lock] = {}
        self._access_times: Dict[str, float] = {}
        self._cleanup_interval = 3600  # 1小时
        self._last_cleanup = time.time()
        self._max_locks = 5000
        self._stats = {
            "hits": 0,
            "misses": 0,
            "cleanups": 0,
        }

    def get_lock(self, chat_id: int, uid: int) -> asyncio.Lock:
        """获取用户级锁"""
        key = f"{chat_id}-{uid}"
        now = time.time()

        # 容量检查
        if len(self._locks) >= self._max_locks:
            self._emergency_cleanup()

        # 更新访问时间
        self._access_times[key] = now

        # 按需清理
        self._maybe_cleanup()

        # 获取或创建锁
        if key not in self._locks:
            self._locks[key] = asyncio.Lock()
            self._stats["misses"] += 1
        else:
            self._stats["hits"] += 1

        return self._locks[key]

    def _maybe_cleanup(self):
        """按需清理过期锁"""
        now = time.time()
        if now - self._last_cleanup < self._cleanup_interval:
            return

        self._last_cleanup = now
        self._cleanup_old_locks()

    def _cleanup_old_locks(self):
        """清理长时间未使用的锁"""
        now = time.time()
        max_age = 86400  # 24小时

        old_keys = [
            key for key, last_used in self._access_times.items()
            if now - last_used > max_age
        ]

        for key in old_keys:
            self._locks.pop(key, None)
            self._access_times.pop(key, None)

        if old_keys:
            self._stats["cleanups"] += len(old_keys)
            logger.debug(f"🧹 用户锁清理: 移除了 {len(old_keys)} 个过期锁")

    async def force_cleanup(self):
        """强制立即清理"""
        old_count = len(self._locks)
        self._cleanup_old_locks()
        new_count = len(self._locks)
        logger.info(f"强制用户锁清理: {old_count} -> {new_count}")

    def _emergency_cleanup(self):
        """紧急清理"""
        now = time.time()
        max_age = 3600  # 1小时

        # 清理超过1小时未使用的
        old_keys = [
            key for key, last_used in self._access_times.items()
            if now - last_used > max_age
        ]

        # 如果还是太多，清理最旧的20%
        if len(self._locks) >= self._max_locks:
            sorted_keys = sorted(self._access_times.items(), key=lambda x: x[1])
            additional = max(100, len(sorted_keys) // 5)
            old_keys.extend([key for key, _ in sorted_keys[:additional]])

        # 去重执行清理
        for key in set(old_keys):
            self._locks.pop(key, None)
            self._access_times.pop(key, None)

        logger.warning(f"🚨 紧急锁清理: 移除了 {len(set(old_keys))} 个锁")

    def get_stats(self) -> Dict[str, Any]:
        """获取锁管理器统计"""
        return {
            "active_locks": len(self._locks),
            "tracked_users": len(self._access_times),
            "last_cleanup": self._last_cleanup,
            "hits": self._stats["hits"],
            "misses": self._stats["misses"],
            "cleanups": self._stats["cleanups"],
            "hit_rate": round(self._stats["hits"] / max(1, self._stats["hits"] + self._stats["misses"]) * 100, 2),
        }


class ActivityTimerManager:
    """活动定时器管理器 - 优化版"""

    def __init__(self):
        self.active_timers: Dict[str, Dict[str, Any]] = {}
        self._cleanup_interval = 300  # 5分钟
        self._last_cleanup = time.time()
        self.activity_timer_callback = None
        self._stats = {
            "started": 0,
            "cancelled": 0,
            "completed": 0,
            "errors": 0,
        }

    def set_activity_timer_callback(self, callback: Callable):
        """设置活动定时器回调"""
        self.activity_timer_callback = callback

    async def start_timer(
        self,
        chat_id: int,
        uid: int,
        act: str,
        limit: int,
        shift: str = "day",
    ) -> bool:
        """启动活动定时器"""
        timer_key = f"{chat_id}-{uid}-{shift}"

        if timer_key in self.active_timers:
            await self.cancel_timer(timer_key, preserve_message=False)

        if not self.activity_timer_callback:
            logger.error("❌ ActivityTimerManager: 未设置回调函数")
            return False

        timer_task = asyncio.create_task(
            self._activity_timer_wrapper(chat_id, uid, act, limit, shift),
            name=f"timer_{timer_key}",
        )

        self.active_timers[timer_key] = {
            "task": timer_task,
            "activity": act,
            "limit": limit,
            "shift": shift,
            "chat_id": chat_id,
            "uid": uid,
            "start_time": time.time(),
        }

        self._stats["started"] += 1
        logger.info(f"⏰ 启动定时器: {timer_key} - {act}（班次: {shift}）")
        return True

    async def cancel_timer(self, timer_key: str, preserve_message: bool = False) -> int:
        """取消并清理指定的定时器"""
        keys_to_cancel = [
            k for k in self.active_timers.keys() if k.startswith(timer_key)
        ]

        for key in keys_to_cancel:
            timer_info = self.active_timers.pop(key, None)
            if not timer_info:
                continue

            task = timer_info.get("task")
            if task and not task.done():
                # 传递 preserve_message 到任务
                if hasattr(task, "preserve_message"):
                    task.preserve_message = preserve_message

                task.cancel()
                try:
                    await task
                    self._stats["cancelled"] += 1
                except asyncio.CancelledError:
                    logger.info(f"⏹️ 定时器任务已取消: {key}")
                except Exception as e:
                    self._stats["errors"] += 1
                    logger.error(f"❌ 定时器任务取消异常 ({key}): {e}")

            # 清理消息ID
            if not preserve_message:
                chat_id = timer_info.get("chat_id")
                uid = timer_info.get("uid")
                if chat_id and uid:
                    try:
                        await db.clear_user_checkin_message(chat_id, uid)
                        logger.debug(f"🧹 定时器消息ID已清理: {key}")
                    except Exception as e:
                        logger.error(f"❌ 定时器消息清理异常 ({key}): {e}")

        return len(keys_to_cancel)

    async def cancel_all_timers(self) -> int:
        """取消所有定时器"""
        keys = list(self.active_timers.keys())
        cancelled = 0

        for key in keys:
            try:
                await self.cancel_timer(key, preserve_message=False)
                cancelled += 1
            except Exception as e:
                logger.error(f"❌ 取消定时器 {key} 失败: {e}")

        logger.info(f"✅ 已取消所有定时器: {cancelled} 个")
        return cancelled

    async def cancel_all_timers_for_group(
        self, chat_id: int, preserve_message: bool = False
    ) -> int:
        """取消指定群组的所有定时器"""
        prefix = f"{chat_id}-"
        keys_to_cancel = [k for k in self.active_timers.keys() if k.startswith(prefix)]
        
        cancelled = 0
        for key in keys_to_cancel:
            await self.cancel_timer(key, preserve_message=preserve_message)
            cancelled += 1

        if cancelled > 0:
            logger.info(f"🗑️ 已取消群组 {chat_id} 的 {cancelled} 个定时器")

        return cancelled

    async def _activity_timer_wrapper(
        self, chat_id: int, uid: int, act: str, limit: int, shift: str
    ):
        """定时器包装器"""
        timer_key = f"{chat_id}-{uid}-{shift}"
        preserve_message = getattr(asyncio.current_task(), "preserve_message", False)

        try:
            from main import activity_timer
            await activity_timer(chat_id, uid, act, limit, shift, preserve_message)
            self._stats["completed"] += 1
        except asyncio.CancelledError:
            logger.info(f"定时器 {timer_key} 被取消")
            if preserve_message:
                logger.debug(f"⏭️ 被取消的定时器保留消息ID")
        except Exception as e:
            self._stats["errors"] += 1
            logger.error(f"定时器异常 {timer_key}: {e}")
            import traceback
            logger.error(traceback.format_exc())
        finally:
            self.active_timers.pop(timer_key, None)
            logger.debug(f"已清理定时器: {timer_key}")

    async def cleanup_finished_timers(self):
        """清理已完成定时器"""
        now = time.time()
        if now - self._last_cleanup < self._cleanup_interval:
            return

        finished_keys = [
            key for key, info in self.active_timers.items()
            if info.get("task") and info["task"].done()
        ]

        for key in finished_keys:
            self.active_timers.pop(key, None)

        if finished_keys:
            logger.debug(f"🧹 定时器清理: 移除了 {len(finished_keys)} 个已完成定时器")

        self._last_cleanup = now

    def get_stats(self) -> Dict[str, Any]:
        """获取定时器统计"""
        return {
            "active_timers": len(self.active_timers),
            "started": self._stats["started"],
            "cancelled": self._stats["cancelled"],
            "completed": self._stats["completed"],
            "errors": self._stats["errors"],
        }


class EnhancedPerformanceOptimizer:
    """增强版性能优化器 - 优化版"""

    def __init__(self):
        self.cleanup_interval = 300
        self.last_cleanup = time.time()
        self.is_render = self._detect_render_environment()
        self.render_memory_limit = 400  # MB
        self._stats = {
            "cleanups": 0,
            "emergency_cleanups": 0,
            "total_freed_mb": 0,
        }

        logger.info(f"🧠 性能优化器初始化 - Render环境: {self.is_render}")

    def _detect_render_environment(self) -> bool:
        """检测是否运行在 Render 环境"""
        return bool(os.environ.get("RENDER") or 
                   os.environ.get("RENDER_EXTERNAL_URL") or 
                   os.environ.get("PORT"))

    async def memory_cleanup(self) -> Optional[float]:
        """智能内存清理"""
        if self.is_render:
            return await self._render_cleanup()
        else:
            await self._regular_cleanup()
            return None

    async def _render_cleanup(self) -> float:
        """Render 环境专用清理"""
        try:
            process = psutil.Process()
            memory_mb = process.memory_info().rss / 1024 / 1024

            logger.debug(f"🔵 Render 内存监测: {memory_mb:.1f} MB")

            if memory_mb > self.render_memory_limit:
                logger.warning(f"🚨 Render 内存过高 {memory_mb:.1f}MB，执行紧急清理")

                # 清理缓存
                cache_stats = global_cache.get_stats()
                old_cache_size = cache_stats.get("size", 0)
                global_cache.clear_all()

                # 清理任务
                await task_manager.cleanup_tasks()

                # 清理数据库缓存
                await db.cleanup_cache()

                # 垃圾回收
                collected = gc.collect()
                
                # 再次检查内存
                new_memory = process.memory_info().rss / 1024 / 1024
                freed_mb = memory_mb - new_memory

                self._stats["emergency_cleanups"] += 1
                self._stats["total_freed_mb"] += freed_mb

                logger.info(
                    f"🆘 紧急清理完成:\n"
                    f"   ├─ 清缓存: {old_cache_size} 项\n"
                    f"   ├─ GC回收: {collected} 对象\n"
                    f"   ├─ 内存释放: {freed_mb:.1f} MB\n"
                    f"   └─ 当前内存: {new_memory:.1f} MB"
                )

            return memory_mb

        except Exception as e:
            logger.error(f"❌ Render 内存清理失败: {e}")
            return 0.0

    async def _regular_cleanup(self):
        """普通环境的智能周期清理"""
        try:
            now = time.time()
            if now - self.last_cleanup < self.cleanup_interval:
                return

            logger.debug("🟢 执行周期性内存清理...")

            # 并发清理
            await asyncio.gather(
                task_manager.cleanup_tasks(),
                global_cache.clear_expired(),
                db.cleanup_cache(),
                return_exceptions=True
            )

            # 垃圾回收
            collected = gc.collect()
            
            if collected > 0:
                logger.info(f"✅ 周期清理完成 - GC回收对象: {collected}")
                self._stats["cleanups"] += 1

            self.last_cleanup = now

        except Exception as e:
            logger.error(f"❌ 周期清理失败: {e}")

    def memory_usage_ok(self) -> bool:
        """检查内存使用是否正常"""
        try:
            process = psutil.Process()
            memory_mb = process.memory_info().rss / 1024 / 1024
            memory_percent = process.memory_percent()

            if self.is_render:
                return memory_mb < self.render_memory_limit
            else:
                return memory_percent < 80
        except Exception:
            return True

    def get_memory_info(self) -> Dict[str, Any]:
        """获取当前内存信息"""
        try:
            process = psutil.Process()
            memory_mb = process.memory_info().rss / 1024 / 1024
            memory_percent = process.memory_percent()

            return {
                "memory_usage_mb": round(memory_mb, 1),
                "memory_percent": round(memory_percent, 1),
                "is_render": self.is_render,
                "render_memory_limit": self.render_memory_limit,
                "needs_cleanup": memory_mb > self.render_memory_limit if self.is_render else memory_percent > 80,
                "status": "healthy" if self.memory_usage_ok() else "warning",
                "stats": self._stats.copy(),
            }
        except Exception as e:
            logger.error(f"❌ 获取内存信息失败: {e}")
            return {"error": str(e)}


class HeartbeatManager:
    """心跳管理器 - 优化版"""

    def __init__(self):
        self._last_heartbeat = time.time()
        self._is_running = False
        self._task: Optional[asyncio.Task] = None
        self._stats = {
            "beats": 0,
            "missed": 0,
        }

    async def initialize(self):
        """初始化心跳管理器"""
        if self._is_running:
            return
            
        self._is_running = True
        self._task = asyncio.create_task(self._heartbeat_loop())
        logger.info("✅ 心跳管理器已初始化")

    async def stop(self):
        """停止心跳管理器"""
        self._is_running = False
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("🛑 心跳管理器已停止")

    async def _heartbeat_loop(self):
        """心跳循环"""
        while self._is_running:
            try:
                self._last_heartbeat = time.time()
                self._stats["beats"] += 1
                await asyncio.sleep(60)
            except asyncio.CancelledError:
                break
            except Exception as e:
                self._stats["missed"] += 1
                logger.error(f"❌ 心跳循环异常: {e}")
                await asyncio.sleep(10)

    def get_status(self) -> Dict[str, Any]:
        """获取心跳状态"""
        now = time.time()
        last_beat_ago = now - self._last_heartbeat
        is_healthy = last_beat_ago < 120  # 2分钟内

        return {
            "is_running": self._is_running,
            "last_heartbeat": self._last_heartbeat,
            "last_heartbeat_ago": round(last_beat_ago, 2),
            "status": "healthy" if is_healthy else "unhealthy",
            "stats": self._stats.copy(),
        }


class ShiftStateManager:
    """班次状态管理器 - 优化版"""

    def __init__(self):
        self._check_interval = 300  # 5分钟
        self._is_running = False
        self._task: Optional[asyncio.Task] = None
        self.logger = logging.getLogger("GroupCheckInBot.ShiftStateManager")
        self._stats = {
            "cleanups": 0,
            "total_cleaned": 0,
        }

    async def start(self):
        """启动清理任务"""
        if self._is_running:
            return
            
        self._is_running = True
        self._task = asyncio.create_task(self._cleanup_loop())
        self.logger.info("✅ 班次状态管理器已启动")

    async def stop(self):
        """停止清理任务"""
        self._is_running = False
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self.logger.info("🛑 班次状态管理器已停止")

    async def _cleanup_loop(self):
        """清理循环"""
        while self._is_running:
            try:
                await asyncio.sleep(self._check_interval)

                from database import db
                cleaned = await db.cleanup_expired_shift_states()

                if cleaned > 0:
                    self._stats["cleanups"] += 1
                    self._stats["total_cleaned"] += cleaned
                    self.logger.info(f"🧹 自动清理了 {cleaned} 个过期班次状态")

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"❌ 清理循环异常: {e}")
                await asyncio.sleep(60)

    def get_stats(self) -> Dict[str, Any]:
        """获取统计信息"""
        return self._stats.copy()


# ========== 工具函数 ==========

def get_beijing_time() -> datetime:
    """获取北京时间"""
    return datetime.now(beijing_tz)


def calculate_cross_day_time_diff(
    current_dt: datetime,
    expected_time: str,
    checkin_type: str,
    record_date: Optional[date] = None,
) -> Tuple[float, int, datetime]:
    """智能化的时间差计算"""
    try:
        expected_hour, expected_minute = map(int, expected_time.split(":"))

        if record_date is None:
            logger.error(f"❌ 缺少 record_date 参数，使用当前日期")
            record_date = current_dt.date()

        expected_dt = datetime.combine(
            record_date, dt_time(expected_hour, expected_minute)
        ).replace(tzinfo=current_dt.tzinfo)

        logger.debug(
            f"📅 时间差计算 - 日期: {record_date}, "
            f"期望: {expected_dt.strftime('%H:%M')}, "
            f"实际: {current_dt.strftime('%H:%M')}"
        )

        time_diff_seconds = int((current_dt - expected_dt).total_seconds())
        time_diff_minutes = time_diff_seconds / 60

        return time_diff_minutes, time_diff_seconds, expected_dt

    except Exception as e:
        logger.error(f"❌ 时间差计算出错: {e}")
        return 0.0, 0, current_dt


def rate_limit(rate: int = 1, per: int = 1):
    """速率限制装饰器 - 优化版"""
    def decorator(func):
        calls: List[float] = []

        @wraps(func)
        async def wrapper(*args, **kwargs):
            nonlocal calls
            now = time.time()
            
            # 清理过期记录
            calls = [t for t in calls if now - t < per]

            if len(calls) >= rate:
                if args and isinstance(args[0], types.Message):
                    await args[0].answer(
                        "⏳ 操作过于频繁，请稍后再试",
                        reply_to_message_id=args[0].message_id
                    )
                return

            calls.append(now)
            return await func(*args, **kwargs)

        return wrapper
    return decorator


async def send_reset_notification(
    chat_id: int, completion_result: Dict[str, Any], reset_time: datetime
):
    """发送重置通知 - 优化版"""
    try:
        completed = completion_result.get("completed_count", 0)
        total_fines = completion_result.get("total_fines", 0)
        details = completion_result.get("details", [])

        if completed == 0:
            notification_text = (
                f"🔄 <b>系统重置完成</b>\n"
                f"🏢 群组: <code>{chat_id}</code>\n"
                f"⏰ 重置时间: <code>{reset_time.strftime('%m/%d %H:%M')}</code>\n"
                f"✅ 没有进行中的活动需要结束"
            )
        else:
            notification_lines = [
                f"🔄 <b>系统重置完成通知</b>",
                f"🏢 群组: <code>{chat_id}</code>",
                f"⏰ 重置时间: <code>{reset_time.strftime('%m/%d %H:%M')}</code>",
                f"📊 自动结束活动: <code>{completed}</code> 个",
                f"💰 总罚款金额: <code>{total_fines}</code> 元",
            ]

            if details:
                notification_lines.append("")
                notification_lines.append("📋 <b>活动结束详情:</b>")
                for i, detail in enumerate(details[:5], 1):
                    user_link = MessageFormatter.format_user_link(
                        detail["user_id"], detail.get("nickname")
                    )
                    time_str = MessageFormatter.format_time(detail["elapsed_time"])
                    fine = f" (罚款: {detail['fine_amount']}元)" if detail["fine_amount"] > 0 else ""
                    overtime = " ⏰超时" if detail["is_overtime"] else ""
                    
                    notification_lines.append(
                        f"{i}. {user_link} - {detail['activity']} "
                        f"({time_str}){fine}{overtime}"
                    )

                if len(details) > 5:
                    notification_lines.append(f"... 还有 {len(details) - 5} 个活动")

                notification_lines.append("")
                notification_lines.append("💡 所有进行中的活动已自动结束并计入月度统计")

            notification_text = "\n".join(notification_lines)

        await notification_service.send_notification(chat_id, notification_text)
        logger.info(f"✅ 重置通知发送成功: {chat_id}")

    except Exception as e:
        logger.error(f"❌ 发送重置通知失败 {chat_id}: {e}")


def init_notification_service(bot_manager_instance=None, bot_instance=None):
    """初始化通知服务 - 优化版"""
    global notification_service

    if "notification_service" not in globals():
        logger.error("❌ notification_service 全局实例不存在")
        return

    if bot_manager_instance:
        notification_service.bot_manager = bot_manager_instance
        logger.info(f"✅ notification_service.bot_manager 已设置")

    if bot_instance:
        notification_service.bot = bot_instance
        logger.info(f"✅ notification_service.bot 已设置")

    logger.info(
        f"📊 通知服务状态: "
        f"bot_manager={notification_service.bot_manager is not None}, "
        f"bot={notification_service.bot is not None}"
    )


# ========== 全局实例 ==========

user_lock_manager = UserLockManager()
timer_manager = ActivityTimerManager()
performance_optimizer = EnhancedPerformanceOptimizer()
heartbeat_manager = HeartbeatManager()
notification_service = NotificationService()
shift_state_manager = ShiftStateManager()
