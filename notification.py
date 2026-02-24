"""
通知服务 - 完整保留所有推送功能
"""

import logging
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List
from aiogram.types import FSInputFile

logger = logging.getLogger("GroupCheckInBot.Notification")


class NotificationService:
    """统一通知服务 - 完整版"""

    def __init__(self):
        self.bot_manager = None
        self.bot = None
        self._last_sent: Dict[str, float] = {}
        self._rate_limit = 60  # 60秒内不重复
        self.db = None  # 会在初始化时设置

    def init(self, bot_manager=None, bot=None, db=None):
        """初始化"""
        if bot_manager:
            self.bot_manager = bot_manager
        if bot:
            self.bot = bot
        if db:
            self.db = db

    async def send_message(self, chat_id: int, text: str, **kwargs) -> bool:
        """发送消息"""
        # 去重
        key = f"{chat_id}:{hash(text)}"
        now = datetime.now().timestamp()
        if key in self._last_sent and now - self._last_sent[key] < self._rate_limit:
            logger.debug(f"跳过重复消息: {key}")
            return True
        self._last_sent[key] = now

        # 发送
        if self.bot_manager and hasattr(self.bot_manager, "send_message_with_retry"):
            return await self.bot_manager.send_message_with_retry(
                chat_id, text, **kwargs
            )
        elif self.bot:
            try:
                await self.bot.send_message(chat_id, text, **kwargs)
                return True
            except Exception as e:
                logger.error(f"发送消息失败: {e}")
                return False
        return False

    async def send_document(
        self, chat_id: int, document, caption: str = "", **kwargs
    ) -> bool:
        """发送文档"""
        if self.bot_manager and hasattr(self.bot_manager, "send_document_with_retry"):
            return await self.bot_manager.send_document_with_retry(
                chat_id, document, caption=caption, **kwargs
            )
        elif self.bot:
            try:
                await self.bot.send_document(
                    chat_id, document, caption=caption, **kwargs
                )
                return True
            except Exception as e:
                logger.error(f"发送文档失败: {e}")
                return False
        return False

    async def send_with_push_settings(
        self, chat_id: int, text: str, notification_type: str = "all"
    ) -> bool:
        """根据推送设置发送"""
        if not self.db:
            return await self.send_message(chat_id, text)

        push = await self.db.get_push_settings()
        group = await self.db.get_group(chat_id)

        sent = False

        # 频道
        if push.get("enable_channel_push") and group and group.get("channel_id"):
            if await self.send_message(group["channel_id"], text):
                sent = True

        # 通知群组
        if (
            push.get("enable_group_push")
            and group
            and group.get("notification_group_id")
        ):
            if await self.send_message(group["notification_group_id"], text):
                sent = True

        # 管理员
        if not sent and push.get("enable_admin_push"):
            from config import Config

            for admin in Config.ADMINS:
                if await self.send_message(admin, text):
                    sent = True
                    break

        return sent

    async def notify_work(
        self,
        chat_id: int,
        user_id: int,
        user_name: str,
        checkin_time: str,
        expected_time: str,
        action: str,
        status: str,
        fine: int,
        shift: str,
        extra_group: Optional[int] = None,
    ):
        """发送上下班通知"""
        shift_text = "白班" if shift == "day" else "夜班"

        if action == "上班":
            title = "⚠️ 上班迟到" if fine > 0 else "✅ 上班打卡"
        else:
            title = "⚠️ 下班早退" if fine < 0 else "✅ 下班打卡"

        text = (
            f"{title} <code>{shift_text}</code>\n"
            f"👤 {self._format_user(user_id, user_name)}\n"
            f"⏰ 打卡: <code>{checkin_time}</code>\n"
            f"📅 期望: <code>{expected_time}</code>\n"
            f"📊 状态: {status}\n"
        )
        if fine:
            text += f"💰 罚款: <code>{fine}</code> 分"

        # 发送到主群
        await self.send_message(chat_id, text, parse_mode="HTML")

        # 发送到额外群组
        if extra_group:
            extra_text = f"<code>{shift_text}</code> {self._format_user(user_id, user_name)} {action}了！"
            if fine:
                extra_text += f" (罚款 {fine}分)"
            await self.send_message(extra_group, extra_text, parse_mode="HTML")

    async def notify_activity(
        self,
        chat_id: int,
        user_id: int,
        user_name: str,
        activity: str,
        action: str,
        extra: Dict[str, Any] = None,
    ):
        """发送活动通知"""
        if activity != "吃饭":
            return

        extra = extra or {}

        if action == "start":
            text = (
                f"🍽️ 吃饭通知 <code>{extra.get('shift', '白班')}</code>\n"
                f"{self._format_user(user_id, user_name)} 去吃饭了\n"
                f"⏰ {extra.get('time', '')}"
            )
        elif action == "end":
            text = (
                f"🍽️ 吃饭结束\n"
                f"{self._format_user(user_id, user_name)} 吃饭回来了\n"
                f"⏱️ 耗时: {extra.get('duration', '')}"
            )
        else:
            return

        await self.send_with_push_settings(chat_id, text, parse_mode="HTML")

    async def notify_overtime(
        self,
        chat_id: int,
        user_id: int,
        user_name: str,
        activity: str,
        elapsed: int,
        fine: int,
        shift: str,
    ):
        """发送超时通知"""
        shift_text = "白班" if shift == "day" else "夜班"
        text = (
            f"🚨 超时回座通知 <code>{shift_text}</code>\n"
            f"👤 {self._format_user(user_id, user_name)}\n"
            f"📝 活动: <code>{activity}</code>\n"
            f"⏱️ 时长: {self._format_duration(elapsed)}\n"
        )
        if fine:
            text += f"💰 罚款: <code>{fine}</code> 分"

        await self.send_with_push_settings(chat_id, text, parse_mode="HTML")

    async def notify_reset(
        self,
        chat_id: int,
        completed: Dict,
        reset_time: datetime,
    ):
        """发送重置通知"""
        text = (
            f"🔄 系统重置完成\n"
            f"⏰ {reset_time.strftime('%m/%d %H:%M')}\n"
            f"📊 结束活动: {completed.get('completed_count', 0)} 个\n"
            f"💰 总罚款: {completed.get('total_fines', 0)} 分"
        )

        details = completed.get("details", [])
        if details:
            text += "\n\n📋 详情:\n"
            for d in details[:5]:
                text += f"• {d.get('nickname', '用户')}: {d['activity']} "
                if d.get("fine"):
                    text += f"(罚款 {d['fine']})"
                text += "\n"
            if len(details) > 5:
                text += f"... 还有 {len(details)-5} 个"

        await self.send_with_push_settings(chat_id, text, parse_mode="HTML")

    async def notify_startup(self):
        """发送启动通知"""
        from config import Config

        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        text = f"🤖 机器人已启动\n⏰ {now}"

        for admin in Config.ADMINS:
            await self.send_message(admin, text)

    async def notify_shutdown(self, uptime: float):
        """发送关闭通知"""
        from config import Config

        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        uptime_str = self._format_duration(int(uptime))
        text = f"🛑 机器人已关闭\n⏰ {now}\n⏱️ 运行: {uptime_str}"

        for admin in Config.ADMINS:
            await self.send_message(admin, text)

    def _format_user(self, user_id: int, user_name: str) -> str:
        """格式化用户链接"""
        clean = str(user_name).replace("<", "").replace(">", "").replace("&", "")
        return f'<a href="tg://user?id={user_id}">{clean}</a>'

    def _format_duration(self, seconds: int) -> str:
        """格式化时长"""
        if not seconds:
            return "0秒"
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
        return "".join(parts)


# 全局实例
notification = NotificationService()
