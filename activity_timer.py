"""
活动定时器管理 - 完整保留所有定时功能
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, Callable
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

logger = logging.getLogger("GroupCheckInBot.Timer")


class ActivityTimer:
    """单个活动定时器"""

    def __init__(
        self,
        chat_id: int,
        user_id: int,
        activity: str,
        time_limit: int,
        shift: str,
        callback: Callable,
    ):
        self.chat_id = chat_id
        self.user_id = user_id
        self.activity = activity
        self.time_limit = time_limit  # 分钟
        self.shift = shift
        self.callback = callback
        self.task: Optional[asyncio.Task] = None
        self.should_stop = False
        self.start_time = datetime.now()

        # 警告标记
        self.warned = set()
        self.force_sent = False

    async def start(self):
        """启动定时器"""
        self.task = asyncio.create_task(self._run())
        return self.task

    async def stop(self, preserve_message: bool = False):
        """停止定时器"""
        self.should_stop = True
        if self.task and not self.task.done():
            self.task.cancel()
            try:
                await self.task
            except asyncio.CancelledError:
                pass

    async def _run(self):
        """运行定时器"""
        while not self.should_stop:
            try:
                now = datetime.now()
                elapsed = (now - self.start_time).total_seconds()
                remaining = self.time_limit * 60 - elapsed

                # 1分钟警告
                if 0 < remaining <= 60 and "1min" not in self.warned:
                    self.warned.add("1min")
                    await self.callback(
                        self.chat_id,
                        self.user_id,
                        self.activity,
                        "warning",
                        1,
                        self.shift,
                    )

                # 超时提醒
                if remaining <= 0:
                    overtime = int(-remaining // 60)

                    # 即时超时
                    if overtime == 0 and "0min" not in self.warned:
                        self.warned.add("0min")
                        await self.callback(
                            self.chat_id,
                            self.user_id,
                            self.activity,
                            "timeout",
                            0,
                            self.shift,
                        )

                    # 5分钟超时
                    elif overtime == 5 and "5min" not in self.warned:
                        self.warned.add("5min")
                        await self.callback(
                            self.chat_id,
                            self.user_id,
                            self.activity,
                            "timeout",
                            5,
                            self.shift,
                        )

                    # 10分钟倍数
                    elif (
                        overtime >= 10
                        and overtime % 10 == 0
                        and f"{overtime}min" not in self.warned
                    ):
                        self.warned.add(f"{overtime}min")
                        await self.callback(
                            self.chat_id,
                            self.user_id,
                            self.activity,
                            "timeout",
                            overtime,
                            self.shift,
                        )

                    # 2小时强制结束
                    if overtime >= 120 and not self.force_sent:
                        self.force_sent = True
                        await self.callback(
                            self.chat_id,
                            self.user_id,
                            self.activity,
                            "force",
                            overtime,
                            self.shift,
                        )
                        break

                await asyncio.sleep(10)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"定时器运行错误: {e}")
                await asyncio.sleep(30)

    def get_info(self) -> Dict:
        """获取定时器信息"""
        elapsed = (datetime.now() - self.start_time).total_seconds()
        return {
            "chat_id": self.chat_id,
            "user_id": self.user_id,
            "activity": self.activity,
            "shift": self.shift,
            "elapsed": int(elapsed),
            "remaining": max(0, self.time_limit * 60 - int(elapsed)),
            "start_time": self.start_time.isoformat(),
        }


class TimerManager:
    """定时器管理器 - 完整版"""

    def __init__(self):
        self.timers: Dict[str, ActivityTimer] = {}
        self._callback: Optional[Callable] = None

    def set_callback(self, callback: Callable):
        """设置回调函数"""
        self._callback = callback

    async def start(
        self,
        chat_id: int,
        user_id: int,
        activity: str,
        time_limit: int,
        shift: str = "day",
    ) -> bool:
        """启动定时器"""
        if not self._callback:
            logger.error("未设置回调函数")
            return False

        key = f"{chat_id}-{user_id}"

        # 取消旧定时器
        if key in self.timers:
            await self.stop(key, preserve_message=False)

        timer = ActivityTimer(
            chat_id, user_id, activity, time_limit, shift, self._callback
        )
        self.timers[key] = timer
        await timer.start()
        logger.info(f"⏰ 启动定时器: {key} ({activity}, {shift})")
        return True

    async def stop(self, key: str, preserve_message: bool = False):
        """停止定时器"""
        if key in self.timers:
            await self.timers[key].stop(preserve_message)
            del self.timers[key]
            logger.info(f"⏹️ 停止定时器: {key}")

    async def stop_all(self, preserve_message: bool = False):
        """停止所有定时器"""
        for key in list(self.timers.keys()):
            await self.stop(key, preserve_message)

    async def stop_group(self, chat_id: int, preserve_message: bool = False):
        """停止群组所有定时器"""
        prefix = f"{chat_id}-"
        for key in list(self.timers.keys()):
            if key.startswith(prefix):
                await self.stop(key, preserve_message)

    async def stop_user(
        self, chat_id: int, user_id: int, preserve_message: bool = False
    ):
        """停止用户定时器"""
        key = f"{chat_id}-{user_id}"
        await self.stop(key, preserve_message)

    def get_timer(self, chat_id: int, user_id: int) -> Optional[Dict]:
        """获取定时器信息"""
        key = f"{chat_id}-{user_id}"
        timer = self.timers.get(key)
        return timer.get_info() if timer else None

    def get_all_timers(self) -> Dict[str, Dict]:
        """获取所有定时器信息"""
        return {k: t.get_info() for k, t in self.timers.items()}

    def count(self) -> int:
        """获取定时器数量"""
        return len(self.timers)

    def count_group(self, chat_id: int) -> int:
        """获取群组定时器数量"""
        prefix = f"{chat_id}-"
        return sum(1 for k in self.timers.keys() if k.startswith(prefix))


# 全局实例
timer_manager = TimerManager()
