"""
重置管理器 - 完整保留所有重置功能
"""

import logging
import asyncio
import time
from datetime import datetime, timedelta, date
from typing import Dict, Any, Optional, List

from cache_config import CacheKeys, TTL
from performance import global_cache

logger = logging.getLogger("GroupCheckInBot.ResetManager")


class ResetManager:
    """重置管理器 - 完整版"""

    def __init__(self, db, notification, dual_reset):
        self.db = db
        self.notification = notification
        self.dual_reset = dual_reset
        self._running = False
        self._task = None

    async def start(self):
        """启动重置任务"""
        self._running = True
        self._task = asyncio.create_task(self._run())
        logger.info("🔄 重置管理器已启动")

    async def stop(self):
        """停止重置任务"""
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("🛑 重置管理器已停止")

    async def _run(self):
        """运行重置循环"""
        while self._running:
            try:
                await self._check_resets()
                await asyncio.sleep(30)  # 30秒检查一次
            except Exception as e:
                logger.error(f"重置检查错误: {e}")
                await asyncio.sleep(60)

    async def _check_resets(self):
        """检查是否需要重置"""
        now = self.db.get_beijing_time()
        groups = await self.db.get_all_groups()

        logger.debug(
            f"检查重置，当前时间 {now.strftime('%H:%M:%S')}, 群组数 {len(groups)}"
        )

        for chat_id in groups:
            try:
                await self._check_group_reset(chat_id, now)
            except Exception as e:
                logger.error(f"检查群组 {chat_id} 重置失败: {e}")

    async def _check_group_reset(self, chat_id: int, now: datetime):
        """检查单个群组重置"""
        group = await self.db.get_group(chat_id)
        if not group:
            return

        reset_hour = group.get("reset_hour", 0)
        reset_minute = group.get("reset_minute", 0)

        # 检查硬重置
        if now.hour == reset_hour and now.minute == reset_minute:
            await self._do_hard_reset(chat_id, now)

        # 检查软重置
        soft_hour, soft_min = await self.db.get_group_soft_reset_time(chat_id)
        if soft_hour > 0 or soft_min > 0:
            if now.hour == soft_hour and now.minute == soft_min:
                await self._do_soft_reset(chat_id, now)

    async def _do_hard_reset(self, chat_id: int, now: datetime):
        """执行硬重置"""
        # 检查是否已执行
        reset_date = now.date()
        reset_key = CacheKeys.reset(chat_id, "hard", reset_date.strftime("%Y%m%d"))

        if global_cache.get(reset_key):
            logger.debug(f"群组 {chat_id} 今天已执行硬重置")
            return

        logger.info(f"🚀 开始硬重置 {chat_id}")
        start_time = time.time()

        try:
            # 获取配置
            config = await self.db.get_shift_config(chat_id)
            is_dual = config.get("dual_mode", False)

            # 计算目标日期
            business = await self.db.get_business_date(chat_id, now)
            target_date = business - timedelta(days=1)

            # 根据模式选择重置方式
            if is_dual:
                # 双班模式使用专用重置
                await self.dual_reset.handle_reset(chat_id, target_date=target_date)
            else:
                # 单班模式
                # 导出数据
                from data_export import data_exporter

                await data_exporter.export_group_data(
                    chat_id, target_date, is_daily_reset=True
                )

                # 完成所有活动
                completed = await self.db.complete_all_activities_before_reset(
                    chat_id, now
                )

                # 重置数据
                await self.db.reset_group(chat_id, target_date)

                # 发送通知
                await self.notification.notify_reset(chat_id, completed, now)

            # 记录已执行
            global_cache.set(reset_key, True, ttl=TTL["reset"])

            elapsed = time.time() - start_time
            logger.info(f"✅ 硬重置完成 {chat_id}, 耗时 {elapsed:.2f}秒")

        except Exception as e:
            logger.error(f"❌ 硬重置失败 {chat_id}: {e}")

    async def _do_soft_reset(self, chat_id: int, now: datetime):
        """执行软重置"""
        # 检查是否已执行
        reset_date = now.date()
        reset_key = CacheKeys.reset(chat_id, "soft", reset_date.strftime("%Y%m%d"))

        if global_cache.get(reset_key):
            logger.debug(f"群组 {chat_id} 今天已执行软重置")
            return

        logger.info(f"🔄 开始软重置 {chat_id}")

        try:
            # 获取所有用户
            users = await self.db.get_group_members(chat_id)
            reset_count = 0

            for user in users:
                try:
                    success = await self.db.reset_user_soft(chat_id, user["user_id"])
                    if success:
                        reset_count += 1
                except Exception as e:
                    logger.error(f"重置用户 {user['user_id']} 失败: {e}")

            # 停止定时器
            from activity_timer import timer_manager

            await timer_manager.stop_group(chat_id)

            # 发送通知
            text = (
                f"🔄 软重置完成\n"
                f"⏰ {now.strftime('%m/%d %H:%M')}\n"
                f"👥 重置用户：{reset_count} 人\n"
                f"⏱️ 定时器已取消"
            )
            await self.notification.send_with_push_settings(chat_id, text)

            # 记录已执行
            global_cache.set(reset_key, True, ttl=TTL["reset"])

            logger.info(f"✅ 软重置完成 {chat_id}, 重置 {reset_count} 人")

        except Exception as e:
            logger.error(f"❌ 软重置失败 {chat_id}: {e}")

    async def manual_reset(
        self,
        chat_id: int,
        mode: str = "hard",
        target_date: Optional[date] = None,
    ) -> Dict[str, Any]:
        """手动执行重置"""
        now = self.db.get_beijing_time()

        if mode == "hard":
            # 硬重置
            if target_date is None:
                business = await self.db.get_business_date(chat_id, now)
                target_date = business - timedelta(days=1)

            # 导出数据
            from data_export import data_exporter

            await data_exporter.export_group_data(
                chat_id, target_date, is_daily_reset=True
            )

            # 完成活动
            completed = await self.db.complete_all_activities_before_reset(chat_id, now)

            # 重置数据
            await self.db.reset_group(chat_id, target_date)

            # 清除缓存
            global_cache.delete(
                CacheKeys.reset(chat_id, "hard", target_date.strftime("%Y%m%d"))
            )

            return {
                "mode": "hard",
                "completed": completed,
                "target_date": target_date,
            }

        elif mode == "soft":
            # 软重置
            users = await self.db.get_group_members(chat_id)
            reset_count = 0

            for user in users:
                if await self.db.reset_user_soft(chat_id, user["user_id"]):
                    reset_count += 1

            # 停止定时器
            from activity_timer import timer_manager

            await timer_manager.stop_group(chat_id)

            # 清除缓存
            global_cache.delete(
                CacheKeys.reset(chat_id, "soft", now.date().strftime("%Y%m%d"))
            )

            return {
                "mode": "soft",
                "reset_count": reset_count,
            }

        else:
            raise ValueError(f"未知模式: {mode}")


# 全局实例
reset_manager = None


def init_reset_manager(db_instance, notification_instance, dual_reset_instance):
    """初始化重置管理器"""
    global reset_manager
    reset_manager = ResetManager(
        db_instance, notification_instance, dual_reset_instance
    )
    return reset_manager
