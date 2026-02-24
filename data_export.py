"""
数据导出模块 - 完整保留所有导出功能
"""

import logging
import csv
import os
import asyncio
import time
import aiofiles
from datetime import datetime, timedelta, date
from typing import Dict, Any, List, Optional
from io import StringIO

from aiogram.types import FSInputFile

logger = logging.getLogger("GroupCheckInBot.DataExport")


class DataExporter:
    """数据导出器 - 完整版"""

    def __init__(self, db, bot, notification):
        self.db = db
        self.bot = bot
        self.notification = notification

    async def export_group_data(
        self,
        chat_id: int,
        target_date: Optional[date] = None,
        file_name: Optional[str] = None,
        is_daily_reset: bool = False,
        push_file: bool = True,
    ) -> bool:
        """导出群组数据"""
        start_time = time.time()
        op_id = f"export_{chat_id}_{int(start_time)}"

        try:
            # 确定日期
            if target_date is None:
                now = self.db.get_beijing_time()
                business = await self.db.get_business_date(chat_id, now)
                config = await self.db.get_shift_config(chat_id)
                day_start = config.get("day_start", "09:00")
                day_start_h = int(day_start.split(":")[0])

                if now.hour < day_start_h:
                    target_date = business - timedelta(days=1)
                else:
                    target_date = business
            else:
                if hasattr(target_date, "date"):
                    target_date = target_date.date()

            # 生成文件名
            if not file_name:
                if is_daily_reset:
                    file_name = f"backup_{chat_id}_{target_date}.csv"
                else:
                    now_str = self.db.get_beijing_time().strftime("%Y%m%d_%H%M%S")
                    file_name = f"export_{chat_id}_{target_date}_{now_str}.csv"

            logger.info(f"📊 [{op_id}] 导出 {chat_id} 数据，日期={target_date}")

            # 获取统计
            stats = await self.db.get_group_stats(chat_id, target_date)
            if not stats:
                logger.warning(f"⚠️ [{op_id}] 没有数据")
                if not is_daily_reset:
                    await self.bot.send_message(chat_id, "⚠️ 当前没有数据需要导出")
                return True

            # 获取活动配置
            activities = await self.db.get_activity_configs()
            activity_names = sorted(activities.keys())

            # 生成CSV
            csv_buffer = StringIO()
            writer = csv.writer(csv_buffer, lineterminator="\n")

            # 表头
            headers = ["用户ID", "昵称", "班次"]
            for act in activity_names:
                headers.extend([f"{act}次数", f"{act}时长"])

            headers.extend(
                [
                    "总时长",
                    "总次数",
                    "罚款",
                    "上班次数",
                    "下班次数",
                    "上班罚款",
                    "下班罚款",
                    "迟到次数",
                    "早退次数",
                    "工作时长",
                    "工作天数",
                ]
            )
            writer.writerow(headers)

            # 数据行
            for s in stats:
                row = [
                    s["user_id"],
                    s.get("nickname", f"用户{s['user_id']}"),
                    "白班" if s.get("shift") == "day" else "夜班",
                ]

                # 活动数据
                acts = s.get("activities", {})
                if isinstance(acts, str):
                    try:
                        import json

                        acts = json.loads(acts)
                    except:
                        acts = {}

                for act in activity_names:
                    act_data = acts.get(act, {})
                    if isinstance(act_data, dict):
                        count = act_data.get("count", 0)
                        time_sec = act_data.get("time", 0)
                    else:
                        count, time_sec = 0, 0
                    row.extend([count, self._format_time(time_sec)])

                # 统计数据
                row.extend(
                    [
                        self._format_time(s.get("total_time", 0)),
                        s.get("total_count", 0),
                        s.get("total_fines", 0),
                        s.get("work_start_count", 0),
                        s.get("work_end_count", 0),
                        s.get("work_start_fines", 0),
                        s.get("work_end_fines", 0),
                        s.get("late_count", 0),
                        s.get("early_count", 0),
                        self._format_time(s.get("work_hours", 0)),
                        s.get("work_days", 0),
                    ]
                )

                writer.writerow(row)

            csv_content = csv_buffer.getvalue()
            csv_buffer.close()

            # 保存文件
            temp_file = f"temp_{op_id}_{file_name}"
            async with aiofiles.open(temp_file, "w", encoding="utf-8-sig") as f:
                await f.write(csv_content)

            # 获取群组标题
            try:
                chat = await self.bot.get_chat(chat_id)
                chat_title = chat.title or f"群组{chat_id}"
            except:
                chat_title = f"群组{chat_id}"

            # 构建说明
            caption = (
                f"📊 数据导出\n"
                f"🏢 {chat_title}\n"
                f"📅 {target_date}\n"
                f"⏰ {self.db.get_beijing_time().strftime('%Y-%m-%d %H:%M:%S')}\n"
                f"👥 {len(stats)} 条记录"
            )

            # 发送文件
            doc = FSInputFile(temp_file, filename=file_name)

            if push_file:
                await self.bot.send_document(
                    chat_id, doc, caption=caption, parse_mode="HTML"
                )

            # 推送到通知服务
            await self.notification.send_document(chat_id, doc, caption=caption)

            # 清理
            os.remove(temp_file)

            elapsed = time.time() - start_time
            logger.info(f"✅ [{op_id}] 导出完成，耗时 {elapsed:.2f}秒")
            return True

        except Exception as e:
            logger.error(f"❌ [{op_id}] 导出失败: {e}")
            if not is_daily_reset:
                await self.bot.send_message(chat_id, f"❌ 导出失败: {e}")
            return False

    async def export_monthly_data(
        self,
        chat_id: int,
        year: Optional[int] = None,
        month: Optional[int] = None,
        file_name: Optional[str] = None,
    ) -> bool:
        """导出月度数据"""
        start_time = time.time()

        try:
            if year is None or month is None:
                now = self.db.get_beijing_time()
                year = now.year
                month = now.month

            if not file_name:
                file_name = f"monthly_{chat_id}_{year}{month:02d}.csv"

            logger.info(f"📊 导出月度数据 {chat_id} {year}年{month}月")

            # 获取月度统计
            stats = await self.db.get_monthly_statistics(chat_id, year, month)
            if not stats:
                logger.warning("没有数据")
                return False

            # 获取活动配置
            activities = await self.db.get_activity_configs()
            activity_names = sorted(activities.keys())

            # 生成CSV
            csv_buffer = StringIO()
            writer = csv.writer(csv_buffer, lineterminator="\n")

            # 表头
            headers = ["用户ID", "昵称"]
            for act in activity_names:
                headers.extend([f"{act}次数", f"{act}时长"])

            headers.extend(
                [
                    "总时长",
                    "总次数",
                    "罚款",
                    "上班次数",
                    "下班次数",
                    "上班罚款",
                    "下班罚款",
                    "迟到次数",
                    "早退次数",
                    "工作时长",
                    "工作天数",
                    "超时次数",
                    "超时时长",
                ]
            )
            writer.writerow(headers)

            # 数据行
            for s in stats:
                row = [
                    s["user_id"],
                    s.get("nickname", f"用户{s['user_id']}"),
                ]

                acts = s.get("activities", {})
                if isinstance(acts, str):
                    try:
                        import json

                        acts = json.loads(acts)
                    except:
                        acts = {}

                for act in activity_names:
                    act_data = acts.get(act, {})
                    if isinstance(act_data, dict):
                        count = act_data.get("count", 0)
                        time_sec = act_data.get("time", 0)
                    else:
                        count, time_sec = 0, 0
                    row.extend([count, self._format_time(time_sec)])

                row.extend(
                    [
                        self._format_time(s.get("total_time", 0)),
                        s.get("total_count", 0),
                        s.get("total_fines", 0),
                        s.get("work_start_count", 0),
                        s.get("work_end_count", 0),
                        s.get("work_start_fines", 0),
                        s.get("work_end_fines", 0),
                        s.get("late_count", 0),
                        s.get("early_count", 0),
                        self._format_time(s.get("work_hours", 0)),
                        s.get("work_days", 0),
                        s.get("overtime_count", 0),
                        self._format_time(s.get("overtime_time", 0)),
                    ]
                )

                writer.writerow(row)

            csv_content = csv_buffer.getvalue()
            csv_buffer.close()

            # 保存文件
            temp_file = f"temp_{file_name}"
            async with aiofiles.open(temp_file, "w", encoding="utf-8-sig") as f:
                await f.write(csv_content)

            # 获取群组标题
            try:
                chat = await self.bot.get_chat(chat_id)
                chat_title = chat.title or f"群组{chat_id}"
            except:
                chat_title = f"群组{chat_id}"

            # 构建说明
            caption = (
                f"📊 月度数据导出\n"
                f"🏢 {chat_title}\n"
                f"📅 {year}年{month}月\n"
                f"⏰ {self.db.get_beijing_time().strftime('%Y-%m-%d %H:%M:%S')}\n"
                f"👥 {len(stats)} 条记录"
            )

            # 发送文件
            doc = FSInputFile(temp_file, filename=file_name)
            await self.bot.send_document(
                chat_id, doc, caption=caption, parse_mode="HTML"
            )

            # 推送到通知服务
            await self.notification.send_document(chat_id, doc, caption=caption)

            # 清理
            os.remove(temp_file)

            elapsed = time.time() - start_time
            logger.info(f"✅ 月度导出完成，耗时 {elapsed:.2f}秒")
            return True

        except Exception as e:
            logger.error(f"❌ 月度导出失败: {e}")
            return False

    async def export_and_push_csv(
        self,
        chat_id: int,
        target_date: Optional[date] = None,
        file_name: Optional[str] = None,
        is_daily_reset: bool = False,
        from_monthly: bool = False,
        push_file: bool = True,
    ) -> bool:
        """导出并推送CSV（兼容旧接口）"""
        if from_monthly:
            if target_date:
                return await self.export_monthly_data(
                    chat_id, target_date.year, target_date.month, file_name
                )
            else:
                return await self.export_monthly_data(chat_id, file_name=file_name)
        else:
            return await self.export_group_data(
                chat_id, target_date, file_name, is_daily_reset, push_file
            )

    def _format_time(self, seconds: int) -> str:
        """格式化时间"""
        if not seconds:
            return "0分0秒"
        h = seconds // 3600
        m = (seconds % 3600) // 60
        s = seconds % 60
        if h:
            return f"{h}时{m}分{s}秒"
        return f"{m}分{s}秒"


# 全局实例
data_exporter = None


def init_data_exporter(db_instance, bot_instance, notification_instance):
    """初始化数据导出器"""
    global data_exporter
    data_exporter = DataExporter(db_instance, bot_instance, notification_instance)
    return data_exporter
