"""
双班重置 - 完整保留所有双班重置逻辑
"""

import logging
import asyncio
import time
import traceback
from datetime import datetime, timedelta, date
from typing import Dict, Optional, Any, List

logger = logging.getLogger("GroupCheckInBot.DualShiftReset")


class DualShiftReset:
    """双班重置管理器"""

    def __init__(self, db, notification):
        self.db = db
        self.notification = notification
        self._reset_cache = {}

    async def handle_reset(
        self,
        chat_id: int,
        operator_id: Optional[int] = None,
        target_date: Optional[date] = None,
    ) -> Optional[bool]:
        """
        硬重置总调度入口
        """
        try:
            config = await self.db.get_shift_config(chat_id)
            is_dual = config.get("dual_mode", False)

            if not is_dual:
                logger.info(f"单班模式: {chat_id}")
                return None

            logger.info(f"双班模式: {chat_id}")

            if target_date:
                return await self._do_reset(chat_id, operator_id, target_date)
            else:
                return await self._do_reset(chat_id, operator_id)

        except Exception as e:
            logger.error(f"重置调度失败 {chat_id}: {e}")
            logger.error(traceback.format_exc())
            return None

    async def _do_reset(
        self,
        chat_id: int,
        operator_id: Optional[int] = None,
        forced_target: Optional[date] = None,
    ) -> bool:
        """执行双班重置"""
        try:
            now = self.db.get_beijing_time()
            date_range = await self.db.get_business_date_range(chat_id, now)
            business_today = date_range["business_today"]
            business_yesterday = date_range["business_yesterday"]
            natural_today = date_range["natural_today"]

            logger.info(f"📅 日期: 自然今天={natural_today}, 业务今天={business_today}")

            # 确定目标日期
            if forced_target:
                target = forced_target
                logger.info(f"使用强制日期: {target}")
            else:
                group = await self.db.get_group(chat_id)
                reset_hour = group.get("reset_hour", 0)
                reset_minute = group.get("reset_minute", 0)

                reset_today = datetime.combine(
                    natural_today,
                    datetime.strptime(
                        f"{reset_hour:02d}:{reset_minute:02d}", "%H:%M"
                    ).time(),
                ).replace(tzinfo=now.tzinfo)

                execute_today = reset_today + timedelta(hours=2)
                execute_yesterday = reset_today - timedelta(days=1) + timedelta(hours=2)

                # 5分钟窗口
                window = 300
                if abs((now - execute_today).total_seconds()) <= window:
                    target = business_yesterday
                elif abs((now - execute_yesterday).total_seconds()) <= window:
                    target = business_yesterday - timedelta(days=1)
                else:
                    logger.debug("不在执行窗口内")
                    return False

            # 幂等检查
            reset_key = f"dual_reset:{chat_id}:{target}"
            if reset_key in self._reset_cache:
                logger.info(f"已执行过: {chat_id}")
                return True

            logger.info(f"🚀 开始重置 {chat_id}, 目标={target}")
            start_time = time.time()

            # 并发执行
            task1 = asyncio.create_task(
                self._force_end_activities(chat_id, now, target, business_today)
            )
            task2 = asyncio.create_task(
                self._complete_missing_work(chat_id, target, business_today)
            )

            results = await asyncio.gather(task1, task2, return_exceptions=True)

            force_stats = (
                results[0]
                if not isinstance(results[0], Exception)
                else {"total": 0, "success": 0}
            )
            complete_stats = (
                results[1]
                if not isinstance(results[1], Exception)
                else {"total": 0, "success": 0}
            )

            # 导出数据
            export_ok = await self._export_data(chat_id, target)

            # 清理数据
            cleanup = await self._cleanup_data(chat_id, target, business_today)

            # 清理班次状态
            deleted = await self.db.cleanup_expired_shifts(16)

            # 发送通知
            asyncio.create_task(
                self._send_notification(
                    chat_id, force_stats, complete_stats, export_ok, cleanup, now
                )
            )

            # 记录幂等
            self._reset_cache[reset_key] = True

            elapsed = time.time() - start_time
            logger.info(f"✅ 重置完成 {chat_id}, 耗时 {elapsed:.2f}秒")
            return True

        except Exception as e:
            logger.error(f"重置失败 {chat_id}: {e}")
            logger.error(traceback.format_exc())
            return False

    async def _force_end_activities(
        self, chat_id: int, now: datetime, target: date, business_today: date
    ) -> Dict:
        """强制结束所有活动"""
        stats = {"total": 0, "success": 0, "failed": 0, "fines": 0}

        async with self.db.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT user_id, nickname, current_activity, activity_start_time, shift
                FROM users WHERE chat_id = $1 AND current_activity IS NOT NULL
            """,
                chat_id,
            )

            stats["total"] = len(rows)

            for row in rows:
                try:
                    start = datetime.fromisoformat(str(row["activity_start_time"]))
                    elapsed = int((now - start).total_seconds())

                    limit = await self.db.get_activity_time_limit(
                        row["current_activity"]
                    )
                    is_overtime = elapsed > limit * 60
                    fine = 0

                    if is_overtime:
                        fine = await self.db.calculate_fine(
                            row["current_activity"], (elapsed - limit * 60) / 60
                        )

                    forced = (
                        target
                        if start.date() <= target
                        else business_today - timedelta(days=1)
                    )

                    await self.db.complete_activity(
                        chat_id,
                        row["user_id"],
                        row["current_activity"],
                        elapsed,
                        fine,
                        is_overtime,
                        row["shift"],
                        forced_date=forced,
                    )

                    stats["success"] += 1
                    stats["fines"] += fine

                except Exception as e:
                    logger.error(f"结束活动失败 {row['user_id']}: {e}")
                    stats["failed"] += 1

        return stats

    async def _complete_missing_work(
        self, chat_id: int, target: date, business_today: date
    ) -> Dict:
        """补全未打卡的下班记录"""
        stats = {"total": 0, "success": 0, "failed": 0, "fines": 0}

        async with self.db.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT user_id, shift, shift_detail, checkin_time
                FROM work_records
                WHERE chat_id = $1 AND record_date = $2
                  AND checkin_type = 'work_start'
                  AND NOT EXISTS (
                      SELECT 1 FROM work_records w2
                      WHERE w2.chat_id = work_records.chat_id
                        AND w2.user_id = work_records.user_id
                        AND w2.record_date = work_records.record_date
                        AND w2.shift = work_records.shift
                        AND w2.checkin_type = 'work_end'
                  )
            """,
                chat_id,
                target,
            )

            stats["total"] = len(rows)

            if not rows:
                return stats

            group = await self.db.get_group(chat_id)
            reset_hour = group.get("reset_hour", 0)
            reset_minute = group.get("reset_minute", 0)
            auto_end = f"{reset_hour:02d}:{reset_minute:02d}"

            config = await self.db.get_shift_config(chat_id)

            for row in rows:
                try:
                    if row["shift"] == "day":
                        expected = config.get("day_end", "18:00")
                        end_date = target
                    else:
                        expected = config.get("day_start", "09:00")
                        end_date = target + timedelta(days=1)

                    start = datetime.strptime(row["checkin_time"], "%H:%M").time()
                    start_dt = datetime.combine(target, start)
                    end_dt = datetime.combine(
                        end_date, datetime.strptime(auto_end, "%H:%M").time()
                    )

                    if end_dt < start_dt:
                        end_dt += timedelta(days=1)

                    diff = int((end_dt - start_dt).total_seconds() / 60)
                    expected_dt = datetime.combine(
                        end_date, datetime.strptime(expected, "%H:%M").time()
                    )
                    diff_expected = int((end_dt - expected_dt).total_seconds() / 60)

                    fine = 0
                    if diff_expected < 0:
                        rates = await self.db.get_work_fine_rates("work_end")
                        if rates:
                            for k in sorted([int(x) for x in rates.keys()]):
                                if abs(diff_expected) >= k:
                                    fine = rates[str(k)]

                    status = (
                        f"🚨 自动下班 (早退 {abs(diff_expected)}分钟)"
                        if diff_expected < 0
                        else "✅ 自动下班"
                    )

                    await self.db.add_work_record(
                        chat_id,
                        row["user_id"],
                        target,
                        "work_end",
                        auto_end,
                        status,
                        diff_expected,
                        fine,
                        row["shift"],
                        row.get("shift_detail"),
                    )

                    stats["success"] += 1
                    stats["fines"] += fine

                except Exception as e:
                    logger.error(f"补全失败 {row['user_id']}: {e}")
                    stats["failed"] += 1

        return stats

    async def _export_data(self, chat_id: int, target: date) -> bool:
        """导出数据"""
        try:
            from data_export import export_group_data

            return await export_group_data(
                chat_id, target, is_daily_reset=True, push_file=True
            )
        except Exception as e:
            logger.error(f"导出失败: {e}")
            return False

    async def _cleanup_data(self, chat_id: int, target: date, today: date) -> Dict:
        """清理数据"""
        stats = {"user_activities": 0, "work_records": 0, "daily_stats": 0}

        async with self.db.pool.acquire() as conn:
            async with conn.transaction():
                result = await conn.execute(
                    "DELETE FROM user_activities WHERE chat_id = $1 AND activity_date = $2",
                    chat_id,
                    target,
                )
                stats["user_activities"] = self._parse_count(result)

                result = await conn.execute(
                    "DELETE FROM work_records WHERE chat_id = $1 AND record_date = $2",
                    chat_id,
                    target,
                )
                stats["work_records"] = self._parse_count(result)

                result = await conn.execute(
                    "DELETE FROM daily_statistics WHERE chat_id = $1 AND record_date = $2",
                    chat_id,
                    target,
                )
                stats["daily_stats"] = self._parse_count(result)

        return stats

    async def recover_states(self):
        """恢复班次状态"""
        logger.info("🔄 恢复班次状态...")
        count = 0

        groups = await self.db.get_all_groups()
        for chat_id in groups:
            try:
                if not await self.db.is_dual_mode_enabled(chat_id):
                    continue

                async with self.db.pool.acquire() as conn:
                    rows = await conn.fetch(
                        """
                        SELECT user_id, shift, record_date
                        FROM work_records
                        WHERE chat_id = $1 AND checkin_type = 'work_start'
                          AND NOT EXISTS (
                              SELECT 1 FROM work_records w2
                              WHERE w2.chat_id = work_records.chat_id
                                AND w2.user_id = work_records.user_id
                                AND w2.record_date = work_records.record_date
                                AND w2.shift = work_records.shift
                                AND w2.checkin_type = 'work_end'
                          )
                    """,
                        chat_id,
                    )

                    for r in rows:
                        await self.db.set_shift_state(
                            chat_id, r["user_id"], r["shift"], r["record_date"]
                        )
                        count += 1
            except Exception as e:
                logger.error(f"恢复失败 {chat_id}: {e}")

        logger.info(f"✅ 恢复 {count} 个班次状态")
        return count

    async def _send_notification(
        self,
        chat_id: int,
        force: Dict,
        complete: Dict,
        export: bool,
        cleanup: Dict,
        reset_time: datetime,
    ):
        """发送通知"""
        try:
            text = (
                f"🔄 双班重置完成\n"
                f"⏰ {reset_time.strftime('%m/%d %H:%M')}\n"
                f"📊 结束活动: {force.get('success',0)}/{force.get('total',0)}\n"
                f"📝 补全下班: {complete.get('success',0)}/{complete.get('total',0)}\n"
                f"📎 导出: {'✅' if export else '❌'}\n"
                f"🗑️ 清理: {cleanup.get('user_activities',0)}条"
            )
            await self.notification.send_with_push_settings(chat_id, text)
        except Exception as e:
            logger.error(f"发送通知失败: {e}")

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


# 全局实例
dual_reset = None


def init_dual_reset(db_instance, notification_instance):
    """初始化双班重置"""
    global dual_reset
    dual_reset = DualShiftReset(db_instance, notification_instance)
    return dual_reset
