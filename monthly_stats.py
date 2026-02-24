"""
月度统计模块 - 完整保留所有月度统计功能
"""

import logging
from datetime import datetime, timedelta, date
from typing import Dict, Any, List, Optional

from utils import MessageFormatter

logger = logging.getLogger("GroupCheckInBot.MonthlyStats")


class MonthlyStats:
    """月度统计管理器 - 完整版"""

    def __init__(self, db):
        self.db = db

    async def generate_report(
        self,
        chat_id: int,
        year: Optional[int] = None,
        month: Optional[int] = None,
    ) -> Optional[str]:
        """生成月度报告"""
        if year is None or month is None:
            now = self.db.get_beijing_time()
            year = now.year
            month = now.month

        # 获取统计数据
        stats = await self.db.get_monthly_statistics(chat_id, year, month)
        work_stats = await self.db.get_monthly_work_stats(chat_id, year, month)
        ranking = await self.db.get_monthly_ranking(chat_id, year, month)

        if not stats and not work_stats:
            return None

        # 获取群组信息
        chat_title = str(chat_id)
        try:
            from main import bot

            chat = await bot.get_chat(chat_id)
            chat_title = chat.title or chat_title
        except:
            pass

        # 构建报告
        now = self.db.get_beijing_time()
        report = (
            f"📊 <b>{year}年{month}月打卡统计报告</b>\n"
            f"🏢 群组：<code>{chat_title}</code>\n"
            f"📅 生成：<code>{now.strftime('%Y-%m-%d %H:%M:%S')}</code>\n"
            f"{MessageFormatter.create_dashed_line()}\n\n"
        )

        # 总体统计
        total_users = len(stats)
        total_time = sum(s.get("total_time", 0) for s in stats)
        total_count = sum(s.get("total_count", 0) for s in stats)
        total_fines = sum(s.get("total_fines", 0) for s in stats)
        total_work_days = sum(s.get("work_days", 0) for s in stats)
        total_work_hours = sum(s.get("work_hours", 0) for s in stats)

        report += (
            f"👥 <b>总体统计</b>\n"
            f"• 活跃用户：<code>{total_users}</code> 人\n"
            f"• 活动时长：<code>{MessageFormatter.format_time(int(total_time))}</code>\n"
            f"• 活动次数：<code>{total_count}</code> 次\n"
            f"• 工作天数：<code>{total_work_days}</code> 天\n"
            f"• 工作时长：<code>{MessageFormatter.format_time(int(total_work_hours))}</code>\n"
            f"• 扣除绩效：<code>{total_fines}</code> 分\n\n"
        )

        # 上下班统计
        total_start = sum(w.get("work_start_count", 0) for w in work_stats)
        total_end = sum(w.get("work_end_count", 0) for w in work_stats)
        total_work_fines = sum(
            w.get("work_start_fines", 0) + w.get("work_end_fines", 0)
            for w in work_stats
        )

        if total_start > 0 or total_end > 0:
            report += (
                f"🕒 <b>上下班统计</b>\n"
                f"• 上班打卡：<code>{total_start}</code> 次\n"
                f"• 下班打卡：<code>{total_end}</code> 次\n"
                f"• 上下班罚款：<code>{total_work_fines}</code> 分\n\n"
            )

        # 工作时长排行
        if stats:
            work_ranking = sorted(
                [s for s in stats if s.get("work_hours", 0) > 0],
                key=lambda x: x.get("work_hours", 0),
                reverse=True,
            )[:5]

            if work_ranking:
                report += f"👤 <b>工作时长排行</b>\n"
                for i, s in enumerate(work_ranking, 1):
                    hours = MessageFormatter.format_time(int(s.get("work_hours", 0)))
                    days = s.get("work_days", 0)
                    name = s.get("nickname", f"用户{s['user_id']}")
                    report += f"  <code>{i}.</code> {name} - {hours} ({days}天)\n"
                report += "\n"

        # 活动排行榜
        has_activity = False
        report += f"🏆 <b>活动排行榜</b>\n"

        for act, users in ranking.items():
            if users:
                has_activity = True
                report += f"📈 <code>{act}</code>：\n"
                for i, u in enumerate(users[:3], 1):
                    time_str = MessageFormatter.format_time(int(u.get("total_time", 0)))
                    count = u.get("total_count", 0)
                    name = u.get("nickname", "未知")
                    report += f"  <code>{i}.</code> {name} - {time_str} ({count}次)\n"
                report += "\n"

        if not has_activity:
            report += "暂无活动数据\n\n"

        # 月度总结
        if total_count > 0:
            avg_time = total_time / total_count
            report += f"• 平均每次活动：<code>{MessageFormatter.format_time(int(avg_time))}</code>\n"

        if total_work_days > 0:
            avg_work = total_work_hours / total_work_days
            report += f"• 平均每日工作：<code>{MessageFormatter.format_time(int(avg_work))}</code>\n"

        if total_users > 0:
            avg_activity = total_count / total_users
            report += f"• 人均活动次数：<code>{avg_activity:.1f}</code> 次\n"
            avg_work_days = total_work_days / total_users
            report += f"• 人均工作天数：<code>{avg_work_days:.1f}</code> 天\n"

        report += f"\n{MessageFormatter.create_dashed_line()}\n"
        report += f"💡 <i>基于月度统计表生成</i>"

        return report

    async def get_status(self, chat_id: int) -> str:
        """获取月度统计状态"""
        async with self.db.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT
                    DATE_TRUNC('month', statistic_date) AS month,
                    COUNT(*) AS records,
                    COUNT(DISTINCT user_id) AS users,
                    COUNT(DISTINCT activity_name) AS activities
                FROM monthly_statistics
                WHERE chat_id = $1
                GROUP BY month
                ORDER BY month DESC
            """,
                chat_id,
            )

            total = (
                await conn.fetchval(
                    "SELECT COUNT(*) FROM monthly_statistics WHERE chat_id = $1",
                    chat_id,
                )
                or 0
            )

            users = (
                await conn.fetchval(
                    "SELECT COUNT(DISTINCT user_id) FROM monthly_statistics WHERE chat_id = $1",
                    chat_id,
                )
                or 0
            )

        if not rows:
            return "📊 暂无月度统计数据"

        earliest = min(r["month"] for r in rows).strftime("%Y年%m月")
        latest = max(r["month"] for r in rows).strftime("%Y年%m月")

        text = (
            f"📊 <b>月度统计数据状态</b>\n\n"
            f"📅 数据范围：<code>{earliest} - {latest}</code>\n"
            f"👥 总用户数：<code>{users}</code> 人\n"
            f"💾 总记录数：<code>{total}</code> 条\n\n"
            f"<b>最近12个月：</b>\n"
        )

        for r in rows[:12]:
            month_str = r["month"].strftime("%Y年%m月")
            text += f"• {month_str}: <code>{r['records']}</code> 条, {r['users']} 人\n"

        if len(rows) > 12:
            text += f"• ... 还有 {len(rows) - 12} 个月\n"

        text += (
            "\n💡 <b>管理命令：</b>\n"
            "• <code>/cleanup_monthly</code> - 自动清理（保留90天）\n"
            "• <code>/cleanup_monthly 年 月</code> - 清理指定月份\n"
            "• <code>/cleanup_monthly all</code> - 清理所有数据"
        )

        return text


# 全局实例
monthly_stats = None


def init_monthly_stats(db_instance):
    """初始化月度统计"""
    global monthly_stats
    monthly_stats = MonthlyStats(db_instance)
    return monthly_stats


# 便捷函数
async def generate_monthly_report(
    chat_id: int, year: int = None, month: int = None
) -> Optional[str]:
    """生成月度报告"""
    return await monthly_stats.generate_report(chat_id, year, month)


async def get_monthly_stats_status(chat_id: int) -> str:
    """获取月度统计状态"""
    return await monthly_stats.get_status(chat_id)
