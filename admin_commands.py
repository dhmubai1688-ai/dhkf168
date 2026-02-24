"""
管理员命令处理器 - 完整保留所有管理员命令
"""

import logging
import asyncio
import re
from datetime import datetime, timedelta, date
from typing import Dict, Any, Optional

from aiogram import types
from aiogram.filters import Command
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton

logger = logging.getLogger("GroupCheckInBot.AdminCommands")


class AdminCommands:
    """管理员命令处理器 - 完整版"""

    def __init__(self, db, bot, notification, shift_manager, dual_reset):
        self.db = db
        self.bot = bot
        self.notification = notification
        self.shift_manager = shift_manager
        self.dual_reset = dual_reset

    # ========== 频道和群组设置 ==========
    async def cmd_setchannel(self, message: types.Message):
        """设置频道"""
        args = message.text.split(maxsplit=1)
        if len(args) < 2:
            await message.answer(
                "❌ 用法：/setchannel <频道ID>\n" "📝 示例：/setchannel -1001234567890"
            )
            return

        try:
            channel_id = int(args[1].strip())
            if channel_id > 0:
                await message.answer("❌ 频道ID应该是负数格式（如 -100xxx）")
                return

            await self.db.update_group_channel(message.chat.id, channel_id)
            await message.answer(
                f"✅ 已绑定频道：<code>{channel_id}</code>", parse_mode="HTML"
            )

        except ValueError:
            await message.answer("❌ 频道ID必须是数字")
        except Exception as e:
            await message.answer(f"❌ 绑定失败：{e}")

    async def cmd_setgroup(self, message: types.Message):
        """设置通知群组"""
        args = message.text.split(maxsplit=1)
        if len(args) < 2:
            await message.answer(
                "❌ 用法：/setgroup <群组ID>\n" "📝 示例：/setgroup -1001234567890"
            )
            return

        try:
            group_id = int(args[1].strip())
            await self.db.update_group_notification(message.chat.id, group_id)
            await message.answer(
                f"✅ 已绑定通知群组：<code>{group_id}</code>", parse_mode="HTML"
            )
        except ValueError:
            await message.answer("❌ 群组ID必须是数字")
        except Exception as e:
            await message.answer(f"❌ 绑定失败：{e}")

    async def cmd_addextrawork(self, message: types.Message):
        """添加上下班额外群组"""
        args = message.text.split(maxsplit=1)
        if len(args) < 2:
            await message.answer(
                "❌ 用法：/addextrawork <群组ID>\n"
                "📝 示例：/addextrawork -1001234567890"
            )
            return

        try:
            group_id = int(args[1].strip())
            if group_id > 0:
                await message.answer("❌ 群组ID应该是负数格式（如 -100xxx）")
                return

            await self.db.update_group_extra_work(message.chat.id, group_id)
            await message.answer(
                f"✅ 已添加上下班额外群组：<code>{group_id}</code>", parse_mode="HTML"
            )
        except ValueError:
            await message.answer("❌ 群组ID必须是数字")
        except Exception as e:
            await message.answer(f"❌ 设置失败：{e}")

    async def cmd_clearextrawork(self, message: types.Message):
        """清除额外群组"""
        try:
            old = await self.db.get_extra_work_group(message.chat.id)
            if not old:
                await message.answer("⚠️ 当前没有设置额外群组")
                return

            await self.db.clear_extra_work_group(message.chat.id)
            await message.answer(
                f"✅ 已清除额外群组 <code>{old}</code>", parse_mode="HTML"
            )
        except Exception as e:
            await message.answer(f"❌ 清除失败：{e}")

    async def cmd_showeverypush(self, message: types.Message):
        """显示所有推送配置"""
        chat_id = message.chat.id
        group = await self.db.get_group(chat_id) or {}
        push = await self.db.get_push_settings()
        extra = await self.db.get_extra_work_group(chat_id)

        text = (
            f"📢 推送配置总览\n\n"
            f"🔴 超时通知：{f'频道 {group.get("channel_id")}' if group.get('channel_id') else '未设置'}\n"
            f"🍽️ 吃饭通知：{f'群组 {group.get("notification_group_id")}' if group.get('notification_group_id') else '当前群组'}\n"
            f"🕒 上下班通知：当前群组 + {f'频道 {group.get("channel_id")}' if group.get('channel_id') else '无'}\n"
            f"📎 额外推送：{f'群组 {extra}' if extra else '未设置'}\n\n"
            f"⚙️ 推送开关：\n"
            f"• 频道推送：{'✅' if push.get('enable_channel_push') else '❌'}\n"
            f"• 群组推送：{'✅' if push.get('enable_group_push') else '❌'}\n"
            f"• 管理员推送：{'✅' if push.get('enable_admin_push') else '❌'}"
        )
        await message.answer(text, parse_mode="HTML")

    # ========== 时间设置 ==========
    async def cmd_setworktime(self, message: types.Message):
        """设置上下班时间"""
        args = message.text.split()
        if len(args) != 3:
            await message.answer(
                "❌ 用法：/setworktime <上班时间> <下班时间>\n"
                "📝 示例：/setworktime 09:00 18:00"
            )
            return

        start, end = args[1], args[2]
        pattern = re.compile(r"^([0-1]?[0-9]|2[0-3]):([0-5][0-9])$")

        if not pattern.match(start) or not pattern.match(end):
            await message.answer("❌ 时间格式错误，请使用 HH:MM 格式")
            return

        await self.db.update_group_work_time(message.chat.id, start, end)
        await message.answer(
            f"✅ 上下班时间设置成功\n"
            f"🟢 上班：<code>{start}</code>\n"
            f"🔴 下班：<code>{end}</code>",
            parse_mode="HTML",
        )

    async def cmd_setresettime(self, message: types.Message):
        """设置重置时间"""
        args = message.text.split()
        if len(args) != 3:
            await message.answer(
                "❌ 用法：/setresettime <小时> <分钟>\n" "📝 示例：/setresettime 4 0"
            )
            return

        try:
            hour, minute = int(args[1]), int(args[2])
            if not (0 <= hour <= 23 and 0 <= minute <= 59):
                await message.answer("❌ 小时必须在0-23，分钟必须在0-59")
                return

            await self.db.update_group_reset_time(message.chat.id, hour, minute)
            await message.answer(
                f"✅ 重置时间已设置为：<code>{hour:02d}:{minute:02d}</code>",
                parse_mode="HTML",
            )
        except ValueError:
            await message.answer("❌ 请输入有效的数字")

    async def cmd_setsoftresettime(self, message: types.Message):
        """设置软重置时间"""
        args = message.text.split()
        if len(args) != 3:
            await message.answer(
                "❌ 用法：/setsoftresettime <小时> <分钟>\n"
                "📝 示例：/setsoftresettime 12 0"
            )
            return

        try:
            hour, minute = int(args[1]), int(args[2])
            if not (0 <= hour <= 23 and 0 <= minute <= 59):
                await message.answer("❌ 小时必须在0-23，分钟必须在0-59")
                return

            await self.db.update_group_soft_reset_time(message.chat.id, hour, minute)
            if hour == 0 and minute == 0:
                await message.answer("✅ 软重置功能已禁用")
            else:
                await message.answer(
                    f"✅ 软重置时间已设置为：<code>{hour:02d}:{minute:02d}</code>",
                    parse_mode="HTML",
                )
        except ValueError:
            await message.answer("❌ 请输入有效的数字")

    async def cmd_resettime(self, message: types.Message):
        """查看重置时间"""
        chat_id = message.chat.id
        group = await self.db.get_group(chat_id) or {}
        reset_hour = group.get("reset_hour", 0)
        reset_minute = group.get("reset_minute", 0)
        soft_hour, soft_min = await self.db.get_group_soft_reset_time(chat_id)

        text = (
            f"⏰ 重置时间设置\n\n"
            f"🔄 硬重置：<code>{reset_hour:02d}:{reset_minute:02d}</code>\n"
            f"🔄 软重置：<code>{soft_hour:02d}:{soft_min:02d}</code>\n\n"
            f"💡 使用 /setresettime 修改硬重置时间\n"
            f"💡 使用 /setsoftresettime 修改软重置时间"
        )
        await message.answer(text, parse_mode="HTML")

    async def cmd_setshiftgrace(self, message: types.Message):
        """设置宽容窗口"""
        args = message.text.split()
        if len(args) != 3:
            await message.answer(
                "❌ 用法：/setshiftgrace <上班前分钟> <下班后分钟>\n"
                "📝 示例：/setshiftgrace 120 360"
            )
            return

        try:
            before, after = int(args[1]), int(args[2])
            if before < 0 or after < 0:
                await message.answer("❌ 时间不能为负数")
                return

            await self.db.update_shift_grace(message.chat.id, before, after)
            await message.answer(
                f"✅ 时间窗口已更新\n"
                f"• 上班前允许：<code>{before}</code>分钟\n"
                f"• 下班后允许：<code>{after}</code>分钟",
                parse_mode="HTML",
            )
        except ValueError:
            await message.answer("❌ 请输入有效的数字")

    async def cmd_setworkendgrace(self, message: types.Message):
        """设置下班专用窗口"""
        args = message.text.split()
        if len(args) != 3:
            await message.answer(
                "❌ 用法：/setworkendgrace <下班前分钟> <下班后分钟>\n"
                "📝 示例：/setworkendgrace 120 360"
            )
            return

        try:
            before, after = int(args[1]), int(args[2])
            if before < 0 or after < 0:
                await message.answer("❌ 时间不能为负数")
                return

            await self.db.update_workend_grace(message.chat.id, before, after)
            await message.answer(
                f"✅ 下班窗口已更新\n"
                f"• 下班前允许：<code>{before}</code>分钟\n"
                f"• 下班后允许：<code>{after}</code>分钟",
                parse_mode="HTML",
            )
        except ValueError:
            await message.answer("❌ 请输入有效的数字")

    async def cmd_worktime(self, message: types.Message):
        """查看工作时间"""
        chat_id = message.chat.id
        work = await self.db.get_group_work_time(chat_id)
        enabled = await self.db.has_work_hours_enabled(chat_id)

        status = "🟢 已启用" if enabled else "🔴 未启用"
        await message.answer(
            f"🕒 工作时间设置\n\n"
            f"📊 状态：{status}\n"
            f"🟢 上班：<code>{work['work_start']}</code>\n"
            f"🔴 下班：<code>{work['work_end']}</code>",
            parse_mode="HTML",
        )

    # ========== 活动管理 ==========
    async def cmd_addactivity(self, message: types.Message):
        """添加活动"""
        args = message.text.split()
        if len(args) != 4:
            await message.answer(
                "❌ 用法：/addactivity <活动名> <次数> <分钟>\n"
                "📝 示例：/addactivity 小厕 10 5"
            )
            return

        try:
            act, times, limit = args[1], int(args[2]), int(args[3])
            existed = await self.db.activity_exists(act)
            await self.db.update_activity(act, times, limit)
            await self.db.force_refresh_activity_cache()

            if existed:
                await message.answer(
                    f"✅ 已修改活动 <code>{act}</code>", parse_mode="HTML"
                )
            else:
                await message.answer(
                    f"✅ 已添加活动 <code>{act}</code>", parse_mode="HTML"
                )
        except ValueError:
            await message.answer("❌ 次数和分钟必须是数字")
        except Exception as e:
            await message.answer(f"❌ 添加失败：{e}")

    async def cmd_delactivity(self, message: types.Message):
        """删除活动"""
        args = message.text.split()
        if len(args) != 2:
            await message.answer("❌ 用法：/delactivity <活动名>")
            return

        act = args[1]
        if not await self.db.activity_exists(act):
            await message.answer(
                f"❌ 活动 <code>{act}</code> 不存在", parse_mode="HTML"
            )
            return

        await self.db.delete_activity(act)
        await self.db.force_refresh_activity_cache()
        await message.answer(f"✅ 已删除活动 <code>{act}</code>", parse_mode="HTML")

    async def cmd_actnum(self, message: types.Message):
        """设置活动人数限制"""
        args = message.text.split()
        if len(args) != 3:
            await message.answer(
                "❌ 用法：/actnum <活动名> <人数>\n" "📝 示例：/actnum 小厕 3"
            )
            return

        try:
            act, limit = args[1], int(args[2])
            if limit < 0:
                await message.answer("❌ 人数不能为负数")
                return

            if not await self.db.activity_exists(act):
                await message.answer(
                    f"❌ 活动 <code>{act}</code> 不存在", parse_mode="HTML"
                )
                return

            if limit == 0:
                await self.db.remove_activity_user_limit(act)
                await message.answer(
                    f"✅ 已取消活动 <code>{act}</code> 的人数限制", parse_mode="HTML"
                )
            else:
                await self.db.set_activity_user_limit(act, limit)
                current = await self.db.get_current_activity_users(message.chat.id, act)
                await message.answer(
                    f"✅ 已设置活动 <code>{act}</code> 人数限制为 <code>{limit}</code> 人\n"
                    f"当前进行：<code>{current}</code> 人\n"
                    f"剩余名额：<code>{limit - current}</code> 人",
                    parse_mode="HTML",
                )
        except ValueError:
            await message.answer("❌ 人数必须是数字")

    async def cmd_actstatus(self, message: types.Message):
        """查看活动状态"""
        chat_id = message.chat.id
        limits = await self.db.get_all_activity_limits()

        if not limits:
            await message.answer("📊 当前没有设置任何活动人数限制")
            return

        text = "📊 活动人数限制状态\n\n"
        for act, max_users in limits.items():
            current = await self.db.get_current_activity_users(chat_id, act)
            remaining = max(0, max_users - current) if max_users > 0 else "无限制"
            icon = "🟢" if remaining == "无限制" or remaining > 0 else "🔴"

            text += f"{icon} <code>{act}</code>\n"
            text += (
                f"   • 限制：<code>{max_users if max_users > 0 else '无限制'}</code>\n"
            )
            text += f"   • 当前：<code>{current}</code> 人\n"
            text += f"   • 剩余：<code>{remaining}</code> 人\n\n"

        await message.answer(text, parse_mode="HTML")

    # ========== 罚款管理 ==========
    async def cmd_setfine(self, message: types.Message):
        """设置单个活动罚款"""
        args = message.text.split()
        if len(args) != 4:
            await message.answer("❌ 用法：/setfine <活动名> <分钟> <金额>")
            return

        try:
            act, minutes, amount = args[1], args[2], int(args[3])
            if not await self.db.activity_exists(act):
                await message.answer(
                    f"❌ 活动 <code>{act}</code> 不存在", parse_mode="HTML"
                )
                return

            await self.db.update_fine(act, minutes, amount)
            await message.answer(
                f"✅ 已设置活动 <code>{act}</code> 罚款：\n"
                f"⏱️ {minutes}分钟 → 💰 {amount}分",
                parse_mode="HTML",
            )
        except ValueError:
            await message.answer("❌ 金额必须是数字")

    async def cmd_setfines_all(self, message: types.Message):
        """统一设置所有活动罚款"""
        args = message.text.split()
        if len(args) < 3 or (len(args) - 1) % 2 != 0:
            await message.answer(
                "❌ 用法：/setfines_all <分钟1> <金额1> [分钟2 金额2 ...]"
            )
            return

        try:
            pairs = args[1:]
            segments = {}
            for i in range(0, len(pairs), 2):
                t, f = int(pairs[i]), int(pairs[i + 1])
                if t <= 0 or f < 0:
                    await message.answer("❌ 分钟必须为正数，金额不能为负数")
                    return
                segments[str(t)] = f

            activities = await self.db.get_activity_configs()
            for act in activities.keys():
                for t, f in segments.items():
                    await self.db.update_fine(act, t, f)

            text = "✅ 已为所有活动设置分段罚款：\n" + "\n".join(
                f"• {t}分钟 → {f}分" for t, f in segments.items()
            )
            await message.answer(text, parse_mode="HTML")
        except ValueError:
            await message.answer("❌ 请输入有效的数字")

    async def cmd_finesstatus(self, message: types.Message):
        """查看罚款状态"""
        chat_id = message.chat.id
        activities = await self.db.get_activity_configs()
        fines = await self.db.get_fine_rates()
        work_fines = await self.db.get_work_fine_rates()

        text = "💰 罚款设置状态\n\n"

        for act in activities.keys():
            act_fines = fines.get(act, {})
            text += f"🔹 <code>{act}</code>\n"
            if act_fines:
                for t, f in sorted(act_fines.items(), key=lambda x: int(x[0])):
                    text += f"   • {t}分钟：{f}分\n"
            else:
                text += f"   • 未设置\n"
            text += "\n"

        text += "⏰ 上下班罚款\n"
        for ct in ["work_start", "work_end"]:
            ct_fines = work_fines.get(ct, {})
            name = "上班迟到" if ct == "work_start" else "下班早退"
            text += f"🔹 {name}\n"
            if ct_fines:
                for t, f in sorted(ct_fines.items(), key=lambda x: int(x[0])):
                    text += f"   • {t}分钟：{f}分\n"
            else:
                text += f"   • 未设置\n"
            text += "\n"

        await message.answer(text, parse_mode="HTML")

    async def cmd_setworkfine(self, message: types.Message):
        """设置上下班罚款"""
        args = message.text.split()
        if len(args) < 4 or (len(args) - 2) % 2 != 0:
            await message.answer(
                "❌ 用法：/setworkfine <work_start|work_end> <分钟1> <金额1> [分钟2 金额2 ...]"
            )
            return

        ct = args[1]
        if ct not in ["work_start", "work_end"]:
            await message.answer("❌ 类型必须是 work_start 或 work_end")
            return

        try:
            await self.db.clear_work_fine(ct)

            text = []
            for i in range(2, len(args), 2):
                m, a = int(args[i]), int(args[i + 1])
                await self.db.update_work_fine(ct, str(m), a)
                text.append(f"• {m}分钟 → {a}分")

            name = "上班迟到" if ct == "work_start" else "下班早退"
            await message.answer(
                f"✅ 已设置{name}罚款：\n" + "\n".join(text), parse_mode="HTML"
            )
        except ValueError:
            await message.answer("❌ 请输入有效的数字")

    # ========== 双班模式 ==========
    async def cmd_setdualmode(self, message: types.Message):
        """设置双班模式"""
        args = message.text.split()
        chat_id = message.chat.id

        if len(args) < 2:
            await message.answer(
                "❌ 用法：\n"
                "• 开启：/setdualmode on <开始时间> <结束时间>\n"
                "• 关闭：/setdualmode off"
            )
            return

        mode = args[1].lower()

        try:
            if mode == "on":
                if len(args) != 4:
                    await message.answer(
                        "❌ 开启需要指定时间：/setdualmode on 09:00 21:00"
                    )
                    return

                start, end = args[2], args[3]
                pattern = re.compile(r"^([0-1]?[0-9]|2[0-3]):([0-5][0-9])$")
                if not pattern.match(start) or not pattern.match(end):
                    await message.answer("❌ 时间格式错误")
                    return

                business = await self.db.get_business_date(chat_id)

                async with self.db.pool.acquire() as conn:
                    # 清理历史状态
                    await conn.execute(
                        "DELETE FROM shift_states WHERE chat_id = $1 AND record_date < $2",
                        chat_id,
                        business,
                    )
                    await self.db.update_group_dual_mode(chat_id, True, start, end)

                await message.answer(
                    f"✅ 双班模式已开启\n" f"📊 白班时间：<code>{start} - {end}</code>",
                    parse_mode="HTML",
                )

            elif mode == "off":
                await self.db.update_group_dual_mode(chat_id, False)
                await message.answer("✅ 双班模式已关闭")
            else:
                await message.answer("❌ 参数错误，请使用 on 或 off")

        except Exception as e:
            await message.answer(f"❌ 设置失败：{e}")

    async def cmd_checkdual(self, message: types.Message):
        """检查双班配置"""
        chat_id = message.chat.id
        group = await self.db.get_group(chat_id) or {}
        config = await self.db.get_shift_config(chat_id)

        reset_hour = group.get("reset_hour", 0)
        reset_minute = group.get("reset_minute", 0)
        is_dual = config.get("dual_mode", False)

        now = self.db.get_beijing_time()
        reset_today = now.replace(hour=reset_hour, minute=reset_minute, second=0)
        execute_time = reset_today + timedelta(hours=2)

        text = (
            f"🔍 双班配置检查\n\n"
            f"• 群组：<code>{chat_id}</code>\n"
            f"• 双班模式：{'✅ 开启' if is_dual else '❌ 关闭'}\n"
            f"• 重置时间：<code>{reset_hour:02d}:{reset_minute:02d}</code>\n"
            f"• 执行时间：<code>{execute_time.strftime('%H:%M')}</code>\n"
            f"• 当前时间：<code>{now.strftime('%H:%M:%S')}</code>\n"
        )

        if is_dual and now < execute_time:
            left = execute_time - now
            minutes = int(left.total_seconds() / 60)
            text += f"⏳ 距离执行还有：{minutes}分钟"

        await message.answer(text, parse_mode="HTML")

    # ========== 数据管理 ==========
    async def cmd_export(self, message: types.Message):
        """导出数据"""
        from data_export import export_group_data

        await message.answer("⏳ 正在导出数据...")
        try:
            await export_group_data(message.chat.id)
            await message.answer("✅ 数据已导出")
        except Exception as e:
            await message.answer(f"❌ 导出失败：{e}")

    async def cmd_exportmonthly(self, message: types.Message):
        """导出月度数据"""
        from data_export import export_monthly_data

        args = message.text.split()
        year, month = None, None

        if len(args) >= 3:
            try:
                year, month = int(args[1]), int(args[2])
                if not (1 <= month <= 12):
                    await message.answer("❌ 月份必须在1-12之间")
                    return
            except ValueError:
                await message.answer("❌ 请输入有效的年份和月份")
                return

        await message.answer("⏳ 正在导出月度数据...")
        try:
            await export_monthly_data(message.chat.id, year, month)
            await message.answer("✅ 月度数据已导出")
        except Exception as e:
            await message.answer(f"❌ 导出失败：{e}")

    async def cmd_monthlyreport(self, message: types.Message):
        """生成月度报告"""
        from monthly_stats import generate_monthly_report

        args = message.text.split()
        year, month = None, None

        if len(args) >= 3:
            try:
                year, month = int(args[1]), int(args[2])
            except ValueError:
                await message.answer("❌ 请输入有效的年份和月份")
                return

        await message.answer("⏳ 正在生成月度报告...")
        try:
            report = await generate_monthly_report(message.chat.id, year, month)
            if report:
                await message.answer(report, parse_mode="HTML")
            else:
                await message.answer("⚠️ 没有数据")
        except Exception as e:
            await message.answer(f"❌ 生成失败：{e}")

    async def cmd_cleanup_monthly(self, message: types.Message):
        """清理月度数据"""
        args = message.text.split()

        if len(args) >= 3:
            try:
                year, month = int(args[1]), int(args[2])
                deleted = await self.db.cleanup_specific_month(year, month)
                await message.answer(f"✅ 已清理 {deleted} 条记录")
            except ValueError:
                await message.answer("❌ 请输入有效的年份和月份")
        elif len(args) == 2 and args[1].lower() == "all":
            deleted = await self.db.cleanup_monthly(9999)
            await message.answer(f"✅ 已清理所有 {deleted} 条记录")
        else:
            deleted = await self.db.cleanup_monthly(90)
            await message.answer(f"✅ 已清理 {deleted} 条记录（保留90天）")

    async def cmd_monthly_stats_status(self, message: types.Message):
        """查看月度统计状态"""
        from monthly_stats import get_monthly_stats_status

        status = await get_monthly_stats_status(message.chat.id)
        await message.answer(status, parse_mode="HTML")

    async def cmd_cleanup_inactive(self, message: types.Message):
        """清理未活动用户"""
        args = message.text.split()
        days = 30

        if len(args) > 1:
            try:
                days = int(args[1])
                if days < 7:
                    await message.answer("❌ 天数不能少于7天")
                    return
            except ValueError:
                await message.answer("❌ 请输入有效的数字")
                return

        await message.answer(f"⏳ 正在清理 {days} 天未活动的用户...")
        deleted = await self.db.cleanup_inactive_users(days)
        await message.answer(f"✅ 已清理 {deleted} 个用户")

    async def cmd_resetuser(self, message: types.Message):
        """重置指定用户"""
        args = message.text.split()
        if len(args) < 2:
            await message.answer("❌ 用法：/resetuser <用户ID> [confirm]")
            return

        try:
            target = int(args[1])
            confirm = len(args) == 3 and args[2].lower() == "confirm"

            if not confirm:
                await message.answer(
                    f"⚠️ 确认重置用户 <code>{target}</code>？\n请输入 /resetuser {target} confirm",
                    parse_mode="HTML",
                )
                return

            success = await self.db.reset_user_daily(message.chat.id, target)
            if success:
                await message.answer(
                    f"✅ 已重置用户 <code>{target}</code> 的数据", parse_mode="HTML"
                )
            else:
                await message.answer(f"❌ 重置失败")
        except ValueError:
            await message.answer("❌ 用户ID必须是数字")

    # ========== 系统命令 ==========
    async def cmd_fixmessages(self, message: types.Message):
        """修复消息引用"""
        chat_id = message.chat.id

        result = await self.db.execute_with_retry(
            "fix_messages",
            "UPDATE users SET checkin_message_id = NULL WHERE chat_id = $1 AND checkin_message_id IS NOT NULL",
            chat_id,
        )
        count = self._parse_count(result)

        await message.answer(f"✅ 已清除 {count} 个消息引用")

    async def cmd_testgroupaccess(self, message: types.Message):
        """测试群组访问"""
        args = message.text.split()
        if len(args) < 2:
            await message.answer("❌ 用法：/testgroupaccess <群组ID>")
            return

        try:
            target = int(args[1])
            extra = await self.db.get_extra_work_group(message.chat.id)

            try:
                chat = await self.bot.get_chat(target)
                test = await self.bot.send_message(
                    target, f"🧪 测试消息 {datetime.now().strftime('%H:%M:%S')}"
                )
                await self.bot.delete_message(target, test.message_id)

                text = f"✅ 群组 <code>{target}</code> 可访问\n"
                text += f"• 标题：{chat.title}\n"
                text += f"• 类型：{chat.type}\n"
                if extra and extra == target:
                    text += f"✅ 与配置一致"
                elif extra:
                    text += f"⚠️ 配置的群组是 {extra}"

            except Exception as e:
                text = f"❌ 群组 <code>{target}</code> 访问失败\n"
                text += f"• 错误：{e}"

            await message.answer(text, parse_mode="HTML")
        except ValueError:
            await message.answer("❌ 群组ID必须是数字")

    async def cmd_checkperms(self, message: types.Message):
        """检查机器人权限"""
        chat_id = message.chat.id
        extra = await self.db.get_extra_work_group(chat_id)
        group = await self.db.get_group(chat_id) or {}

        text = f"🔍 机器人权限检查\n\n"
        text += f"🤖 ID: <code>{self.bot.id}</code>\n\n"

        # 当前群组
        try:
            member = await self.bot.get_chat_member(chat_id, self.bot.id)
            text += f"📊 当前群组 <code>{chat_id}</code>:\n"
            text += f"• 状态：{member.status}\n"
            text += f"• 管理员：{'是' if member.status in ['administrator', 'creator'] else '否'}\n"
        except Exception as e:
            text += f"❌ 无法获取权限: {e}\n"

        # 额外群组
        if extra:
            text += f"\n📊 额外群组 <code>{extra}</code>:\n"
            try:
                member = await self.bot.get_chat_member(extra, self.bot.id)
                text += f"• 状态：{member.status}\n"
            except Exception as e:
                text += f"• ❌ {e}\n"

        # 频道
        if group.get("channel_id"):
            text += f"\n📊 频道 <code>{group['channel_id']}</code>:\n"
            try:
                member = await self.bot.get_chat_member(
                    group["channel_id"], self.bot.id
                )
                text += f"• 状态：{member.status}\n"
            except Exception as e:
                text += f"• ❌ {e}\n"

        await message.answer(text, parse_mode="HTML")

    async def cmd_showsettings(self, message: types.Message):
        """显示所有设置"""
        chat_id = message.chat.id
        group = await self.db.get_group(chat_id) or {}
        activities = await self.db.get_activity_configs()
        fines = await self.db.get_fine_rates()
        work_fines = await self.db.get_work_fine_rates()
        work = await self.db.get_group_work_time(chat_id)
        config = await self.db.get_shift_config(chat_id)
        soft_hour, soft_min = await self.db.get_group_soft_reset_time(chat_id)
        extra = await self.db.get_extra_work_group(chat_id)

        text = (
            f"🔧 当前设置\n\n"
            f"📋 基本设置\n"
            f"• 频道：{group.get('channel_id', '未设置')}\n"
            f"• 通知群组：{group.get('notification_group_id', '未设置')}\n"
            f"• 额外群组：{extra or '未设置'}\n\n"
            f"⏰ 时间设置\n"
            f"• 重置：{group.get('reset_hour',0):02d}:{group.get('reset_minute',0):02d}\n"
            f"• 软重置：{soft_hour:02d}:{soft_min:02d}\n"
            f"• 上班：{work['work_start']}\n"
            f"• 下班：{work['work_end']}\n"
            f"• 双班：{'开启' if config.get('dual_mode') else '关闭'}\n\n"
            f"🎯 活动设置\n"
        )

        for act, v in activities.items():
            text += f"• {act}：{v['max_times']}次/{v['time_limit']}分钟\n"

        text += f"\n💰 罚款设置\n"
        for act, fs in fines.items():
            if fs:
                text += (
                    f"• {act}：" + " ".join(f"{k}:{v}分" for k, v in fs.items()) + "\n"
                )

        text += f"\n⏰ 上下班罚款\n"
        for ct, fs in work_fines.items():
            name = "上班" if ct == "work_start" else "下班"
            if fs:
                text += (
                    f"• {name}：" + " ".join(f"{k}:{v}分" for k, v in fs.items()) + "\n"
                )

        await message.answer(text, parse_mode="HTML")

    def _parse_count(self, result: str) -> int:
        """解析SQL结果"""
        if not result or not isinstance(result, str):
            return 0
        try:
            parts = result.split()
            if len(parts) >= 2 and parts[0] in ("UPDATE", "DELETE", "INSERT"):
                return int(parts[-1])
        except:
            pass
        return 0


# ========== 注册函数 ==========
def register_admin_commands(dp, admin_commands):
    """注册管理员命令"""

    # 频道和群组
    dp.message.register(admin_commands.cmd_setchannel, Command("setchannel"))
    dp.message.register(admin_commands.cmd_setgroup, Command("setgroup"))
    dp.message.register(admin_commands.cmd_addextrawork, Command("addextrawork"))
    dp.message.register(admin_commands.cmd_clearextrawork, Command("clearextrawork"))
    dp.message.register(admin_commands.cmd_showeverypush, Command("showeverypush"))

    # 时间设置
    dp.message.register(admin_commands.cmd_setworktime, Command("setworktime"))
    dp.message.register(admin_commands.cmd_setresettime, Command("setresettime"))
    dp.message.register(
        admin_commands.cmd_setsoftresettime, Command("setsoftresettime")
    )
    dp.message.register(admin_commands.cmd_resettime, Command("resettime"))
    dp.message.register(admin_commands.cmd_setshiftgrace, Command("setshiftgrace"))
    dp.message.register(admin_commands.cmd_setworkendgrace, Command("setworkendgrace"))
    dp.message.register(admin_commands.cmd_worktime, Command("worktime"))

    # 活动管理
    dp.message.register(admin_commands.cmd_addactivity, Command("addactivity"))
    dp.message.register(admin_commands.cmd_delactivity, Command("delactivity"))
    dp.message.register(admin_commands.cmd_actnum, Command("actnum"))
    dp.message.register(admin_commands.cmd_actstatus, Command("actstatus"))

    # 罚款管理
    dp.message.register(admin_commands.cmd_setfine, Command("setfine"))
    dp.message.register(admin_commands.cmd_setfines_all, Command("setfines_all"))
    dp.message.register(admin_commands.cmd_finesstatus, Command("finesstatus"))
    dp.message.register(admin_commands.cmd_setworkfine, Command("setworkfine"))

    # 双班模式
    dp.message.register(admin_commands.cmd_setdualmode, Command("setdualmode"))
    dp.message.register(admin_commands.cmd_checkdual, Command("checkdual"))

    # 数据管理
    dp.message.register(admin_commands.cmd_export, Command("export"))
    dp.message.register(admin_commands.cmd_exportmonthly, Command("exportmonthly"))
    dp.message.register(admin_commands.cmd_monthlyreport, Command("monthlyreport"))
    dp.message.register(admin_commands.cmd_cleanup_monthly, Command("cleanup_monthly"))
    dp.message.register(
        admin_commands.cmd_monthly_stats_status, Command("monthly_stats_status")
    )
    dp.message.register(
        admin_commands.cmd_cleanup_inactive, Command("cleanup_inactive")
    )
    dp.message.register(admin_commands.cmd_resetuser, Command("resetuser"))

    # 系统命令
    dp.message.register(admin_commands.cmd_fixmessages, Command("fixmessages"))
    dp.message.register(admin_commands.cmd_testgroupaccess, Command("testgroupaccess"))
    dp.message.register(admin_commands.cmd_checkperms, Command("checkperms"))
    dp.message.register(admin_commands.cmd_showsettings, Command("showsettings"))
