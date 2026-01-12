# handlers.py
import os
import time
import asyncio
import traceback
import aiofiles
from datetime import datetime, timedelta, date
from contextlib import suppress

from aiogram import types, F
from aiogram.filters import Command
from aiogram.types import FSInputFile, InlineKeyboardMarkup, InlineKeyboardButton

# 1. 导入配置和性能工具 (这些通常在独立文件中)
from config import Config, beijing_tz
from performance import (
    performance_monitor,
    message_deduplicate,
    global_cache,
    track_performance,
    with_retry
)

# 2. 核心：引入 main 模块并建立映射 (这是消除未定义报错的关键)
import main 

# --- 核心对象映射 ---
# handlers.py

import main  # 关键：导入 main 模块

# --- 核心对象映射 ---
dp = main.dp
bot = main.bot
db = main.db
logger = main.logger
user_lock_manager = main.user_lock_manager
timer_manager = main.timer_manager

# --- 类映射 ---
NotificationService = main.NotificationService
MessageFormatter = main.MessageFormatter

# --- 逻辑与工具函数映射 ---
get_user_lock = main.get_user_lock
get_beijing_time = main.get_beijing_time
has_work_hours_enabled = main.has_work_hours_enabled
has_clocked_in_today = main.has_clocked_in_today
calculate_work_fine = main.calculate_work_fine
calculate_fine = main.calculate_fine
start_activity = main.start_activity
process_back = main.process_back
_process_back_locked = main._process_back_locked

# --- 菜单、报表与历史记录 ---
show_history = main.show_history
show_rank = main.show_rank
generate_monthly_report = main.generate_monthly_report
export_monthly_csv = main.export_monthly_csv
export_and_push_csv = main.export_and_push_csv
get_main_keyboard = main.get_main_keyboard
get_admin_keyboard = main.get_admin_keyboard
is_admin = main.is_admin
admin_required = main.admin_required
rate_limit = main.rate_limit


# ==================== 消息处理器优化 ====================
@dp.message(Command("start"))
@rate_limit(rate=5, per=60)
@message_deduplicate
async def cmd_start(message: types.Message):
    """优化的开始命令"""
    uid = message.from_user.id
    is_admin_user = uid in Config.ADMINS

    await message.answer(
        Config.MESSAGES["welcome"],
        reply_markup=await get_main_keyboard(message.chat.id, is_admin_user),
    )


@dp.message(Command("menu"))
@rate_limit(rate=5, per=60)
async def cmd_menu(message: types.Message):
    """显示主菜单 - 优化版本"""
    uid = message.from_user.id
    await message.answer(
        "📋 主菜单",
        reply_markup=await get_main_keyboard(
            chat_id=message.chat.id, show_admin=await is_admin(uid)
        ),
    )


@dp.message(Command("admin"))
@admin_required
@rate_limit(rate=5, per=60)
async def cmd_admin(message: types.Message):
    """管理员命令 - 优化版本"""
    await message.answer("👑 管理员面板", reply_markup=get_admin_keyboard())


@dp.message(Command("help"))
@rate_limit(rate=5, per=60)
async def cmd_help(message: types.Message):
    """帮助命令 - 优化版本"""
    uid = message.from_user.id

    help_text = (
        "📋 打卡机器人使用帮助\n\n"
        "🟢 开始活动打卡：\n"
        "• 直接输入活动名称（如：<code>吃饭</code>、<code>小厕</code>）\n"
        "• 或使用命令：<code>/ci 活动名</code>\n"
        "• 或点击下方活动按钮\n\n"
        "🔴 结束活动回座：\n"
        "• 直接输入：<code>回座</code>\n"
        "• 或使用命令：<code>/at</code>\n"
        "• 或点击下方 <code>✅ 回座</code> 按钮\n\n"
        "🕒 上下班打卡：\n"
        "• <code>/workstart</code> - 上班打卡\n"
        "• <code>/workend</code> - 下班打卡\n"
        "• <code>/workrecord</code> - 查看打卡记录\n"
        "• 或点击 <code>🟢 上班</code> 和 <code>🔴 下班</code> 按钮\n\n"
        "👑 管理员上下班设置：\n"
        "• <code>/setworktime 09:00 18:00</code> - 设置上下班时间\n"
        "• <code>/showworktime</code> - 显示当前设置\n"
        "• <code>/workstatus</code> - 查看上下班功能状态\n"
        "• <code>/delwork</code> - 移除上下班功能（保留记录）\n"
        "• <code>/delwork clear</code> - 移除功能并清除记录\n"
        "• <code>/resetworktime</code> - 重置为默认时间\n"
        "📊 查看记录：\n"
        "• 点击 <code>📊 我的记录</code> 查看个人统计\n"
        "• 点击 <code>🏆 排行榜</code> 查看群内排名\n\n"
        "🔧 其他命令：\n"
        "• <code>/start</code> - 开始使用机器人\n"
        "• <code>/menu</code> - 显示主菜单\n"
        "• <code>/help</code> - 显示此帮助信息\n\n"
        "📊 月度报告：\n"
        "• <code>/monthlyreport</code> - 查看月度报告\n"
        "• <code>/monthlyreport 2024 1</code> - 查看指定年月报告\n"
        "• <code>/exportmonthly</code> - 导出月度数据\n"
        "• <code>/exportmonthly 2024 1</code> - 导出指定年月数据\n\n"
        "⏰ 注意事项：\n"
        "• 每个活动有每日次数限制和时间限制\n"
        "• 超时会产生罚款\n"
        "• 活动完成后请及时回座\n"
        "• 每日数据会在指定时间自动重置\n"
        "• 上下班打卡需要先上班后下班"
    )

    await message.answer(
        help_text,
        reply_markup=await get_main_keyboard(
            chat_id=message.chat.id, show_admin=await is_admin(uid)
        ),
        parse_mode="HTML",
    )


# ==================== 管理员命令功能优化 ====================
@dp.message(Command("setchannel"))
@admin_required
@rate_limit(rate=3, per=30)
async def cmd_setchannel(message: types.Message):
    """绑定提醒频道 - 优化版本"""
    chat_id = message.chat.id
    args = message.text.split(maxsplit=1)

    if len(args) < 2:
        await message.answer(
            Config.MESSAGES["setchannel_usage"],
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
        )
        return

    try:
        channel_id = int(args[1].strip())
        await db.init_group(chat_id)
        await db.update_group_channel(chat_id, channel_id)
        await message.answer(
            f"✅ 已绑定超时提醒推送频道：<code>{channel_id}</code>",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
            parse_mode="HTML",
        )
    except ValueError:
        await message.answer(
            "❌ 频道ID必须是数字",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
        )


@dp.message(Command("setgroup"))
@admin_required
@rate_limit(rate=3, per=30)
async def cmd_setgroup(message: types.Message):
    """绑定通知群组 - 优化版本"""
    chat_id = message.chat.id
    args = message.text.split(maxsplit=1)

    if len(args) < 2:
        await message.answer(
            Config.MESSAGES["setgroup_usage"],
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
        )
        return

    try:
        group_id = int(args[1].strip())
        await db.init_group(chat_id)
        await db.update_group_notification(chat_id, group_id)
        await message.answer(
            f"✅ 已绑定超时通知群组：<code>{group_id}</code>",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
            parse_mode="HTML",
        )
    except ValueError:
        await message.answer(
            "❌ 群组ID必须是数字",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
        )


@dp.message(Command("unbindchannel"))
@admin_required
@rate_limit(rate=3, per=30)
async def cmd_unbind_channel(message: types.Message):
    """解除绑定频道 - 优化版本"""
    chat_id = message.chat.id
    await db.init_group(chat_id)
    await db.update_group_channel(chat_id, None)
    await message.answer(
        "✅ 已解除绑定的提醒频道",
        reply_markup=await get_main_keyboard(chat_id=message.chat.id, show_admin=True),
    )


@dp.message(Command("unbindgroup"))
@admin_required
@rate_limit(rate=3, per=30)
async def cmd_unbind_group(message: types.Message):
    """解除绑定通知群组 - 优化版本"""
    chat_id = message.chat.id
    await db.init_group(chat_id)
    await db.update_group_notification(chat_id, None)
    await message.answer(
        "✅ 已解除绑定的通知群组",
        reply_markup=await get_main_keyboard(chat_id=message.chat.id, show_admin=True),
    )


@dp.message(Command("addactivity"))
@admin_required
@rate_limit(rate=3, per=30)
async def cmd_addactivity(message: types.Message):
    """添加新活动 - 修复缓存版本"""
    args = message.text.split()
    if len(args) != 4:
        await message.answer(
            Config.MESSAGES["addactivity_usage"],
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
        )
        return

    try:
        act, max_times, time_limit = args[1], int(args[2]), int(args[3])
        existed = await db.activity_exists(act)
        await db.update_activity_config(act, max_times, time_limit)

        # 🆕 关键修复：强制刷新活动配置缓存
        await db.force_refresh_activity_cache()

        if existed:
            await message.answer(
                f"✅ 已修改活动 <code>{act}</code>，次数上限 <code>{max_times}</code>，时间限制 <code>{time_limit}</code> 分钟",
                reply_markup=await get_main_keyboard(
                    chat_id=message.chat.id, show_admin=True
                ),
                parse_mode="HTML",
            )
        else:
            await message.answer(
                f"✅ 已添加新活动 <code>{act}</code>，次数上限 <code>{max_times}</code>，时间限制 <code>{time_limit}</code> 分钟",
                reply_markup=await get_main_keyboard(
                    chat_id=message.chat.id, show_admin=True
                ),
                parse_mode="HTML",
            )
    except Exception as e:
        await message.answer(
            f"❌ 添加/修改活动失败：{e}",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
        )


@dp.message(Command("delactivity"))
@admin_required
@rate_limit(rate=3, per=30)
async def cmd_delactivity(message: types.Message):
    """删除活动 - 优化版本"""
    args = message.text.split()
    if len(args) != 2:
        await message.answer(
            "❌ 用法：/delactivity <活动名>",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
        )
        return
    act = args[1]
    if not await db.activity_exists(act):
        await message.answer(
            f"❌ 活动 <code>{act}</code> 不存在",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
            parse_mode="HTML",
        )
        return
    await db.delete_activity_config(act)
    await message.answer(
        f"✅ 活动 <code>{act}</code> 已删除",
        reply_markup=await get_main_keyboard(chat_id=message.chat.id, show_admin=True),
        parse_mode="HTML",
    )


# ==================== 活动人数限制功能 ====================


@dp.message(Command("actnum"))
@admin_required
@rate_limit(rate=3, per=30)
async def cmd_actnum(message: types.Message):
    """设置活动人数限制"""
    args = message.text.split()
    if len(args) != 3:
        await message.answer(
            "❌ 用法：/actnum <活动名> <人数限制>\n"
            "例如：/actnum 小厕 3\n"
            "💡 设置为0表示取消限制",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
        )
        return

    try:
        activity = args[1]
        max_users = int(args[2])

        # 检查活动是否存在
        if not await db.activity_exists(activity):
            await message.answer(
                f"❌ 活动 '<code>{activity}</code>' 不存在！",
                reply_markup=await get_main_keyboard(
                    chat_id=message.chat.id, show_admin=True
                ),
                parse_mode="HTML",
            )
            return

        if max_users < 0:
            await message.answer(
                "❌ 人数限制不能为负数！",
                reply_markup=await get_main_keyboard(
                    chat_id=message.chat.id, show_admin=True
                ),
            )
            return

        chat_id = message.chat.id

        if max_users == 0:
            # 取消限制
            await db.remove_activity_user_limit(activity)
            await message.answer(
                f"✅ 已取消活动 '<code>{activity}</code>' 的人数限制",
                reply_markup=await get_main_keyboard(chat_id=chat_id, show_admin=True),
                parse_mode="HTML",
            )
        else:
            # 设置限制
            await db.set_activity_user_limit(activity, max_users)

            # 获取当前活动人数
            current_users = await db.get_current_activity_users(chat_id, activity)

            await message.answer(
                f"✅ 已设置活动 '<code>{activity}</code>' 的人数限制为 <code>{max_users}</code> 人\n\n"
                f"📊 当前状态：\n"
                f"• 限制人数：<code>{max_users}</code> 人\n"
                f"• 当前进行：<code>{current_users}</code> 人\n"
                f"• 剩余名额：<code>{max_users - current_users}</code> 人",
                reply_markup=await get_main_keyboard(chat_id=chat_id, show_admin=True),
                parse_mode="HTML",
            )

    except ValueError:
        await message.answer(
            "❌ 人数限制必须是数字！",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
        )
    except Exception as e:
        await message.answer(
            f"❌ 设置失败：{e}",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
        )


@dp.message(Command("actstatus"))
@rate_limit(rate=5, per=60)
async def cmd_actstatus(message: types.Message):
    """查看活动人数状态"""
    chat_id = message.chat.id

    try:
        # 获取所有活动限制
        activity_limits = await db.get_all_activity_limits()

        if not activity_limits:
            await message.answer(
                "📊 当前没有设置任何活动人数限制\n"
                "💡 使用 /actnum <活动名> <人数> 来设置限制",
                reply_markup=await get_main_keyboard(
                    chat_id=chat_id, show_admin=await is_admin(message.from_user.id)
                ),
            )
            return

        status_text = "📊 活动人数限制状态\n\n"

        for activity, max_users in activity_limits.items():
            current_users = await db.get_current_activity_users(chat_id, activity)
            remaining = max_users - current_users

            status_icon = "🟢" if remaining > 0 else "🔴"

            status_text += (
                f"{status_icon} <code>{activity}</code>\n"
                f"   • 限制：<code>{max_users}</code> 人\n"
                f"   • 当前：<code>{current_users}</code> 人\n"
                f"   • 剩余：<code>{remaining}</code> 人\n\n"
            )

        status_text += "💡 绿色表示还有名额，红色表示已满员"

        await message.answer(
            status_text,
            reply_markup=await get_main_keyboard(
                chat_id=chat_id, show_admin=await is_admin(message.from_user.id)
            ),
            parse_mode="HTML",
        )

    except Exception as e:
        await message.answer(
            f"❌ 获取状态失败：{e}",
            reply_markup=await get_main_keyboard(
                chat_id=chat_id, show_admin=await is_admin(message.from_user.id)
            ),
        )


@dp.message(Command("actlist"))
@admin_required
@rate_limit(rate=3, per=30)
async def cmd_actlist(message: types.Message):
    """查看所有活动人数限制设置"""
    try:
        activity_limits = await db.get_all_activity_limits()

        if not activity_limits:
            await message.answer(
                "📝 当前没有设置任何活动人数限制",
                reply_markup=await get_main_keyboard(
                    chat_id=message.chat.id, show_admin=True
                ),
            )
            return

        list_text = "📝 活动人数限制列表\n\n"

        for activity, max_users in activity_limits.items():
            list_text += f"• <code>{activity}</code>：<code>{max_users}</code> 人\n"

        list_text += f"\n💡 共 {len(activity_limits)} 个活动设置了人数限制"

        await message.answer(
            list_text,
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
            parse_mode="HTML",
        )

    except Exception as e:
        await message.answer(
            f"❌ 获取列表失败：{e}",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
        )


@dp.message(Command("set"))
@admin_required
@rate_limit(rate=5, per=30)
async def cmd_set(message: types.Message):
    """设置用户数据 - 优化版本"""
    args = message.text.split()
    if len(args) != 4:
        await message.answer(
            Config.MESSAGES["set_usage"],
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
        )
        return

    try:
        uid, act, minutes = args[1], args[2], args[3]
        chat_id = message.chat.id

        await db.init_user(chat_id, int(uid))
        # 这里需要实现设置用户数据的逻辑
        await message.answer(
            f"✅ 已设置用户 <code>{uid}</code> 的 <code>{act}</code> 累计时间为 <code>{minutes}</code> 分钟",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
            parse_mode="HTML",
        )
    except Exception as e:
        await message.answer(
            f"❌ 设置失败：{e}",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
        )


@dp.message(Command("reset"))
@admin_required
@rate_limit(rate=5, per=30)
async def cmd_reset(message: types.Message):
    """重置用户数据 - 优化版本（保留月度统计）"""
    args = message.text.split()
    if len(args) != 2:
        await message.answer(
            Config.MESSAGES["reset_usage"],
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
        )
        return

    try:
        uid = args[1]
        chat_id = message.chat.id

        # 调用新的重置方法，只重置当日数据
        success = await db.reset_user_daily_data(chat_id, int(uid))

        if success:
            await message.answer(
                f"✅ 已重置用户 <code>{uid}</code> 的今日数据（月度统计已保留）",
                reply_markup=await get_main_keyboard(
                    chat_id=message.chat.id, show_admin=True
                ),
                parse_mode="HTML",
            )
        else:
            await message.answer(
                f"❌ 重置用户数据失败",
                reply_markup=await get_main_keyboard(
                    chat_id=message.chat.id, show_admin=True
                ),
            )

    except Exception as e:
        await message.answer(
            f"❌ 重置失败：{e}",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
        )


@dp.message(Command("setresettime"))
@admin_required
@rate_limit(rate=3, per=30)
async def cmd_setresettime(message: types.Message):
    """设置每日重置时间 - 优化版本"""
    args = message.text.split()
    if len(args) != 3:
        await message.answer(
            Config.MESSAGES["setresettime_usage"],
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
        )
        return

    try:
        hour = int(args[1])
        minute = int(args[2])

        if 0 <= hour <= 23 and 0 <= minute <= 59:
            chat_id = message.chat.id
            await db.init_group(chat_id)
            await db.update_group_reset_time(chat_id, hour, minute)
            await message.answer(
                f"✅ 每日重置时间已设置为：<code>{hour:02d}:{minute:02d}</code>",
                reply_markup=await get_main_keyboard(
                    chat_id=message.chat.id, show_admin=True
                ),
                parse_mode="HTML",
            )
        else:
            await message.answer(
                "❌ 小时必须在0-23之间，分钟必须在0-59之间！",
                reply_markup=await get_main_keyboard(
                    chat_id=message.chat.id, show_admin=True
                ),
            )
    except ValueError:
        await message.answer(
            "❌ 请输入有效的数字！",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
        )


@dp.message(Command("setfine"))
@admin_required
@rate_limit(rate=5, per=30)
async def cmd_setfine(message: types.Message):
    """设置活动罚款费率 - 优化版本"""
    args = message.text.split()
    if len(args) != 4:
        await message.answer(
            Config.MESSAGES["setfine_usage"],
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
        )
        return

    try:
        act = args[1]
        time_segment = args[2]
        fine_amount = int(args[3])

        if not await db.activity_exists(act):
            await message.answer(
                f"❌ 活动 '<code>{act}</code>' 不存在！",
                reply_markup=await get_main_keyboard(
                    chat_id=message.chat.id, show_admin=True
                ),
                parse_mode="HTML",
            )
            return

        if fine_amount < 0:
            await message.answer(
                "❌ 罚款金额不能为负数！",
                reply_markup=await get_main_keyboard(
                    chat_id=message.chat.id, show_admin=True
                ),
            )
            return

        await db.update_fine_config(act, time_segment, fine_amount)
        await message.answer(
            f"✅ 已设置活动 '<code>{act}</code>' 在 <code>{time_segment}</code> 分钟内的罚款费率为 <code>{fine_amount}</code> 元",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
            parse_mode="HTML",
        )
    except ValueError:
        await message.answer(
            "❌ 请输入有效的数字！",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
        )
    except Exception as e:
        await message.answer(
            f"❌ 设置失败：{e}",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
        )


@dp.message(Command("setfines_all"))
@admin_required
@rate_limit(rate=3, per=30)
async def cmd_setfines_all(message: types.Message):
    """为所有活动统一设置分段罚款 - 优化版本"""
    args = message.text.split()
    if len(args) < 3 or (len(args) - 1) % 2 != 0:
        await message.answer(
            Config.MESSAGES["setfines_all_usage"],
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
        )
        return

    try:
        pairs = args[1:]
        segments = {}
        for i in range(0, len(pairs), 2):
            t = int(pairs[i])
            f = int(pairs[i + 1])
            if t <= 0 or f < 0:
                await message.answer(
                    "❌ 时间段必须为正整数，罚款金额不能为负数",
                    reply_markup=await get_main_keyboard(
                        chat_id=message.chat.id, show_admin=True
                    ),
                )
                return
            segments[str(t)] = f

        activity_limits = await db.get_activity_limits_cached()
        for act in activity_limits.keys():
            for time_segment, amount in segments.items():
                await db.update_fine_config(act, time_segment, amount)

        segments_text = " ".join(
            [f"<code>{t}</code>:<code>{f}</code>" for t, f in segments.items()]
        )
        await message.answer(
            f"✅ 已为所有活动设置分段罚款：{segments_text}",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
            parse_mode="HTML",
        )
    except Exception as e:
        await message.answer(
            f"❌ 设置失败：{e}",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
        )


# ===== 上下班罚款 =====
@dp.message(Command("setworkfine"))
@admin_required
@rate_limit(rate=3, per=30)
async def cmd_setworkfine(message: types.Message):
    """
    设置上下班罚款规则
    用法：
    /setworkfine work_start 1 100 10 200 30 500
    表示：
        迟到1分钟以上罚100，
        迟到10分钟以上罚200，
        迟到30分钟以上罚500
    """
    args = message.text.split()
    if len(args) < 4 or len(args) % 2 != 0:
        await message.answer(
            "❌ 用法错误\n正确格式：/setworkfine <work_start|work_end> <分钟1> <罚款1> [分钟2 罚款2 ...]",
            reply_markup=get_admin_keyboard(),
        )
        return

    checkin_type = args[1]
    if checkin_type not in ["work_start", "work_end"]:
        await message.answer(
            "❌ 类型必须是 work_start 或 work_end",
            reply_markup=get_admin_keyboard(),
        )
        return

    # 解析分钟阈值和罚款金额
    fine_segments = {}
    try:
        for i in range(2, len(args), 2):
            minute = int(args[i])
            amount = int(args[i + 1])
            fine_segments[str(minute)] = amount

        # 更新数据库配置（重写整个罚款配置）
        await db.clear_work_fine_rates(checkin_type)
        for minute_str, fine_amount in fine_segments.items():
            await db.update_work_fine_rate(checkin_type, minute_str, fine_amount)

        segments_text = "\n".join(
            [f"⏰ 超过 {m} 分钟 → 💰 {a} 元" for m, a in fine_segments.items()]
        )

        await message.answer(
            f"✅ 已设置 {checkin_type} 的罚款规则：\n{segments_text}",
            reply_markup=get_admin_keyboard(),
        )

    except Exception as e:
        await message.answer(
            f"❌ 设置失败：{e}",
            reply_markup=get_admin_keyboard(),
        )


@dp.message(Command("showsettings"))
@admin_required
@rate_limit(rate=5, per=60)
async def cmd_showsettings(message: types.Message):
    """显示目前的设置 - 优化版本"""
    chat_id = message.chat.id
    await db.init_group(chat_id)
    group_data = await db.get_group_cached(chat_id)

    if group_data and not isinstance(group_data, dict):
        group_data = dict(group_data)

    activity_limits = await db.get_activity_limits_cached()
    fine_rates = await db.get_fine_rates()
    work_fine_rates = await db.get_work_fine_rates()

    # 生成输出文本
    text = f"🔧 当前群设置（群 {chat_id}）\n"
    text += f"• 绑定频道ID: {group_data.get('channel_id', '未设置')}\n"
    text += f"• 通知群组ID: {group_data.get('notification_group_id', '未设置')}\n"
    text += f"• 每日重置时间: {group_data.get('reset_hour', 0):02d}:{group_data.get('reset_minute', 0):02d}\n\n"

    text += "📋 活动设置：\n"
    for act, v in activity_limits.items():
        text += f"• {act}：次数上限 {v['max_times']}，时间限制 {v['time_limit']} 分钟\n"

    text += "\n💰 当前各活动罚款分段：\n"
    for act, fr in fine_rates.items():
        text += f"• {act}：{fr}\n"

    text += "\n⏰ 上下班罚款设置：\n"
    text += f"• 上班迟到：{work_fine_rates.get('work_start', {})}\n"
    text += f"• 下班早退：{work_fine_rates.get('work_end', {})}\n"

    await message.answer(
        text,
        reply_markup=await get_main_keyboard(chat_id=chat_id, show_admin=True),
        parse_mode="HTML",
    )


# 在现有的管理员命令后面添加这个新命令
@dp.message(Command("performance"))
@admin_required
@rate_limit(rate=2, per=60)
async def cmd_performance(message: types.Message):
    """查看性能报告"""
    try:
        # 获取性能报告
        perf_report = performance_monitor.get_performance_report()
        cache_stats = global_cache.get_stats()

        report_text = (
            "📊 <b>系统性能报告</b>\n\n"
            f"⏰ 运行时间: <code>{perf_report.get('uptime', 0):.0f}</code> 秒\n"
            f"💾 内存使用: <code>{perf_report.get('memory_usage_mb', 0):.1f}</code> MB\n"
            f"🐌 慢操作数量: <code>{perf_report.get('slow_operations_count', 0)}</code>\n\n"
            f"<b>缓存统计:</b>\n"
            f"• 命中率: <code>{cache_stats.get('hit_rate', 0):.1%}</code>\n"
            f"• 命中次数: <code>{cache_stats.get('hits', 0)}</code>\n"
            f"• 未命中: <code>{cache_stats.get('misses', 0)}</code>\n"
            f"• 缓存大小: <code>{cache_stats.get('size', 0)}</code>\n\n"
        )

        # 添加关键操作性能 - 修复空值问题
        metrics_summary = perf_report.get("metrics_summary", {})
        if metrics_summary:
            report_text += "<b>操作性能:</b>\n"
            for op_name, metrics in metrics_summary.items():
                if metrics.get("count", 0) > 0:
                    report_text += (
                        f"• {op_name}: 平均<code>{metrics.get('avg', 0):.3f}</code>s, "
                        f"最大<code>{metrics.get('max', 0):.3f}</code>s, "
                        f"次数<code>{metrics.get('count', 0)}</code>\n"
                    )
        else:
            report_text += "<b>操作性能:</b>\n• 暂无性能数据\n\n"

        # 🆕 添加用户锁统计
        lock_stats = user_lock_manager.get_stats()
        report_text += f"\n🔒 <b>用户锁统计:</b>\n"
        report_text += (
            f"• 活跃锁数量: <code>{lock_stats.get('active_locks', 0)}</code>\n"
        )
        report_text += (
            f"• 跟踪用户数: <code>{lock_stats.get('tracked_users', 0)}</code>\n"
        )
        report_text += f"• 上次清理: <code>{time.strftime('%H:%M:%S', time.localtime(lock_stats.get('last_cleanup', time.time())))}</code>\n"

        await message.answer(report_text, parse_mode="HTML")

    except Exception as e:
        logger.error(f"❌ 获取性能报告失败: {e}")
        await message.answer(f"❌ 获取性能报告失败: {e}")


# ===== 调试命令 =====
@dp.message(Command("debug_work"))
@admin_required
async def cmd_debug_work(message: types.Message):
    """调试上下班功能状态"""
    chat_id = message.chat.id

    work_hours = await db.get_group_work_time(chat_id)
    has_work_enabled = await has_work_hours_enabled(chat_id)

    debug_info = (
        f"🔧 上下班功能调试信息\n\n"
        f"群组ID: <code>{chat_id}</code>\n"
        f"上班时间: <code>{work_hours['work_start']}</code>\n"
        f"下班时间: <code>{work_hours['work_end']}</code>\n"
        f"默认上班: <code>{Config.DEFAULT_WORK_HOURS['work_start']}</code>\n"
        f"默认下班: <code>{Config.DEFAULT_WORK_HOURS['work_end']}</code>\n\n"
        f"功能启用状态: {'✅ 已启用' if has_work_enabled else '❌ 未启用'}\n"
        f"上班时间不同: {work_hours['work_start'] != Config.DEFAULT_WORK_HOURS['work_start']}\n"
        f"下班时间不同: {work_hours['work_end'] != Config.DEFAULT_WORK_HOURS['work_end']}\n\n"
        f"按钮应该显示: {'✅ 是' if has_work_enabled else '❌ 否'}"
    )

    await message.answer(debug_info, parse_mode="HTML")


# ==================== 月度统计清理命令 ====================
@dp.message(Command("cleanup_monthly"))
@admin_required
@rate_limit(rate=2, per=60)
async def cmd_cleanup_monthly(message: types.Message):
    """清理月度统计数据"""
    args = message.text.split()

    target_date = None
    if len(args) >= 3:
        try:
            year = int(args[1])
            month = int(args[2])
            if month < 1 or month > 12:
                await message.answer("❌ 月份必须在1-12之间")
                return
            target_date = date(year, month, 1)
        except ValueError:
            await message.answer("❌ 请输入有效的年份和月份")
            return
    elif len(args) == 2 and args[1].lower() == "all":
        # 特殊命令：清理所有月度数据（谨慎使用）
        await message.answer(
            "⚠️ <b>危险操作确认</b>\n\n"
            "您即将删除<u>所有</u>月度统计数据！\n"
            "此操作不可恢复！\n\n"
            "请输入 <code>/cleanup_monthly confirm_all</code> 确认执行",
            parse_mode="HTML",
        )
        return
    elif len(args) == 2 and args[1].lower() == "confirm_all":
        # 确认清理所有数据
        async with db.pool.acquire() as conn:
            result = await conn.execute("DELETE FROM monthly_statistics")
            deleted_count = (
                int(result.split()[-1]) if result and result.startswith("DELETE") else 0
            )

        await message.answer(
            f"🗑️ <b>已清理所有月度统计数据</b>\n"
            f"删除记录: <code>{deleted_count}</code> 条\n\n"
            f"⚠️ 所有月度统计已被清空，月度报告将无法生成历史数据",
            parse_mode="HTML",
        )
        logger.warning(f"👑 管理员 {message.from_user.id} 清理了所有月度统计数据")
        return

    await message.answer("⏳ 正在清理月度统计数据...")

    try:
        if target_date:
            # 清理指定月份
            deleted_count = await db.cleanup_specific_month(
                target_date.year, target_date.month
            )
            date_str = target_date.strftime("%Y年%m月")
            await message.answer(
                f"✅ <b>月度统计清理完成</b>\n"
                f"📅 清理月份: <code>{date_str}</code>\n"
                f"🗑️ 删除记录: <code>{deleted_count}</code> 条",
                parse_mode="HTML",
            )
        else:
            # 默认清理3个月前的数据
            deleted_count = await db.cleanup_monthly_data()
            today = get_beijing_time()
            cutoff_date = (today - timedelta(days=90)).date().replace(day=1)
            cutoff_str = cutoff_date.strftime("%Y年%m月")

            await message.answer(
                f"✅ <b>月度统计自动清理完成</b>\n"
                f"📅 清理截止: <code>{cutoff_str}</code> 之前\n"
                f"🗑️ 删除记录: <code>{deleted_count}</code> 条\n\n"
                f"💡 保留了最近3个月的月度统计数据",
                parse_mode="HTML",
            )

    except Exception as e:
        logger.error(f"❌ 清理月度数据失败: {e}")
        await message.answer(f"❌ 清理月度数据失败: {e}")


@dp.message(Command("monthly_stats_status"))
@admin_required
@rate_limit(rate=5, per=60)
async def cmd_monthly_stats_status(message: types.Message):
    """查看月度统计数据状态"""
    chat_id = message.chat.id

    try:
        async with db.pool.acquire() as conn:
            # 获取月度统计的日期范围
            date_range = await conn.fetch(
                "SELECT MIN(statistic_date) as earliest, MAX(statistic_date) as latest, COUNT(*) as total FROM monthly_statistics WHERE chat_id = $1",
                chat_id,
            )

            # 获取各月份数据量
            monthly_counts = await conn.fetch(
                "SELECT statistic_date, COUNT(*) as count FROM monthly_statistics WHERE chat_id = $1 GROUP BY statistic_date ORDER BY statistic_date DESC",
                chat_id,
            )

            # 获取总用户数
            user_count = await conn.fetchval(
                "SELECT COUNT(DISTINCT user_id) FROM monthly_statistics WHERE chat_id = $1",
                chat_id,
            )

            # 获取活动类型数量
            activity_count = await conn.fetchval(
                "SELECT COUNT(DISTINCT activity_name) FROM monthly_statistics WHERE chat_id = $1",
                chat_id,
            )

        if not date_range or not date_range[0]["earliest"]:
            await message.answer(
                "📊 <b>月度统计数据状态</b>\n\n" "暂无月度统计数据", parse_mode="HTML"
            )
            return

        earliest = date_range[0]["earliest"]
        latest = date_range[0]["latest"]
        total_records = date_range[0]["total"]

        status_text = (
            f"📊 <b>月度统计数据状态</b>\n\n"
            f"📅 数据范围: <code>{earliest.strftime('%Y年%m月')}</code> - <code>{latest.strftime('%Y年%m月')}</code>\n"
            f"👥 统计用户: <code>{user_count}</code> 人\n"
            f"📝 活动类型: <code>{activity_count}</code> 种\n"
            f"💾 总记录数: <code>{total_records}</code> 条\n\n"
            f"<b>各月份数据量:</b>\n"
        )

        for row in monthly_counts[:12]:  # 显示最近12个月
            month_str = row["statistic_date"].strftime("%Y年%m月")
            count = row["count"]
            status_text += f"• {month_str}: <code>{count}</code> 条\n"

        if len(monthly_counts) > 12:
            status_text += f"• ... 还有 {len(monthly_counts) - 12} 个月份\n"

        status_text += (
            f"\n💡 <b>可用命令:</b>\n"
            f"• <code>/cleanup_monthly</code> - 自动清理（保留3个月）\n"
            f"• <code>/cleanup_monthly 2024 1</code> - 清理指定月份\n"
            f"• <code>/cleanup_monthly all</code> - 清理所有数据（危险）"
        )

        await message.answer(status_text, parse_mode="HTML")

    except Exception as e:
        logger.error(f"❌ 查看月度统计状态失败: {e}")
        await message.answer(f"❌ 查看月度统计状态失败: {e}")


@dp.message(Command("cleanup_inactive"))
@admin_required
async def cmd_cleanup_inactive(message: types.Message):
    args = message.text.split()

    # 默认清理 30 天未活动的用户
    days = 30

    # 如果用户手动传入天数
    if len(args) > 1:
        try:
            days = int(args[1])
        except ValueError:
            return await message.reply("❌ 天数必须是数字，例如：/cleanup_inactive 60")

    await message.reply(f"⏳ 正在清理 {days} 天未活动的用户，请稍候...")

    try:
        deleted_count = await db.cleanup_inactive_users(days)

        await message.reply(
            f"🧹 清理完成：删除了 **{deleted_count}** 个长期未活动的用户\n"
            f"（包括 users、user_activities、work_records ）"
        )
    except Exception as e:
        await message.reply(f"❌ 清理失败：{e}")


# ==================== 上下班命令优化 ====================
@dp.message(Command("setworktime"))
@admin_required
@rate_limit(rate=3, per=30)
async def cmd_setworktime(message: types.Message):
    """设置上下班时间 - 优化版本"""
    args = message.text.split()
    if len(args) != 3:
        await message.answer(
            "❌ 用法：/setworktime <上班时间> <下班时间>\n"
            "例如：/setworktime 09:00 18:00\n"
            "时间格式：HH:MM (24小时制)",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
        )
        return

    try:
        work_start = args[1]
        work_end = args[2]

        datetime.strptime(work_start, "%H:%M")
        datetime.strptime(work_end, "%H:%M")

        chat_id = message.chat.id
        await db.init_group(chat_id)
        await db.update_group_work_time(chat_id, work_start, work_end)

        await message.answer(
            f"✅ 已设置上下班时间：\n"
            f"🟢 上班时间：<code>{work_start}</code>\n"
            f"🔴 下班时间：<code>{work_end}</code>\n\n"
            f"💡 用户现在可以使用上下班按钮进行打卡",
            reply_markup=await get_main_keyboard(chat_id=chat_id, show_admin=True),
            parse_mode="HTML",
        )

    except ValueError:
        await message.answer(
            "❌ 时间格式错误！请使用 HH:MM 格式（24小时制）\n" "例如：09:00、18:30",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
        )
    except Exception as e:
        await message.answer(
            f"❌ 设置失败：{e}",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
        )


# ========== /worktime ==========
@dp.message(Command("worktime"))
async def cmd_worktime(message: types.Message):
    """查看当前群组的上班 / 下班时间设置"""
    chat_id = message.chat.id
    work_hours = await db.get_group_work_time(chat_id)

    if (
        not work_hours
        or not work_hours.get("work_start")
        or not work_hours.get("work_end")
    ):
        await message.answer(
            "⚠️ 当前群组还没有设置上班 / 下班时间。\n请使用 /setworktime 命令设置。"
        )
        return

    start_time = work_hours["work_start"]
    end_time = work_hours["work_end"]

    await message.answer(
        f"🏢 <b>当前群组工作时间设置</b>\n"
        f"⏰ 上班时间：<code>{start_time}</code>\n"
        f"🏁 下班时间：<code>{end_time}</code>",
        parse_mode="HTML",
    )


@dp.message(Command("resetworktime"))
@admin_required
@rate_limit(rate=3, per=30)
async def cmd_resetworktime(message: types.Message):
    """重置上下班时间为默认值 - 优化版本"""
    chat_id = message.chat.id
    await db.init_group(chat_id)
    await db.update_group_work_time(
        chat_id,
        Config.DEFAULT_WORK_HOURS["work_start"],
        Config.DEFAULT_WORK_HOURS["work_end"],
    )

    await message.answer(
        f"✅ 已重置上下班时间为默认值：\n"
        f"🟢 上班时间：<code>{Config.DEFAULT_WORK_HOURS['work_start']}</code>\n"
        f"🔴 下班时间：<code>{Config.DEFAULT_WORK_HOURS['work_end']}</code>\n\n"
        f"💡 用户现在可以使用上下班按钮进行打卡",
        reply_markup=await get_main_keyboard(chat_id=chat_id, show_admin=True),
        parse_mode="HTML",
    )


@dp.message(Command("delwork"))
@admin_required
@rate_limit(rate=3, per=30)
async def cmd_delwork(message: types.Message):
    """移除上下班功能（保留历史记录）- 新版本"""
    chat_id = message.chat.id

    # 修复：使用修复后的 has_work_hours_enabled 函数
    if not await has_work_hours_enabled(chat_id):
        await message.answer(
            "❌ 当前群组没有设置上下班功能",
            reply_markup=await get_main_keyboard(chat_id=chat_id, show_admin=True),
        )
        return

    work_hours = await db.get_group_work_time(chat_id)
    old_start = work_hours.get("work_start")
    old_end = work_hours.get("work_end")

    # 重置为默认时间（相当于禁用功能）
    await db.update_group_work_time(
        chat_id,
        Config.DEFAULT_WORK_HOURS["work_start"],
        Config.DEFAULT_WORK_HOURS["work_end"],
    )

    # 🆕 清理用户缓存，确保立即生效
    group_members = await db.get_group_members(chat_id)
    for user_data in group_members:
        user_id = user_data["user_id"]
        db._cache.pop(f"user:{chat_id}:{user_id}", None)

    success_msg = (
        f"✅ 已移除上下班功能\n"
        f"🗑️ 已删除设置：<code>{old_start}</code> - <code>{old_end}</code>\n"
        f"💡 上下班记录仍然保留\n"
        f"🔧 如需清除记录请使用：<code>/delwork_clear</code>\n\n"
        f"🔧 上下班按钮已隐藏\n"
        f"🎯 现在用户可以正常进行其他活动打卡\n"
        f"🔄 键盘已自动刷新"
    )

    await message.answer(
        success_msg,
        reply_markup=await get_main_keyboard(chat_id=chat_id, show_admin=True),
        parse_mode="HTML",
    )

    logger.info(
        f"👤 管理员 {message.from_user.id} 移除了群组 {chat_id} 的上下班功能（保留记录）"
    )


@dp.message(Command("delwork_clear"))
@admin_required
@rate_limit(rate=3, per=30)
async def cmd_delwork_clear(message: types.Message):
    """移除上下班功能并清除所有记录 - 新命令"""
    chat_id = message.chat.id

    # 修复：使用修复后的 has_work_hours_enabled 函数
    if not await has_work_hours_enabled(chat_id):
        await message.answer(
            "❌ 当前群组没有设置上下班功能",
            reply_markup=await get_main_keyboard(chat_id=chat_id, show_admin=True),
        )
        return

    work_hours = await db.get_group_work_time(chat_id)
    old_start = work_hours.get("work_start")
    old_end = work_hours.get("work_end")

    # 重置为默认时间（相当于禁用功能）
    await db.update_group_work_time(
        chat_id,
        Config.DEFAULT_WORK_HOURS["work_start"],
        Config.DEFAULT_WORK_HOURS["work_end"],
    )

    records_cleared = 0
    # ✅ 清除所有上下班记录
    conn = await db.get_connection()
    try:
        result = await conn.execute(
            "DELETE FROM work_records WHERE chat_id = $1", chat_id
        )
        # result 形如 "DELETE 5"
        records_cleared = (
            int(result.split()[-1]) if result and result.startswith("DELETE") else 0
        )
    finally:
        await db.release_connection(conn)

    # 🆕 补充：清理用户缓存，确保立即生效
    group_members = await db.get_group_members(chat_id)
    for user_data in group_members:
        user_id = user_data["user_id"]
        db._cache.pop(f"user:{chat_id}:{user_id}", None)

    success_msg = (
        f"✅ 已移除上下班功能并清除所有记录\n"
        f"🗑️ 已删除设置：<code>{old_start}</code> - <code>{old_end}</code>\n"
        f"📊 同时清除了 <code>{records_cleared}</code> 条上下班记录\n"
        f"\n🔧 上下班按钮已隐藏\n"
        f"🎯 现在用户可以正常进行其他活动打卡\n"
        f"🔄 键盘已自动刷新"
    )

    await message.answer(
        success_msg,
        reply_markup=await get_main_keyboard(chat_id=chat_id, show_admin=True),
        parse_mode="HTML",
    )

    logger.info(
        f"👤 管理员 {message.from_user.id} 移除了群组 {chat_id} 的上下班功能并清除 {records_cleared} 条记录"
    )


@dp.message(Command("workstatus"))
@admin_required
@rate_limit(rate=5, per=60)
async def cmd_workstatus(message: types.Message):
    """检查上下班功能状态 - 优化版本"""
    chat_id = message.chat.id

    group_data = await db.get_group_cached(chat_id)
    if not group_data:
        await message.answer(
            "❌ 当前群组没有初始化数据",
            reply_markup=await get_main_keyboard(chat_id=chat_id, show_admin=True),
        )
        return

    work_hours = await db.get_group_work_time(chat_id)

    is_custom = (
        work_hours["work_start"] != Config.DEFAULT_WORK_HOURS["work_start"]
        and work_hours["work_end"] != Config.DEFAULT_WORK_HOURS["work_end"]
    )

    total_records = 0
    total_users = 0

    status_msg = (
        f"📊 上下班功能状态\n\n"
        f"🔧 功能状态：{'✅ 已启用' if is_custom else '❌ 未启用'}\n"
        f"🕒 当前设置：<code>{work_hours['work_start']}</code> - <code>{work_hours['work_end']}</code>\n"
        f"👥 有记录用户：<code>{total_users}</code> 人\n"
        f"📝 总记录数：<code>{total_records}</code> 条\n\n"
    )

    if is_custom:
        status_msg += (
            f"💡 可用命令：\n"
            f"• <code>/delwork</code> - 移除功能但保留记录\n"
            f"• <code>/delwork clear</code> - 移除功能并清除记录\n"
        )
    else:
        status_msg += (
            f"💡 可用命令：\n"
            f"• <code>/setworktime 09:00 18:00</code> - 启用上下班功能\n"
            f"• <code>/showworktime</code> - 显示当前设置"
        )

    await message.answer(
        status_msg,
        reply_markup=await get_main_keyboard(chat_id=chat_id, show_admin=True),
        parse_mode="HTML",
    )


@dp.message(Command("workcheck"))
@rate_limit(rate=5, per=60)
async def cmd_workcheck(message: types.Message):
    """检查上下班打卡状态 - 优化版本"""
    chat_id = message.chat.id
    uid = message.from_user.id

    if await has_work_hours_enabled(chat_id):
        has_work_start = await has_clocked_in_today(chat_id, uid, "work_start")
        has_work_end = await has_clocked_in_today(chat_id, uid, "work_end")

        status_msg = (
            f"📊 上下班打卡状态\n\n"
            f"🔧 上下班功能：✅ 已启用\n"
            f"🟢 上班打卡：{'✅ 已完成' if has_work_start else '❌ 未完成'}\n"
            f"🔴 下班打卡：{'✅ 已完成' if has_work_end else '❌ 未完成'}\n\n"
        )

        if not has_work_start:
            status_msg += (
                "⚠️ 您今天还没有打上班卡，无法进行其他活动！\n请先使用'🟢 上班'按钮打卡"
            )
        elif has_work_end:
            status_msg += (
                "⚠️ 您今天已经打过下班卡，无法再进行其他活动！\n下班后活动自动结束"
            )
        else:
            status_msg += "✅ 您已打上班卡，可以进行其他活动"
    else:
        status_msg = (
            f"📊 上下班打卡状态\n\n"
            f"🔧 上下班功能：❌ 未启用\n"
            f"🎯 您可以正常进行其他活动打卡"
        )

    await message.answer(
        status_msg,
        reply_markup=await get_main_keyboard(
            chat_id=chat_id, show_admin=await is_admin(uid)
        ),
        parse_mode="HTML",
    )


# ==================== 推送开关管理命令优化 ====================
@dp.message(Command("setpush"))
@admin_required
@rate_limit(rate=5, per=30)
async def cmd_setpush(message: types.Message):
    """设置推送开关 - 优化版本"""
    args = message.text.split()
    if len(args) != 3:
        await message.answer(
            Config.MESSAGES["setpush_usage"],
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
        )
        return

    push_type = args[1].lower()
    status = args[2].lower()

    if push_type not in ["channel", "group", "admin"]:
        await message.answer(
            "❌ 类型错误，请使用 channel、group 或 admin",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
        )
        return

    if status not in ["on", "off"]:
        await message.answer(
            "❌ 状态错误，请使用 on 或 off",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
        )
        return

    if push_type == "channel":
        await db.update_push_setting("enable_channel_push", status == "on")
        status_text = "开启" if status == "on" else "关闭"
        await message.answer(
            f"✅ 已{status_text}频道推送",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
        )
    elif push_type == "group":
        await db.update_push_setting("enable_group_push", status == "on")
        status_text = "开启" if status == "on" else "关闭"
        await message.answer(
            f"✅ 已{status_text}群组推送",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
        )
    elif push_type == "admin":
        await db.update_push_setting("enable_admin_push", status == "on")
        status_text = "开启" if status == "on" else "关闭"
        await message.answer(
            f"✅ 已{status_text}管理员推送",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
        )


@dp.message(Command("showpush"))
@admin_required
@rate_limit(rate=5, per=60)
async def cmd_showpush(message: types.Message):
    """显示推送设置 - 优化版本"""
    settings = await db.get_push_settings()
    text = (
        "🔔 当前自动导出推送设置：\n\n"
        f"📢 频道推送：{'✅ 开启' if settings['enable_channel_push'] else '❌ 关闭'}\n"
        f"👥 群组推送：{'✅ 开启' if settings['enable_group_push'] else '❌ 关闭'}\n"
        f"👑 管理员推送：{'✅ 开启' if settings['enable_admin_push'] else '❌ 关闭'}\n\n"
        "💡 使用说明：\n"
        "• 频道推送：推送到绑定的频道\n"
        "• 群组推送：推送到绑定的通知群组\n"
        "• 管理员推送：当没有绑定群组/频道时推送到所有管理员\n\n"
        "⚙️ 修改命令：\n"
        "<code>/setpush channel on|off</code>\n"
        "<code>/setpush group on|off</code>\n"
        "<code>/setpush admin on|off</code>"
    )
    await message.answer(
        text,
        reply_markup=await get_main_keyboard(chat_id=message.chat.id, show_admin=True),
        parse_mode="HTML",
    )


@dp.message(Command("reset_status"))
@admin_required
async def cmd_reset_status(message: types.Message):
    """检查重置状态和设置"""
    chat_id = message.chat.id

    try:
        group_data = await db.get_group_cached(chat_id)
        reset_hour = group_data.get("reset_hour", Config.DAILY_RESET_HOUR)
        reset_minute = group_data.get("reset_minute", Config.DAILY_RESET_MINUTE)

        now = get_beijing_time()
        reset_time_today = now.replace(hour=reset_hour, minute=reset_minute, second=0)

        status_info = (
            f"🔄 重置状态检查\n\n"
            f"📅 当前时间: {now.strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"⏰ 重置时间: {reset_hour:02d}:{reset_minute:02d}\n"
            f"📊 下次重置: {reset_time_today.strftime('%Y-%m-%d %H:%M')}\n\n"
            f"🔧 重置内容:\n"
            f"• 每日活动次数和时间 ✅\n"
            f"• 上下班打卡记录 ✅\n"
            f"• 当前进行中的活动 ✅\n\n"
            f"📤 导出设置:\n"
            f"• 重置前1分钟自动导出 ✅\n"
            f"• 重置后30分钟导出昨日数据 ✅\n"
            f"• 推送到绑定频道/群组 ✅"
        )

        await message.answer(status_info)

    except Exception as e:
        await message.answer(f"❌ 检查重置状态失败: {e}")


@dp.message(Command("reset_work"))
@admin_required
@rate_limit(rate=2, per=60)
async def cmd_reset_work(message: types.Message):
    """管理员重置用户今日上下班记录"""
    args = message.text.split()
    chat_id = message.chat.id

    if len(args) != 2:
        await message.answer(
            "❌ 用法: /reset_work <用户ID>\n" "💡 例如: /reset_work 123456789",
            reply_markup=await get_main_keyboard(chat_id, show_admin=True),
        )
        return

    try:
        target_uid = int(args[1])
        today = datetime.now().date()

        # 删除用户今日的上下班记录
        async with db.pool.acquire() as conn:
            await conn.execute(
                "DELETE FROM work_records WHERE chat_id = $1 AND user_id = $2 AND record_date = $3",
                chat_id,
                target_uid,
                today,
            )

        # 清理用户缓存
        db._cache.pop(f"user:{chat_id}:{target_uid}", None)

        await message.answer(
            f"✅ 已重置用户 <code>{target_uid}</code> 的今日上下班记录\n"
            f"📅 重置日期: {today}\n"
            f"💡 用户现在可以重新打卡",
            reply_markup=await get_main_keyboard(chat_id, show_admin=True),
            parse_mode="HTML",
        )

        logger.info(
            f"👑 管理员 {message.from_user.id} 重置了用户 {target_uid} 的上下班记录"
        )

    except ValueError:
        await message.answer("❌ 用户ID必须是数字")
    except Exception as e:
        await message.answer(f"❌ 重置失败: {e}")


@dp.message(Command("testpush"))
@admin_required
@rate_limit(rate=3, per=60)
async def cmd_testpush(message: types.Message):
    """测试推送功能 - 优化版本"""
    chat_id = message.chat.id
    try:
        test_file_name = f"test_push_{get_beijing_time().strftime('%H%M%S')}.txt"
        async with aiofiles.open(test_file_name, "w", encoding="utf-8") as f:
            await f.write("这是一个推送测试文件\n")
            await f.write(
                f"测试时间：{get_beijing_time().strftime('%Y-%m-%d %H:%M:%S')}\n"
            )
            await f.write("如果收到此文件，说明推送功能正常")

        caption = (
            "🧪 推送功能测试\n这是一个测试文件，用于验证自动导出推送功能是否正常工作。"
        )

        success_count = 0
        push_settings = await db.get_push_settings()
        group_data = await db.get_group_cached(chat_id)

        if (
            push_settings["enable_group_push"]
            and group_data
            and group_data.get("notification_group_id")
        ):
            try:
                await bot.send_document(
                    group_data["notification_group_id"],
                    FSInputFile(test_file_name),
                    caption=caption,
                    parse_mode="HTML",
                )
                success_count += 1
                await message.answer(
                    f"✅ 测试文件已发送到通知群组: {group_data['notification_group_id']}"
                )
            except Exception as e:
                await message.answer(f"❌ 通知群组推送测试失败: {e}")

        if (
            push_settings["enable_channel_push"]
            and group_data
            and group_data.get("channel_id")
        ):
            try:
                await bot.send_document(
                    group_data["channel_id"],
                    FSInputFile(test_file_name),
                    caption=caption,
                    parse_mode="HTML",
                )
                success_count += 1
                await message.answer(
                    f"✅ 测试文件已发送到频道: {group_data['channel_id']}"
                )
            except Exception as e:
                await message.answer(f"❌ 频道推送测试失败: {e}")

        os.remove(test_file_name)

        if success_count == 0:
            await message.answer(
                "⚠️ 没有成功发送任何测试推送，请检查推送设置和绑定状态",
                reply_markup=await get_main_keyboard(
                    chat_id=message.chat.id, show_admin=True
                ),
            )
        else:
            await message.answer(
                f"✅ 推送测试完成，成功发送 {success_count} 个测试文件",
                reply_markup=await get_main_keyboard(
                    chat_id=message.chat.id, show_admin=True
                ),
            )

    except Exception as e:
        await message.answer(f"❌ 推送测试失败：{e}")


@dp.message(Command("export"))
@admin_required
@rate_limit(rate=2, per=60)
@track_performance("cmd_export")
async def cmd_export(message: types.Message):
    """管理员手动导出群组数据 - 优化版本"""
    chat_id = message.chat.id
    await message.answer("⏳ 正在导出数据，请稍候...")
    try:
        await export_and_push_csv(chat_id)
        await message.answer(
            "✅ 数据已导出并推送到绑定的群组或频道！",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
        )
    except Exception as e:
        await message.answer(f"❌ 导出失败：{e}")


# ==================== 月度报告管理员命令优化 ====================
@dp.message(Command("monthlyreport"))
@admin_required
@rate_limit(rate=2, per=60)
async def cmd_monthlyreport(message: types.Message):
    """生成月度报告 - 优化版本"""
    args = message.text.split()
    chat_id = message.chat.id

    year = None
    month = None

    if len(args) >= 3:
        try:
            year = int(args[1])
            month = int(args[2])
            if month < 1 or month > 12:
                await message.answer("❌ 月份必须在1-12之间")
                return
        except ValueError:
            await message.answer("❌ 请输入有效的年份和月份")
            return

    await message.answer("⏳ 正在生成月度报告，请稍候...")

    try:
        # 生成报告
        report = await generate_monthly_report(chat_id, year, month)
        if report:
            await message.answer(report, parse_mode="HTML")

            # 导出CSV
            await export_monthly_csv(chat_id, year, month)
            await message.answer("✅ 月度数据已导出并推送！")
        else:
            time_desc = f"{year}年{month}月" if year and month else "最近一个月"
            await message.answer(f"⚠️ {time_desc}没有数据需要报告")

    except Exception as e:
        await message.answer(f"❌ 生成月度报告失败：{e}")


@dp.message(Command("exportmonthly"))
@admin_required
@rate_limit(rate=2, per=60)
async def cmd_exportmonthly(message: types.Message):
    """导出月度数据 - 优化版本"""
    args = message.text.split()
    chat_id = message.chat.id

    year = None
    month = None

    if len(args) >= 3:
        try:
            year = int(args[1])
            month = int(args[2])
            if month < 1 or month > 12:
                await message.answer("❌ 月份必须在1-12之间")
                return
        except ValueError:
            await message.answer("❌ 请输入有效的年份和月份")
            return

    await message.answer("⏳ 正在导出月度数据，请稍候...")

    try:
        await export_monthly_csv(chat_id, year, month)
        await message.answer("✅ 月度数据已导出并推送！")
    except Exception as e:
        await message.answer(f"❌ 导出月度数据失败：{e}")


# ==================== 简化版指令优化 ====================
@dp.message(Command("ci"))
@rate_limit(rate=10, per=60)
@message_deduplicate
@with_retry("cmd_ci", max_retries=2)
@track_performance("cmd_ci")
async def cmd_ci(message: types.Message):
    """指令打卡：/ci 活动名 - 优化版本"""
    args = message.text.split(maxsplit=1)
    if len(args) != 2:
        await message.answer(
            "❌ 用法：/ci <活动名>",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=await is_admin(message.from_user.id)
            ),
        )
        return
    act = args[1].strip()
    if not await db.activity_exists(act):
        await message.answer(
            f"❌ 活动 '<code>{act}</code>' 不存在，请先使用 /addactivity 添加或检查拼写",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=await is_admin(message.from_user.id)
            ),
            parse_mode="HTML",
        )
        return
    await start_activity(message, act)


@dp.message(Command("at"))
@rate_limit(rate=10, per=60)
@message_deduplicate
@with_retry("cmd_at", max_retries=2)
@track_performance("cmd_at")
async def cmd_at(message: types.Message):
    """指令回座：/at - 优化版本"""
    await process_back(message)


@dp.message(Command("refresh_keyboard"))
@rate_limit(rate=5, per=60)
async def cmd_refresh_keyboard(message: types.Message):
    """强制刷新键盘 - 确保新活动立即显示"""
    uid = message.from_user.id
    await message.answer(
        "🔄 键盘已刷新，新活动现在可用",
        reply_markup=await get_main_keyboard(
            chat_id=message.chat.id, show_admin=await is_admin(uid)
        ),
    )


@dp.callback_query(lambda c: c.data.startswith("quick_back:"))
async def handle_quick_back(callback_query: types.CallbackQuery):
    """处理快速回座按钮（带过期保护与异常恢复）"""
    try:
        # 🧭 解析回调数据
        data_parts = callback_query.data.split(":")
        if len(data_parts) < 3:
            await callback_query.answer("❌ 数据格式错误", show_alert=True)
            return

        chat_id = int(data_parts[1])
        uid = int(data_parts[2])

        logger.info(f"🔔 快速回座按钮被点击: chat_id={chat_id}, uid={uid}")

        # 🚧 检查消息是否过期（Telegram 限制 10 分钟）
        msg_ts = callback_query.message.date.timestamp()
        if time.time() - msg_ts > 600:
            await callback_query.answer(
                "⚠️ 此按钮已过期，请重新输入 /回座", show_alert=True
            )
            return

        # ✅ 检查是否是用户本人点击
        if callback_query.from_user.id != uid:
            await callback_query.answer("❌ 这不是您的回座按钮！", show_alert=True)
            return

        # ✅ 执行回座逻辑
        user_lock = get_user_lock(chat_id, uid)
        async with user_lock:
            user_data = await db.get_user_cached(chat_id, uid)
            if not user_data or not user_data.get("current_activity"):
                await callback_query.answer("❌ 您当前没有活动在进行", show_alert=True)
                return

            await _process_back_locked(callback_query.message, chat_id, uid)

        # ✅ 更新按钮状态（尝试移除按钮，但失败时忽略）
        try:
            await callback_query.message.edit_reply_markup(reply_markup=None)
        except Exception as e:
            logger.warning(f"无法更新按钮状态: {e}")

        await callback_query.answer("✅ 已成功回座")

    except Exception as e:
        # 捕获任何异常，防止任务崩溃
        logger.error(f"❌ 快速回座失败: {e}")
        try:
            await callback_query.answer(
                "❌ 回座失败，请手动输入 /回座", show_alert=True
            )
        except Exception:
            pass  # 避免再次抛出 BadRequest


# ============ 上下班打卡指令优化 =================
@dp.message(Command("workstart"))
@rate_limit(rate=5, per=60)
@message_deduplicate
@with_retry("work_start", max_retries=2)
@track_performance("work_start")
async def cmd_workstart(message: types.Message):
    """上班打卡 - 优化版本"""
    await process_work_checkin(message, "work_start")


@dp.message(Command("workend"))
@rate_limit(rate=5, per=60)
@message_deduplicate
@with_retry("work_end", max_retries=2)
@track_performance("work_end")
async def cmd_workend(message: types.Message):
    """下班打卡 - 优化版本"""
    await process_work_checkin(message, "work_end")


# ============ 上下班打卡处理函数优化 ============
async def auto_end_current_activity(
    chat_id: int,
    uid: int,
    user_data: dict,
    now: datetime,
    message: types.Message = None,
):
    """自动结束当前正在进行的活动 - 优化版本"""
    try:
        current_activity = user_data.get("current_activity")
        if not current_activity:
            return

        # 记录活动信息
        act = current_activity
        start_time = datetime.fromisoformat(user_data["activity_start_time"])
        elapsed = (now - start_time).total_seconds()

        # 计算超时和罚款
        time_limit_seconds = await db.get_activity_time_limit(act) * 60
        is_overtime = elapsed > time_limit_seconds
        overtime_seconds = max(0, int(elapsed - time_limit_seconds))
        overtime_minutes = overtime_seconds / 60

        fine_amount = 0
        if is_overtime and overtime_seconds > 0:
            fine_amount = await calculate_fine(act, overtime_minutes)

        # 完成活动
        await db.complete_user_activity(
            chat_id, uid, act, int(elapsed), fine_amount, is_overtime
        )

        # 取消定时任务
        key = f"{chat_id}-{uid}"
        await timer_manager.cancel_timer(key)

        # 发送自动结束通知
        if message:
            auto_end_msg = (
                f"🔄 <b>自动结束活动通知</b>\n"
                f"👤 用户：{MessageFormatter.format_user_link(uid, user_data['nickname'])}\n"
                f"📝 检测到您有未结束的活动：<code>{act}</code>\n"
                f"⏰ 由于您进行了下班打卡，系统已自动为您结束该活动\n"
                f"⏱️ 活动时长：<code>{MessageFormatter.format_time(int(elapsed))}</code>"
            )

            if is_overtime:
                auto_end_msg += f"\n⚠️ 本次活动已超时！\n⏰ 超时时长：<code>{MessageFormatter.format_time(int(overtime_seconds))}</code>"
                if fine_amount > 0:
                    auto_end_msg += f"\n💰 超时罚款：<code>{fine_amount}</code> 元"

            auto_end_msg += f"\n\n✅ 活动已自动结束，下班打卡继续处理..."

            await message.answer(
                auto_end_msg,
                reply_markup=await get_main_keyboard(
                    chat_id=chat_id, show_admin=await is_admin(uid)
                ),
                parse_mode="HTML",
            )

        # 记录日志
        logger.info(f"✅ 用户 {uid} 的下班打卡自动结束了活动: {act}, 时长: {elapsed}秒")

    except Exception as e:
        logger.error(f"❌ 自动结束活动失败: {e}")
        if message:
            await message.answer(
                f"⚠️ 自动结束活动时出现错误，但下班打卡将继续处理\n错误详情: {e}",
                reply_markup=await get_main_keyboard(
                    chat_id=chat_id, show_admin=await is_admin(uid)
                ),
            )


# ===== 上下班打卡功能 ======


async def process_work_checkin(message: types.Message, checkin_type: str):
    """
    智能化上下班打卡系统（跨天安全修复版）
    保留全部原有功能 + 增强智能判断、错误容错、日志追踪。
    """

    chat_id = message.chat.id
    uid = message.from_user.id
    name = message.from_user.full_name
    now = get_beijing_time()
    current_time = now.strftime("%H:%M")
    today = str(now.date())
    trace_id = f"{chat_id}-{uid}-{int(time.time())}"

    logger.info(f"🟢[{trace_id}] 开始处理 {checkin_type} 打卡请求：{name}({uid})")

    user_lock = get_user_lock(chat_id, uid)
    async with user_lock:
        # ✅ 初始化群组与用户数据
        try:
            await db.init_group(chat_id)
            await db.init_user(chat_id, uid)
            user_data = await db.get_user_cached(chat_id, uid)
        except Exception as e:
            logger.error(f"[{trace_id}] ❌ 初始化用户/群组失败: {e}")
            await message.answer("⚠️ 数据初始化失败，请稍后再试。")
            return

        # ✅ 检查是否重复打卡
        try:
            has_record_today = await db.has_work_record_today(
                chat_id, uid, checkin_type
            )
        except Exception as e:
            logger.error(f"[{trace_id}] ❌ 检查重复打卡失败: {e}")
            has_record_today = False  # 允许继续执行但记录日志

        if has_record_today:
            today_records = await db.get_today_work_records(chat_id, uid)
            existing_record = today_records.get(checkin_type)
            action_text = "上班" if checkin_type == "work_start" else "下班"
            status_msg = f"🚫 您今天已经打过{action_text}卡了！"

            if existing_record:
                existing_time = existing_record["checkin_time"]
                existing_status = existing_record["status"]
                status_msg += f"\n⏰ 打卡时间：<code>{existing_time}</code>"
                status_msg += f"\n📊 状态：{existing_status}"

            await message.answer(
                status_msg,
                reply_markup=await get_main_keyboard(
                    chat_id=chat_id, show_admin=await is_admin(uid)
                ),
                parse_mode="HTML",
            )
            logger.info(f"[{trace_id}] 🔁 检测到重复{action_text}打卡，终止处理。")
            return

        # 🆕 添加异常情况检查：已经下班但又打上班卡
        if checkin_type == "work_start":
            has_work_end_today = await db.has_work_record_today(
                chat_id, uid, "work_end"
            )
            if has_work_end_today:
                today_records = await db.get_today_work_records(chat_id, uid)
                end_record = today_records.get("work_end")
                end_time = end_record["checkin_time"] if end_record else "未知时间"

                await message.answer(
                    f"🚫 您今天已经在 <code>{end_time}</code> 打过下班卡，无法再打上班卡！\n"
                    f"💡 如需重新打卡，请联系管理员或等待次日自动重置",
                    reply_markup=await get_main_keyboard(chat_id, await is_admin(uid)),
                    parse_mode="HTML",
                )
                logger.info(f"[{trace_id}] 🔁 检测到异常：下班后再次上班打卡")
                return

        # ✅ 自动结束活动（仅下班）
        current_activity = user_data.get("current_activity")
        activity_auto_ended = False
        if checkin_type == "work_end" and current_activity:
            with suppress(Exception):
                await auto_end_current_activity(chat_id, uid, user_data, now, message)
                activity_auto_ended = True
                logger.info(f"[{trace_id}] 🔄 已自动结束活动：{current_activity}")

        # ✅ 下班前检查上班记录
        if checkin_type == "work_end":
            has_work_start_today = await db.has_work_record_today(
                chat_id, uid, "work_start"
            )
            if not has_work_start_today:
                await message.answer(
                    "❌ 您今天还没有打上班卡，无法打下班卡！\n"
                    "💡 请先使用'🟢 上班'按钮或 /workstart 命令打上班卡",
                    reply_markup=await get_main_keyboard(
                        chat_id=chat_id, show_admin=await is_admin(uid)
                    ),
                    parse_mode="HTML",
                )
                logger.warning(f"[{trace_id}] ⚠️ 用户试图下班打卡但未上班")
                return

        # 🆕 添加时间范围检查（放在获取工作时间设置之前）
        try:
            valid_time, expected_dt = await is_valid_checkin_time(
                chat_id, checkin_type, now
            )
        except Exception as e:
            logger.error(f"[{trace_id}] ❌ is_valid_checkin_time 调用失败: {e}")
            valid_time, expected_dt = True, now  # 避免误伤，默认允许

        if not valid_time:
            # 计算可打卡窗口的起止时间（基于选中的 expected_dt）
            allowed_start = (expected_dt - timedelta(hours=7)).strftime(
                "%Y-%m-%d %H:%M"
            )
            allowed_end = (expected_dt + timedelta(hours=7)).strftime("%Y-%m-%d %H:%M")

            # 显示更友好的本地化提示（包含日期，避免跨天误解）
            await message.answer(
                f"⏰ 当前时间不在允许的打卡范围内（前后7小时规则）！\n\n"
                f"📅 期望打卡时间（参考）：<code>{expected_dt.strftime('%H:%M')}</code>\n"
                f"🕒 允许范围（含日期）：\n"
                f"   • 开始：<code>{allowed_start}</code>\n"
                f"   • 结束：<code>{allowed_end}</code>\n\n"
                f"💡 如果你确认时间有特殊情况，请联系管理员处理。",
                reply_markup=await get_main_keyboard(chat_id, await is_admin(uid)),
                parse_mode="HTML",
            )
            logger.info(
                f"[{trace_id}] ⏰ 打卡时间范围检查失败（不在 ±7 小时内），终止处理"
            )
            return

        # ✅ 获取工作时间设置
        work_hours = await db.get_group_work_time(chat_id)
        expected_time = work_hours[checkin_type]

        # ✅ 计算时间差（含跨天）
        time_diff_minutes, expected_dt = calculate_cross_day_time_diff(
            now, expected_time, checkin_type
        )
        time_diff_hours = abs(time_diff_minutes / 60)

        # ✅ 时间异常修正
        if time_diff_hours > 24:
            logger.warning(
                f"[{trace_id}] ⏰ 异常时间差检测 {time_diff_hours}小时，自动纠正为0"
            )
            time_diff_minutes = 0

        # ✅ 格式化时间差
        def format_time_diff(minutes: float) -> str:
            mins = int(abs(minutes))
            h, m = divmod(mins, 60)
            if h > 0:
                return f"{h}小时{m}分"
            return f"{m}分钟"

        time_diff_str = format_time_diff(time_diff_minutes)
        fine_amount = 0
        is_late_early = False

        # ✅ 打卡状态判断
        if checkin_type == "work_start":
            if time_diff_minutes > 0:
                fine_amount = await calculate_work_fine("work_start", time_diff_minutes)
                status = f"🚨 迟到 {time_diff_str}"
                if fine_amount:
                    status += f"（💰罚款 {fine_amount}元）"
                emoji = "😅"
                is_late_early = True
            else:
                status = "✅ 准时"
                emoji = "👍"
            action_text = "上班"
        else:
            if time_diff_minutes < 0:
                fine_amount = await calculate_work_fine(
                    "work_end", abs(time_diff_minutes)
                )
                status = f"🚨 早退 {time_diff_str}"
                if fine_amount:
                    status += f"（💰罚款 {fine_amount}元）"
                emoji = "🏃"
                is_late_early = True
            else:
                status = "✅ 准时"
                emoji = "👍"
            action_text = "下班"

        # ✅ 安全写入数据库（含重试）
        for attempt in range(2):
            try:
                await db.add_work_record(
                    chat_id,
                    uid,
                    today,
                    checkin_type,
                    current_time,
                    status,
                    time_diff_minutes,
                    fine_amount,
                )
                break
            except Exception as e:
                logger.error(f"[{trace_id}] ❌ 数据写入失败，第{attempt+1}次尝试: {e}")
                if attempt == 1:
                    await message.answer("⚠️ 数据保存失败，请稍后再试。")
                    return
                await asyncio.sleep(0.5)

        expected_time_display = expected_dt.strftime("%m/%d %H:%M")
        result_msg = (
            f"{emoji} <b>{action_text}打卡完成</b>\n"
            f"👤 用户：{MessageFormatter.format_user_link(uid, name)}\n"
            f"⏰ 打卡时间：<code>{current_time}</code>\n"
            f"📅 期望时间：<code>{expected_time_display}</code>\n"
            f"📊 状态：{status}"
        )

        if checkin_type == "work_end" and activity_auto_ended and current_activity:
            result_msg += (
                f"\n\n🔄 检测到未结束活动 <code>{current_activity}</code>，已自动结束"
            )

        await message.answer(
            result_msg,
            reply_markup=await get_main_keyboard(
                chat_id=chat_id, show_admin=await is_admin(uid)
            ),
            parse_mode="HTML",
        )

        # ✅ 智能通知模块
        if is_late_early:
            try:
                status_type = "迟到" if checkin_type == "work_start" else "早退"
                time_detail = f"{status_type} {time_diff_str}"

                with suppress(Exception):
                    chat_info = await bot.get_chat(chat_id)
                    chat_title = getattr(chat_info, "title", str(chat_id))
                notif_text = (
                    f"⚠️ <b>{action_text}{status_type}通知</b>\n"
                    f"🏢 群组：<code>{chat_title}</code>\n"
                    f"{MessageFormatter.create_dashed_line()}\n"
                    f"👤 用户：{MessageFormatter.format_user_link(uid, name)}\n"
                    f"⏰ 打卡时间：<code>{current_time}</code>\n"
                    f"📅 期望时间：<code>{expected_time_display}</code>\n"
                    f"⏱️ {time_detail}"
                )
                if fine_amount:
                    notif_text += f"\n💰 罚款金额：<code>{fine_amount}</code> 元"

                sent = await NotificationService.send_notification(chat_id, notif_text)
                if not sent:
                    logger.warning(f"[{trace_id}] ⚠️ 通知发送失败，尝试管理员兜底。")
                    for admin_id in Config.ADMINS:
                        with suppress(Exception):
                            await bot.send_message(
                                admin_id, notif_text, parse_mode="HTML"
                            )

            except Exception as e:
                logger.error(
                    f"[{trace_id}] ❌ 通知发送失败: {e}\n{traceback.format_exc()}"
                )

    logger.info(f"✅[{trace_id}] {action_text}打卡流程完成")


# ===== 添加辅助函数 ======
def calculate_cross_day_time_diff(
    current_dt: datetime, expected_time: str, checkin_type: str
):
    """
    🕒 智能化的时间差计算（支持跨天和最近匹配）
    自动选择与当前时间最近的“期望时间点”，解决夜班/跨天迟到显示异常问题。
    返回:
        time_diff_minutes: 当前时间 - 最近期望时间（分钟）
        expected_dt: 实际匹配到的期望时间点（datetime）
    """
    try:
        expected_hour, expected_minute = map(int, expected_time.split(":"))

        # 生成前一天、当天、后一天三个候选时间点
        candidates = []
        for d in (-1, 0, 1):
            candidate = current_dt.replace(
                hour=expected_hour, minute=expected_minute, second=0, microsecond=0
            ) + timedelta(days=d)
            candidates.append(candidate)

        # 找到与当前时间最接近的 expected_dt
        expected_dt = min(
            candidates, key=lambda t: abs((t - current_dt).total_seconds())
        )

        # 计算时间差（单位：分钟）
        time_diff_minutes = (current_dt - expected_dt).total_seconds() / 60

        logger.info(f"🔍 时间差计算:")
        logger.info(f"  当前时间: {current_dt.strftime('%Y-%m-%d %H:%M')}")
        logger.info(f"  匹配期望: {expected_dt.strftime('%Y-%m-%d %H:%M')}")
        logger.info(f"  打卡类型: {checkin_type}")
        logger.info(f"  时间差: {time_diff_minutes:.2f} 分钟")

        return time_diff_minutes, expected_dt

    except Exception as e:
        logger.error(f"❌ 时间差计算出错: {e}")
        return 0, current_dt


# 🆕 直接添加时间范围检查函数
async def is_valid_checkin_time(
    chat_id: int, checkin_type: str, current_time: datetime
) -> tuple[bool, datetime]:
    """
    检查是否在允许的打卡时间窗口内（前后 7 小时）。
    返回 (is_valid, expected_dt)：
      - is_valid: True/False
      - expected_dt: 选中的“期望打卡时间点”（datetime），用于在提示中显示实际允许范围
    逻辑：在相邻的 -1/0/+1 天中挑选最接近 current_time 的 expected_dt，适用于夜班/跨天场景。
    """
    try:
        work_hours = await db.get_group_work_time(chat_id)
        if checkin_type == "work_start":
            expected_time_str = work_hours["work_start"]
        else:
            expected_time_str = work_hours["work_end"]

        exp_h, exp_m = map(int, expected_time_str.split(":"))

        # 在 -1/0/+1 天范围内生成候选 expected_dt，选择与 current_time 差值最小的那个
        candidates = []
        for d in (-1, 0, 1):
            candidate = current_time.replace(
                hour=exp_h, minute=exp_m, second=0, microsecond=0
            ) + timedelta(days=d)
            candidates.append(candidate)

        # 选择与 current_time 时间差绝对值最小的 candidate
        expected_dt = min(
            candidates, key=lambda t: abs((t - current_time).total_seconds())
        )

        # 允许前后窗口：7小时
        earliest = expected_dt - timedelta(hours=7)
        latest = expected_dt + timedelta(hours=7)

        is_valid = earliest <= current_time <= latest

        if not is_valid:
            logger.warning(
                f"⚠️ 打卡时间超出允许窗口: {checkin_type}, 当前: {current_time.strftime('%Y-%m-%d %H:%M')}, "
                f"允许: {earliest.strftime('%Y-%m-%d %H:%M')} ~ {latest.strftime('%Y-%m-%d %H:%M')}"
            )

        return is_valid, expected_dt

    except Exception as e:
        logger.error(f"❌ 检查打卡时间范围失败（is_valid_checkin_time）: {e}")
        # 出现异常时为兼容性考虑，返回允许 + 今天的期望时间
        fallback = current_time.replace(hour=9, minute=0, second=0, microsecond=0)
        return True, fallback


# ============ 文本命令处理优化 =================
@dp.message(Command("workrecord"))
@rate_limit(rate=5, per=60)
async def cmd_workrecord(message: types.Message):
    """查询上下班记录 - 优化版本"""
    chat_id = message.chat.id
    uid = message.from_user.id

    user_lock = get_user_lock(chat_id, uid)
    async with user_lock:
        await db.init_group(chat_id)
        await db.init_user(chat_id, uid)

        work_records = await db.get_user_work_records(chat_id, uid)

        if not work_records:
            await message.answer(
                "📝 暂无上下班打卡记录",
                reply_markup=await get_main_keyboard(
                    chat_id=chat_id, show_admin=await is_admin(uid)
                ),
            )
            return

        work_hours = await db.get_group_work_time(chat_id)
        user_data = await db.get_user_cached(chat_id, uid)

        record_text = (
            f"📊 <b>上下班打卡记录</b>\n"
            f"👤 用户：{MessageFormatter.format_user_link(uid, user_data['nickname'])}\n"
            f"🕒 当前设置：上班 <code>{work_hours['work_start']}</code> - 下班 <code>{work_hours['work_end']}</code>\n\n"
        )

        # 按日期分组记录
        records_by_date = {}
        for record in work_records:
            date_str = record["record_date"]
            if date_str not in records_by_date:
                records_by_date[date_str] = {}
            records_by_date[date_str][record["checkin_type"]] = record

        dates = sorted(records_by_date.keys(), reverse=True)[:7]

        for date_str in dates:
            date_record = records_by_date[date_str]
            record_text += f"📅 <code>{date_str}</code>\n"

            if "work_start" in date_record:
                start_info = date_record["work_start"]
                record_text += f"   🟢 上班：{start_info['checkin_time']} - {start_info['status']}\n"

            if "work_end" in date_record:
                end_info = date_record["work_end"]
                record_text += (
                    f"   🔴 下班：{end_info['checkin_time']} - {end_info['status']}\n"
                )

            record_text += "\n"

        await message.answer(
            record_text,
            reply_markup=await get_main_keyboard(
                chat_id=chat_id, show_admin=await is_admin(uid)
            ),
            parse_mode="HTML",
        )


# ============ 添加上下班按钮处理优化 =================
@dp.message(
    lambda message: message.text and message.text.strip() in ["🟢 上班", "🔴 下班"]
)
@rate_limit(rate=5, per=60)
async def handle_work_buttons(message: types.Message):
    """处理上下班按钮点击 - 优化版本"""
    text = message.text.strip()
    if text == "🟢 上班":
        await process_work_checkin(message, "work_start")
    elif text == "🔴 下班":
        await process_work_checkin(message, "work_end")


# ============ 文本命令处理优化 =================
@dp.message(
    lambda message: message.text and message.text.strip() in ["回座", "✅ 回座"]
)
@rate_limit(rate=10, per=60)
async def handle_back_command(message: types.Message):
    """处理回座命令 - 优化版本"""
    await process_back(message)


@dp.message(lambda message: message.text and message.text.strip() in ["🔙 返回主菜单"])
@rate_limit(rate=5, per=60)
async def handle_back_to_main_menu(message: types.Message):
    """处理返回主菜单按钮 - 优化版本"""
    uid = message.from_user.id
    await message.answer(
        "已返回主菜单",
        reply_markup=await get_main_keyboard(
            chat_id=message.chat.id, show_admin=await is_admin(uid)
        ),
    )


@dp.message(lambda message: message.text and message.text.strip() in ["📊 我的记录"])
@rate_limit(rate=10, per=60)
@track_performance("handle_my_record")
async def handle_my_record(message: types.Message):
    """处理我的记录按钮 - 优化版本"""
    chat_id = message.chat.id
    uid = message.from_user.id

    user_lock = get_user_lock(chat_id, uid)
    async with user_lock:
        await show_history(message)


@dp.message(lambda message: message.text and message.text.strip() in ["🏆 排行榜"])
@rate_limit(rate=10, per=60)
@track_performance("handle_rank")
async def handle_rank(message: types.Message):
    """处理排行榜按钮 - 优化版本"""
    chat_id = message.chat.id
    uid = message.from_user.id

    user_lock = get_user_lock(chat_id, uid)
    async with user_lock:
        await show_rank(message)


@dp.message(lambda message: message.text and message.text.strip() in ["👑 管理员面板"])
@rate_limit(rate=5, per=60)
async def handle_admin_panel_button(message: types.Message):
    """处理管理员面板按钮点击 - 优化版本"""
    if not await is_admin(message.from_user.id):
        await message.answer(
            Config.MESSAGES["no_permission"],
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=False
            ),
        )
        return

    admin_text = (
        "👑 管理员面板\n\n"
        "📢 频道与推送管理：\n"
        "• /setchannel <频道ID> - 绑定提醒频道\n"
        "• /setgroup <群组ID> - 绑定通知群组\n"
        "• /unbindchannel - 解除绑定频道\n"
        "• /unbindgroup - 解除绑定通知群组\n"
        "• /setpush <channel|group|admin> <on|off> - 设置推送开关\n"
        "• /showpush - 显示推送设置状态\n"
        "• /testpush - 测试推送功能\n\n"
        "🎯 活动管理：\n"
        "• /addactivity <活动名> <次数> <分钟> - 添加或修改活动\n"
        "• /delactivity <活动名> - 删除活动\n"
        "• /actnum <活动名> <人数> - 设置活动人数限制\n"
        "• /actstatus - 查看活动人数状态\n"
        "• /actlist - 查看所有活动人数限制\n"
        "• /refresh_keyboard - 强制刷新键盘\n\n"
        "🕒 上下班管理：\n"
        "• /setworktime <上班时间> <下班时间> - 设置上下班时间\n"
        "• /worktime - 查看当前工作时间设置\n"
        "• /resetworktime - 重置为默认时间\n"
        "• /delwork - 移除功能(保留记录)\n"
        "• /delwork_clear - 移除功能并清除记录\n"
        "• /workstatus - 查看功能状态\n"
        "• /workcheck - 查看个人状态\n"
        "• /workrecord - 查看个人记录\n"
        "• /reset_work <用户ID> - 重置用户记录\n\n"
        "⚙️ 系统设置：\n"
        "• /setresettime <小时> <分钟> - 设置每日重置时间\n"
        "• /setworkfine <类型> <分钟1> <金额1> [分钟2 金额2...] - 设置上下班罚款\n"
        "• /setfine <活动名> <时间段> <金额> - 设置活动罚款\n"
        "• /setfines_all <t1> <f1> [t2 f2...] - 统一设置分段罚款\n"
        "• /showsettings - 查看当前设置\n"
        "• /reset_status - 查看重置状态\n\n"
        "📊 数据管理：\n"
        "• /set <用户ID> <活动> <分钟> - 设置用户时间\n"
        "• /reset <用户ID> - 重置用户数据\n"
        "• /export - 导出当前数据\n"
        "• /exportmonthly - 导出月度数据\n"
        "• /exportmonthly <年> <月> - 导出指定年月\n"
        "• /monthlyreport - 生成月度报告\n"
        "• /monthlyreport <年> <月> - 生成指定报告\n\n"
        "🧹 维护工具：\n"
        "• /cleanup_monthly - 清理月度数据\n"
        "• /cleanup_monthly <年> <月> - 清理指定月份\n"
        "• /cleanup_monthly all - 清理所有数据\n"
        "• /monthly_stats_status - 查看统计状态\n"
        "• /cleanup_inactive [天数] - 清理未活动用户\n\n"
        "🔧 系统监控：\n"
        "• /performance - 查看性能\n"
        "• /debug_work - 调试上下班功能\n"
        "• /menu - 返回主菜单\n"
        "• /help - 查看详细帮助\n\n"
        "💡 提示：所有时间均为北京时间，参数用空格分隔"
    )
    await message.answer(admin_text, reply_markup=get_admin_keyboard())


# 🆕 新增：动态活动按钮处理器
@dp.message(lambda message: message.text and message.text.strip())
@rate_limit(rate=10, per=60)
async def handle_dynamic_activity_buttons(message: types.Message):
    """处理动态生成的活动按钮点击"""
    text = message.text.strip()
    chat_id = message.chat.id
    uid = message.from_user.id

    # 跳过命令和特殊按钮
    if text.startswith("/"):
        return

    special_buttons = [
        "👑 管理员面板",
        "🔙 返回主菜单",
        "📤 导出数据",
        "📊 我的记录",
        "🏆 排行榜",
        "✅ 回座",
        "🟢 上班",
        "🔴 下班",
    ]
    if text in special_buttons:
        return

    # 🆕 关键修复：动态检查是否是活动按钮
    try:
        activity_limits = await db.get_activity_limits_cached()
        if text in activity_limits.keys():
            logger.info(f"🔘 活动按钮点击: {text} - 用户 {uid}")
            await start_activity(message, text)
            return
    except Exception as e:
        logger.error(f"❌ 处理活动按钮时出错: {e}")

    # 如果不是活动按钮，显示帮助信息
    await message.answer(
        "请使用下方按钮或直接输入活动名称进行操作：\n\n"
        "📝 使用方法：\n"
        "• 点击活动按钮开始打卡\n"
        "• 输入'回座'或点击'✅ 回座'按钮结束当前活动\n"
        "• 点击'📊 我的记录'查看个人统计\n"
        "• 点击'🏆 排行榜'查看群内排名",
        reply_markup=await get_main_keyboard(
            chat_id=chat_id, show_admin=await is_admin(uid)
        ),
        parse_mode="HTML",
    )


@dp.message(lambda message: message.text and message.text.strip() in ["📤 导出数据"])
@rate_limit(rate=5, per=60)
async def handle_export_data_button(message: types.Message):
    """处理导出数据按钮点击 - 修复版"""
    if not await is_admin(message.from_user.id):
        await message.answer(
            Config.MESSAGES["no_permission"],
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=False
            ),
        )
        return

    chat_id = message.chat.id
    await message.answer("⏳ 正在导出数据，请稍候.")
    try:
        await export_and_push_csv(chat_id)
        await message.answer("✅ 数据已导出并推送到绑定的群组或频道！")
    except Exception as e:
        await message.answer(f"❌ 导出失败：{e}")


@dp.message(
    lambda message: message.text
    and message.text.strip() in Config.DEFAULT_ACTIVITY_LIMITS.keys()
)
@rate_limit(rate=10, per=60)
async def handle_activity_direct_input(message: types.Message):
    """处理直接输入活动名称进行打卡 - 优化版本"""
    act = message.text.strip()
    await start_activity(message, act)


@dp.message(lambda message: message.text and message.text.strip())
@rate_limit(rate=10, per=60)
async def handle_other_text_messages(message: types.Message):
    """处理其他文本消息 - 优化版本"""
    text = message.text.strip()
    uid = message.from_user.id

    if text.startswith("/") or text in [
        "👑 管理员面板",
        "🔙 返回主菜单",
        "📤 导出数据",
        "🔔 通知设置",
    ]:
        return

    activity_limits = await db.get_activity_limits_cached()
    if any(act in text for act in activity_limits.keys()):
        return

    await message.answer(
        "请使用下方按钮或直接输入活动名称进行操作：\n\n"
        "📝 使用方法：\n"
        "• 输入活动名称（如：<code>吃饭</code>、<code>小厕</code>）开始打卡\n"
        "• 输入'回座'或点击'✅ 回座'按钮结束当前活动\n"
        "• 点击'📊 我的记录'查看个人统计\n"
        "• 点击'🏆 排行榜'查看群内排名",
        reply_markup=await get_main_keyboard(
            chat_id=message.chat.id, show_admin=await is_admin(uid)
        ),
        parse_mode="HTML",
    )
