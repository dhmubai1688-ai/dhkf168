"""
用户命令处理器 - 完整保留所有用户命令
"""

import logging
from datetime import datetime, timedelta
from typing import Optional

from aiogram import types
from aiogram.filters import Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

logger = logging.getLogger("GroupCheckInBot.UserCommands")


class UserCommands:
    """用户命令处理器 - 完整版"""

    def __init__(self, db, bot, notification, shift_manager, timer_manager):
        self.db = db
        self.bot = bot
        self.notification = notification
        self.shift_manager = shift_manager
        self.timer_manager = timer_manager
        self.user_locks = {}

    def get_lock(self, chat_id: int, user_id: int):
        """获取用户锁"""
        key = f"{chat_id}-{user_id}"
        if key not in self.user_locks:
            self.user_locks[key] = asyncio.Lock()
        return self.user_locks[key]

    # ========== 基础命令 ==========
    async def cmd_start(self, message: types.Message):
        """开始命令"""
        from main import get_main_keyboard

        uid = message.from_user.id
        is_admin = await self._is_admin(uid)

        await message.answer(
            "👋 欢迎使用打卡机器人！\n\n请使用下方按钮开始打卡活动。",
            reply_markup=await get_main_keyboard(message.chat.id, is_admin),
        )

    async def cmd_help(self, message: types.Message):
        """帮助命令"""
        from main import get_main_keyboard

        uid = message.from_user.id
        is_admin = await self._is_admin(uid)

        text = (
            "📋 使用帮助\n\n"
            "🟢 开始活动：\n"
            "• 直接输入活动名称\n"
            "• 或使用命令：/ci 活动名\n"
            "• 或点击下方活动按钮\n\n"
            "🔴 结束活动：\n"
            "• 直接输入：回座\n"
            "• 或使用命令：/at\n"
            "• 或点击 ✅ 回座 按钮\n\n"
            "🕒 上下班打卡：\n"
            "• /workstart - 上班打卡\n"
            "• /workend - 下班打卡\n"
            "• 或点击 🟢 上班 / 🔴 下班 按钮\n\n"
            "📊 查看记录：\n"
            "• /myinfo - 我的记录\n"
            "• /ranking - 排行榜\n"
            "• /myinfoday - 白班记录\n"
            "• /myinfonight - 夜班记录\n"
            "• /rankingday - 白班排行\n"
            "• /rankingnight - 夜班排行\n\n"
            "🔧 其他：\n"
            "• /start - 开始\n"
            "• /help - 帮助\n"
            "• /menu - 主菜单"
        )

        await message.answer(
            text, reply_markup=await get_main_keyboard(message.chat.id, is_admin)
        )

    async def cmd_menu(self, message: types.Message):
        """主菜单"""
        from main import get_main_keyboard

        uid = message.from_user.id
        is_admin = await self._is_admin(uid)

        await message.answer(
            "📋 主菜单", reply_markup=await get_main_keyboard(message.chat.id, is_admin)
        )

    # ========== 活动命令 ==========
    async def cmd_ci(self, message: types.Message):
        """指令打卡"""
        args = message.text.split(maxsplit=1)
        if len(args) != 2:
            await message.answer("❌ 用法：/ci <活动名>")
            return

        act = args[1].strip()

        # 别名处理
        aliases = {
            "抽烟": "抽烟或休息",
            "休息": "抽烟或休息",
            "smoke": "抽烟或休息",
            "吸烟": "抽烟或休息",
        }
        if act in aliases:
            act = aliases[act]

        if not await self.db.activity_exists(act):
            await message.answer(f"❌ 活动 '{act}' 不存在")
            return

        await self.start_activity(message, act)

    async def cmd_at(self, message: types.Message):
        """指令回座"""
        await self.end_activity(message)

    async def start_activity(self, message: types.Message, activity: str):
        """开始活动"""
        chat_id = message.chat.id
        user_id = message.from_user.id
        lock = self.get_lock(chat_id, user_id)

        async with lock:
            # 重置检查
            await self._reset_if_needed(chat_id, user_id)

            # 检查活动存在
            if not await self.db.activity_exists(activity):
                await message.answer(f"❌ 活动 '{activity}' 不存在")
                return

            # 检查已有活动
            user = await self.db.get_user(chat_id, user_id)
            if user and user.get("current_activity"):
                await message.answer(f"❌ 您正在进行活动: {user['current_activity']}")
                return

            # 获取班次状态
            state = await self.db.get_active_shift(chat_id, user_id)
            if not state:
                await message.answer("❌ 您没有进行中的班次，请先打卡上班！")
                return

            # 班次判定
            now = self.db.get_beijing_time()
            shift_info = await self.shift_manager.determine(
                chat_id=chat_id,
                current_time=now,
                checkin_type="activity",
                active_shift=state["shift"],
                active_record_date=state["record_date"],
            )

            # 检查是否可以活动
            can, reason = await self._can_perform_activity(
                chat_id, user_id, shift_info.shift, shift_info.record_date
            )
            if not can:
                await message.answer(reason)
                return

            # 检查次数限制
            count = await self.db.get_activity_count(
                chat_id, user_id, activity, shift_info.shift
            )
            max_times = await self.db.get_activity_max_times(activity)
            if count >= max_times:
                shift_text = "白班" if shift_info.shift == "day" else "夜班"
                await message.answer(
                    f"❌ {shift_text}的 '{activity}' 次数已达上限\n"
                    f"📊 当前：{count}/{max_times}"
                )
                return

            # 检查人数限制
            user_limit = await self.db.get_activity_user_limit(activity)
            if user_limit > 0:
                current = await self.db.get_current_activity_users(chat_id, activity)
                if current >= user_limit:
                    await message.answer(f"❌ 活动 '{activity}' 人数已满！")
                    return

            # 开始活动
            name = message.from_user.full_name
            await self.db.update_user_activity(
                chat_id, user_id, activity, now, name, shift_info.shift
            )

            # 启动定时器
            time_limit = await self.db.get_activity_time_limit(activity)
            await self.timer_manager.start(
                chat_id, user_id, activity, time_limit, shift_info.shift
            )

            # 发送消息
            from main import get_main_keyboard
            from utils import MessageFormatter

            sent = await message.answer(
                MessageFormatter.format_activity_message(
                    user_id,
                    name,
                    activity,
                    now.strftime("%H:%M:%S"),
                    count + 1,
                    max_times,
                    time_limit,
                    shift_info.shift,
                ),
                reply_markup=await get_main_keyboard(
                    chat_id, await self._is_admin(user_id)
                ),
                parse_mode="HTML",
            )

            await self.db.update_checkin_message(chat_id, user_id, sent.message_id)

            # 吃饭通知
            if activity == "吃饭":
                await self.notification.notify_activity(
                    chat_id,
                    user_id,
                    name,
                    activity,
                    "start",
                    {"shift": shift_info.shift, "time": now.strftime("%H:%M:%S")},
                )

            logger.info(f"✅ {user_id} 开始活动 {activity} ({shift_info.shift})")

    async def end_activity(self, message: types.Message):
        """结束活动"""
        chat_id = message.chat.id
        user_id = message.from_user.id
        lock = self.get_lock(chat_id, user_id)

        async with lock:
            user = await self.db.get_user(chat_id, user_id)
            if not user or not user.get("current_activity"):
                await message.answer("❌ 您当前没有活动")
                return

            activity = user["current_activity"]
            start = datetime.fromisoformat(str(user["activity_start_time"]))
            now = self.db.get_beijing_time()
            elapsed = int((now - start).total_seconds())
            shift = user.get("shift", "day")

            # 计算超时
            time_limit = await self.db.get_activity_time_limit(activity)
            is_overtime = elapsed > time_limit * 60
            overtime_sec = max(0, elapsed - time_limit * 60)

            fine = 0
            if is_overtime:
                fine = await self.db.calculate_fine(activity, overtime_sec / 60)

            # 获取归属日期
            state = await self.db.get_active_shift(chat_id, user_id)
            if state:
                shift_info = await self.shift_manager.determine(
                    chat_id=chat_id,
                    current_time=start,
                    checkin_type="activity",
                    active_shift=state["shift"],
                    active_record_date=state["record_date"],
                )
                forced_date = shift_info.record_date
            else:
                forced_date = start.date()

            # 完成活动
            await self.db.complete_activity(
                chat_id,
                user_id,
                activity,
                elapsed,
                fine,
                is_overtime,
                shift,
                forced_date,
            )

            # 停止定时器
            await self.timer_manager.stop_user(chat_id, user_id, preserve_message=True)

            # 获取今日统计
            activities = await self.db.get_user_activities(chat_id, user_id)
            today_count = activities.get(shift, {}).get(activity, {}).get("count", 0)

            # 发送消息
            from main import get_main_keyboard
            from utils import MessageFormatter

            text = MessageFormatter.format_back_message(
                user_id,
                user.get("nickname", ""),
                activity,
                now.strftime("%H:%M:%S"),
                MessageFormatter.format_time(elapsed),
                MessageFormatter.format_time(
                    activities.get(shift, {}).get(activity, {}).get("time", 0)
                ),
                MessageFormatter.format_time(user.get("total_accumulated_time", 0)),
                {activity: today_count},
                user.get("total_activity_count", 0),
                is_overtime,
                overtime_sec,
                fine,
            )

            await message.answer(
                text,
                reply_markup=await get_main_keyboard(
                    chat_id, await self._is_admin(user_id)
                ),
                parse_mode="HTML",
            )

            # 吃饭结束通知
            if activity == "吃饭":
                await self.notification.notify_activity(
                    chat_id,
                    user_id,
                    user.get("nickname", ""),
                    activity,
                    "end",
                    {"duration": MessageFormatter.format_time(elapsed)},
                )

            # 超时通知
            if is_overtime and fine:
                await self.notification.notify_overtime(
                    chat_id,
                    user_id,
                    user.get("nickname", ""),
                    activity,
                    elapsed,
                    fine,
                    shift,
                )

            logger.info(f"✅ {user_id} 结束活动 {activity} ({shift})")

    # ========== 上下班命令 ==========
    async def cmd_workstart(self, message: types.Message):
        """上班打卡"""
        await self._work_checkin(message, "work_start")

    async def cmd_workend(self, message: types.Message):
        """下班打卡"""
        await self._work_checkin(message, "work_end")

    async def _work_checkin(self, message: types.Message, checkin_type: str):
        """上下班打卡核心逻辑"""
        chat_id = message.chat.id
        user_id = message.from_user.id
        name = message.from_user.full_name
        lock = self.get_lock(chat_id, user_id)

        async with lock:
            # 检查功能启用
            if not await self.db.has_work_hours_enabled(chat_id):
                await message.answer("❌ 本群组未启用上下班功能")
                return

            # 重置检查
            await self._reset_if_needed(chat_id, user_id)

            # 班次判定
            now = self.db.get_beijing_time()
            shift_info = await self.shift_manager.determine(
                chat_id=chat_id,
                current_time=now,
                checkin_type=checkin_type,
            )

            if not shift_info.in_window:
                await message.answer("❌ 当前不在打卡窗口内")
                return

            action_text = "上班" if checkin_type == "work_start" else "下班"

            # ===== 上班打卡 =====
            if checkin_type == "work_start":
                # 检查重复
                has = await self._check_record(
                    chat_id, user_id, "work_start", shift_info
                )
                if has:
                    await message.answer(f"❌ 您本班次已经打过{action_text}卡了！")
                    return

                # 检查是否已下班
                has_end = await self._check_record(
                    chat_id, user_id, "work_end", shift_info
                )
                if has_end:
                    await message.answer(
                        f"❌ 您本班次已经下班，无法再打{action_text}卡！"
                    )
                    return

                # 计算迟到
                config = await self.db.get_shift_config(chat_id)
                if shift_info.shift == "day":
                    expected = config.get("day_start", "09:00")
                    expected_date = shift_info.record_date
                else:
                    expected = config.get("day_end", "21:00")
                    expected_date = shift_info.record_date

                expected_dt = datetime.combine(
                    expected_date, datetime.strptime(expected, "%H:%M").time()
                ).replace(tzinfo=now.tzinfo)

                diff = int((now - expected_dt).total_seconds() / 60)
                fine = 0
                status = "✅ 准时"

                if diff > 0:
                    fine = await self._calc_work_fine("work_start", diff)
                    status = f"🚨 迟到 {self._format_duration(diff*60)}"

                # 记录
                await self.db.add_work_record(
                    chat_id,
                    user_id,
                    shift_info.record_date,
                    checkin_type,
                    now.strftime("%H:%M"),
                    status,
                    diff,
                    fine,
                    shift_info.shift,
                    shift_info.shift_detail,
                )

                # 设置班次状态
                await self.db.set_shift_state(
                    chat_id, user_id, shift_info.shift, shift_info.record_date
                )

                # 发送消息
                await message.answer(
                    f"✅ {action_text}打卡完成\n"
                    f"👤 {self._format_user_link(user_id, name)}\n"
                    f"⏰ {now.strftime('%H:%M')}\n"
                    f"📊 {status}",
                    parse_mode="HTML",
                )

                # 通知
                group = await self.db.get_group(chat_id)
                await self.notification.notify_work(
                    chat_id,
                    user_id,
                    name,
                    now.strftime("%H:%M"),
                    expected_dt.strftime("%H:%M"),
                    action_text,
                    status,
                    fine,
                    shift_info.shift,
                    group.get("extra_work_group") if group else None,
                )

            # ===== 下班打卡 =====
            else:
                # 检查重复
                has = await self._check_record(chat_id, user_id, "work_end", shift_info)
                if has:
                    await message.answer(f"❌ 您本班次已经打过{action_text}卡了！")
                    return

                # 检查是否有上班记录
                work_date = shift_info.record_date
                if shift_info.shift == "night":
                    work_date = shift_info.record_date - timedelta(days=1)

                has_start = await self._check_record_date(
                    chat_id, user_id, "work_start", shift_info.shift, work_date
                )
                if not has_start:
                    await message.answer(f"❌ 未找到上班记录，无法{action_text}打卡！")
                    return

                # 计算早退
                config = await self.db.get_shift_config(chat_id)
                if shift_info.shift == "day":
                    expected = config.get("day_end", "18:00")
                    expected_date = shift_info.record_date
                else:
                    expected = config.get("day_start", "09:00")
                    expected_date = shift_info.record_date + timedelta(days=1)

                expected_dt = datetime.combine(
                    expected_date, datetime.strptime(expected, "%H:%M").time()
                ).replace(tzinfo=now.tzinfo)

                diff = int((now - expected_dt).total_seconds() / 60)
                fine = 0
                status = "✅ 准时"

                if diff < 0:
                    fine = await self._calc_work_fine("work_end", abs(diff))
                    status = f"🚨 早退 {self._format_duration(abs(diff)*60)}"
                elif diff > 0:
                    status = f"✅ 加班 {self._format_duration(diff*60)}"

                # 结束活动
                user = await self.db.get_user(chat_id, user_id)
                if user and user.get("current_activity"):
                    await self._force_end_activity(
                        chat_id, user_id, user, shift_info.shift
                    )

                # 记录
                await self.db.add_work_record(
                    chat_id,
                    user_id,
                    shift_info.record_date,
                    checkin_type,
                    now.strftime("%H:%M"),
                    status,
                    diff,
                    fine,
                    shift_info.shift,
                    shift_info.shift_detail,
                )

                # 清除班次状态
                await self.db.clear_shift_state(chat_id, user_id, shift_info.shift)

                # 发送消息
                await message.answer(
                    f"✅ {action_text}打卡完成\n"
                    f"👤 {self._format_user_link(user_id, name)}\n"
                    f"⏰ {now.strftime('%H:%M')}\n"
                    f"📊 {status}",
                    parse_mode="HTML",
                )

                # 通知
                group = await self.db.get_group(chat_id)
                await self.notification.notify_work(
                    chat_id,
                    user_id,
                    name,
                    now.strftime("%H:%M"),
                    expected_dt.strftime("%H:%M"),
                    action_text,
                    status,
                    fine,
                    shift_info.shift,
                    group.get("extra_work_group") if group else None,
                )

    # ========== 记录查看 ==========
    async def cmd_myinfo(self, message: types.Message):
        """我的记录"""
        await self._show_history(message)

    async def cmd_myinfo_day(self, message: types.Message):
        """白班记录"""
        await self._show_history(message, "day")

    async def cmd_myinfo_night(self, message: types.Message):
        """夜班记录"""
        await self._show_history(message, "night")

    async def _show_history(self, message: types.Message, shift: Optional[str] = None):
        """显示记录"""
        chat_id = message.chat.id
        user_id = message.from_user.id

        user = await self.db.get_user(chat_id, user_id)
        if not user:
            await message.answer("暂无记录")
            return

        now = self.db.get_beijing_time()
        business = await self.db.get_business_date(chat_id, now)
        config = await self.db.get_shift_config(chat_id)
        day_start = config.get("day_start", "09:00")
        day_start_h = int(day_start.split(":")[0])

        # 确定查询日期
        if shift == "night":
            query_date = business - timedelta(days=1)
        elif shift == "day" and now.hour < day_start_h:
            query_date = business - timedelta(days=1)
        else:
            query_date = business

        # 获取活动记录
        activities = await self.db.get_user_activities(chat_id, user_id, query_date)

        # 获取工作记录
        work = await self.db.get_work_records(
            chat_id, user_id, shift, query_date, query_date
        )

        # 构建消息
        from utils import MessageFormatter

        text = f"👤 用户：{MessageFormatter.format_user_link(user_id, user.get('nickname', ''))}\n"
        text += f"📅 日期：{query_date}\n\n"

        if shift:
            text += f"📊 【{'白班' if shift=='day' else '夜班'}】记录\n"
            shift_data = activities.get(shift, {})
            if shift_data:
                for act, data in shift_data.items():
                    text += f"• {act}: {MessageFormatter.format_time(data['time'])} ({data['count']}次)\n"
            else:
                text += "暂无活动记录\n"
        else:
            for s in ["day", "night"]:
                s_data = activities.get(s, {})
                if s_data:
                    text += f"\n【{'白班' if s=='day' else '夜班'}】\n"
                    for act, data in s_data.items():
                        text += f"• {act}: {MessageFormatter.format_time(data['time'])} ({data['count']}次)\n"

        if work:
            text += "\n🕒 上下班\n"
            for ct in ["work_start", "work_end"]:
                if ct in work and work[ct]:
                    latest = work[ct][0]
                    text += f"• {'上班' if ct=='work_start' else '下班'}: {latest['checkin_time']} ({latest['status']})\n"

        # 罚款
        fines = 0
        async with self.db.pool.acquire() as conn:
            fines = (
                await conn.fetchval(
                    """
                SELECT SUM(accumulated_time) FROM daily_statistics
                WHERE chat_id = $1 AND user_id = $2 AND record_date = $3
                AND activity_name IN ('total_fines', 'work_fines', 'work_start_fines', 'work_end_fines')
            """,
                    chat_id,
                    user_id,
                    query_date,
                )
                or 0
            )

        if fines:
            text += f"\n💰 罚款：{fines}分"

        from main import get_main_keyboard

        await message.answer(
            text,
            reply_markup=await get_main_keyboard(
                chat_id, await self._is_admin(user_id)
            ),
            parse_mode="HTML",
        )

    # ========== 排行榜 ==========
    async def cmd_ranking(self, message: types.Message):
        """排行榜"""
        await self._show_rank(message)

    async def cmd_ranking_day(self, message: types.Message):
        """白班排行榜"""
        await self._show_rank(message, "day")

    async def cmd_ranking_night(self, message: types.Message):
        """夜班排行榜"""
        await self._show_rank(message, "night")

    async def _show_rank(self, message: types.Message, shift: Optional[str] = None):
        """显示排行榜"""
        chat_id = message.chat.id

        now = self.db.get_beijing_time()
        business = await self.db.get_business_date(chat_id, now)
        config = await self.db.get_shift_config(chat_id)
        day_start = config.get("day_start", "09:00")
        day_start_h = int(day_start.split(":")[0])

        # 确定查询日期
        if shift == "night":
            query_date = business - timedelta(days=1)
        elif shift == "day" and now.hour < day_start_h:
            query_date = business - timedelta(days=1)
        else:
            query_date = business

        # 获取统计
        stats = await self.db.get_group_stats(chat_id, query_date)
        if shift:
            stats = [s for s in stats if s.get("shift") == shift]

        # 排序
        stats.sort(key=lambda x: x.get("total_time", 0), reverse=True)

        from utils import MessageFormatter

        text = f"🏆 排行榜\n📅 {query_date}\n\n"
        if shift:
            text += f"【{'白班' if shift=='day' else '夜班'}】\n"

        for i, s in enumerate(stats[:10], 1):
            name = s.get("nickname", f"用户{s['user_id']}")
            time_str = MessageFormatter.format_time(s.get("total_time", 0))
            count = s.get("total_count", 0)
            text += f"{i}. {name} - {time_str} ({count}次)\n"

        if not stats:
            text += "暂无数据"

        from main import get_main_keyboard

        await message.answer(
            text,
            reply_markup=await get_main_keyboard(
                chat_id, await self._is_admin(message.from_user.id)
            ),
            parse_mode="HTML",
        )

    # ========== 回调处理 ==========
    async def handle_back_callback(self, callback: types.CallbackQuery):
        """处理回座回调"""
        try:
            data = callback.data.split(":")
            if len(data) < 4:
                await callback.answer("❌ 数据错误")
                return

            chat_id = int(data[1])
            user_id = int(data[2])
            shift = data[3]

            if callback.from_user.id != user_id:
                await callback.answer("❌ 这不是您的按钮")
                return

            # 创建模拟消息
            class MockMessage:
                def __init__(self, cid, uid, mid):
                    self.chat = type("obj", (object,), {"id": cid})
                    self.from_user = type("obj", (object,), {"id": uid})
                    self.message_id = mid

                async def answer(self, text, **kwargs):
                    await callback.message.answer(text, **kwargs)

            msg = MockMessage(chat_id, user_id, callback.message.message_id)
            await self.end_activity(msg)

            await callback.answer("✅ 已回座")
            await callback.message.edit_reply_markup(reply_markup=None)

        except Exception as e:
            logger.error(f"回调失败: {e}")
            await callback.answer("❌ 处理失败")

    # ========== 辅助方法 ==========
    async def _reset_if_needed(self, chat_id: int, user_id: int):
        """检查并重置"""
        now = self.db.get_beijing_time()
        business = await self.db.get_business_date(chat_id, now)

        user = await self.db.get_user(chat_id, user_id)
        if not user:
            await self.db.init_user(chat_id, user_id)
            return

        last = user.get("last_updated")
        if isinstance(last, str):
            try:
                last = datetime.fromisoformat(last).date()
            except:
                last = business

        if last < business:
            logger.info(f"重置用户: {chat_id}-{user_id}")
            if user.get("current_activity"):
                await self._force_end_activity(
                    chat_id, user_id, user, user.get("shift", "day")
                )
            await self.db.reset_user_daily(chat_id, user_id, business)

    async def _force_end_activity(
        self, chat_id: int, user_id: int, user: Dict, shift: str
    ):
        """强制结束活动"""
        try:
            activity = user["current_activity"]
            start = datetime.fromisoformat(str(user["activity_start_time"]))
            now = self.db.get_beijing_time()
            elapsed = int((now - start).total_seconds())

            limit = await self.db.get_activity_time_limit(activity)
            fine = await self.db.calculate_fine(
                activity, max(0, elapsed - limit * 60) / 60
            )

            await self.db.complete_activity(
                chat_id, user_id, activity, elapsed, fine, True, shift
            )
        except Exception as e:
            logger.error(f"强制结束失败: {e}")

    async def _can_perform_activity(
        self, chat_id: int, user_id: int, shift: str, record_date: date
    ) -> tuple[bool, str]:
        """检查是否可以活动"""
        state = await self.db.get_shift_state(chat_id, user_id, shift)
        if not state:
            shift_text = "白班" if shift == "day" else "夜班"
            return False, f"❌ 您没有进行中的{shift_text}班次，请先打卡上班！"

        start = state["shift_start_time"]
        if isinstance(start, str):
            start = datetime.fromisoformat(start)

        if self.db.get_beijing_time() - start > timedelta(hours=16):
            await self.db.clear_shift_state(chat_id, user_id, shift)
            shift_text = "白班" if shift == "day" else "夜班"
            return False, f"❌ 您的{shift_text}班次已过期，请重新打卡上班！"

        async with self.db.pool.acquire() as conn:
            has_end = await conn.fetchval(
                """
                SELECT 1 FROM work_records
                WHERE chat_id = $1 AND user_id = $2 AND checkin_type = 'work_end'
                  AND shift = $3 AND record_date = $4
            """,
                chat_id,
                user_id,
                shift,
                state["record_date"],
            )

            if has_end:
                shift_text = "白班" if shift == "day" else "夜班"
                return False, f"❌ 您本{shift_text}已下班，无法进行活动！"

        return True, ""

    async def _check_record(self, chat_id: int, user_id: int, ct: str, info) -> bool:
        """检查记录"""
        return await self._check_record_date(
            chat_id, user_id, ct, info.shift, info.record_date
        )

    async def _check_record_date(
        self, chat_id: int, user_id: int, ct: str, shift: str, date: date
    ) -> bool:
        """按日期检查记录"""
        async with self.db.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT 1 FROM work_records
                WHERE chat_id = $1 AND user_id = $2 AND checkin_type = $3
                  AND shift = $4 AND record_date = $5
            """,
                chat_id,
                user_id,
                ct,
                shift,
                date,
            )
            return bool(row)

    async def _calc_work_fine(self, ct: str, minutes: float) -> int:
        """计算上下班罚款"""
        rates = await self.db.get_work_fine_rates(ct)
        if not rates:
            return 0
        segments = sorted([int(k) for k in rates.keys()])
        for s in segments:
            if minutes >= s:
                return rates[str(s)]
        return 0

    async def _is_admin(self, user_id: int) -> bool:
        """检查管理员"""
        from config import Config

        return user_id in Config.ADMINS

    def _format_user_link(self, user_id: int, name: str) -> str:
        """格式化用户链接"""
        clean = str(name).replace("<", "").replace(">", "").replace("&", "")
        return f'<a href="tg://user?id={user_id}">{clean}</a>'

    def _format_duration(self, seconds: int) -> str:
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


# ========== 注册函数 ==========
def register_user_commands(dp, user_commands):
    """注册用户命令"""

    # 基础命令
    dp.message.register(user_commands.cmd_start, Command("start"))
    dp.message.register(user_commands.cmd_help, Command("help"))
    dp.message.register(user_commands.cmd_menu, Command("menu"))

    # 活动命令
    dp.message.register(user_commands.cmd_ci, Command("ci"))
    dp.message.register(user_commands.cmd_at, Command("at"))

    # 上下班命令
    dp.message.register(user_commands.cmd_workstart, Command("workstart"))
    dp.message.register(user_commands.cmd_workend, Command("workend"))

    # 记录查看
    dp.message.register(user_commands.cmd_myinfo, Command("myinfo"))
    dp.message.register(user_commands.cmd_myinfo_day, Command("myinfoday"))
    dp.message.register(user_commands.cmd_myinfo_night, Command("myinfonight"))

    # 排行榜
    dp.message.register(user_commands.cmd_ranking, Command("ranking"))
    dp.message.register(user_commands.cmd_ranking_day, Command("rankingday"))
    dp.message.register(user_commands.cmd_ranking_night, Command("rankingnight"))

    # 回调
    dp.callback_query.register(
        user_commands.handle_back_callback,
        lambda c: c.data and c.data.startswith("back:"),
    )
