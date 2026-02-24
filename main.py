"""
主程序入口 - 完整版
"""

import asyncio
import logging
import os
import sys
import signal
from datetime import datetime

from aiogram import Bot, Dispatcher
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import BotCommand, BotCommandScopeAllChatAdministrators

from config import Config
from bot_manager import init_bot_manager, bot_manager
from database import db
from shift_manager import init_shift_manager, shift_manager
from activity_timer import timer_manager
from notification import notification, init_notification_service
from dual_shift_reset import init_dual_reset, dual_reset
from data_export import init_data_exporter, data_exporter
from monthly_stats import init_monthly_stats, monthly_stats
from reset_manager import init_reset_manager, reset_manager
from admin_commands import AdminCommands, register_admin_commands
from user_commands import UserCommands, register_user_commands
from utils import (
    heartbeat_manager,
    user_lock_manager,
    shift_state_manager,
    init_shift_state_manager,
)
from performance import performance_monitor, global_cache, task_manager

# 配置日志
logging.basicConfig(
    level=getattr(logging, Config.LOG_LEVEL),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        (
            logging.FileHandler("bot.log", encoding="utf-8")
            if Config.LOG_LEVEL != "DEBUG"
            else logging.NullHandler()
        ),
    ],
)
logger = logging.getLogger("GroupCheckInBot")

# 全局变量
bot = None
dp = None
admin_commands = None
user_commands = None


# ========== 键盘生成 ==========
async def get_main_keyboard(chat_id: int, show_admin: bool = False):
    """获取主键盘"""
    from aiogram.types import ReplyKeyboardMarkup, KeyboardButton

    activities = await db.get_activity_configs()

    buttons = []
    row = []
    for act in activities.keys():
        row.append(KeyboardButton(text=act))
        if len(row) >= 3:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)

    # 检查是否启用上下班
    if await db.has_work_hours_enabled(chat_id):
        buttons.append([KeyboardButton(text="🟢 上班"), KeyboardButton(text="🔴 下班")])

    buttons.append([KeyboardButton(text="✅ 回座")])

    if show_admin:
        buttons.append(
            [
                KeyboardButton(text="👑 管理员面板"),
                KeyboardButton(text="📊 我的记录"),
                KeyboardButton(text="🏆 排行榜"),
            ]
        )
    else:
        buttons.append(
            [KeyboardButton(text="📊 我的记录"), KeyboardButton(text="🏆 排行榜")]
        )

    return ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)


def get_admin_keyboard():
    """获取管理员键盘"""
    from aiogram.types import ReplyKeyboardMarkup, KeyboardButton

    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="👑 管理员面板"), KeyboardButton(text="📤 导出数据")],
            [KeyboardButton(text="🔙 返回主菜单")],
        ],
        resize_keyboard=True,
    )


# ========== 按钮处理器 ==========
async def handle_admin_panel(message):
    """处理管理员面板按钮"""
    if not await is_admin(message.from_user.id):
        await message.answer("❌ 无权限")
        return

    await admin_commands.cmd_showsettings(message)


async def handle_export_button(message):
    """处理导出按钮"""
    await admin_commands.cmd_export(message)


async def handle_my_record(message):
    """处理我的记录按钮"""
    await user_commands.cmd_myinfo(message)


async def handle_rank(message):
    """处理排行榜按钮"""
    await user_commands.cmd_ranking(message)


async def handle_back_to_main(message):
    """处理返回主菜单"""
    uid = message.from_user.id
    await message.answer(
        "📋 主菜单",
        reply_markup=await get_main_keyboard(message.chat.id, await is_admin(uid)),
    )


async def handle_work_buttons(message):
    """处理上下班按钮"""
    text = message.text.strip()
    if text == "🟢 上班":
        await user_commands.cmd_workstart(message)
    elif text == "🔴 下班":
        await user_commands.cmd_workend(message)


async def handle_all_text(message):
    """处理所有文本消息"""
    text = message.text.strip()

    # 检查是否是活动
    activities = await db.get_activity_configs()
    if text in activities:
        await user_commands.start_activity(message, text)
        return

    # 检查是否是回座
    if text in ["✅ 回座", "回座"]:
        await user_commands.end_activity(message)
        return

    # 其他情况
    uid = message.from_user.id
    await message.answer(
        "请使用下方按钮进行操作",
        reply_markup=await get_main_keyboard(message.chat.id, await is_admin(uid)),
    )


# ========== 辅助函数 ==========
async def is_admin(user_id: int) -> bool:
    """检查管理员"""
    return user_id in Config.ADMINS


async def on_startup():
    """启动时执行"""
    logger.info("🎯 机器人启动中...")

    # 设置命令菜单
    user_commands_list = [
        BotCommand(command="start", description="开始"),
        BotCommand(command="help", description="帮助"),
        BotCommand(command="menu", description="主菜单"),
        BotCommand(command="ci", description="打卡"),
        BotCommand(command="at", description="回座"),
        BotCommand(command="workstart", description="上班"),
        BotCommand(command="workend", description="下班"),
        BotCommand(command="myinfo", description="我的记录"),
        BotCommand(command="ranking", description="排行榜"),
        BotCommand(command="myinfoday", description="白班记录"),
        BotCommand(command="myinfonight", description="夜班记录"),
        BotCommand(command="rankingday", description="白班排行"),
        BotCommand(command="rankingnight", description="夜班排行"),
    ]

    admin_commands_list = user_commands_list + [
        BotCommand(command="admin", description="管理员面板"),
        BotCommand(command="export", description="导出数据"),
        BotCommand(command="showsettings", description="查看设置"),
    ]

    await bot.set_my_commands(user_commands_list)
    await bot.set_my_commands(
        admin_commands_list, scope=BotCommandScopeAllChatAdministrators()
    )

    # 发送启动通知
    await notification.notify_startup()

    logger.info("✅ 启动完成")


async def on_shutdown():
    """关闭时执行"""
    logger.info("🛑 机器人关闭中...")

    # 停止所有服务
    await reset_manager.stop()
    await heartbeat_manager.stop()
    if shift_state_manager:
        await shift_state_manager.stop()
    await timer_manager.stop_all()

    # 关闭数据库
    await db.close()

    # 关闭Bot
    if bot:
        await bot.session.close()

    # 发送关闭通知
    uptime = time.time() - start_time
    await notification.notify_shutdown(uptime)

    logger.info("✅ 关闭完成")


# ========== 主函数 ==========
async def main():
    """主函数"""
    global bot, dp, admin_commands, user_commands, start_time

    start_time = time.time()

    try:
        # 初始化Bot管理器
        init_bot_manager(Config.TOKEN)
        await bot_manager.initialize()

        bot = bot_manager.bot
        dp = bot_manager.dispatcher

        # 初始化数据库
        await db.initialize()

        # 初始化服务
        init_notification_service(bot_manager, bot, db)
        init_shift_manager(db)
        init_dual_reset(db, notification)
        init_data_exporter(db, bot, notification)
        init_monthly_stats(db)
        init_reset_manager(db, notification, dual_reset)

        # 初始化班次状态管理器
        global shift_state_manager
        shift_state_manager = init_shift_state_manager(db)

        # 初始化命令处理器
        admin_commands = AdminCommands(db, bot, notification, shift_manager, dual_reset)
        user_commands = UserCommands(
            db, bot, notification, shift_manager, timer_manager
        )

        # 注册命令
        register_admin_commands(dp, admin_commands)
        register_user_commands(dp, user_commands)

        # 注册按钮处理器
        dp.message.register(handle_admin_panel, lambda m: m.text == "👑 管理员面板")
        dp.message.register(handle_export_button, lambda m: m.text == "📤 导出数据")
        dp.message.register(handle_my_record, lambda m: m.text == "📊 我的记录")
        dp.message.register(handle_rank, lambda m: m.text == "🏆 排行榜")
        dp.message.register(handle_back_to_main, lambda m: m.text == "🔙 返回主菜单")
        dp.message.register(
            handle_work_buttons, lambda m: m.text in ["🟢 上班", "🔴 下班"]
        )
        dp.message.register(handle_all_text)

        # 注册生命周期
        dp.startup.register(on_startup)
        dp.shutdown.register(on_shutdown)

        # 启动后台服务
        await reset_manager.start()
        await heartbeat_manager.start()
        await shift_state_manager.start()

        # 恢复班次状态
        await dual_reset.recover_states()

        # 启动轮询
        logger.info("🚀 开始轮询...")
        await dp.start_polling(bot)

    except KeyboardInterrupt:
        logger.info("收到中断信号")
    except Exception as e:
        logger.error(f"运行错误: {e}", exc_info=True)
    finally:
        await on_shutdown()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("程序已停止")
    except Exception as e:
        logger.error(f"致命错误: {e}", exc_info=True)
        sys.exit(1)
