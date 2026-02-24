"""
班次管理器 - 完整保留所有班次判定逻辑
"""

import logging
from datetime import datetime, timedelta, date
from typing import Dict, Optional, Any
from dataclasses import dataclass

logger = logging.getLogger("GroupCheckInBot.ShiftManager")


@dataclass
class ShiftInfo:
    """班次信息"""

    shift: str  # 'day' 或 'night'
    shift_detail: str  # 'day', 'night_last', 'night_tonight'
    record_date: date
    business_date: date
    is_dual: bool
    in_window: bool
    window_info: Dict[str, Any]
    using_state: bool


class ShiftManager:
    """班次管理器 - 完整版"""

    def __init__(self, db):
        self.db = db

    async def determine(
        self,
        chat_id: int,
        current_time: Optional[datetime] = None,
        checkin_type: str = "work_start",
        active_shift: Optional[str] = None,
        active_record_date: Optional[date] = None,
    ) -> ShiftInfo:
        """
        企业级班次判定 - 完整保留所有逻辑
        """
        now = current_time or self.db.get_beijing_time()
        shift_config = await self.db.get_shift_config(chat_id) or {}
        is_dual = shift_config.get("dual_mode", False)

        # ===== 单班模式 =====
        if not is_dual:
            business_date = await self.db.get_business_date(
                chat_id=chat_id,
                current_dt=now,
            )
            return ShiftInfo(
                shift="day",
                shift_detail="day",
                record_date=business_date,
                business_date=business_date,
                is_dual=False,
                in_window=True,
                window_info={},
                using_state=False,
            )

        # ===== 状态模型优先 =====
        if active_shift and active_record_date:
            return await self._determine_with_state(
                chat_id,
                now,
                shift_config,
                active_shift,
                active_record_date,
                checkin_type,
            )

        # ===== 无状态模式 =====
        return await self._determine_without_state(
            chat_id, now, shift_config, checkin_type
        )

    async def _determine_with_state(
        self,
        chat_id: int,
        now: datetime,
        config: Dict,
        active_shift: str,
        active_record_date: date,
        checkin_type: str,
    ) -> ShiftInfo:
        """有状态班次判定"""

        if active_shift not in ("day", "night"):
            raise ValueError(f"非法班次: {active_shift}")

        shift = active_shift
        record_date = active_record_date

        # 计算班次详情
        if shift == "day":
            shift_detail = "day"
        else:
            day_end = config.get("day_end", "21:00")
            day_end_time = datetime.strptime(day_end, "%H:%M").time()
            night_start = datetime.combine(record_date, day_end_time).replace(
                tzinfo=now.tzinfo
            )
            night_end = night_start + timedelta(days=1)
            shift_detail = (
                "night_tonight" if night_start <= now < night_end else "night_last"
            )

        # 获取窗口
        window_info = self._calculate_window(
            config, checkin_type, now, active_shift, record_date
        )

        # 判断窗口
        in_window = (
            True
            if checkin_type == "activity"
            else self._is_in_window(now, shift, shift_detail, checkin_type, window_info)
        )

        # 业务日期
        business_date = await self.db.get_business_date(
            chat_id=chat_id,
            current_dt=now,
            shift=shift,
            checkin_type=checkin_type,
            shift_detail=shift_detail,
            record_date=record_date,
        )

        return ShiftInfo(
            shift=shift,
            shift_detail=shift_detail,
            record_date=record_date,
            business_date=business_date,
            is_dual=True,
            in_window=in_window,
            window_info=window_info,
            using_state=True,
        )

    async def _determine_without_state(
        self,
        chat_id: int,
        now: datetime,
        config: Dict,
        checkin_type: str,
    ) -> ShiftInfo:
        """无状态班次判定"""

        window_info = self._calculate_window(config, checkin_type, now) or {}
        shift_detail = window_info.get("current_shift")

        if shift_detail is None:
            shift_detail = self._fallback_shift_detail(now, config)

        shift = "night" if shift_detail.startswith("night") else "day"
        in_window = (
            True
            if checkin_type == "activity"
            else self._is_in_window(now, shift, shift_detail, checkin_type, window_info)
        )

        record_date = await self.db.get_business_date(
            chat_id=chat_id,
            current_dt=now,
            shift=shift,
            checkin_type=checkin_type,
            shift_detail=shift_detail,
        )

        return ShiftInfo(
            shift=shift,
            shift_detail=shift_detail,
            record_date=record_date,
            business_date=record_date,
            is_dual=True,
            in_window=in_window,
            window_info=window_info,
            using_state=False,
        )

    def _calculate_window(
        self,
        config: Dict,
        checkin_type: str,
        now: datetime,
        active_shift: Optional[str] = None,
        active_record_date: Optional[date] = None,
    ) -> Dict:
        """计算打卡窗口 - 完整版"""

        tz = now.tzinfo

        # 解析时间
        day_start = config.get("day_start", "09:00")
        day_end = config.get("day_end", "21:00")
        grace_before = config.get("grace_before", 120)
        grace_after = config.get("grace_after", 360)
        workend_before = config.get("workend_grace_before", 120)
        workend_after = config.get("workend_grace_after", 360)

        # 确定基础日期
        if active_record_date:
            base_date = active_record_date
        else:
            base_date = now.date()

        day_start_dt = datetime.combine(
            base_date, datetime.strptime(day_start, "%H:%M").time()
        ).replace(tzinfo=tz)
        day_end_dt = datetime.combine(
            base_date, datetime.strptime(day_end, "%H:%M").time()
        ).replace(tzinfo=tz)

        # activity特殊处理
        if checkin_type == "activity":
            if active_shift:
                if active_shift == "day":
                    current = "day"
                else:
                    current = "night_tonight" if now >= day_end_dt else "night_last"
            else:
                if day_start_dt <= now < day_end_dt:
                    current = "day"
                elif now >= day_end_dt:
                    current = "night_tonight"
                else:
                    current = "night_last"

            return {"current_shift": current}

        # 构建窗口
        windows = {
            "day": {
                "work_start": {
                    "start": day_start_dt - timedelta(minutes=grace_before),
                    "end": day_start_dt + timedelta(minutes=grace_after),
                },
                "work_end": {
                    "start": day_end_dt - timedelta(minutes=workend_before),
                    "end": day_end_dt + timedelta(minutes=workend_after),
                },
            },
            "night_last": {
                "work_start": {
                    "start": day_end_dt
                    - timedelta(days=1)
                    - timedelta(minutes=workend_before),
                    "end": day_end_dt
                    - timedelta(days=1)
                    + timedelta(minutes=workend_after),
                },
                "work_end": {
                    "start": day_start_dt - timedelta(minutes=grace_before),
                    "end": day_start_dt + timedelta(minutes=grace_after),
                },
            },
            "night_tonight": {
                "work_start": {
                    "start": day_end_dt - timedelta(minutes=workend_before),
                    "end": day_end_dt + timedelta(minutes=workend_after),
                },
                "work_end": {
                    "start": day_start_dt
                    + timedelta(days=1)
                    - timedelta(minutes=grace_before),
                    "end": day_start_dt
                    + timedelta(days=1)
                    + timedelta(minutes=grace_after),
                },
            },
        }

        # 确定当前班次
        current = None
        if checkin_type in ("work_start", "work_end"):
            lookup = checkin_type
            for name, w in windows.items():
                if w[lookup]["start"] <= now <= w[lookup]["end"]:
                    current = name
                    break

        if current is None and active_shift:
            if active_shift == "day":
                current = "day"
            else:
                current = "night_tonight" if now >= day_end_dt else "night_last"

        return {
            "day_window": windows["day"],
            "night_window": {
                "last_night": windows["night_last"],
                "tonight": windows["night_tonight"],
            },
            "current_shift": current,
        }

    def _is_in_window(
        self,
        now: datetime,
        shift: str,
        shift_detail: str,
        checkin_type: str,
        window_info: Dict,
    ) -> bool:
        """检查是否在窗口内"""
        try:
            if checkin_type == "work_start":
                if shift == "day":
                    target = window_info.get("day_window", {}).get("work_start", {})
                else:
                    night = window_info.get("night_window", {})
                    if shift_detail == "night_last":
                        target = night.get("last_night", {}).get("work_start", {})
                    else:
                        target = night.get("tonight", {}).get("work_start", {})
            else:  # work_end
                if shift == "day":
                    target = window_info.get("day_window", {}).get("work_end", {})
                else:
                    night = window_info.get("night_window", {})
                    if shift_detail == "night_last":
                        target = night.get("last_night", {}).get("work_end", {})
                    else:
                        target = night.get("tonight", {}).get("work_end", {})

            return bool(
                target.get("start")
                and target.get("end")
                and target["start"] <= now <= target["end"]
            )
        except Exception as e:
            logger.error(f"窗口检查失败: {e}")
            return False

    def _fallback_shift_detail(self, now: datetime, config: Dict) -> str:
        """降级班次计算"""
        day_start = config.get("day_start", "09:00")
        day_end = config.get("day_end", "21:00")

        day_start_dt = datetime.combine(
            now.date(), datetime.strptime(day_start, "%H:%M").time()
        ).replace(tzinfo=now.tzinfo)
        day_end_dt = datetime.combine(
            now.date(), datetime.strptime(day_end, "%H:%M").time()
        ).replace(tzinfo=now.tzinfo)

        if day_start_dt <= now < day_end_dt:
            return "day"
        elif now >= day_end_dt:
            return "night_tonight"
        else:
            return "night_last"


# 全局实例
shift_manager = None


def init_shift_manager(db_instance):
    """初始化班次管理器"""
    global shift_manager
    shift_manager = ShiftManager(db_instance)
    return shift_manager
