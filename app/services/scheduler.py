import logging
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from app.core.config import settings
from app.db import get_conn
from app.services.parser import parse_all

logger = logging.getLogger(__name__)

scheduler = AsyncIOScheduler(timezone="Europe/Moscow")

async def setup_scheduler(force_reload: bool = False):
    pool = await get_conn()
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT key, value FROM public.settings")
        settings_db = {r["key"]: r["value"] for r in rows}

    scheduler.remove_all_jobs()
    day_map = {"monday":"mon","tuesday":"tue","wednesday":"wed","thursday":"thu","friday":"fri","saturday":"sat","sunday":"sun"}
    
    # Use DB settings if available, otherwise env settings
    day_str = settings_db.get("SCHEDULE_DAY") or settings.SCHEDULE_DAY
    day = day_map.get(day_str.lower(), "mon")
    
    hour_str = settings_db.get("RUN_WEEKLY_AT_HOUR")
    hour = int(hour_str) if hour_str else settings.RUN_WEEKLY_AT_HOUR
    
    min_str = settings_db.get("RUN_WEEKLY_AT_MIN")
    minute = int(min_str) if min_str else settings.RUN_WEEKLY_AT_MIN

    scheduler.add_job(
        parse_all,
        CronTrigger(day_of_week=day, hour=hour, minute=minute, timezone="Europe/Moscow"),
        id="weekly_parser", replace_existing=True,
    )
    logger.info(f"🗓️ Автозапуск установлен: {day} в {hour:02d}:{minute:02d} (МСК)")
