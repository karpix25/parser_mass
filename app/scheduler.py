import asyncio, logging
from datetime import datetime, timedelta
logger = logging.getLogger(__name__)

class Scheduler:
    def __init__(self): self.tasks=[]
    async def start(self): logger.info("🕒 Scheduler started")

    def every_hours(self, coro_fn, hours:int):
        async def loop():
            while True:
                try: await coro_fn()
                except Exception as e: logger.exception("Scheduled job failed: %s", e)
                await asyncio.sleep(hours*3600)
        logger.info("📅 Job scheduled every %sh", hours)
        self.tasks.append(asyncio.create_task(loop()))

    def weekly_at_utc(self, coro_fn, weekday:str, hour:int, minute:int):
        wd = dict(monday=0,tuesday=1,wednesday=2,thursday=3,friday=4,saturday=5,sunday=6)[weekday.lower()]
        async def loop():
            while True:
                now = datetime.utcnow()
                days = (wd - now.weekday()) % 7
                run = (now + timedelta(days=days)).replace(hour=hour, minute=minute, second=0, microsecond=0)
                if run <= now: run += timedelta(days=7)
                await asyncio.sleep((run-now).total_seconds())
                try: await coro_fn()
                except Exception as e: logger.exception("Weekly job failed: %s", e)
        logger.info("📅 Job scheduled weekly on %s %02d:%02d UTC", weekday, hour, minute)
        self.tasks.append(asyncio.create_task(loop()))

scheduler = Scheduler()
