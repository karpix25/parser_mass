import json, asyncio, logging, sys
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from app.core.config import settings
from app.db import init_db
from app.services.scheduler import scheduler, setup_scheduler
from app.api.routes import router

# Setup logging
class EmojiFormatter(logging.Formatter):
    COLORS = {
        "INFO": "\033[92m",    # зеленый
        "WARNING": "\033[93m", # желтый
        "ERROR": "\033[91m",   # красный
        "DEBUG": "\033[94m",   # синий
        "RESET": "\033[0m"
    }
    EMOJIS = {
        "INFO": "ℹ️",
        "WARNING": "⚠️",
        "ERROR": "🚫",
        "DEBUG": "🔍",
    }

    def format(self, record):
        color = self.COLORS.get(record.levelname, "")
        emoji = self.EMOJIS.get(record.levelname, "")
        reset = self.COLORS["RESET"]
        # time = datetime.now().strftime("%H:%M:%S") # datetime not imported
        # Let's use record.asctime if available or just skip time for simplicity in this formatter
        # or import datetime.
        # Actually, let's keep it simple or use loguru as planned?
        # The plan said "Use loguru", but I haven't fully replaced logging with loguru everywhere yet.
        # Let's keep the existing formatter for now to minimize visual changes for the user, 
        # but import datetime.
        from datetime import datetime
        time = datetime.now().strftime("%H:%M:%S")
        message = super().format(record)
        return f"{color}{emoji} [{time}] {record.levelname:<8}{reset} {message}"

handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(EmojiFormatter("%(message)s"))
logging.basicConfig(level=settings.LOG_LEVEL, handlers=[handler])

logger = logging.getLogger("app.main")

app = FastAPI()

# Mount static if needed, or just templates
# app.mount("/static", StaticFiles(directory="static"), name="static")

app.include_router(router)

@app.on_event("startup")
async def startup():
    await init_db()
    logger.info("✅ Database initialized")
    await setup_scheduler()
    scheduler.start()
    logger.info("🕒 Scheduler started (weekly + manual mode)")

