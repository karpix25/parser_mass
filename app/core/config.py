import os
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    # App
    APP_PORT: int = 8000
    LOG_LEVEL: str = "INFO"
    
    # Database
    DATABASE_URL: str | None = None
    PG_USER: str | None = None
    PG_PASSWORD: str | None = None
    PG_HOST: str | None = None
    PG_PORT: int = 5432
    PG_DATABASE: str | None = None
    PG_SCHEMA: str = "public"
    
    # Google Sheets
    ACCOUNTS_SHEET_URL: str | None = None
    TAGS_SHEET_URL: str | None = None
    YOUTUBE_SHEET_URL: str | None = None
    TIKTOK_SHEET_URL: str | None = None
    OUTPUT_SHEET_URL: str | None = None  # Deprecated in favor of platform specific
    YOUTUBE_OUTPUT_SHEET_URL: str | None = None
    TIKTOK_OUTPUT_SHEET_URL: str | None = None
    GOOGLE_CREDENTIALS_B64: str | None = None
    
    # Instagram / RapidAPI
    RAPIDAPI_KEY: str
    RAPIDAPI_HOST: str
    IG_API_BASE: str
    MAX_REQ_PER_MIN: int = 240
    
    # Scheduler
    SCHEDULE_DAY: str = "monday"
    RUN_WEEKLY_AT_HOUR: int = 2
    RUN_WEEKLY_AT_MIN: int = 0

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )

settings = Settings()
