# app/db.py
from app.core.config import settings
import asyncpg
from loguru import logger
from urllib.parse import urlsplit, urlunsplit

db_pool: asyncpg.Pool | None = None


def _mask_dsn(dsn: str) -> str:
    try:
        parsed = urlsplit(dsn)
        if not parsed.password:
            return dsn
        username = parsed.username or ""
        host = parsed.hostname or ""
        port = f":{parsed.port}" if parsed.port else ""
        netloc = f"{username}:***@{host}{port}" if username else f"***@{host}{port}"
        return urlunsplit((parsed.scheme, netloc, parsed.path, parsed.query, parsed.fragment))
    except Exception:
        return "<hidden>"


async def init_db() -> asyncpg.Pool:
    """
    Инициализация пула соединений с PostgreSQL.
    Вызывается один раз при старте, далее возвращает уже созданный пул.
    """
    global db_pool
    if db_pool is not None:
        logger.info("🔁 DB pool already initialized")
        return db_pool

    if settings.DATABASE_URL:
        dsn = settings.DATABASE_URL
    else:
        missing = [
            name for name, value in {
                "PG_USER": settings.PG_USER,
                "PG_PASSWORD": settings.PG_PASSWORD,
                "PG_HOST": settings.PG_HOST,
                "PG_DATABASE": settings.PG_DATABASE,
            }.items()
            if not value
        ]
        if missing:
            raise RuntimeError(
                "Database config is incomplete. Set DATABASE_URL or provide: "
                + ", ".join(missing)
            )

        dsn = (
            f"postgresql://{settings.PG_USER}:{settings.PG_PASSWORD}"
            f"@{settings.PG_HOST}:{settings.PG_PORT}/{settings.PG_DATABASE}"
        )

    logger.info(f"🗄️ Connecting to Postgres: {_mask_dsn(dsn)}")

    # ✅ безопасная конфигурация для PgBouncer или прямого подключения
    db_pool = await asyncpg.create_pool(
        dsn,
        min_size=2,
        max_size=10,
        statement_cache_size=0,  # 🔧 отключает prepared statements (важно при PgBouncer)
        command_timeout=60,      # ⏱️ защита от зависших запросов
    )

    logger.info("✅ DB initialized and pool ready")

    # (опционально) гарантируем схему/таблицы, если их ещё нет
    await _ensure_schema_and_tables(db_pool)

    return db_pool


async def get_conn() -> asyncpg.Pool:
    """
    Безопасный способ получить пул.
    Если пул ещё не создан (например, задача запущена планировщиком в отдельном контексте),
    инициализируем его.
    """
    global db_pool
    if db_pool is None:
        await init_db()
    return db_pool


# ---------- helpers ----------

async def _ensure_schema_and_tables(pool: asyncpg.Pool) -> None:
    schema = settings.PG_SCHEMA

    create_sql = f"""
    CREATE SCHEMA IF NOT EXISTS {schema};

    CREATE TABLE IF NOT EXISTS {schema}.parse_runs (
        id             BIGSERIAL PRIMARY KEY,
        started_at     TIMESTAMP WITH TIME ZONE NOT NULL,
        finished_at    TIMESTAMP WITH TIME ZONE NOT NULL,
        status         TEXT NOT NULL,
        total_accounts INTEGER NOT NULL DEFAULT 0,
        new_videos     INTEGER NOT NULL DEFAULT 0,
        errors         INTEGER NOT NULL DEFAULT 0,
        accounts_json  TEXT NULL
    );

    CREATE TABLE IF NOT EXISTS {schema}.video_stats (
        id           BIGSERIAL PRIMARY KEY,
        client_tag   TEXT NULL,
        platform     TEXT NOT NULL,
        account      TEXT NOT NULL,
        video_id     TEXT NOT NULL,
        video_url    TEXT NOT NULL,
        publish_date DATE NOT NULL,
        iso_year     INTEGER NOT NULL,
        week         INTEGER NOT NULL,
        views        BIGINT NOT NULL DEFAULT 0,
        likes        BIGINT NOT NULL DEFAULT 0,
        comments     BIGINT NOT NULL DEFAULT 0,
        created_at   TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
        updated_at   TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
        company      TEXT NULL,
        product      TEXT NULL,
        caption      TEXT NULL
    );

    -- уникальность по URL, чтобы ON CONFLICT (video_url) DO NOTHING работал
    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM pg_indexes
            WHERE schemaname = '{schema}' AND indexname = 'ux_video_stats_video_url'
        ) THEN
            CREATE UNIQUE INDEX ux_video_stats_video_url
                ON {schema}.video_stats (video_url);
        END IF;

        IF NOT EXISTS (
            SELECT 1 FROM pg_indexes
            WHERE schemaname = '{schema}' AND indexname = 'ux_video_stats_video_id'
        ) THEN
            CREATE UNIQUE INDEX ux_video_stats_video_id
                ON {schema}.video_stats (video_id);
        END IF;
    END$$;

    CREATE TABLE IF NOT EXISTS {schema}.reels_views_history (
        id              BIGSERIAL PRIMARY KEY,
        video_id        TEXT NOT NULL,
        company_tag     TEXT,
        platform        TEXT,
        views_count     BIGINT,
        week_start_date DATE NOT NULL,
        collected_at    TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
        UNIQUE (video_id, week_start_date)
    );

    CREATE TABLE IF NOT EXISTS {schema}.settings (
        key   TEXT PRIMARY KEY,
        value TEXT
    );

    -- Ensure accounts_json column exists (migration for existing tables)
    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1 
            FROM information_schema.columns 
            WHERE table_schema = '{schema}' 
            AND table_name = 'parse_runs' 
            AND column_name = 'accounts_json'
        ) THEN
            ALTER TABLE {schema}.parse_runs ADD COLUMN accounts_json TEXT NULL;
        END IF;
    END$$;
    """

    async with pool.acquire() as conn:
        await conn.execute(create_sql)
