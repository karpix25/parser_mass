import logging
from app.core.config import settings

logger = logging.getLogger(__name__)

async def update_views_history(conn):
    """
    Updates the reels_views_history table with the latest views count.
    """
    logger.info("📊 Updating reels_views_history...")
    try:
        await conn.execute(f"""
        INSERT INTO {settings.PG_SCHEMA}.reels_views_history (video_id, company_tag, platform, views_count, week_start_date)
        SELECT
          video_id,
          company AS company_tag,
          platform,
          CAST(views AS BIGINT) AS views_count,
          (date_trunc('week', (now() AT TIME ZONE 'America/Argentina/Buenos_Aires')))::date AS week_start_date
        FROM {settings.PG_SCHEMA}.video_stats
        WHERE company IS NOT NULL
        ON CONFLICT (video_id, week_start_date) DO UPDATE
        SET
          views_count = EXCLUDED.views_count,
          collected_at = now(),
          company_tag = EXCLUDED.company_tag,
          platform = EXCLUDED.platform;
        """)
        logger.info("✅ reels_views_history updated successfully.")
    except Exception as e:
        logger.error(f"❌ Failed to update reels_views_history: {e}")
