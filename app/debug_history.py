
import asyncio
import logging
import sys
import os
from dotenv import load_dotenv

# Load env BEFORE importing app
load_dotenv()

# Ensure app is in path
sys.path.append(os.getcwd())

# Now import app modules
try:
    from app.db import init_db, get_conn
    from app.core.config import settings
except ImportError as e:
    print(f"Import Error: {e}")
    sys.exit(1)
except Exception as e:
    print(f"Config Error: {e}")
    sys.exit(1)

# Setup basic logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def run_debug():
    try:
        await init_db()
        pool = await get_conn()
        async with pool.acquire() as conn:
            print("Connected to DB.")
            
            # Simple check of video_stats count
            stats_count = await conn.fetchval(f"SELECT COUNT(*) FROM {settings.PG_SCHEMA}.video_stats")
            print(f"Total entries in video_stats: {stats_count}")

            # 1. Check total rows in history
            try:
                count = await conn.fetchval(f"SELECT COUNT(*) FROM {settings.PG_SCHEMA}.reels_views_history")
                print(f"Total rows in history: {count}")
            except Exception as e:
                print(f"Error querying history count: {e}")
                return

            # 2. Get top 5 videos with most history entries
            print("\n--- Videos with most history entries ---")
            rows = await conn.fetch(f"""
                SELECT video_id, COUNT(*) as cnt 
                FROM {settings.PG_SCHEMA}.reels_views_history 
                GROUP BY video_id 
                ORDER BY cnt DESC 
                LIMIT 5
            """)
            for r in rows:
                print(f"Video {r['video_id']}: {r['cnt']} entries")
                # Show details for this video
                details = await conn.fetch(f"""
                    SELECT week_start_date, views_count, collected_at
                    FROM {settings.PG_SCHEMA}.reels_views_history 
                    WHERE video_id = $1 
                    ORDER BY week_start_date DESC
                """, r['video_id'])
                for d in details:
                    print(f"   Date: {d['week_start_date']} | Views: {d['views_count']} | Collected: {d['collected_at']}")

    except Exception as e:
        logger.error(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(run_debug())
