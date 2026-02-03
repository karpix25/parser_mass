# app/parser_youtube.py
import os
import aiohttp
import asyncio
import logging
from datetime import datetime
from tenacity import RetryError

from app.tagging import match_tags
from app.core.utils import api_retry

logger = logging.getLogger(__name__)

def _must(n: str) -> str:
    v = os.getenv(n)
    if not v:
        raise RuntimeError(f"ENV {n} is required")
    return v

SC_API_KEY = os.getenv("SCRAPECREATORS_KEY", "")
SC_BASE = "https://api.scrapecreators.com/v1/youtube"

# мягкий ограничитель на случай массовых запусков
MAX_RPM = int(os.getenv("SC_MAX_REQ_PER_MIN", "120"))
_MIN_INTERVAL = max(0.01, 60.0 / max(1, MAX_RPM))


# ----------------------------
# Низкоуровневые запросы SC
# ----------------------------
@api_retry
async def _get_json(session: aiohttp.ClientSession, url: str, params: dict | None = None) -> dict:
    headers = {"x-api-key": SC_API_KEY, "accept": "application/json"}
    async with session.get(url, headers=headers, params=params, timeout=60) as resp:
        if resp.status != 200:
            txt = await resp.text()
            raise aiohttp.ClientResponseError(
                resp.request_info,
                resp.history,
                status=resp.status,
                message=f"SC {url} -> {resp.status}: {txt[:300]}",
            )
        return await resp.json()


async def fetch_shorts_simple(session, channel_id: str, amount: int = 20):
    """
    Получает список YouTube Shorts с канала по handle или channelId.
    Возвращает массив словарей с {id, url, title, thumbnail, viewCountInt}
    """
    params = {"amount": str(amount)}

    # Определяем — это handle или channelId
    if channel_id.startswith("UC"):
        params["channelId"] = channel_id
    else:
        params["handle"] = channel_id

    url = f"{SC_BASE}/channel/shorts/simple"

    try:
        data = await _get_json(session, url, params)
        logger.debug("📦 Shorts response example: %s", data[:1] if isinstance(data, list) else data)
        return data
    except Exception as e:
        logger.warning("⚠️ Fetch shorts failed after retries: %s", e)
        return []


async def fetch_video_details(session, video_url: str):
    """
    Возвращает полные детали видео (просмотры, лайки, описание, канал и т.д.)
    """
    params = {"url": video_url}
    try:
        return await _get_json(session, f"{SC_BASE}/video", params)
    except Exception as e:
        logger.warning("⚠️ Fetch video details failed after retries: %s", e)
        return None


def _iso_from_text(date_text: str) -> tuple[int, int, datetime | None]:
    """
    Пробуем аккуратно преобразовать publishDateText из ScrapeCreators в UTC дату.
    Если не вышло — вернём (текущий год/неделю, None).
    """
    try:
        # Пример: "Oct 21, 2025"
        dt = datetime.strptime(date_text, "%b %d, %Y")
    except Exception:
        try:
            # Иногда приходит “2025-10-21” или иные форматы
            dt = datetime.fromisoformat(date_text.replace("Z","").replace("T"," "))
        except Exception:
            dt = datetime.utcnow()
            return dt.isocalendar().year, dt.isocalendar().week, None
    iso = dt.isocalendar()
    return iso.year, iso.week, dt


# ---------------------------------------
# Высокоуровневый обработчик одного канала
# ---------------------------------------
async def process_youtube_channel(
    session: aiohttp.ClientSession,
    conn,                       # asyncpg connection
    PG_SCHEMA: str,
    channel_id: str,
    amount: int,
    tags: list[dict],
    *,
    log_prefix: str = "YT",
    sleep_between: float = 0.2,
) -> dict:
    """
    Полный цикл для одного канала:
      1) получаем список shorts (id + url);
      2) по каждому — детальные данные;
      3) парсим заголовок/описание на теги;
      4) upsert в таблицу video_stats;
      5) возвращаем статистику по каналу.

    Возвращает dict:
      {
        'channel_id': ...,
        'total_shorts': N,
        'details_ok': K,
        'inserted': a,
        'updated': b,
        'skipped': c,
        'failed': f
      }
    """
    stats = {
        "channel_id": channel_id,
        "total_shorts": 0,
        "details_ok": 0,
        "inserted": 0,
        "updated": 0,
        "skipped": 0,
        "failed": 0,
    }

    logger.info("🎯 [%s] Start channel %s | amount=%s", log_prefix, channel_id, amount)
    shorts = await fetch_shorts_simple(session, channel_id, amount)
    stats["total_shorts"] = len(shorts)
    logger.info("🎬 [%s] %s: fetched %d shorts", log_prefix, channel_id, stats["total_shorts"])

    if not shorts:
        logger.warning("🚫 [%s] %s: no shorts, skip channel", log_prefix, channel_id)
        return stats

    async with conn.transaction():
        for idx, s in enumerate(shorts, start=1):
            v_url = s.get("url")
            v_id = s.get("id")
            if not v_url:
                stats["skipped"] += 1
                logger.debug("↩️ [%s] %s: #%d skipped (no url)", log_prefix, channel_id, idx)
                continue

            try:
                details = await fetch_video_details(session, v_url)
                if not details:
                    stats["failed"] += 1
                    logger.warning("❌ [%s] %s: #%d details None (%s)", log_prefix, channel_id, idx, v_url)
                    await asyncio.sleep(sleep_between)
                    continue

                stats["details_ok"] += 1

                title = details.get("title") or ""
                descr = details.get("description") or ""
                text = f"{title}\n{descr}".strip()

                client_tag, company, product, matched_list = match_tags(text, tags)

                publish_text = details.get("publishDateText") or ""
                iso_year, week, dt = _iso_from_text(publish_text)
                publish_date = dt.date() if dt else datetime.utcnow().date()

                channel = details.get("channel") or {}
                account = channel.get("handle") or channel.get("title") or channel.get("id") or channel_id

                views = int(details.get("viewCountInt") or 0)
                likes = int(details.get("likeCountInt") or 0)
                comments = int(details.get("commentCountInt") or 0)

                # upsert
                res = await conn.execute(
                    f"""
                    INSERT INTO {PG_SCHEMA}.video_stats
                        (platform, account, video_id, video_url, publish_date,
                         iso_year, week, likes, views, comments, caption,
                         client_tag, company, product, created_at, updated_at)
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,NOW(),NOW())
                    ON CONFLICT (video_url) DO UPDATE SET
                        account = EXCLUDED.account,
                        likes = EXCLUDED.likes,
                        views = EXCLUDED.views,
                        comments = EXCLUDED.comments,
                        caption = EXCLUDED.caption,
                        client_tag = EXCLUDED.client_tag,
                        company = EXCLUDED.company,
                        product = EXCLUDED.product,
                        updated_at = NOW();
                    """,
                    "youtube",
                    account,
                    v_id or details.get("id") or "",
                    details.get("url") or v_url,
                    publish_date,
                    iso_year,
                    week,
                    likes,
                    views,
                    comments,
                    text,
                    client_tag,
                    company,
                    product,
                )

                # asyncpg возвращает статус типа "INSERT 0 1" или "UPDATE 1"
                if res.startswith("INSERT"):
                    stats["inserted"] += 1
                    action = "INS"
                elif res.startswith("UPDATE"):
                    stats["updated"] += 1
                    action = "UPD"
                else:
                    stats["skipped"] += 1
                    action = "SKIP"

                logger.debug(
                    "✅ [%s] %s: #%d %s id=%s views=%s likes=%s matched=%s",
                    log_prefix, channel_id, idx, action, (v_id or details.get("id")), views, likes, matched_list
                )

            except Exception as e:
                stats["failed"] += 1
                logger.exception("💥 [%s] %s: #%d fatal on %s | err=%s", log_prefix, channel_id, idx, v_url, e)

            await asyncio.sleep(sleep_between)

            # периодический прогресс
            if idx % 25 == 0 or idx == stats["total_shorts"]:
                logger.info(
                    "📊 [%s] %s: progress %d/%d | ins=%d upd=%d skip=%d fail=%d",
                    log_prefix, channel_id, idx, stats["total_shorts"],
                    stats["inserted"], stats["updated"], stats["skipped"], stats["failed"]
                )

    logger.info(
        "🏁 [%s] %s: done | total=%d details_ok=%d ins=%d upd=%d skip=%d fail=%d",
        log_prefix, channel_id, stats["total_shorts"], stats["details_ok"],
        stats["inserted"], stats["updated"], stats["skipped"], stats["failed"]
    )
    return stats
