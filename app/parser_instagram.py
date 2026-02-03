import aiohttp
import asyncio
import logging
from datetime import datetime
from tenacity import RetryError

from app.core.config import settings
from app.core.utils import api_retry

logger = logging.getLogger(__name__)

# === ENV ===
RAPIDAPI_KEY  = settings.RAPIDAPI_KEY
RAPIDAPI_HOST = settings.RAPIDAPI_HOST
IG_API_BASE   = settings.IG_API_BASE
MAX_RPM       = settings.MAX_REQ_PER_MIN
_min_interval = max(0.01, 60.0 / max(1, MAX_RPM))

def _iso(dt: datetime):
    y, w, _ = dt.isocalendar()
    return y, w


def _format_error(err: Exception) -> str:
    """Return a readable error string, unwrapping RetryError if needed."""
    if isinstance(err, RetryError) and getattr(err, "last_attempt", None):
        inner = err.last_attempt.exception()
        if inner:
            return _format_error(inner)
    detail = str(err).strip()
    return f"{err.__class__.__name__}: {detail}" if detail else err.__class__.__name__

@api_retry
async def _fetch_page(session: aiohttp.ClientSession, params: dict, headers: dict) -> dict:
    async with session.get(IG_API_BASE, headers=headers, params=params, timeout=60) as resp:
        if resp.status != 200:
            text = await resp.text()
            # Raise exception to trigger retry
            raise aiohttp.ClientResponseError(
                resp.request_info,
                resp.history,
                status=resp.status,
                message=f"API error {resp.status}: {text[:200]}",
            )
        return await resp.json()

# === CORE PARSER ===
async def fetch_instagram_videos(username: str, session: aiohttp.ClientSession):
    """
    Парсер reels по username с устойчивыми retry (3x на страницу)
    и fallback на битые токены. Возвращает (videos, pagination_failed, error_reason)
    """
    headers = {
        "x-rapidapi-key": RAPIDAPI_KEY,
        "x-rapidapi-host": RAPIDAPI_HOST,
        "accept": "application/json",
    }
    params = {
        "username_or_id_or_url": username.strip(),
        "url_embed_safe": "false",
    }

    out = []
    token = None
    page = 0
    pagination_failed = False
    last_error: str | None = None

    logger.info("📸 Fetching videos for: %s", username)
    logger.debug("🧩 API BASE: %s", IG_API_BASE)

    while True:
        page += 1
        if token:
            params["pagination_token"] = token
        elif "pagination_token" in params:
            del params["pagination_token"]

        data = {}
        try:
            data = await _fetch_page(session, params, headers)
        except RetryError as err:
            last_error = _format_error(err)
            logger.error("🚫 %s: no data after retries (page %d): %s", username, page, last_error)
            pagination_failed = True
            break
        except Exception as e:
            last_error = _format_error(e)
            logger.error("🚫 %s: unexpected error (page %d): %s", username, page, last_error)
            pagination_failed = True
            break

        if not data:
             # Should be caught by RetryError usually if it was a network issue, 
             # but if it returns empty json with 200 OK?
            logger.warning("🚫 %s: empty data received (page %d)", username, page)
            break

        data_obj = data.get("data", {})
        reels = data_obj.get("items") or []
        token = data.get("pagination_token")

        # === fallback для битых токенов ===
        # Logic: if token exists but reels is empty, try again?
        # The original logic had a specific retry loop for this case.
        # Let's keep it simple for now: if we got 200 OK but no reels and have a token, 
        # it might be a glitch or end of stream.
        # Original logic: "fallback для битых токенов... while retry_token < 3..."
        # If we want to preserve this exact behavior, we can wrap it.
        # But maybe we can rely on the fact that if it's a transient issue, the next run will catch it?
        # Or we can just try one more time if it looks suspicious.
        
        if token and not reels:
             logger.warning("⚠️ %s: empty page %d with token — trying one more time", username, page)
             await asyncio.sleep(2)
             try:
                 data = await _fetch_page(session, params, headers)
                 reels = data.get("data", {}).get("items") or []
                 if reels:
                     logger.info("✅ %s: recovered after token retry", username)
             except Exception as retry_err:
                 last_error = _format_error(retry_err)
                 logger.warning("⚠️ %s: retry after empty page failed: %s", username, last_error)

        logger.info("📦 %s: %d videos fetched (page %d)", username, len(reels), page)
        logger.debug("🔁 has_token=%s, token=%s", bool(token), (token[:40] + '…') if token else None)

        if not reels:
            logger.warning("🚫 No videos found for %s (page %d)", username, page)
            break

        for m in reels:
            taken = (
                m.get("taken_at")
                or (m.get("caption") or {}).get("created_at_utc")
                or (m.get("caption") or {}).get("created_at")
            )
            dt = datetime.utcfromtimestamp(taken) if isinstance(taken, (int, float)) else datetime.utcnow()
            iso_year, week = _iso(dt)

            permalink = m.get("permalink")
            shortcode = m.get("code") or m.get("shortcode")
            video_url = permalink or (f"https://www.instagram.com/reel/{shortcode}/" if shortcode else None)

            out.append({
                "platform": "instagram",
                "account": username,
                "video_id": str(m.get("id") or m.get("pk") or ""),
                "video_url": video_url,
                "publish_date": dt.date(),
                "iso_year": iso_year,
                "week": week,
                "likes": int(m.get("like_count") or 0),
                "views": int(m.get("play_count") or m.get("view_count") or 0),
                "comments": int(m.get("comment_count") or 0),
                "caption": (
                    (m.get("caption") or {}).get("text")
                    if isinstance(m.get("caption"), dict)
                    else (m.get("caption_text") or m.get("caption") or "")
                ) or "",
            })

        logger.info("📈 %s: total so far %d videos", username, len(out))

        if not token:
            logger.info("✅ %s: pagination ended normally (no token)", username)
            break

        await asyncio.sleep(_min_interval)

    logger.info(
        "🏁 Done %s: total %d videos parsed (pagination_failed=%s)",
        username,
        len(out),
        pagination_failed,
    )
    return [v for v in out if v["video_url"]], pagination_failed, last_error
