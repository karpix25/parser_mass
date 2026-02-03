import os, json
from datetime import datetime, timezone, date
from typing import Any, Dict, List, Optional, Tuple
import aiohttp
from aiolimiter import AsyncLimiter
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from .utils import iso_year_week, extract_hashtags, find_keywords

IG_API_BASE = os.getenv("IG_API_BASE", "").rstrip("/")
RAPIDAPI_HOST = os.getenv("RAPIDAPI_HOST")
RAPIDAPI_KEY  = os.getenv("RAPIDAPI_KEY")

class TemporaryError(Exception): ...
class PermanentError(Exception): ...

def _parse_date(ts: Any) -> Optional[date]:
    if not ts: return None
    try:
        if isinstance(ts, (int, float)):
            return datetime.fromtimestamp(float(ts), tz=timezone.utc).date()
        if isinstance(ts, str):
            if ts.isdigit():
                return datetime.fromtimestamp(float(ts), tz=timezone.utc).date()
            return datetime.fromisoformat(ts.replace("Z","+00:00")).date()
    except Exception:
        return None
    return None

def normalize_item(raw: Dict[str, Any], account: str, keywords: list[str]) -> Dict[str, Any]:
    # Try several common fields from RapidAPI Instagram responses
    video_id   = raw.get("id") or raw.get("pk") or raw.get("video_id")
    video_url  = raw.get("permalink") or raw.get("url") or raw.get("video_url")
    caption    = (raw.get("caption") or raw.get("title") or raw.get("text") or "").strip()
    likes      = int(raw.get("like_count") or raw.get("likes") or 0)
    comments   = int(raw.get("comment_count") or raw.get("comments") or 0)
    views      = int(raw.get("play_count") or raw.get("view_count") or raw.get("views") or 0)
    published  = _parse_date(raw.get("timestamp") or raw.get("taken_at") or raw.get("publish_time"))
    iso_y, iso_w = (iso_year_week(published) if published else (None, None))
    hashtags = extract_hashtags(caption)
    kws = find_keywords(caption, keywords)
    return {
        "platform": "instagram",
        "account": account,
        "video_id": video_id,
        "video_url": video_url,
        "publish_date": published,
        "iso_year": iso_y,
        "iso_week": iso_w,
        "likes": likes,
        "comments": comments,
        "views": views,
        "caption": caption,
        "hashtags": hashtags,
        "keywords": kws,
        "extra_json": raw
    }

class InstaClient:
    def __init__(self, session: aiohttp.ClientSession, limiter: AsyncLimiter):
        self.sess = session
        self.limiter = limiter

    @retry(stop=stop_after_attempt(5),
           wait=wait_exponential(multiplier=2, min=2, max=30),
           retry=retry_if_exception_type(TemporaryError))
    async def list_reels_page(self, username: str, pagination_token: str | None) -> Dict[str, Any]:
        headers = {
            "x-rapidapi-host": RAPIDAPI_HOST,
            "x-rapidapi-key":  RAPIDAPI_KEY,
        }
        params = {"username_or_id_or_url": username}
        if pagination_token:
            params["pagination_token"] = pagination_token

        async with self.limiter:
            async with self.sess.get(IG_API_BASE, headers=headers, params=params, timeout=60) as r:
                txt = await r.text()
                if r.status in (403,404):
                    raise PermanentError(f"{r.status} for {username}: {txt}")
                if r.status in (429,500,502,503,504):
                    raise TemporaryError(f"{r.status} for {username}: {txt}")
                r.raise_for_status()
                return await r.json()

    async def iter_all(self, username: str, keywords: list[str]):
        token, total = None, 0
        while True:
            data = await self.list_reels_page(username, token)
            items = data.get("data") or data.get("items") or data.get("reels") or []
            token = data.get("pagination_token") or data.get("next") or data.get("next_cursor")
            if not items:
                break
            for it in items:
                yield normalize_item(it, username, keywords)
                total += 1
            if not token:
                break
