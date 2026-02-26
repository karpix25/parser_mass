
# app/parser_tiktok.py
import os
import aiohttp
import asyncio
import logging
from datetime import datetime
from app.tagging import match_tags
from app.parser_youtube import _iso_from_text
from app.core.utils import api_retry

logger = logging.getLogger(__name__)

SC_API_KEY = os.getenv("SCRAPECREATORS_KEY", "")
SC_BASE = "https://api.scrapecreators.com/v3/tiktok"

MAX_RPM = int(os.getenv("SC_MAX_REQ_PER_MIN", "120"))
_MIN_INTERVAL = max(0.01, 60.0 / max(1, MAX_RPM))

@api_retry
async def _fetch_json(session: aiohttp.ClientSession, url: str, params: dict) -> dict:
    headers = {"x-api-key": SC_API_KEY, "accept": "application/json"}
    async with session.get(url, headers=headers, params=params, timeout=60) as resp:
        if resp.status != 200:
            txt = await resp.text()
            raise aiohttp.ClientResponseError(
                resp.request_info,
                resp.history,
                status=resp.status,
                message=f"TikTok API {resp.status}: {txt[:200]}",
            )
        return await resp.json()

async def fetch_tiktok_videos(session: aiohttp.ClientSession, user_id: str | None = None, amount: int = 20, handle: str | None = None):
    """
    Получает список видео TikTok по user_id или handle с поддержкой пагинации (API v3).
    Возвращает (all_videos, is_not_found)
    """
    all_videos = []
    cursor = "0"
    is_not_found = False
    
    # URL из скриншота: https://api.scrapecreators.com/v3/tiktok/profile/videos
    # ВАЖНО: именно через слэш '/', а не через тире '-'
    url = "https://api.scrapecreators.com/v3/tiktok/profile/videos"
    
    while len(all_videos) < amount:
        params = {
            "max_cursor": cursor,
            "sort_by": "latest", # v3 expects 'latest' or 'popular'
        }
        if handle:
            params["handle"] = handle
        else:
            params["user_id"] = user_id
        
        try:
            data = await _fetch_json(session, url, params)
            
            # В v3 ответ содержит объект с видео (aweme_list) и курсором
            page_videos = data.get("aweme_list") or data.get("videos") or []
            if not page_videos and isinstance(data, list):
                page_videos = data
            
            if not page_videos:
                logger.debug("🎞️ TikTok %s: страница пуста (или конец списка)", (handle or user_id))
                break
                
            all_videos.extend(page_videos)
            
            # Проверяем, есть ли еще видео (has_more) и получаем следующий курсор
            has_more = data.get("has_more", False)
            cursor = data.get("max_cursor")
            
            logger.debug("🎞️ TikTok %s: получена страница (%d видео, след. курсор: %s, has_more: %s)", 
                         (handle or user_id), len(page_videos), cursor, has_more)

            if not has_more or not cursor or cursor == "0" or cursor == 0:
                break
            
            # Небольшая задержка между страницами
            await asyncio.sleep(0.5)
            
        except aiohttp.ClientResponseError as e:
            if e.status == 404:
                logger.warning("🚫 TikTok %s: профиль не найден (404) по ссылке %s", (handle or user_id), url)
                is_not_found = True
                break
            logger.warning("⚠️ TikTok %s: ошибка API (%s): %s", (handle or user_id), e.status, e)
            break
        except Exception as e:
            logger.warning("⚠️ TikTok %s: ошибка при пагинации: %s", (handle or user_id), e)
            break
            
    return all_videos[:amount], is_not_found


async def process_tiktok_profile(
    session,
    conn,
    PG_SCHEMA: str,
    user_id: str,
    amount: int,
    tags: list,
    sheet_username: str | None = None,
):
    """
    Основная обработка TikTok профиля
    """
    results = {
        "total_videos": 0,
        "inserted": 0,
        "updated": 0,
        "failed": 0,
        "account_name": user_id,
        "sheet_username": sheet_username,
    }
    label_name = sheet_username or user_id
    logger.info("🎯 TikTok %s: старт обработки | лимит=%s", label_name, amount)
    videos, is_not_found = await fetch_tiktok_videos(session, user_id, amount, handle=sheet_username)
    results["total_videos"] = len(videos)
    results["is_not_found"] = is_not_found
    if is_not_found:
        return results
    if not videos:
        logger.warning("⚠️ TikTok %s: пустой ответ или превышен лимит после повторов", user_id)
        return results

    author = (videos[0] or {}).get("author") if isinstance(videos[0], dict) else {}
    account_name = None
    if isinstance(author, dict):
        for key in ("unique_id", "uniqueId", "handle", "username", "author", "name", "nickname"):
            value = author.get(key)
            if value:
                account_name = str(value).strip()
                break
    if not account_name:
        alt = videos[0].get("author_name") if isinstance(videos[0], dict) else None
        if alt:
            account_name = str(alt).strip()
    if not account_name:
        logger.warning("⚠️ TikTok %s: не удалось получить логин из ответа, используем user_id", user_id)
        account_name = user_id
    results["account_name"] = account_name
    stored_account = sheet_username or account_name or user_id
    results["stored_account"] = stored_account
    if sheet_username and sheet_username != account_name:
        account_label = f"{sheet_username} ↔ {account_name}"
    elif account_name != user_id:
        account_label = f"{account_name} ({user_id})"
    else:
        account_label = user_id

    for idx, v in enumerate(videos, start=1):
        try:
            video_id = v.get("id") or v.get("aweme_id")
            url = v.get("url") or f"https://www.tiktok.com/@/video/{video_id}"
            title = v.get("title") or v.get("desc") or ""
            description = v.get("desc") or v.get("description") or ""
            stats = v.get("stats") or v.get("statistics") or {}

            # --- анализ текста
            text_for_tags = f"{title}\n{description}".strip()
            client_tag, company, product, _matched = match_tags(text_for_tags, tags)

            publish_time = v.get("create_time") or v.get("published_at")
            if publish_time:
                publish_date = datetime.utcfromtimestamp(int(publish_time)).date()
                iso = datetime.utcfromtimestamp(int(publish_time)).isocalendar()
                iso_year, week = iso.year, iso.week
            else:
                now = datetime.utcnow()
                iso_year, week, publish_date = now.year, now.isocalendar().week, now.date()

            views = int(stats.get("play_count") or 0)
            likes = int(stats.get("digg_count") or 0)
            comments = int(stats.get("comment_count") or 0)

            async with conn.transaction():
                await conn.execute(f"""
                INSERT INTO {PG_SCHEMA}.video_stats
                    (platform, account, video_id, video_url, publish_date,
                     iso_year, week, likes, views, comments, caption,
                     client_tag, company, product, created_at, updated_at)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,NOW(),NOW())
                ON CONFLICT (video_id) DO UPDATE SET
                    account = EXCLUDED.account,
                    video_url = EXCLUDED.video_url,
                    likes = EXCLUDED.likes,
                    views = EXCLUDED.views,
                    comments = EXCLUDED.comments,
                    caption = EXCLUDED.caption,
                    client_tag = EXCLUDED.client_tag,
                    company = EXCLUDED.company,
                    product = EXCLUDED.product,
                    updated_at = NOW();
                """,
                    "tiktok", stored_account, video_id, url,
                    publish_date, iso_year, week,
                    likes, views, comments, title,
                    client_tag, company, product
                )
            results["inserted"] += 1
            if idx % 20 == 0 or idx == len(videos):
                logger.info(
                    "📈 TikTok %s: прогресс %s/%s | ins=%s fail=%s",
                    account_label, idx, len(videos), results["inserted"], results["failed"]
                )
        except Exception as e:
            logger.warning("⚠️ TikTok %s: видео %s не обработано (%s)", account_label, video_id, e)
            results["failed"] += 1

        await asyncio.sleep(_MIN_INTERVAL)

    logger.info(
        "🏁 TikTok %s: готово | всего=%s ins=%s fail=%s",
        account_label, results["total_videos"], results["inserted"], results["failed"]
    )
    return results
