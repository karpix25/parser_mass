import asyncio
import logging
import os
import json
import base64
import gspread
import aiohttp
from datetime import datetime
from tenacity import retry, stop_after_attempt, wait_fixed

from app.core.config import settings

logger = logging.getLogger(__name__)

SC_API_KEY = os.getenv("SCRAPECREATORS_KEY", "")
# Используем v1 как на скриншотах пользователя, хотя в проекте местами v3.
# Для enrichment задачи следуем указаниям пользователя.
SC_YT_BASE = "https://api.scrapecreators.com/v1/youtube/channel"
SC_TT_BASE = "https://api.scrapecreators.com/v1/tiktok/profile"

YOUTUBE_PROFILE_HEADERS = ["профиль", "profile", "handle", "username", "канал", "channel"]
YOUTUBE_ID_HEADERS = ["id профиля", "id_профиля", "idпрофиля", "channel_id", "channel id"]
VIDEO_COUNT_HEADERS = ["видео", "video", "amount", "количество_видео", "количество видео"]
SUBSCRIBERS_HEADERS = ["подписки", "subscribers", "followers", "подписчики"]
UPDATED_AT_HEADERS = ["дата обновления", "дата_обновления", "date", "updated_at"]
TIKTOK_HANDLE_HEADERS = ["handle", "логин", "login", "username", "usernames", "профиль", "profile"]
TIKTOK_ID_HEADERS = ["id_профиля", "idпрофиля", "id_profile", "user_id", "user id", "id профиля"]

def _get_gclient():
    from app.services.gsheets import get_gspread_client
    return get_gspread_client()

async def _fetch_sc_data(session, url, params):
    headers = {"x-api-key": SC_API_KEY}
    try:
        async with session.get(url, headers=headers, params=params, timeout=30) as resp:
            if resp.status == 200:
                data = await resp.json()
                # АПИ иногда возвращает 200, но внутри пишет accountDoesNotExist: true
                if data.get("accountDoesNotExist"):
                    return {"error": "not_found"}
                return data
            elif resp.status == 404:
                # Специальный код для "удален / не найден"
                return {"error": "not_found"}
            else:
                text = await resp.text()
                logger.debug(f"SC API Error {resp.status} for {params}: {text[:200]}")
                return None
    except Exception as e:
        logger.error(f"SC Request failed: {e}")
        return None

def _find_col_idx(headers: list[str], possible_names: list[str]) -> int:
    """Returns 1-based index of the column, or -1 if not found."""
    headers_norm = [h.strip().lower() for h in headers]
    for name in possible_names:
        if name in headers_norm:
            return headers_norm.index(name) + 1
    return -1


def _normalize_profile_value(value: str | None) -> str:
    if not value:
        return ""
    cleaned = str(value).strip()
    for prefix in (
        "https://www.youtube.com/@",
        "https://youtube.com/@",
        "http://www.youtube.com/@",
        "http://youtube.com/@",
        "https://www.youtube.com/",
        "https://youtube.com/",
        "http://www.youtube.com/",
        "http://youtube.com/",
    ):
        if cleaned.startswith(prefix):
            cleaned = cleaned[len(prefix):]
            break
    cleaned = cleaned.strip().strip("/")
    if cleaned.startswith("@"):
        cleaned = cleaned[1:]
    return cleaned.casefold()


def _extract_numeric_count(raw_value) -> int | None:
    if raw_value in (None, ""):
        return None
    if isinstance(raw_value, (int, float)):
        return int(raw_value)

    import re

    match = re.search(r"([\d,]+)", str(raw_value))
    if not match:
        return None
    try:
        return int(match.group(1).replace(",", ""))
    except ValueError:
        return None


def _normalize_tiktok_handle(value: str | None) -> str:
    if not value:
        return ""
    cleaned = str(value).strip()
    for prefix in (
        "https://www.tiktok.com/@",
        "https://tiktok.com/@",
        "http://www.tiktok.com/@",
        "http://tiktok.com/@",
    ):
        if cleaned.startswith(prefix):
            cleaned = cleaned[len(prefix):]
            break
    cleaned = cleaned.strip().strip("/")
    if "?" in cleaned:
        cleaned = cleaned.split("?", 1)[0]
    if cleaned.startswith("@"):
        cleaned = cleaned[1:]
    return cleaned.casefold()


async def fetch_youtube_profile_metadata(
    session: aiohttp.ClientSession,
    *,
    profile: str | None = None,
    channel_id: str | None = None,
) -> dict | None:
    params = {}
    if channel_id:
        params["channelId"] = channel_id
    elif profile:
        clean_profile = str(profile).strip()
        if "youtube.com" in clean_profile:
            params["url"] = clean_profile
        else:
            params["handle"] = clean_profile.lstrip("@")
    else:
        return None

    data = await _fetch_sc_data(session, SC_YT_BASE, params)
    if not data:
        return None
    if data.get("error") == "not_found":
        return {"is_not_found": True}

    channel_data = data.get("channel") if isinstance(data.get("channel"), dict) else {}
    resolved_channel_id = (
        data.get("channelId")
        or data.get("id")
        or channel_data.get("id")
    )
    subscriber_count = data.get("subscriberCount") or data.get("subscriberCountInt")
    video_count = (
        data.get("videoCount")
        or data.get("videoCountInt")
        or _extract_numeric_count(data.get("videoCountText"))
    )

    return {
        "is_not_found": False,
        "channel_id": str(resolved_channel_id).strip() if resolved_channel_id else None,
        "subscriber_count": _extract_numeric_count(subscriber_count),
        "video_count": _extract_numeric_count(video_count),
    }


async def fetch_tiktok_profile_metadata(
    session: aiohttp.ClientSession,
    *,
    handle: str | None = None,
) -> dict | None:
    clean_handle = _normalize_tiktok_handle(handle)
    if not clean_handle:
        return None

    data = await _fetch_sc_data(session, SC_TT_BASE, {"handle": clean_handle})
    if not data:
        return None
    if data.get("error") == "not_found":
        return {"is_not_found": True}

    user_data = data.get("user") or {}
    stats = data.get("stats") or {}
    resolved_user_id = user_data.get("id") or user_data.get("uid")
    resolved_handle = user_data.get("uniqueId") or user_data.get("unique_id") or clean_handle

    return {
        "is_not_found": False,
        "user_id": str(resolved_user_id).strip() if resolved_user_id else None,
        "handle": str(resolved_handle).strip() if resolved_handle else clean_handle,
        "subscriber_count": _extract_numeric_count(stats.get("followerCount")),
        "video_count": _extract_numeric_count(stats.get("videoCount")),
    }


def _open_sheet_by_url(gc: gspread.Client, target_url: str):
    import re
    from gspread.utils import extract_id_from_url

    sh = gc.open_by_key(extract_id_from_url(target_url))
    match = re.search(r'(?:gid=)(\d+)', target_url)
    if match:
        return sh.get_worksheet_by_id(int(match.group(1)))
    return sh.sheet1


async def update_youtube_profile_row(
    profile: str,
    *,
    channel_id: str | None = None,
    video_count: int | None = None,
    subscriber_count: int | None = None,
    updated_at: str | None = None,
):
    target_url = settings.YOUTUBE_OUTPUT_SHEET_URL or settings.YOUTUBE_SHEET_URL
    if not target_url or not profile:
        return

    try:
        loop = asyncio.get_event_loop()
        gc = await loop.run_in_executor(None, _get_gclient)
        if not gc:
            return

        ws = _open_sheet_by_url(gc, target_url)
        all_values = ws.get_all_values()
        if not all_values:
            return

        headers = all_values[0]
        col_profile_idx = _find_col_idx(headers, YOUTUBE_PROFILE_HEADERS)
        col_id_idx = _find_col_idx(headers, YOUTUBE_ID_HEADERS)
        col_video_idx = _find_col_idx(headers, VIDEO_COUNT_HEADERS)
        col_subs_idx = _find_col_idx(headers, SUBSCRIBERS_HEADERS)
        col_date_idx = _find_col_idx(headers, UPDATED_AT_HEADERS)

        if col_profile_idx == -1:
            logger.warning("⚠️ Could not find YouTube profile column in sheet headers: %s", headers)
            return

        profile_key = _normalize_profile_value(profile)
        target_row_idx = -1
        for i, row in enumerate(all_values[1:], start=2):
            if len(row) < col_profile_idx:
                continue
            if _normalize_profile_value(row[col_profile_idx - 1]) == profile_key:
                target_row_idx = i
                break

        if target_row_idx == -1:
            logger.warning("⚠️ Could not find YouTube profile %s in sheet to update.", profile)
            return

        cells_to_update = []
        if channel_id and col_id_idx != -1:
            cells_to_update.append(gspread.Cell(target_row_idx, col_id_idx, str(channel_id)))
        if video_count is not None and col_video_idx != -1:
            cells_to_update.append(gspread.Cell(target_row_idx, col_video_idx, str(video_count)))
        if subscriber_count is not None and col_subs_idx != -1:
            cells_to_update.append(gspread.Cell(target_row_idx, col_subs_idx, str(subscriber_count)))
        if col_date_idx != -1:
            cells_to_update.append(
                gspread.Cell(
                    target_row_idx,
                    col_date_idx,
                    updated_at or datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                )
            )

        if cells_to_update:
            ws.update_cells(cells_to_update)
            logger.info("✅ Updated YouTube row for profile %s (row %s)", profile, target_row_idx)

    except Exception as e:
        logger.error(f"Error updating YouTube row for {profile}: {e}")


async def update_tiktok_profile_row(
    handle: str,
    *,
    user_id: str | None = None,
    video_count: int | None = None,
    subscriber_count: int | None = None,
    updated_at: str | None = None,
):
    target_url = settings.TIKTOK_OUTPUT_SHEET_URL or settings.TIKTOK_SHEET_URL
    if not target_url or not handle:
        return

    try:
        loop = asyncio.get_event_loop()
        gc = await loop.run_in_executor(None, _get_gclient)
        if not gc:
            return

        ws = _open_sheet_by_url(gc, target_url)
        all_values = ws.get_all_values()
        if not all_values:
            return

        headers = all_values[0]
        col_handle_idx = _find_col_idx(headers, TIKTOK_HANDLE_HEADERS)
        col_id_idx = _find_col_idx(headers, TIKTOK_ID_HEADERS)
        col_video_idx = _find_col_idx(headers, VIDEO_COUNT_HEADERS)
        col_subs_idx = _find_col_idx(headers, SUBSCRIBERS_HEADERS)
        col_date_idx = _find_col_idx(headers, UPDATED_AT_HEADERS)

        if col_handle_idx == -1:
            logger.warning("⚠️ Could not find TikTok handle column in sheet headers: %s", headers)
            return

        handle_key = _normalize_tiktok_handle(handle)
        target_row_idx = -1
        for i, row in enumerate(all_values[1:], start=2):
            if len(row) < col_handle_idx:
                continue
            if _normalize_tiktok_handle(row[col_handle_idx - 1]) == handle_key:
                target_row_idx = i
                break

        if target_row_idx == -1:
            logger.warning("⚠️ Could not find TikTok handle %s in sheet to update.", handle)
            return

        cells_to_update = []
        if user_id and col_id_idx != -1:
            cells_to_update.append(gspread.Cell(target_row_idx, col_id_idx, str(user_id)))
        if video_count is not None and col_video_idx != -1:
            cells_to_update.append(gspread.Cell(target_row_idx, col_video_idx, str(video_count)))
        if subscriber_count is not None and col_subs_idx != -1:
            cells_to_update.append(gspread.Cell(target_row_idx, col_subs_idx, str(subscriber_count)))
        if col_date_idx != -1:
            cells_to_update.append(
                gspread.Cell(
                    target_row_idx,
                    col_date_idx,
                    updated_at or datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                )
            )

        if cells_to_update:
            ws.update_cells(cells_to_update)
            logger.info("✅ Updated TikTok row for handle %s (row %s)", handle, target_row_idx)

    except Exception as e:
        logger.error(f"Error updating TikTok row for {handle}: {e}")

async def enrich_youtube_sheet(gc: gspread.Client, session: aiohttp.ClientSession):
    target_url = settings.YOUTUBE_OUTPUT_SHEET_URL or settings.YOUTUBE_SHEET_URL
    if not target_url:
        return
    
    try:
        ws = _open_sheet_by_url(gc, target_url)
        
        headers = ws.row_values(1)
        
        # Индексы колонок (1-based)
        # Для YouTube профиль всегда берется из колонки A (индекс 1)
        col_profile_idx = _find_col_idx(headers, YOUTUBE_PROFILE_HEADERS)
        if col_profile_idx == -1:
            col_profile_idx = 1
        
        # Индексы для записи (ищем по заголовкам)
        col_id_write_idx = _find_col_idx(headers, YOUTUBE_ID_HEADERS)
        col_video_idx = _find_col_idx(headers, VIDEO_COUNT_HEADERS)
        col_subs_idx = _find_col_idx(headers, SUBSCRIBERS_HEADERS)
        col_date_idx = _find_col_idx(headers, UPDATED_AT_HEADERS)
        
        # Проверяем, нашли ли куда писать обновленные данные
        if col_id_write_idx == -1:
            logger.warning(f"⚠️ YouTube Sheet: Could not find ID column for writing. Headers: {headers}")
            return

        # Читаем все данные
        all_values = ws.get_all_values()
        # Пропускаем заголовок
        rows = all_values[1:] 
        
        updates = [] # List of BatchUpdate operations or just cell updates?
        # gspread batch_update is efficient. We can construct a list of Cell objects.
        
        cells_to_update = []
        
        logger.info(f"🔄 Enriching YouTube Sheet ({len(rows)} rows)...")
        
        for i, row in enumerate(rows):
            real_row_idx = i + 2 # 1-based, +1 header
            
            # Безопасное получение значения из колонки А (индекс 0 в массиве)
            profile_val = row[col_profile_idx - 1] if len(row) >= col_profile_idx else ""

            if not profile_val:
                continue
                
            current_channel_id = row[col_id_write_idx - 1] if col_id_write_idx != -1 and len(row) >= col_id_write_idx else ""
            data = await fetch_youtube_profile_metadata(
                session,
                profile=profile_val,
                channel_id=current_channel_id or None,
            )
            
            row_cells = []
            col_status_idx = 7 # Column G

            if data:
                if data.get("is_not_found"):
                    # Записываем причину в колонку G
                    row_cells.append(gspread.Cell(real_row_idx, col_status_idx, "удален"))
                    
                    # Обновляем дату
                    if col_date_idx != -1:
                        today_str = datetime.utcnow().strftime("%Y-%m-%d")
                        row_cells.append(gspread.Cell(real_row_idx, col_date_idx, today_str))
                else:
                    # Успех
                    new_id = data.get("channel_id")
                    subs_count = data.get("subscriber_count")
                    video_count = data.get("video_count")

                    # Очищаем колонку G (ошибку) при успехе
                    row_cells.append(gspread.Cell(real_row_idx, col_status_idx, ""))

                    # Обновляем ID
                    if new_id and col_id_write_idx != -1:
                        row_cells.append(gspread.Cell(real_row_idx, col_id_write_idx, new_id))
                    
                    # Обновляем количество видео
                    if video_count is not None and col_video_idx != -1:
                         row_cells.append(gspread.Cell(real_row_idx, col_video_idx, str(video_count)))
                    
                    # Обновляем подписки
                    if subs_count is not None and col_subs_idx != -1:
                         row_cells.append(gspread.Cell(real_row_idx, col_subs_idx, str(subs_count)))
                    
                    # Обновляем дату
                    if col_date_idx != -1:
                         today_str = datetime.utcnow().strftime("%Y-%m-%d")
                         row_cells.append(gspread.Cell(real_row_idx, col_date_idx, today_str))
            else:
                row_cells.append(gspread.Cell(real_row_idx, col_status_idx, "ошибка API"))

            if row_cells:
                cells_to_update.extend(row_cells)
            
            await asyncio.sleep(0.1) # Throttle slightly

        if cells_to_update:
            ws.update_cells(cells_to_update)
            logger.info(f"✅ YouTube Sheet updated: {len(cells_to_update)} cells changed.")
        else:
            logger.info("YouTube Sheet: No updates needed.")

    except Exception as e:
        logger.error(f"Error enriching YouTube sheet: {e}")


async def enrich_tiktok_sheet(gc: gspread.Client, session: aiohttp.ClientSession):
    target_url = settings.TIKTOK_OUTPUT_SHEET_URL or settings.TIKTOK_SHEET_URL
    if not target_url:
        return

    try:
        ws = _open_sheet_by_url(gc, target_url)
        
        headers = ws.row_values(1)
        
        # Ищем колонки для записи
        # Для TikTok handle всегда берется из колонки A
        col_user_source_idx = _find_col_idx(headers, TIKTOK_HANDLE_HEADERS)
        if col_user_source_idx == -1:
            col_user_source_idx = 1
        
        col_id_write_idx = _find_col_idx(headers, TIKTOK_ID_HEADERS)
        col_video_idx = _find_col_idx(headers, VIDEO_COUNT_HEADERS)
        col_subs_idx = _find_col_idx(headers, SUBSCRIBERS_HEADERS)
        col_date_idx = _find_col_idx(headers, UPDATED_AT_HEADERS)

        all_values = ws.get_all_values()
        rows = all_values[1:]
        
        cells_to_update = []
        logger.info(f"🔄 Enriching TikTok Sheet ({len(rows)} rows)...")

        for i, row in enumerate(rows):
            real_row_idx = i + 2
            
            username = row[col_user_source_idx - 1] if len(row) >= col_user_source_idx else ""
            if not username:
                continue

            data = await fetch_tiktok_profile_metadata(session, handle=username)
            
            row_cells = []
            col_status_idx = 7 # Column G

            if data:
                if data.get("is_not_found"):
                    # Записываем причину в колонку G
                    row_cells.append(gspread.Cell(real_row_idx, col_status_idx, "удален"))
                    
                    # Обновляем Дату
                    if col_date_idx != -1:
                        today_str = datetime.utcnow().strftime("%Y-%m-%d")
                        row_cells.append(gspread.Cell(real_row_idx, col_date_idx, today_str))
                else:
                    # Успех
                    uid = data.get("user_id")
                    v_count = data.get("video_count")
                    s_count = data.get("subscriber_count")

                    # Очищаем колонку G
                    row_cells.append(gspread.Cell(real_row_idx, col_status_idx, ""))
                    
                    # Обновляем ID
                    if uid and col_id_write_idx != -1:
                        row_cells.append(gspread.Cell(real_row_idx, col_id_write_idx, str(uid)))
                    
                    # Обновляем Video Count
                    if v_count is not None and col_video_idx != -1:
                        row_cells.append(gspread.Cell(real_row_idx, col_video_idx, str(v_count)))
                    
                    # Обновляем Подписки
                    if s_count is not None and col_subs_idx != -1:
                        row_cells.append(gspread.Cell(real_row_idx, col_subs_idx, str(s_count)))
                        
                    # Обновляем Дату
                    if col_date_idx != -1:
                        today_str = datetime.utcnow().strftime("%Y-%m-%d")
                        row_cells.append(gspread.Cell(real_row_idx, col_date_idx, today_str))
            else:
                # Ошибка API
                row_cells.append(gspread.Cell(real_row_idx, col_status_idx, "ошибка API"))

            if row_cells:
                cells_to_update.extend(row_cells)
            
            await asyncio.sleep(0.1)

        if cells_to_update:
            ws.update_cells(cells_to_update)
            logger.info(f"✅ TikTok Sheet updated: {len(cells_to_update)} cells changed.")
        else:
            logger.info("TikTok Sheet: No updates needed.")
            
    except Exception as e:
        logger.error(f"Error enriching TikTok sheet: {e}")


async def mark_tiktok_profile_deleted(handle: str):
    """
    Finds the row for handle in the TikTok sheet and marks column G as 'удален'.
    """
    await _mark_profile_deleted_generic(handle, platform="tiktok")


async def mark_youtube_profile_deleted(channel_id: str):
    """
    Finds the row for profile in the YouTube sheet and marks column G as 'удален'.
    """
    await _mark_profile_deleted_generic(channel_id, platform="youtube")


async def _mark_profile_deleted_generic(profile_id: str, platform: str):
    if platform == "tiktok":
        target_url = settings.TIKTOK_OUTPUT_SHEET_URL or settings.TIKTOK_SHEET_URL
        possible_id_headers = TIKTOK_HANDLE_HEADERS
    else:
        target_url = settings.YOUTUBE_OUTPUT_SHEET_URL or settings.YOUTUBE_SHEET_URL
        possible_id_headers = YOUTUBE_PROFILE_HEADERS

    if not target_url:
        return

    try:
        from gspread.utils import extract_id_from_url
        loop = asyncio.get_event_loop()
        gc = await loop.run_in_executor(None, _get_gclient)
        if not gc:
            return
            
        sh = gc.open_by_key(extract_id_from_url(target_url))
        import re
        match = re.search(r'(?:gid=)(\d+)', target_url)
        if match:
            ws = sh.get_worksheet_by_id(int(match.group(1)))
        else:
            ws = sh.sheet1
            
        all_values = ws.get_all_values()
        if not all_values:
            return
            
        headers = all_values[0]
        col_id_idx = _find_col_idx(headers, possible_id_headers)
        if col_id_idx == -1:
            col_id_idx = 1
            
        col_status_idx = 7 # Column G
        
        target_row_idx = -1
        for i, row in enumerate(all_values[1:], start=2):
            if len(row) >= col_id_idx:
                val = str(row[col_id_idx - 1]).strip()
                if platform == "youtube":
                    if _normalize_profile_value(val) == _normalize_profile_value(profile_id):
                        target_row_idx = i
                        break
                elif platform == "tiktok":
                    if _normalize_tiktok_handle(val) == _normalize_tiktok_handle(profile_id):
                        target_row_idx = i
                        break
                elif val == str(profile_id).strip():
                    target_row_idx = i
                    break
        
        if target_row_idx != -1:
            ws.update_cell(target_row_idx, col_status_idx, "удален")
            logger.info(f"✅ Marked {platform} {profile_id} as 'удален' in Google Sheet (row {target_row_idx})")
        else:
            logger.warning(f"⚠️ Could not find {platform} {profile_id} in sheet to mark as deleted.")

    except Exception as e:
        logger.error(f"Error marking {platform} {profile_id} as deleted: {e}")


async def update_video_stats_in_sheet(platform: str, profile_id: str, video_count: int):
    """
    Finds the row for profile_id and updates Video count and Update Date.
    """
    if platform == "tiktok":
        target_url = settings.TIKTOK_OUTPUT_SHEET_URL or settings.TIKTOK_SHEET_URL
        possible_id_headers = ["id_профиля", "idпрофиля", "id_profile", "user_id", "user id", "id профиля"]
    else:
        target_url = settings.YOUTUBE_OUTPUT_SHEET_URL or settings.YOUTUBE_SHEET_URL
        possible_id_headers = ["id профиля", "id_профиля", "idпрофиля", "channel_id", "channel id"]

    if not target_url:
        return

    try:
        from gspread.utils import extract_id_from_url
        loop = asyncio.get_event_loop()
        gc = await loop.run_in_executor(None, _get_gclient)
        if not gc:
            return
            
        sh = gc.open_by_key(extract_id_from_url(target_url))
        import re
        match = re.search(r'(?:gid=)(\d+)', target_url)
        if match:
            ws = sh.get_worksheet_by_id(int(match.group(1)))
        else:
            ws = sh.sheet1
            
        all_values = ws.get_all_values()
        if not all_values:
            return
            
        headers = all_values[0]
        col_id_idx = _find_col_idx(headers, possible_id_headers)
        if col_id_idx == -1:
            col_id_idx = 4 if platform == "youtube" else 2
            
        col_video_idx = _find_col_idx(headers, ["видео", "video", "amount", "количество_видео", "количество видео"])
        col_date_idx = _find_col_idx(headers, ["дата обновления", "дата_обновления", "date", "updated_at"])
        
        target_row_idx = -1
        for i, row in enumerate(all_values[1:], start=2):
            if len(row) >= col_id_idx:
                val = str(row[col_id_idx - 1]).strip()
                if val == str(profile_id).strip():
                    target_row_idx = i
                    break
        
        if target_row_idx != -1:
            cells_to_update = []
            if col_video_idx != -1:
                cells_to_update.append(gspread.Cell(target_row_idx, col_video_idx, str(video_count)))
            if col_date_idx != -1:
                today_str = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
                cells_to_update.append(gspread.Cell(target_row_idx, col_date_idx, today_str))
            
            if cells_to_update:
                ws.update_cells(cells_to_update)
                logger.info(f"✅ Updated {platform} stats for {profile_id} in Google Sheet (row {target_row_idx})")
        else:
            logger.warning(f"⚠️ Could not find {platform} {profile_id} in sheet to update stats.")

    except Exception as e:
        logger.error(f"Error updating stats for {platform} {profile_id}: {e}")


async def enrich_all_sheets():
    """Main entry point called by scheduler/parser."""
    logger.info("🚀 Starting Sheet Enrichment (updating IDs & amounts)...")
    
    # Run gspread init in executor to not block? 
    # gspread auth might do network calls.
    loop = asyncio.get_event_loop()
    gc = await loop.run_in_executor(None, _get_gclient)
    
    if not gc:
        return

    async with aiohttp.ClientSession() as session:
        # Run enrichments
        # Note: gspread calls inside these functions are blocking (sync).
        # ideally we offload them to thread, but mixing async http with sync gspread is tricky in one func.
        # We will wrap the *whole* sync logic block in executor if we want full non-blocking,
        # but here we have mixed: async http calls for SC API, sync calls for Sheet update.
        # 
        # Strategy: Run the sync parts (open, read, update) in executor? Too granular.
        # Simple Strategy: Allow blocking for now since this is a background job runner.
        # OR: Run each sheet processing in a separate thread, but passed the async session? 
        # aiohttp session is not thread safe.
        #
        # Better: run sequentially. The blocking time of gspread is usually small (http requests to google).
        # We accept it blocks the loop briefly.
        
        await enrich_youtube_sheet(gc, session)
        await enrich_tiktok_sheet(gc, session)
    
    logger.info("🏁 Sheet Enrichment complete.")
