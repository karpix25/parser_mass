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

async def enrich_youtube_sheet(gc: gspread.Client, session: aiohttp.ClientSession):
    target_url = settings.YOUTUBE_OUTPUT_SHEET_URL or settings.YOUTUBE_SHEET_URL
    if not target_url:
        return
    
    try:
        import re
        from gspread.utils import extract_id_from_url
        sh = gc.open_by_key(extract_id_from_url(target_url))
        
        match = re.search(r'(?:gid=)(\d+)', target_url)
        if match:
            ws = sh.get_worksheet_by_id(int(match.group(1)))
        else:
            ws = sh.sheet1
        
        headers = ws.row_values(1)
        
        # Индексы колонок (1-based)
        # Для YouTube ID профиля всегда берется из колонки A (индекс 1)
        col_id_source_idx = 1
        
        # Индексы для записи (ищем по заголовкам)
        col_id_write_idx = _find_col_idx(headers, ["id профиля", "id_профиля", "idпрофиля", "channel_id", "channel id"])
        col_video_idx = _find_col_idx(headers, ["видео", "video", "amount", "количество_видео", "количество видео"])
        col_subs_idx = _find_col_idx(headers, ["подписки", "subscribers", "followers"])
        col_date_idx = _find_col_idx(headers, ["дата обновления", "дата_обновления", "date", "updated_at"])
        
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
            current_id_val = row[col_id_source_idx - 1] if len(row) >= col_id_source_idx else ""
            
            if not current_id_val:
                continue
                
            # Запрашиваем инфо
            # API поддерживает handle, id, url. Передаем как есть.
            params = {}
            if "youtube.com" in current_id_val:
                params["url"] = current_id_val
            elif current_id_val.startswith("UC"):
                params["channelId"] = current_id_val
            else:
                params["handle"] = current_id_val
            
            data = await _fetch_sc_data(session, SC_YT_BASE, params)
            
            if data:
                # Если вернулся 404 - аккаунт не найден (удален)
                if data.get("error") == "not_found":
                    if col_subs_idx != -1:
                        cells_to_update.append(gspread.Cell(real_row_idx, col_subs_idx, "УДАЛЕН"))
                    
                    # Обновляем дату (чтобы понимать когда проверили)
                    if col_date_idx != -1:
                         today_str = datetime.utcnow().strftime("%Y-%m-%d")
                         cells_to_update.append(gspread.Cell(real_row_idx, col_date_idx, today_str))
                    continue

                # Парсим ответ
                # SC v1/youtube/channel response fields
                # documentation: { "channelId": "...", "subscriberCount": 2750000, "videoCountText": "9,221 videos" }
                
                new_id = data.get("channelId")
                
                # Ищем количество видео и подписок
                subs_count = data.get("subscriberCount")
                video_count_text = data.get("videoCountText")
                video_count = None
                
                if video_count_text:
                    # '9,221 videos' -> 9221
                    import re
                    match = re.search(r'([\d,]+)', video_count_text)
                    if match:
                        video_count = int(match.group(1).replace(',', ''))
                
                # fallback
                if not video_count:
                    video_count = data.get("videoCount") or data.get("videoCountInt")
                if not subs_count:
                    subs_count = data.get("subscriberCountInt")

                # Обновляем ID если он был не ID (например, handle)
                # Пользователь просил "айди профиля и кол-во видео"
                # Значит в колонку ID лучше писать строгий ID (UC...)
                if new_id and new_id != current_id_val:
                    # Но если мы перезапишем Handle на ID, удобно ли это пользователю?
                    # Пользователь сказал "заносить их в таблицу". Думаю, ID надежнее.
                    cells_to_update.append(gspread.Cell(real_row_idx, col_id_write_idx, new_id))
                
                # Обновляем количество видео
                if video_count is not None and col_video_idx != -1:
                     cells_to_update.append(gspread.Cell(real_row_idx, col_video_idx, str(video_count)))
                
                # Обновляем подписки
                if subs_count is not None and col_subs_idx != -1:
                     cells_to_update.append(gspread.Cell(real_row_idx, col_subs_idx, str(subs_count)))
                
                # Обновляем дату
                if col_date_idx != -1:
                     today_str = datetime.utcnow().strftime("%Y-%m-%d")
                     cells_to_update.append(gspread.Cell(real_row_idx, col_date_idx, today_str))
            
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
        import re
        from gspread.utils import extract_id_from_url
        sh = gc.open_by_key(extract_id_from_url(target_url))
        
        match = re.search(r'(?:gid=)(\d+)', target_url)
        if match:
            ws = sh.get_worksheet_by_id(int(match.group(1)))
        else:
            ws = sh.sheet1
        
        headers = ws.row_values(1)
        
        # Ищем колонки для записи
        # Для TikTok логин всегда берется из колонки A (индекс 1)
        col_user_source_idx = 1
        
        col_id_write_idx   = _find_col_idx(headers, ["id_профиля", "idпрофиля", "id_profile", "user_id", "user id", "id профиля"])
        col_video_idx = _find_col_idx(headers, ["видео", "video", "amount", "количество_видео", "количество видео"])
        col_subs_idx = _find_col_idx(headers, ["подписки", "subscribers", "followers", "подписчики"])
        col_date_idx = _find_col_idx(headers, ["дата обновления", "дата_обновления", "date", "updated_at"])

        all_values = ws.get_all_values()
        rows = all_values[1:]
        
        cells_to_update = []
        logger.info(f"🔄 Enriching TikTok Sheet ({len(rows)} rows)...")

        for i, row in enumerate(rows):
            real_row_idx = i + 2
            
            username = row[col_user_source_idx - 1] if len(row) >= col_user_source_idx else ""
            if not username:
                continue
            
            # Чистим юзернейм от ссылок
            clean_user = username.replace("https://www.tiktok.com/@", "").strip("/")
            clean_user = clean_user.split("?")[0]
            if "@" in clean_user:
                clean_user = clean_user.replace("@", "")

            # Запрос
            params = {"handle": clean_user}
            data = await _fetch_sc_data(session, SC_TT_BASE, params)
            
            if data:
                # Если вернулся 404 - аккаунт не найден (удален)
                if data.get("error") == "not_found":
                    if col_subs_idx != -1:
                        cells_to_update.append(gspread.Cell(real_row_idx, col_subs_idx, "УДАЛЕН"))
                    
                    # Обновляем Дату
                    if col_date_idx != -1:
                        today_str = datetime.utcnow().strftime("%Y-%m-%d")
                        cells_to_update.append(gspread.Cell(real_row_idx, col_date_idx, today_str))
                    continue

                # SC v1/tiktok/profile response
                # user: { id: ... }, stats: { videoCount: ... }
                user_data = data.get("user") or {}
                stats = data.get("stats") or {}
                
                uid = user_data.get("id")
                v_count = stats.get("videoCount")
                s_count = stats.get("followerCount")
                
                # Обновляем ID
                if uid and col_id_write_idx != -1:
                    cells_to_update.append(gspread.Cell(real_row_idx, col_id_write_idx, str(uid)))
                
                # Обновляем Video Count
                if v_count is not None and col_video_idx != -1:
                    cells_to_update.append(gspread.Cell(real_row_idx, col_video_idx, str(v_count)))
                
                # Обновляем Подписки
                if s_count is not None and col_subs_idx != -1:
                    cells_to_update.append(gspread.Cell(real_row_idx, col_subs_idx, str(s_count)))
                    
                # Обновляем Дату
                if col_date_idx != -1:
                    today_str = datetime.utcnow().strftime("%Y-%m-%d")
                    cells_to_update.append(gspread.Cell(real_row_idx, col_date_idx, today_str))
            
            await asyncio.sleep(0.1)

        if cells_to_update:
            ws.update_cells(cells_to_update)
            logger.info(f"✅ TikTok Sheet updated: {len(cells_to_update)} cells changed.")
            
    except Exception as e:
        logger.error(f"Error enriching TikTok sheet: {e}")


async def mark_tiktok_profile_deleted(user_id: str):
    """
    Finds the row for user_id in the TikTok sheet and marks column G as 'удален'.
    """
    await _mark_profile_deleted_generic(user_id, platform="tiktok")


async def mark_youtube_profile_deleted(channel_id: str):
    """
    Finds the row for channel_id in the YouTube sheet and marks column G as 'удален'.
    """
    await _mark_profile_deleted_generic(channel_id, platform="youtube")


async def _mark_profile_deleted_generic(profile_id: str, platform: str):
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
            col_id_idx = 4 if platform == "youtube" else 2 # common fallbacks
            
        col_status_idx = 7 # Column G
        
        target_row_idx = -1
        for i, row in enumerate(all_values[1:], start=2):
            if len(row) >= col_id_idx:
                val = str(row[col_id_idx - 1]).strip()
                if val == str(profile_id).strip():
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
