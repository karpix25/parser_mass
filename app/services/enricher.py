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
    """
    Returns an authenticated gspread client.
    Tries to read JSON credentials from:
    1. GOOGLE_SERVICE_ACCOUNT_JSON environment variable (string content)
    2. service_account.json file
    """
    try:
        # 1. Try ENV variable
        json_creds = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
        if json_creds:
            json_creds = json_creds.strip()
            # If it doesn't look like JSON (no curly braces), try Base64 decoding
            if not json_creds.startswith("{"):
                 try:
                     decoded_bytes = base64.b64decode(json_creds)
                     json_creds = decoded_bytes.decode("utf-8")
                     logger.info("🔓 Decoded Base64 credentials from ENV")
                 except Exception:
                     # e.g. padding error, not base64 -> ignore or let json.loads fail
                     pass
            
            if json_creds.startswith("{"):
                 # Load from dict
                 creds_dict = json.loads(json_creds)
                 
                 # [FIX] Handle escaped newlines in private_key if coming from ENV
                 if "private_key" in creds_dict:
                     pk = creds_dict["private_key"]
                     # If key has literal \n characters but not actual newlines, fix it
                     if "\\n" in pk:
                         creds_dict["private_key"] = pk.replace("\\n", "\n")
                         logger.info("🔧 Fixed escaped newlines in ENV private_key")
                     
                     # Debug print to see what we actually have (safe snippet)
                     debug_key = creds_dict["private_key"]
                     logger.info(f"🔑 Key snippet: {debug_key[:40].replace(chr(10), '[NL]')}")
                 
                 gc = gspread.service_account_from_dict(creds_dict)
                 
                 # [NEW] Verify credentials immediately
                 try:
                     # Attempt a lightweight call to check signature
                     # list_spreadsheet_files raises APIError if auth fails
                     gc.list_spreadsheet_files()
                     logger.info("✅ Authenticated with Google Sheets via ENV variable (Verified)")
                     return gc
                 except Exception as exc:
                     logger.warning(f"⚠️ ENV credentials invalid (Signature check failed): {exc}. Falling back to file...")
            
            # If we fall through here, ENV failed or didn't exist

        # 2. Try file
        if os.path.exists("service_account.json"):
            try:
                gc = gspread.service_account(filename="service_account.json")
                # Verify file too
                gc.list_spreadsheet_files()
                logger.info("✅ Authenticated with Google Sheets via local file (Verified)")
                return gc
            except Exception as e:
                 logger.error(f"❌ File credentials invalid: {e}")
                 return None
        
        logger.warning("⚠️ No valid Google Creds found. Skipping enrichment.")
        return None

    except Exception as e:
        logger.error(f"❌ Failed to auth with Google Sheets: {e}")
        return None

async def _fetch_sc_data(session, url, params):
    headers = {"x-api-key": SC_API_KEY}
    try:
        async with session.get(url, headers=headers, params=params, timeout=30) as resp:
            if resp.status == 200:
                return await resp.json()
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
    if not settings.YOUTUBE_SHEET_URL:
        return
    
    try:
        sh = gc.open_by_url(settings.YOUTUBE_SHEET_URL)
        ws = sh.sheet1 # Берем первый лист
        
        headers = ws.row_values(1)
        
        # Индексы колонок (1-based)
        # Ищем колонку с "каналом" (id или ссылка)
        col_id_idx = _find_col_idx(headers, ["id_профиля", "idпрофиля", "channel_id", "channel id"])
        col_video_idx = _find_col_idx(headers, ["видео", "video", "amount", "количество_видео"])
        
        # Если нет колонки для записи видео, пробуем найти 'amount' или создаем? 
        # Пока предполагаем что структура есть, просто обновляем.
        if col_id_idx == -1 or col_video_idx == -1:
            logger.warning(f"⚠️ YouTube Sheet: Could not find required columns (ID or Video Count). Headers: {headers}")
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
            
            # Безопасное получение значения
            current_id_val = row[col_id_idx - 1] if len(row) >= col_id_idx else ""
            
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
                # Парсим ответ
                # SC v1/youtube/channel response fields
                # Usually: { "id": "...", "statistics": { "videoCount": ... } } OR similar
                # User screenshot endpoint: Channel Details.
                # Let's inspect structure safely
                
                new_id = data.get("id")
                
                # Ищем количество видео
                stats = data.get("statistics") or {}
                video_count = stats.get("videoCount")
                
                # Если в stats пусто, ищем в корне (бывает по-разному в разных версиях API)
                if not video_count:
                    video_count = data.get("videoCount")
                
                if not video_count and "videoCountText" in data:
                     # "1.2K videos" -> parse? No, API usually has Int field.
                     # Let's trust videoCount or videoCountInt
                     video_count = data.get("videoCountInt")

                # Обновляем ID если он был не ID (например, handle)
                # Пользователь просил "айди профиля и кол-во видео"
                # Значит в колонку ID лучше писать строгий ID (UC...)
                if new_id and new_id != current_id_val:
                    # Но если мы перезапишем Handle на ID, удобно ли это пользователю?
                    # Пользователь сказал "заносить их в таблицу". Думаю, ID надежнее.
                    cells_to_update.append(gspread.Cell(real_row_idx, col_id_idx, new_id))
                
                # Обновляем количество видео
                if video_count is not None:
                     cells_to_update.append(gspread.Cell(real_row_idx, col_video_idx, str(video_count)))
            
            await asyncio.sleep(0.1) # Throttle slightly

        if cells_to_update:
            ws.update_cells(cells_to_update)
            logger.info(f"✅ YouTube Sheet updated: {len(cells_to_update)} cells changed.")
        else:
            logger.info("YouTube Sheet: No updates needed.")

    except Exception as e:
        logger.error(f"Error enriching YouTube sheet: {e}")


async def enrich_tiktok_sheet(gc: gspread.Client, session: aiohttp.ClientSession):
    if not settings.TIKTOK_SHEET_URL:
        return

    try:
        sh = gc.open_by_url(settings.TIKTOK_SHEET_URL)
        ws = sh.sheet1 
        
        headers = ws.row_values(1)
        
        # Ищем колонки
        col_user_idx = _find_col_idx(headers, ["usernames", "username", "логин", "login", "handle"])
        col_id_idx   = _find_col_idx(headers, ["id_профиля", "idпрофиля", "id_profile", "user_id"])
        col_video_idx = _find_col_idx(headers, ["видео", "video", "amount", "количество_видео"])
        
        if col_user_idx == -1: # Username обязателен для поиска
            logger.warning("TikTok Sheet: Username column not found.")
            return

        all_values = ws.get_all_values()
        rows = all_values[1:]
        
        cells_to_update = []
        logger.info(f"🔄 Enriching TikTok Sheet ({len(rows)} rows)...")

        for i, row in enumerate(rows):
            real_row_idx = i + 2
            
            username = row[col_user_idx - 1] if len(row) >= col_user_idx else ""
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
                # SC v1/tiktok/profile response
                # user: { id: ... }, stats: { videoCount: ... }
                user_data = data.get("user") or {}
                stats = data.get("stats") or {}
                
                uid = user_data.get("id")
                v_count = stats.get("videoCount")
                
                # Обновляем ID
                if uid and col_id_idx != -1:
                    cells_to_update.append(gspread.Cell(real_row_idx, col_id_idx, str(uid)))
                
                # Обновляем Video Count
                if v_count is not None and col_video_idx != -1:
                    cells_to_update.append(gspread.Cell(real_row_idx, col_video_idx, str(v_count)))
            
            await asyncio.sleep(0.1)

        if cells_to_update:
            ws.update_cells(cells_to_update)
            logger.info(f"✅ TikTok Sheet updated: {len(cells_to_update)} cells changed.")
            
    except Exception as e:
        logger.error(f"Error enriching TikTok sheet: {e}")


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
