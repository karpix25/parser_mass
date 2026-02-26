import asyncio
import csv
import aiohttp
import unicodedata
from time import time
from io import StringIO
import logging
from typing import Callable, Any

from app.core.config import settings

logger = logging.getLogger(__name__)

# Cache storage
_cache: dict[str, list[Any]] = {}
_cache_timestamps: dict[str, float] = {}
_cache_lock = asyncio.Lock()

REFRESH_INTERVAL_MINUTES = 60
REFRESH_INTERVAL_SECONDS = REFRESH_INTERVAL_MINUTES * 60

def _normalize_text(value: str | None, *, to_lower: bool = False) -> str:
    if not value:
        return ""
    value = unicodedata.normalize("NFKC", str(value))
    value = value.replace("\u200b", "").strip()
    if to_lower:
        value = value.lower()
    return value

def _normalize_key(value: str | None) -> str:
    return _normalize_text(value, to_lower=True).replace(" ", "_")

async def _fetch_csv(session: aiohttp.ClientSession, url: str) -> list[dict[str, str]]:
    async with session.get(url, allow_redirects=True) as resp:
        resp.raise_for_status()
        text = await resp.text()
    
    # Handle potential BOM or weird encodings if needed, but usually text() handles it.
    # We use DictReader which assumes the first row is headers.
    # Some sheets might not have headers in the first row (like YouTube CSV-public mentioned in original code?),
    # but the original code for YouTube used csv.reader and manual index lookup.
    # Let's support both DictReader (standard) and list of lists if needed, 
    # but for simplicity, let's try to stick to DictReader where possible or handle the specific case.
    
    # Check if it's the specific YouTube sheet format which seemed to be headerless or specific?
    # Original code: rows = list(csv.reader(StringIO(txt))) ... header = [h.strip().lower() for h in rows[0]]
    # It seems it DOES have headers, just processed manually. DictReader is safer.
    
    return list(csv.DictReader(StringIO(text)))

async def _load_generic(
    key: str,
    url: str | None,
    mapper: Callable[[dict[str, str]], dict | None],
    session: aiohttp.ClientSession | None = None
) -> list[Any]:
    """
    Generic function to load, parse and cache data from a Google Sheet (CSV).
    """
    global _cache, _cache_timestamps

    if not url:
        _cache[key] = []
        _cache_timestamps[key] = time()
        logger.warning(f"⚠️ URL for {key} is not set.")
        return []

    logger.info(f"🔁 Fetching {key} from Google Sheets...")
    
    sess = session or aiohttp.ClientSession()
    created_here = session is None
    try:
        rows = await _fetch_csv(sess, url)
    except Exception as e:
        logger.error(f"❌ Failed to fetch {key}: {e}")
        # If fetch fails, return empty or old cache? For now empty to be safe.
        return []
    finally:
        if created_here:
            await sess.close()

    seen = set()
    clean = []
    skipped_empty = 0
    skipped_dupe = 0

    # Pre-normalize keys for all rows to make lookups easier
    normalized_rows = []
    for r in rows:
        normalized_rows.append({_normalize_key(k): v for k, v in r.items() if k})

    for row in normalized_rows:
        try:
            item = mapper(row)
        except Exception as e:
            logger.warning(f"⚠️ Error mapping row in {key}: {e}")
            continue

        if not item:
            skipped_empty += 1
            continue
        
        # Generate a unique key for deduplication
        # Mapper should return a dict with a special key '_dedup_key' if deduplication is needed,
        # otherwise we assume the whole item is the key if it's hashable, or we skip dedup.
        # Let's standardize: mapper returns a dict. We look for specific fields to dedup.
        
        # For backward compatibility with specific logic:
        dedup_val = item.get("_dedup_key")
        if dedup_val:
            if dedup_val in seen:
                skipped_dupe += 1
                continue
            seen.add(dedup_val)
            del item["_dedup_key"] # remove internal key
        
        clean.append(item)

    _cache[key] = clean
    _cache_timestamps[key] = time()
    logger.info(
        f"✅ Loaded {len(clean)} items for {key} (skipped empty={skipped_empty}, duplicates={skipped_dupe})"
    )
    return clean

async def _get_cached_or_fetch(
    key: str,
    url: str | None,
    mapper: Callable[[dict[str, str]], dict | None],
    session: aiohttp.ClientSession | None = None,
    force: bool = False
) -> list[Any]:
    global _cache, _cache_timestamps
    
    now = time()
    last_update = _cache_timestamps.get(key, 0.0)
    
    if force or key not in _cache or (now - last_update > REFRESH_INTERVAL_SECONDS):
        async with _cache_lock:
            # Double check inside lock
            last_update = _cache_timestamps.get(key, 0.0)
            if force or key not in _cache or (now - last_update > REFRESH_INTERVAL_SECONDS):
                await _load_generic(key, url, mapper, session)
    
    return list(_cache.get(key, []))

# --- Mappers ---

def _map_account(row: dict[str, str]) -> dict | None:
    raw_user = (
        row.get("usernames") or row.get("username") or row.get("account") or row.get("логин")
    )
    username = _normalize_text(raw_user)
    if not username:
        return None
        
    amount_raw = (
        row.get("видео") or row.get("video") or row.get("amount") or row.get("количество_видео") or ""
    )
    try:
        amount = int(_normalize_text(amount_raw, to_lower=True) or "0")
    except ValueError:
        amount = 0
        
    return {
        "username": username,
        "amount": max(amount, 0),
        "_dedup_key": username.casefold()
    }

def _map_tag(row: dict[str, str]) -> dict | None:
    tag = _normalize_text(row.get("хэштег") or row.get("hashtag"), to_lower=True).lstrip("#")
    if not tag:
        return None
        
    return {
        "tag": tag,
        "company": _normalize_text(row.get("компания") or row.get("company")) or None,
        "product": _normalize_text(row.get("продукт") or row.get("product")) or None,
        "_dedup_key": tag
    }

def _map_youtube(row: dict[str, str]) -> dict | None:
    # YouTube sheet logic was a bit specific with indices in original code, 
    # but DictReader with normalized keys should work if headers exist.
    # Expected headers: 'видео', 'id профиля' (or variants)
    
    channel_id = _normalize_text(
        row.get("id_профиля") or row.get("idпрофиля") or row.get("id_profile") or row.get("channel_id")
    )
    if not channel_id:
        # Fallback for weird headers if needed, but let's rely on standard ones first.
        # If the CSV has "id профиля" it becomes "id_профиля" via _normalize_key.
        return None

    amount_raw = row.get("видео") or row.get("video") or row.get("amount") or ""
    try:
        amount = int(_normalize_text(amount_raw, to_lower=True) or "0")
    except ValueError:
        amount = 0
        
    return {
        "channel_id": channel_id,
        "amount": max(amount, 0),
        "_dedup_key": channel_id.casefold()
    }

def _map_tiktok(row: dict[str, str]) -> dict | None:
    username = _normalize_text(
        row.get("usernames") or row.get("username") or row.get("логин") or row.get("login")
    )
    user_id = _normalize_text(
        row.get("id_профиля") or row.get("idпрофиля") or row.get("id-профиля") 
        or row.get("id_profile") or row.get("profile_id") or row.get("user_id")
    )
    
    if not user_id and not username:
        return None
        
    amount_raw = (
        row.get("видео") or row.get("video") or row.get("amount") or row.get("количество_видео") or ""
    )
    try:
        amount = int(_normalize_text(amount_raw, to_lower=True) or "0")
    except ValueError:
        amount = 0
        
    return {
        "user_id": user_id or username,
        "amount": max(amount, 0),
        "username": username or user_id,
        "_dedup_key": (user_id or username).casefold()
    }

# --- Public API ---

async def fetch_accounts(session=None, force: bool = False) -> list[dict]:
    return await _get_cached_or_fetch("accounts", settings.ACCOUNTS_SHEET_URL, _map_account, session, force=force)

async def fetch_tags(session=None, force: bool = False) -> list[dict]:
    return await _get_cached_or_fetch("tags", settings.TAGS_SHEET_URL, _map_tag, session, force=force)

async def fetch_youtube_channels(session=None, force: bool = False) -> list[dict]:
    return await _get_cached_or_fetch("youtube", settings.YOUTUBE_SHEET_URL, _map_youtube, session, force=force)

async def fetch_tiktok_profiles(session=None, force: bool = False) -> list[dict]:
    return await _get_cached_or_fetch("tiktok", settings.TIKTOK_SHEET_URL, _map_tiktok, session, force=force)

async def preload_reference_data(session: aiohttp.ClientSession | None = None, force: bool = False):
    await asyncio.gather(
        fetch_accounts(session, force=force),
        fetch_tags(session, force=force),
        fetch_youtube_channels(session, force=force),
        fetch_tiktok_profiles(session, force=force)
    )
