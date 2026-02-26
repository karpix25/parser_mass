import json
import asyncio
from datetime import datetime, timezone, timedelta
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from aiohttp import ClientSession
import logging

logger = logging.getLogger(__name__)

from app.core.config import settings
from app.core.templates import templates
from app.db import get_conn
from app.sheets import (
    fetch_accounts,
    fetch_youtube_channels,
    fetch_tiktok_profiles,
)
from app.services.parser import parse_all, parse_youtube_only, parse_tiktok_only

router = APIRouter()

@router.get("/settings", response_class=HTMLResponse)
async def settings_page(request: Request):
    pool = await get_conn()
    async with pool.acquire() as conn:
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS public.settings (
            key TEXT PRIMARY KEY,
            value TEXT,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW()
        );
        """)
        rows = await conn.fetch("SELECT key, value FROM public.settings ORDER BY key")

    saved_settings = {r["key"]: r["value"] for r in rows}
    now_moscow = datetime.now(timezone(timedelta(hours=3))).strftime("%Y-%m-%d %H:%M:%S")

    return templates.TemplateResponse(
        "settings.html",
        {"request": request, "settings": saved_settings, "now_moscow": now_moscow, "saved": request.query_params.get("saved")},
    )

@router.post("/settings", response_class=HTMLResponse)
async def save_settings(request: Request):
    form = await request.form()
    pool = await get_conn()
    async with pool.acquire() as conn:
        for k, v in form.items():
            await conn.execute("""
                INSERT INTO public.settings (key, value, updated_at)
                VALUES ($1, $2, NOW())
                ON CONFLICT (key)
                DO UPDATE SET value = EXCLUDED.value, updated_at = NOW();
            """, k, v)
    # Note: We need to reload scheduler here. 
    # Ideally scheduler should be a service we can call.
    # For now, we might need to import setup_scheduler from main or move it to a service.
    # Let's assume we move scheduler setup to app/services/scheduler.py later.
    # For now, we'll leave a TODO or try to import it if possible (circular import risk).
    # A better way is to have a callback or event.
    # Or just move setup_scheduler to app/services/scheduler.py and import it here.
    from app.services.scheduler import setup_scheduler
    await setup_scheduler(force_reload=True)
    
    return RedirectResponse("/settings?saved=1", status_code=303)

@router.get("/", response_class=HTMLResponse)
async def home(request: Request):
    return templates.TemplateResponse("home.html", {"request": request})

@router.get("/runs", response_class=HTMLResponse)
async def runs(request: Request):
    pool = await get_conn()
    async with pool.acquire() as conn:
        rows = await conn.fetch(f"""
            SELECT id, started_at, finished_at, status, total_accounts, new_videos, errors, accounts_json
            FROM {settings.PG_SCHEMA}.parse_runs
            ORDER BY id DESC LIMIT 20
        """)
        acc_rows = await conn.fetch(f"""
            SELECT account, MAX(publish_date) AS publish_date
            FROM {settings.PG_SCHEMA}.video_stats
            GROUP BY account
            ORDER BY MAX(updated_at) DESC NULLS LAST, MAX(publish_date) DESC NULLS LAST
            LIMIT 20
        """)
    runs_data = []
    for r in rows:
        item = dict(r)
        raw_accounts = item.get("accounts_json")
        accounts_list: list[dict[str, str]] = []
        failed_list: list[dict[str, str]] = []
        if raw_accounts:
            try:
                decoded = json.loads(raw_accounts)
                if isinstance(decoded, dict):
                    for platform, entries in decoded.items():
                        if platform == "failed" and isinstance(entries, dict):
                            for fail_platform, fail_entries in entries.items():
                                if isinstance(fail_entries, list):
                                    for fail_entry in fail_entries:
                                        if not fail_entry:
                                            continue
                                        if isinstance(fail_entry, dict):
                                            value = (
                                                fail_entry.get("account")
                                                or fail_entry.get("channel_id")
                                                or fail_entry.get("user_id")
                                                or fail_entry.get("value")
                                                or ""
                                            )
                                            reason = fail_entry.get("reason") or "unknown error"
                                        else:
                                            value = str(fail_entry)
                                            reason = "unknown error"
                                        failed_list.append({
                                            "platform": fail_platform,
                                            "value": str(value),
                                            "reason": str(reason),
                                        })
                        elif isinstance(entries, list):
                            for entry in entries:
                                if not entry:
                                    continue
                                value = (
                                    entry.get("account")
                                    if isinstance(entry, dict) and "account" in entry
                                    else entry.get("channel_id")
                                    if isinstance(entry, dict) and "channel_id" in entry
                                    else entry.get("user_id")
                                    if isinstance(entry, dict) and "user_id" in entry
                                    else entry
                                )
                                accounts_list.append({
                                    "platform": platform,
                                    "value": str(value),
                                })
                elif isinstance(decoded, list):
                    for entry in decoded:
                        if entry:
                            accounts_list.append({
                                "platform": "unknown",
                                "value": str(entry),
                            })
            except json.JSONDecodeError:
                pass
        item["accounts_json"] = accounts_list
        item["failed_accounts"] = failed_list
        runs_data.append(item)

    return templates.TemplateResponse(
        "runs.html",
        {"request": request, "runs": runs_data, "accounts": acc_rows,
         "now_moscow": datetime.now(timezone(timedelta(hours=3))).strftime("%Y-%m-%d %H:%M:%S")},
    )

@router.get("/run-select")
async def select_instagram(request: Request):
    async with ClientSession() as s:
        acc = await fetch_accounts(s)
    return templates.TemplateResponse("run_select.html", {"request": request, "accounts": acc[:200]})

@router.post("/run-selected")
async def run_selected(request: Request):
    form = await request.form()
    selected = form.getlist("accounts")
    if not selected:
        return HTMLResponse("❌ Не выбраны аккаунты", status_code=400)
    asyncio.create_task(parse_all(selected))
    return RedirectResponse("/runs", status_code=303)

@router.get("/run-select-youtube")
async def select_youtube_channels(request: Request):
    async with ClientSession() as s:
        channels = await fetch_youtube_channels(s)
    return templates.TemplateResponse("run_select_youtube.html", {"request": request, "channels": channels[:200]})

@router.post("/run-selected-youtube")
async def run_selected_youtube(request: Request):
    form = await request.form()
    selected = form.getlist("channels")
    if not selected:
        return HTMLResponse("❌ Не выбраны каналы", status_code=400)
    asyncio.create_task(parse_youtube_only(selected))
    return RedirectResponse("/runs", status_code=303)

@router.post("/run-now")
async def run_now():
    asyncio.create_task(parse_all())
    return RedirectResponse("/runs", status_code=303)

@router.get("/run-select-tiktok", response_class=HTMLResponse)
async def select_tiktok_profiles(request: Request):
    if not settings.TIKTOK_SHEET_URL:
        return HTMLResponse("❌ Переменная окружения TIKTOK_SHEET_URL не задана", status_code=500)

    async with ClientSession() as s:
        profiles = await fetch_tiktok_profiles(s)
    return templates.TemplateResponse(
        "run_select_tiktok.html",
        {"request": request, "profiles": profiles[:200]}
    )

@router.post("/run-selected-tiktok")
async def run_selected_tiktok(request: Request):
    logger.info("🚀 Hit /run-selected-tiktok route")
    form = await request.form()
    selected = form.getlist("profiles")
    logger.info(f"📋 Selected profiles: {selected}")
    if not selected:
        return HTMLResponse("❌ Не выбраны профили", status_code=400)
    asyncio.create_task(parse_tiktok_only(selected))
    return RedirectResponse("/runs", status_code=303)


# @router.post("/enrich-sheets")
# async def enrich_sheets_endpoint():
#     # [NEW] Endpoint to trigger enrichment manually
#     from app.services.enricher import enrich_all_sheets
#     asyncio.create_task(enrich_all_sheets())
#     return RedirectResponse("/?enriched=1", status_code=303)

