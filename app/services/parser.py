import asyncio
import json
import logging
from datetime import datetime
from aiohttp import ClientSession

from app.core.config import settings
from app.db import get_conn
from app.sheets import (
    fetch_accounts,
    fetch_tags,
    fetch_youtube_channels,
    fetch_tiktok_profiles,
    preload_reference_data,
)
from app.parser_instagram import fetch_instagram_videos
from app.parser_youtube import process_youtube_channel
from app.parser_tiktok import process_tiktok_profile
from app.tagging import match_tags
from app.services.gsheets import append_to_sheet

logger = logging.getLogger(__name__)

def _describe_exception(err: Exception | None) -> str:
    if err is None:
        return "unknown error"
    name = err.__class__.__name__
    detail = str(err).strip()
    return f"{name}: {detail}" if detail else name

async def safe_run(label, func, retries=3, delay=20):
    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            result = await func()
            return result, None
        except Exception as e:
            last_error = e
            if attempt < retries:
                logger.warning(
                    "%s (%d/%d) raised %s: %s",
                    label,
                    attempt,
                    retries,
                    e.__class__.__name__,
                    e,
                    exc_info=True,
                )
                await asyncio.sleep(delay)
            else:
                logger.exception("%s failed permanently with %s", label, e.__class__.__name__)
    return None, last_error

# ---------------------------------------------------------------------------
# Platform-specific processors
# ---------------------------------------------------------------------------

async def _process_instagram_list(session, pool, tags, only_accounts: set[str] | None):
    """
    Process all Instagram accounts.
    Returns (count_accounts, count_new_videos, list_of_errors)
    """
    ig_accounts_full = await fetch_accounts(session)
    if only_accounts:
        ig_accounts = [a for a in ig_accounts_full if a["username"].casefold() in only_accounts]
    else:
        ig_accounts = ig_accounts_full
    
    total_accounts = len(ig_accounts)
    new_videos = 0
    failed_list = []

    # Instagram is processed sequentially internally to avoid rate limits if needed,
    # or we can parallelize this too if we have many proxies/accounts.
    # For now, keeping it sequential per account as in original, but parallel to other platforms.
    
    async with pool.acquire() as conn:
        for acc in ig_accounts:
            username = acc["username"]
            videos_result, videos_error = await safe_run(
                f"📦 IG {username}", lambda: fetch_instagram_videos(username, session)
            )
            if videos_error or not videos_result:
                reason = _describe_exception(videos_error)
                logger.error("🚫 IG %s failed after retries: %s", username, reason)
                failed_list.append({"account": username, "reason": reason})
                continue

            videos, pagination_failed, fail_reason = videos_result
            if pagination_failed:
                reason = fail_reason or "pagination failed"
                logger.error("⚠️ IG %s pagination failed: %s", username, reason)
                failed_list.append({"account": username, "reason": reason})
                # We still process whatever videos we got

            if not videos:
                continue

            for v in videos:
                client_tag, company, product, _matched = match_tags(v["caption"], tags)

                async with conn.transaction():
                    await conn.execute(f"""
                    INSERT INTO {settings.PG_SCHEMA}.video_stats
                        (platform, account, video_id, video_url, publish_date,
                         iso_year, week, likes, views, comments, caption,
                         client_tag, company, product, created_at, updated_at)
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,NOW(),NOW())
                    ON CONFLICT (video_id) DO UPDATE SET
                        account=EXCLUDED.account,
                        video_url=EXCLUDED.video_url,
                        likes=EXCLUDED.likes, views=EXCLUDED.views, comments=EXCLUDED.comments,
                        caption=EXCLUDED.caption, client_tag=EXCLUDED.client_tag,
                        company=EXCLUDED.company, product=EXCLUDED.product, updated_at=NOW();
                    """,
                        v["platform"], v["account"], v["video_id"], v["video_url"],
                        v["publish_date"], v["iso_year"], v["week"],
                        v["likes"], v["views"], v["comments"], v["caption"],
                        client_tag,
                        company,
                        product
                    )
                    new_videos += 1
            logger.info(f"✅ IG {username} done ({len(videos)} videos)")

    return total_accounts, new_videos, failed_list


async def _process_youtube_list(session, pool, tags, only_accounts: set[str] | None):
    """
    Process all YouTube channels.
    Returns (count_accounts, count_new_videos, list_of_errors)
    """
    if only_accounts:
        # If we are filtering by accounts, we assume the input list contains channel IDs
        # But fetch_youtube_channels returns all from sheet.
        # If only_accounts is passed, we might skip fetching all if we knew them, 
        # but here we fetch all to get 'amount' config from sheet.
        all_channels = await fetch_youtube_channels(session)
        yt_channels = [c for c in all_channels if c["channel_id"].casefold() in only_accounts]
    else:
        yt_channels = await fetch_youtube_channels(session)

    total_accounts = len(yt_channels)
    new_videos = 0
    failed_list = []
    sheet_rows = []

    async with pool.acquire() as conn:
        for ch in yt_channels:
            if int(ch.get("amount") or 0) <= 0:
                continue
            stats, stats_error = await safe_run(
                f"🎬 YT {ch['channel_id']}",
                lambda ch=ch: process_youtube_channel(
                    session=session, conn=conn, PG_SCHEMA=settings.PG_SCHEMA,
                    channel_id=ch["channel_id"], amount=int(ch["amount"]),
                    tags=tags, log_prefix="🎬 YT"
                ),
                retries=3,
                delay=30,
            )
            if stats_error or not stats:
                reason = _describe_exception(stats_error)
                logger.error("🚫 YT %s failed after retries: %s", ch.get("channel_id"), reason)
                failed_list.append(
                    {"channel_id": ch.get("channel_id", ""), "reason": reason}
                )
                continue
            
            inserted = stats.get("inserted", 0)
            updated = stats.get("updated", 0)
            total_new_vids = inserted + updated
            new_videos += total_new_vids
            
            if stats.get("is_not_found"):
                logger.warning(f"🚩 YouTube {ch['channel_id']} is 404. Marking in Google Sheet...")
                from app.services.enricher import mark_youtube_profile_deleted
                asyncio.create_task(mark_youtube_profile_deleted(ch["channel_id"]))
                failed_list.append({"channel_id": ch["channel_id"], "reason": "404 Not Found (deleted)"})
            else:
                # Update existing row instead of appending
                from app.services.enricher import update_video_stats_in_sheet
                asyncio.create_task(update_video_stats_in_sheet("youtube", ch["channel_id"], inserted + updated))
            
            logger.info(f"✅ YT {ch['channel_id']} done | {stats}")

    return total_accounts, new_videos, failed_list, []


async def _process_tiktok_list(session, pool, tags, only_accounts: set[str] | None):
    """
    Process all TikTok profiles.
    Returns (count_accounts, count_new_videos, list_of_errors)
    """
    if only_accounts:
        # If filtering, we assume we don't need the sheet if we just want to parse specific IDs?
        # But we need 'amount' from sheet usually. 
        # For simplicity, let's assume we always fetch sheet to get config.
        # If TIKTOK_SHEET_URL is missing, we return empty.
        if not settings.TIKTOK_SHEET_URL:
             return 0, 0, []
        all_profiles = await fetch_tiktok_profiles(session)
        tiktok_profiles = [p for p in all_profiles if p["user_id"].casefold() in only_accounts]
    else:
        if not settings.TIKTOK_SHEET_URL:
            logger.warning("⚠️ Пропускаем TikTok: переменная TIKTOK_SHEET_URL не задана")
            return 0, 0, []
        tiktok_profiles = await fetch_tiktok_profiles(session)

    total_accounts = len(tiktok_profiles)
    new_videos = 0
    failed_list = []
    sheet_rows = []

    async with pool.acquire() as conn:
        for profile in tiktok_profiles:
            amount = int(profile.get("amount") or 0)
            if amount <= 0:
                continue
            profile_label = profile.get("username") or profile["user_id"]
            stats, stats_error = await safe_run(
                f"🎬 TikTok {profile_label}",
                lambda profile=profile: process_tiktok_profile(
                    session=session,
                    conn=conn,
                    PG_SCHEMA=settings.PG_SCHEMA,
                    user_id=profile["user_id"],
                    amount=amount,
                    tags=tags,
                    sheet_username=profile.get("username"),
                ),
                retries=3,
                delay=20,
            )
            if stats_error or not stats:
                reason = _describe_exception(stats_error)
                logger.error("🚫 TikTok %s failed after retries: %s", profile.get("user_id"), reason)
                failed_list.append(
                    {"user_id": profile.get("user_id", ""), "reason": reason}
                )
                continue
            
            inserted = stats.get("inserted", 0)
            new_videos += inserted
            
            if stats.get("is_not_found"):
                logger.warning(f"🚩 TikTok {profile['user_id']} is 404. Marking in Google Sheet...")
                from app.services.enricher import mark_tiktok_profile_deleted
                asyncio.create_task(mark_tiktok_profile_deleted(profile["user_id"]))
                failed_list.append({"user_id": profile["user_id"], "reason": "404 Not Found (deleted)"})
            else:
                # Update existing row instead of appending
                from app.services.enricher import update_video_stats_in_sheet
                asyncio.create_task(update_video_stats_in_sheet("tiktok", profile["user_id"], inserted))
            
            logger.info(f"✅ TikTok {profile['user_id']} done | {stats}")

    return total_accounts, new_videos, failed_list, []


# ---------------------------------------------------------------------------
# Main Orchestrator
# ---------------------------------------------------------------------------

async def parse_all(only_accounts: list[str] | None = None):
    started = datetime.utcnow()
    pool = await get_conn()
    
    # Prepare filter set
    target_accounts = {a.casefold() for a in only_accounts} if only_accounts else None

    async with ClientSession() as session:
        # [NEW] Enrich sheets with actual data (IDs, video counts)
        try:
            from app.services.enricher import enrich_all_sheets
            await enrich_all_sheets()
        except Exception as e:
            logger.error(f"⚠️ Enrichment failed: {e}")

        await preload_reference_data(session, force=True)
        tags = await fetch_tags(session, force=True)

        # Run platforms in parallel
        logger.info("🚀 Starting parallel parsing for Instagram, YouTube, TikTok...")
        
        results = await asyncio.gather(
            _process_instagram_list(session, pool, tags, target_accounts),
            _process_youtube_list(session, pool, tags, target_accounts),
            _process_tiktok_list(session, pool, tags, target_accounts),
            return_exceptions=True
        )

    # Unpack results
    # results is a list of (total, new, failed) or Exception
    
    total_acc = 0
    total_new = 0
    total_err = 0
    
    failed_accounts = {
        "instagram": [],
        "youtube": [],
        "tiktok": []
    }
    
    platforms = ["instagram", "youtube", "tiktok"]
    for i, res in enumerate(results):
        platform_name = platforms[i]
        if isinstance(res, Exception):
            logger.error(f"🔥 Critical error in {platform_name} parser: {res}", exc_info=res)
            total_err += 1 # Count the whole platform failure as an error? Or just log it.
            failed_accounts[platform_name].append({"error": str(res)})
        else:
            if platform_name == "instagram":
                acc_count, new_count, failures = res
            else:
                acc_count, new_count, failures, sheet_export = res
                if sheet_export:
                    if platform_name == "youtube" and settings.YOUTUBE_OUTPUT_SHEET_URL:
                        logger.info(f"Attempting to write {len(sheet_export)} rows to YouTube Google Sheet...")
                        await append_to_sheet(sheet_export, settings.YOUTUBE_OUTPUT_SHEET_URL)
                    elif platform_name == "tiktok" and settings.TIKTOK_OUTPUT_SHEET_URL:
                        logger.info(f"Attempting to write {len(sheet_export)} rows to TikTok Google Sheet...")
                        await append_to_sheet(sheet_export, settings.TIKTOK_OUTPUT_SHEET_URL)
                    
            total_acc += acc_count
            total_new += new_count
            if failures:
                total_err += len(failures)
                failed_accounts[platform_name].extend(failures)

    finished = datetime.utcnow()
    
    # Prepare JSON log
    failed_summary = {k: v for k, v in failed_accounts.items() if v}
    
    # Note: We don't have the full list of success accounts here easily unless we return them.
    # For simplicity, we just log failed ones in detail. 
    # If we want full list in 'accounts_json', we'd need to return it from helpers.
    # Let's assume for now we just want to track what happened.
    # To keep backward compat with 'accounts_json' structure which had lists of all accounts:
    # We can either return them or just log "all" if only_accounts is None.
    # Let's just log what we have.
    
    accounts_payload = {}
    if only_accounts:
        accounts_payload["target_filter"] = only_accounts
        
    if failed_summary:
        accounts_payload["failed"] = failed_summary
        logger.warning("❗ Failed accounts summary: %s", json.dumps(failed_summary, ensure_ascii=False))

    async with pool.acquire() as conn:
        await conn.execute(f"""
        INSERT INTO {settings.PG_SCHEMA}.parse_runs
            (started_at, finished_at, status, total_accounts, new_videos, errors, accounts_json)
        VALUES ($1,$2,$3,$4,$5,$6,$7)
        """,
            started, finished,
            "success" if total_err == 0 else "partial",
            total_acc, total_new, total_err,
            json.dumps(accounts_payload, ensure_ascii=False)
        )
        
        # [NEW] Update history
        from app.services.history import update_views_history
        await update_views_history(conn)

    logger.info(f"🏁 parse_all complete | acc={total_acc} new={total_new} errors={total_err}")


async def parse_youtube_only(selected_channels: list[str]):
    # Wrapper to reuse logic or keep separate? 
    # For now, keeping separate or reusing helper is fine.
    # Let's reuse helper for consistency.
    started = datetime.utcnow()
    pool = await get_conn()
    target = {c.casefold() for c in selected_channels}
    
    async with ClientSession() as session:
        await preload_reference_data(session, force=True)
        tags = await fetch_tags(session, force=True)
        
        total_acc, total_new, failures, _ = await _process_youtube_list(session, pool, tags, target)

    finished = datetime.utcnow()
    errors = len(failures)
    
    accounts_payload = {"youtube": selected_channels}
    if failures:
        accounts_payload["failed"] = {"youtube": failures}

    async with pool.acquire() as conn:
        await conn.execute(f"""
        INSERT INTO {settings.PG_SCHEMA}.parse_runs
            (started_at, finished_at, status, total_accounts, new_videos, errors, accounts_json)
        VALUES ($1,$2,$3,$4,$5,$6,$7)
        """,
            started, finished,
            "success" if errors == 0 else "partial",
            total_acc, total_new, errors,
            json.dumps(accounts_payload, ensure_ascii=False)
        )
        from app.services.history import update_views_history
        await update_views_history(conn)
        
    logger.info(f"🏁 parse_youtube_only done | {total_acc=} {total_new=} {errors=}")


async def parse_tiktok_only(selected_profiles: list[str]):
    started = datetime.utcnow()
    pool = await get_conn()
    target = {p.casefold() for p in selected_profiles}
    
    async with ClientSession() as session:
        await preload_reference_data(session, force=True)
        tags = await fetch_tags(session, force=True)
        
        total_acc, total_new, failures, _ = await _process_tiktok_list(session, pool, tags, target)

    finished = datetime.utcnow()
    errors = len(failures)
    
    accounts_payload = {"tiktok": selected_profiles}
    if failures:
        accounts_payload["failed"] = {"tiktok": failures}

    async with pool.acquire() as conn:
        await conn.execute(f"""
        INSERT INTO {settings.PG_SCHEMA}.parse_runs
            (started_at, finished_at, status, total_accounts, new_videos, errors, accounts_json)
        VALUES ($1,$2,$3,$4,$5,$6,$7)
        """,
            started, finished,
            "success" if errors == 0 else "partial",
            total_acc, total_new, errors,
            json.dumps(accounts_payload, ensure_ascii=False)
        )
        from app.services.history import update_views_history
        await update_views_history(conn)
        
    logger.info(f"🏁 parse_tiktok_only done | profiles={total_acc} new={total_new} errors={errors}")
