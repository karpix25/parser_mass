import json
import base64
import logging
import re
from typing import List, Any
import gspread
from google.oauth2.service_account import Credentials
import asyncio

from app.core.config import settings

logger = logging.getLogger(__name__)

# Scopes needed for Google Sheets and Drive API
SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]

def get_gspread_client() -> gspread.client.Client | None:
    """
    Initializes and returns a gspread client using the base64 encoded credentials
    from the environment variables.
    """
    if not settings.GOOGLE_CREDENTIALS_B64:
        logger.warning("GOOGLE_CREDENTIALS_B64 is not set. Cannot initialize gspread client.")
        return None

    try:
        # Decode base64 to JSON string
        decoded_bytes = base64.b64decode(settings.GOOGLE_CREDENTIALS_B64)
        decoded_str = decoded_bytes.decode('utf-8')
        
        # Parse JSON string to dict allowing control characters like '\n'
        creds_dict = json.loads(decoded_str, strict=False)
        
        # Initialize credentials
        credentials = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
        
        # Authorize gspread
        gc = gspread.authorize(credentials)
        return gc
    except Exception as e:
        logger.error(f"Failed to initialize Google Sheets client: {e}")
        return None

async def append_to_sheet(rows: List[List[Any]], target_url: str | None = None) -> bool:
    """
    Appends multiple rows to the Google Sheet defined by target_url.
    This runs synchronously under the hood, so we wrap it in an executor.
    """
    if not target_url:
        logger.warning("target_url is not set. Skipping sheet append.")
        return False
        
    if not rows:
        return True # Nothing to append

    def _append_sync():
        try:
            gc = get_gspread_client()
            if not gc:
                return False
                
            sh = gc.open_by_url(target_url)
            
            # Try to grab the exact worksheet if '#gid=...' is in the URL
            match = re.search(r'gid=(\d+)', target_url)
            if match:
                gid = int(match.group(1))
                sheet = sh.get_worksheet_by_id(gid)
            else:
                sheet = sh.sheet1
                
            # Append rows (value_input_option="USER_ENTERED" ensures numbers and dates are parsed correctly)
            sheet.append_rows(rows, value_input_option="USER_ENTERED")
            logger.info(f"Successfully appended {len(rows)} rows to Google Sheet ({sheet.title}).")
            return True
        except Exception as e:
            logger.error(f"Error appending rows to Google Sheet: {e}", exc_info=True)
            return False

    # Run blocking gspread calls in a separate thread
    loop = asyncio.get_running_loop()
    result = await loop.run_in_executor(None, _append_sync)
    return result
