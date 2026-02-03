import logging
import asyncio
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
)
from aiohttp import ClientError

logger = logging.getLogger(__name__)

def create_retry_decorator(
    max_attempts: int = 3,
    min_wait: float = 2.0,
    max_wait: float = 30.0,
    exceptions: tuple = (ClientError, asyncio.TimeoutError),
):
    """
    Creates a standard tenacity retry decorator for network requests.
    """
    return retry(
        stop=stop_after_attempt(max_attempts),
        wait=wait_exponential(multiplier=1, min=min_wait, max=max_wait),
        retry=retry_if_exception_type(exceptions),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,
    )

# Standard retry for API requests
api_retry = create_retry_decorator()
