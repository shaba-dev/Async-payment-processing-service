from typing import Any

import httpx

from app.config import settings


async def send_webhook(url: str, payload: dict[str, Any]) -> None:
    """POST webhook. Любой не-2xx ответ или сетевая ошибка -> исключение."""
    async with httpx.AsyncClient(timeout=settings.webhook_timeout, follow_redirects=False) as client:
        response = await client.post(url, json=payload)
        response.raise_for_status()
