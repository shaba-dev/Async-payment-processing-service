"""Локальный приемник webhook для демонстрации (не часть продакшн-кода)."""
import logging
from typing import Any

from fastapi import FastAPI

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("webhook-receiver")
app = FastAPI(title="Webhook receiver (dev)")


@app.post("/hook")
async def hook(body: dict[str, Any]) -> dict[str, str]:
    logger.info("WEBHOOK RECEIVED: %s", body)
    return {"ok": "true"}
