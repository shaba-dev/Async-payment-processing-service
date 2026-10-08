"""Демо-приёмник webhook'ов. /hook -> 200, /fail -> 500 (для проверки retry и DLQ)."""
import logging

from fastapi import FastAPI, HTTPException, Request

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("webhook-receiver")
app = FastAPI(title="Webhook receiver (demo)")


@app.post("/hook")
async def hook(request: Request):
    log.info("WEBHOOK RECEIVED: %s", await request.json())
    return {"ok": True}


@app.post("/fail")
async def fail(request: Request):
    log.info("WEBHOOK (will fail): %s", await request.json())
    raise HTTPException(500, "simulated failure")
