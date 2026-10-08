"""Outbox relay: переносит события из таблицы outbox в RabbitMQ.

Гарантия — at-least-once: событие помечается опубликованным только после
подтверждения брокера (publisher confirms). Если процесс упадёт между
публикацией и коммитом, сообщение уйдёт повторно — consumer идемпотентен.
`FOR UPDATE SKIP LOCKED` позволяет безопасно запускать несколько relay.
"""
import asyncio
import logging
from datetime import UTC, datetime

from faststream.rabbit import RabbitBroker
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker

from app.config import settings
from app.messaging import EXCHANGE, RK_NEW
from app.models import Outbox

log = logging.getLogger("payments.outbox")


async def relay_once(broker: RabbitBroker, sessions: async_sessionmaker) -> int:
    published = 0
    async with sessions() as session, session.begin():
        rows = (
            await session.scalars(
                select(Outbox)
                .where(Outbox.published_at.is_(None))
                .order_by(Outbox.created_at)
                .limit(settings.outbox_batch_size)
                .with_for_update(skip_locked=True)
            )
        ).all()
        for row in rows:
            try:
                await broker.publish(
                    row.payload,
                    routing_key=RK_NEW,
                    exchange=EXCHANGE,
                    persist=True,
                    message_id=str(row.id),
                    headers={"x-attempt": 1},
                )
            except Exception:
                log.exception("Failed to publish outbox event %s; will retry", row.id)
                break  # сохраняем порядок; уже опубликованные ниже закоммитятся
            row.published_at = datetime.now(UTC)
            published += 1
    return published


async def run_relay(broker: RabbitBroker, sessions: async_sessionmaker) -> None:
    log.info("Outbox relay started")
    while True:
        try:
            published = await relay_once(broker, sessions)
            if published:
                log.info("Published %d outbox event(s)", published)
                continue  # возможно, есть ещё
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("Outbox relay iteration failed")
        await asyncio.sleep(settings.outbox_poll_interval)
