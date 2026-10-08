"""Единственный consumer очереди payments.new.

Делает всё: эмулирует шлюз → обновляет статус в БД → отправляет webhook.
Любая ошибка (в т.ч. недоступный webhook) → повтор через очередь задержки
payments.retry.N (экспоненциальная задержка); после max_attempts → payments.dlq.
Обработка идемпотентна: повторная доставка не пересчитывает статус и не шлёт
уже доставленный webhook.
"""
import asyncio
import logging
import random
import uuid
from datetime import UTC, datetime

import httpx
from faststream import Context, FastStream
from sqlalchemy import select, update

from app.config import settings
from app.db import engine, session_factory
from app.messaging import (
    DLQ_QUEUE,
    EXCHANGE,
    NEW_QUEUE,
    RK_DLQ,
    declare_topology,
    make_broker,
    retry_key,
)
from app.models import Payment, PaymentStatus
from app.schemas import PaymentEvent, PaymentOut

log = logging.getLogger("payments.consumer")

broker = make_broker()
app = FastStream(broker)


@app.after_startup
async def _declare_topology() -> None:
    await declare_topology(broker)


@app.on_shutdown
async def _dispose_engine() -> None:
    await engine.dispose()


async def _emulate_gateway() -> PaymentStatus:
    """Эмуляция внешнего шлюза: 2–5 сек, 90% успех / 10% отказ."""
    await asyncio.sleep(random.uniform(settings.gateway_min_delay, settings.gateway_max_delay))
    ok = random.random() < settings.gateway_success_rate
    return PaymentStatus.SUCCEEDED if ok else PaymentStatus.FAILED


async def _settle_payment(payment_id: uuid.UUID) -> None:
    """Выставляет финальный статус, если платёж ещё pending."""
    async with session_factory() as session:
        payment = await session.get(Payment, payment_id)
        if payment is None:
            raise LookupError(f"Payment {payment_id} not found")
        if payment.status != PaymentStatus.PENDING.value:
            return  # уже обработан (повторная доставка)

    new_status = await _emulate_gateway()  # вне транзакции — не держим блокировку строки

    async with session_factory() as session, session.begin():
        payment = await session.scalar(
            select(Payment).where(Payment.id == payment_id).with_for_update()
        )
        if payment.status == PaymentStatus.PENDING.value:  # перепроверка под блокировкой
            payment.status = new_status.value
            payment.processed_at = datetime.now(UTC)
            log.info("Payment %s -> %s", payment_id, new_status.value)


async def _deliver_webhook(payment_id: uuid.UUID) -> None:
    async with session_factory() as session:
        payment = await session.get(Payment, payment_id)
        if payment.webhook_delivered_at is not None:
            return  # уже доставлен
        body = PaymentOut.model_validate(payment).model_dump(mode="json")
        url = payment.webhook_url

    async with httpx.AsyncClient(timeout=settings.webhook_timeout) as client:
        response = await client.post(url, json=body)
        response.raise_for_status()  # любой не-2xx считается ошибкой доставки

    async with session_factory() as session, session.begin():
        await session.execute(
            update(Payment)
            .where(Payment.id == payment_id)
            .values(webhook_delivered_at=datetime.now(UTC))
        )
    log.info("Webhook delivered for payment %s", payment_id)


@broker.subscriber(NEW_QUEUE, EXCHANGE)
async def handle_new_payment(
    event: PaymentEvent,
    headers: dict = Context("message.headers"),
) -> None:
    attempt = int(headers.get("x-attempt", 1))
    payload = {"payment_id": str(event.payment_id)}
    try:
        await _settle_payment(event.payment_id)
        await _deliver_webhook(event.payment_id)
    except Exception as exc:
        error = f"{type(exc).__name__}: {exc}"[:500]
        if attempt < settings.max_attempts:
            log.warning("Attempt %d/%d failed for %s (%s); scheduling retry",
                        attempt, settings.max_attempts, event.payment_id, error)
            await broker.publish(
                payload,
                routing_key=retry_key(attempt),
                exchange=EXCHANGE,
                persist=True,
                headers={"x-attempt": attempt + 1, "x-last-error": error},
            )
        else:
            log.error("Attempt %d/%d failed for %s (%s); sending to DLQ",
                      attempt, settings.max_attempts, event.payment_id, error)
            await broker.publish(
                payload,
                routing_key=RK_DLQ,
                exchange=EXCHANGE,
                persist=True,
                headers={"x-attempt": attempt, "x-last-error": error},
            )
        # сообщение подтверждается (ack): его судьбу теперь определяет retry/DLQ очередь
