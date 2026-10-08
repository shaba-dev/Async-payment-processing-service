import asyncio
import logging
import random
import uuid

from sqlalchemy import update

from app.config import settings
from app.db import session_factory
from app.models import Payment, PaymentStatus, utcnow
from app.schemas import PaymentOut
from app.webhook import send_webhook

logger = logging.getLogger("processing")


async def emulate_gateway() -> bool:
    """Эмуляция внешнего шлюза: 2-5 сек, 90% успех."""
    await asyncio.sleep(random.uniform(settings.gateway_min_delay, settings.gateway_max_delay))
    return random.random() < settings.gateway_success_rate


async def process_payment(payment_id: uuid.UUID) -> None:
    """Обрабатывает платеж и отправляет webhook.

    Идемпотентно: если платеж уже в финальном статусе (повторная доставка
    сообщения или retry после сбоя webhook), шлюз повторно не вызывается,
    отправляется только webhook.
    """
    async with session_factory() as session:
        payment = await session.get(Payment, payment_id)
        if payment is None:
            logger.error("Payment %s not found, dropping message", payment_id)
            return

        if payment.status == PaymentStatus.PENDING.value:
            success = await emulate_gateway()
            new_status = PaymentStatus.SUCCEEDED if success else PaymentStatus.FAILED
            # Условный UPDATE: статус меняется ровно один раз даже при дублях сообщений
            await session.execute(
                update(Payment)
                .where(Payment.id == payment_id, Payment.status == PaymentStatus.PENDING.value)
                .values(status=new_status.value, processed_at=utcnow())
            )
            await session.commit()
            await session.refresh(payment)
            logger.info("Payment %s -> %s", payment_id, payment.status)

        payload = PaymentOut.model_validate(payment).model_dump(mode="json")

    # Если webhook не доставлен -> исключение -> retry сообщения (см. consumer)
    await send_webhook(payment.webhook_url, payload)
    logger.info("Webhook delivered for payment %s", payment_id)
