import uuid

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.messaging import RK_NEW
from app.models import Outbox, Payment, PaymentStatus
from app.schemas import PaymentCreate


class IdempotencyConflict(Exception):
    """Idempotency-Key уже использован с другим телом запроса."""


def _same_request(payment: Payment, data: PaymentCreate) -> bool:
    return (
        payment.amount == data.amount
        and payment.currency == data.currency.value
        and payment.description == data.description
        and payment.meta == data.metadata
        and payment.webhook_url == str(data.webhook_url)
    )


async def _get_by_key(session: AsyncSession, key: str) -> Payment | None:
    return await session.scalar(select(Payment).where(Payment.idempotency_key == key))


async def create_payment(session: AsyncSession, data: PaymentCreate, idempotency_key: str) -> Payment:
    """Создаёт платёж и событие outbox в ОДНОЙ транзакции.

    Повтор с тем же Idempotency-Key возвращает уже созданный платёж.
    """
    existing = await _get_by_key(session, idempotency_key)
    if existing is None:
        payment = Payment(
            id=uuid.uuid4(),
            idempotency_key=idempotency_key,
            amount=data.amount,
            currency=data.currency.value,
            description=data.description,
            meta=data.metadata,
            status=PaymentStatus.PENDING.value,
            webhook_url=str(data.webhook_url),
        )
        session.add(payment)
        session.add(
            Outbox(event_type=RK_NEW, payload={"payment_id": str(payment.id)})
        )
        try:
            await session.commit()
            return payment
        except IntegrityError:
            # гонка: параллельный запрос с тем же ключом успел раньше
            await session.rollback()
            existing = await _get_by_key(session, idempotency_key)
            if existing is None:
                raise

    if not _same_request(existing, data):
        raise IdempotencyConflict(idempotency_key)
    return existing


async def get_payment(session: AsyncSession, payment_id: uuid.UUID) -> Payment | None:
    return await session.get(Payment, payment_id)
