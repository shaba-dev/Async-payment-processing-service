import uuid

from fastapi import APIRouter, Depends, Header, HTTPException, status
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from typing import Annotated

from app.db import get_session
from app.models import OutboxEvent, Payment
from app.schemas import PaymentAccepted, PaymentCreate, PaymentOut
from app.security import verify_api_key

router = APIRouter(prefix="/api/v1/payments", tags=["payments"], dependencies=[Depends(verify_api_key)])

IdempotencyKey = Annotated[str, Header(alias="Idempotency-Key", min_length=1, max_length=255)]


async def _get_by_key(session: AsyncSession, key: str) -> Payment | None:
    return await session.scalar(select(Payment).where(Payment.idempotency_key == key))


def _replay(existing: Payment, body: PaymentCreate) -> Payment:
    """Повтор запроса с тем же ключом: тот же payload -> тот же ответ, иначе 409."""
    same = (
        existing.amount == body.amount
        and existing.currency == body.currency.value
        and existing.description == body.description
        and existing.meta == body.metadata
        and existing.webhook_url == str(body.webhook_url)
    )
    if not same:
        raise HTTPException(status.HTTP_409_CONFLICT, "Idempotency-Key was already used with a different request body")
    return existing


@router.post("", status_code=status.HTTP_202_ACCEPTED, response_model=PaymentAccepted)
async def create_payment(
    body: PaymentCreate,
    idempotency_key: IdempotencyKey,
    session: AsyncSession = Depends(get_session),
) -> Payment:
    existing = await _get_by_key(session, idempotency_key)
    if existing:
        return _replay(existing, body)

    payment = Payment(
        id=uuid.uuid4(),
        amount=body.amount,
        currency=body.currency.value,
        description=body.description,
        meta=body.metadata,
        idempotency_key=idempotency_key,
        webhook_url=str(body.webhook_url),
    )
    try:
        session.add(payment)
        # Платеж и событие outbox пишутся в одной транзакции
        session.add(OutboxEvent(event_type="payment.created", payload={"payment_id": str(payment.id)}))
        await session.commit()
    except IntegrityError:
        # гонка: параллельный запрос с тем же ключом успел раньше
        await session.rollback()
        existing = await _get_by_key(session, idempotency_key)
        if existing is None:
            raise
        return _replay(existing, body)
    return payment


@router.get("/{payment_id}", response_model=PaymentOut)
async def get_payment(payment_id: uuid.UUID, session: AsyncSession = Depends(get_session)) -> Payment:
    payment = await session.get(Payment, payment_id)
    if payment is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Payment not found")
    return payment
