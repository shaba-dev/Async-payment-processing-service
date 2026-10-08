import asyncio
import logging
import uuid
from contextlib import asynccontextmanager, suppress
from typing import Annotated

from fastapi import APIRouter, Depends, FastAPI, Header, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.db import engine, get_session, session_factory
from app.messaging import declare_topology, make_broker
from app.outbox import run_relay
from app.schemas import PaymentAccepted, PaymentCreate, PaymentOut
from app.security import ApiKeyDep
from app.services import IdempotencyConflict, create_payment, get_payment

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

SessionDep = Annotated[AsyncSession, Depends(get_session)]


@asynccontextmanager
async def lifespan(app: FastAPI):
    broker = make_broker()
    await broker.start()
    await declare_topology(broker)
    relay_task = asyncio.create_task(run_relay(broker, session_factory), name="outbox-relay")
    try:
        yield
    finally:
        relay_task.cancel()
        with suppress(asyncio.CancelledError):
            await relay_task
        await broker.close()
        await engine.dispose()


app = FastAPI(title="Payment Processing Service", version="1.0.0", lifespan=lifespan)

router = APIRouter(prefix="/api/v1", dependencies=[ApiKeyDep], tags=["payments"])


@router.post("/payments", status_code=status.HTTP_202_ACCEPTED, response_model=PaymentAccepted)
async def create(
    body: PaymentCreate,
    session: SessionDep,
    idempotency_key: Annotated[str, Header(alias="Idempotency-Key", min_length=1, max_length=255)],
):
    try:
        return await create_payment(session, body, idempotency_key)
    except IdempotencyConflict:
        raise HTTPException(
            status.HTTP_409_CONFLICT, "Idempotency-Key was already used with a different request body"
        )


@router.get("/payments/{payment_id}", response_model=PaymentOut)
async def read(payment_id: uuid.UUID, session: SessionDep):
    payment = await get_payment(session, payment_id)
    if payment is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Payment not found")
    return payment


app.include_router(router)
