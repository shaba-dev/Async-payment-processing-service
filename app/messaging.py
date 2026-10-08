"""Топология RabbitMQ.

    exchange "payments" (direct)
      ├─ payments.new      ── основная очередь (x-dead-letter -> payments.dlq)
      ├─ payments.retry.1  ── TTL = base*1   ┐ по истечении TTL сообщение
      ├─ payments.retry.2  ── TTL = base*2   ┘ возвращается в payments.new
      └─ payments.dlq      ── Dead Letter Queue (после max_attempts неудач)

Отдельная очередь на каждую попытку нужна, чтобы задержка была экспоненциальной
без head-of-line blocking (TTL в RabbitMQ срабатывает только у головы очереди).
"""
from faststream.rabbit import ExchangeType, RabbitBroker, RabbitExchange, RabbitQueue

from app.config import settings

EXCHANGE = RabbitExchange("payments", type=ExchangeType.DIRECT, durable=True)

RK_NEW = "payments.new"
RK_DLQ = "payments.dlq"


def retry_key(attempt: int) -> str:
    """Ключ очереди задержки после неудачной попытки №attempt."""
    return f"payments.retry.{attempt}"


def retry_delay_ms(attempt: int) -> int:
    return settings.retry_base_delay * 1000 * 2 ** (attempt - 1)


NEW_QUEUE = RabbitQueue(
    RK_NEW,
    durable=True,
    routing_key=RK_NEW,
    arguments={"x-dead-letter-exchange": EXCHANGE.name, "x-dead-letter-routing-key": RK_DLQ},
)

DLQ_QUEUE = RabbitQueue(RK_DLQ, durable=True, routing_key=RK_DLQ)


def _retry_queue(attempt: int) -> RabbitQueue:
    return RabbitQueue(
        retry_key(attempt),
        durable=True,
        routing_key=retry_key(attempt),
        arguments={
            "x-message-ttl": retry_delay_ms(attempt),
            "x-dead-letter-exchange": EXCHANGE.name,
            "x-dead-letter-routing-key": RK_NEW,
        },
    )


def make_broker() -> RabbitBroker:
    return RabbitBroker(settings.rabbitmq_url)


async def declare_topology(broker: RabbitBroker) -> None:
    """Идемпотентно объявляет обменник, очереди и биндинги (вызывается и API, и consumer)."""
    exchange = await broker.declare_exchange(EXCHANGE)
    queues = [
        (NEW_QUEUE, RK_NEW),
        (DLQ_QUEUE, RK_DLQ),
        *((_retry_queue(n), retry_key(n)) for n in range(1, settings.max_attempts)),
    ]
    for queue_def, key in queues:
        queue = await broker.declare_queue(queue_def)
        await queue.bind(exchange, routing_key=key)
