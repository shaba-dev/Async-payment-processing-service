# Payment Processing Service

Асинхронный микросервис обработки платежей: FastAPI + SQLAlchemy 2.0 (async) + PostgreSQL + RabbitMQ (FastStream) + Alembic + Docker.

## Архитектура

```
Client ──POST /api/v1/payments──▶ API ──┐  одна транзакция
                                        ├─▶ payments (status=pending)
                                        └─▶ outbox   (событие)
                       Outbox relay (в процессе API, FOR UPDATE SKIP LOCKED)
                                        │ publish + confirm
                                        ▼
                    exchange "payments" ──▶ payments.new ──▶ Consumer
                                                              1. эмуляция шлюза (2–5 с, 90% успех)
                                                              2. UPDATE payments.status
                                                              3. POST webhook
                               ┌── ошибка, попытка < 3 ───────┘
                               ▼
                payments.retry.1 (TTL 5 с) / payments.retry.2 (TTL 10 с) ──▶ обратно в payments.new
                               │ ошибка на 3-й попытке
                               ▼
                          payments.dlq
```

- **Outbox**: платёж и событие пишутся атомарно; relay публикует события с publisher confirms и только после подтверждения помечает `published_at`. Гарантия — at-least-once.
- **Идемпотентность API**: уникальный `Idempotency-Key`. Повтор с тем же ключом и телом возвращает тот же платёж (202); с другим телом — `409`. Гонки закрыты unique-констрейнтом.
- **Идемпотентность consumer**: финальный статус выставляется только из `pending` (под `SELECT … FOR UPDATE`), webhook не отправляется повторно после успешной доставки (`webhook_delivered_at`).
- **Retry**: 3 попытки на сообщение, задержки экспоненциальные (`RETRY_BASE_DELAY * 2^(n-1)` = 5 с, 10 с). Для каждой попытки своя очередь с TTL + dead-letter обратно в `payments.new` (так нет head-of-line blocking).
- **DLQ**: после 3-й неудачной попытки сообщение уходит в `payments.dlq` с заголовком `x-last-error`.
- «Ошибка» шлюза (10%) — это нормальный итог платежа со статусом `failed`, а не сбой обработки; webhook при этом тоже отправляется.
- Аутентификация: статический ключ в `X-API-Key` на всех эндпоинтах (`401` при отсутствии/неверном ключе).

## Запуск

```bash
docker compose up --build
```

Поднимутся: `postgres`, `rabbitmq` (UI: http://localhost:15672, guest/guest), `migrate` (alembic upgrade head), `api` (http://localhost:8000, Swagger: `/docs`), `consumer` и демо-приёмник webhook'ов `webhook-receiver` (:9000).
API-ключ по умолчанию `secret-api-key` (переопределяется переменной `API_KEY`).

## Примеры

Создание платежа:

```bash
curl -i -X POST http://localhost:8000/api/v1/payments \
  -H "X-API-Key: secret-api-key" \
  -H "Idempotency-Key: order-1001" \
  -H "Content-Type: application/json" \
  -d '{
        "amount": "199.90",
        "currency": "RUB",
        "description": "Заказ №1001",
        "metadata": {"order_id": 1001},
        "webhook_url": "http://webhook-receiver:9000/hook"
      }'
```

Ответ `202 Accepted`:

```json
{"payment_id": "0b7c…", "status": "pending", "created_at": "2026-10-07T12:00:00Z"}
```

Получение платежа (через 2–5 секунд статус станет `succeeded` или `failed`):

```bash
curl -H "X-API-Key: secret-api-key" http://localhost:8000/api/v1/payments/<payment_id>
```

Результат webhook'а видно в логах: `docker compose logs -f webhook-receiver consumer`.

Идемпотентность — повторите первый запрос с тем же `Idempotency-Key`: вернётся тот же `payment_id`, новое событие не создаётся.

### Проверка retry и DLQ

Укажите `"webhook_url": "http://webhook-receiver:9000/fail"` (всегда 500). В логах consumer будут попытки 1/3 → (5 с) → 2/3 → (10 с) → 3/3, после чего сообщение окажется в очереди `payments.dlq` (RabbitMQ UI → Queues). Платёж при этом остаётся обработанным (статус в БД финальный), не доставлен только webhook.

## Конфигурация (env)

| Переменная | По умолчанию | Описание |
|---|---|---|
| `DATABASE_URL` | `postgresql+asyncpg://…` | строка подключения |
| `RABBITMQ_URL` | `amqp://guest:guest@…` | брокер |
| `API_KEY` | `secret-api-key` | ключ для `X-API-Key` |
| `MAX_ATTEMPTS` | `3` | число попыток обработки |
| `RETRY_BASE_DELAY` | `5` | базовая задержка, сек |
| `WEBHOOK_TIMEOUT` | `10` | таймаут webhook, сек |

## Структура

```
app/
  main.py        FastAPI, роуты, lifespan (запускает outbox relay)
  services.py    создание платежа + outbox в одной транзакции, идемпотентность
  outbox.py      relay outbox → RabbitMQ
  messaging.py   обменник, очереди, retry-очереди, DLQ
  consumer.py    FastStream consumer (шлюз, статус, webhook, retry/DLQ)
  models.py / schemas.py / db.py / security.py / config.py
alembic/         миграции (0001: payments, outbox)
tools/webhook_receiver.py   демо-приёмник webhook'ов
```

## Допущения

- Relay работает внутри процесса API; благодаря `SKIP LOCKED` можно масштабировать API горизонтально.
- Webhook-URL не фильтруется (защита от SSRF — за рамками задания).
- Сумма в JSON-ответах — строка (стандартное поведение Pydantic v2 для `Decimal`).
