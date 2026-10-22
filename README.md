# Асинхронный процессинг платежей

FastAPI принимает платёж, PostgreSQL хранит его вместе с событием outbox,
FastStream отправляет событие в RabbitMQ. Один consumer эмулирует шлюз и
уведомляет клиента через webhook.

## Запуск

Нужны Docker и Docker Compose v2. Все опубликованные порты привязаны к localhost.
Значения паролей и API-ключа в примере предназначены только для локальной проверки.

```sh
docker compose --profile demo up --build -d
docker compose ps
```

Сервис `migrate` применяет Alembic-миграции до запуска API и consumer.
Профиль `demo` добавляет локальный приёмник webhook. Основной сервис работает и без него:
`docker compose up --build -d`.

| Сервис | Адрес |
| --- | --- |
| API | http://localhost:18880 |
| Приёмник webhook из профиля demo | http://localhost:18881 |
| RabbitMQ Management | http://localhost:15678, payments / payments |
| PostgreSQL | localhost:55437, payments / payments / payments |
| RabbitMQ AMQP | localhost:5678 |

Проверка состояния и схема API также требуют ключ:

```sh
curl -fsS http://localhost:18880/health -H 'X-API-Key: local-development-key'
curl -fsS http://localhost:18880/openapi.json -H 'X-API-Key: local-development-key'
```

## Примеры API

Создать платёж:

```sh
curl -i http://localhost:18880/api/v1/payments \
  -H 'Content-Type: application/json' \
  -H 'X-API-Key: local-development-key' \
  -H 'Idempotency-Key: order-42' \
  -d '{"amount":"1250.50","currency":"RUB","description":"Order #42","metadata":{"order_id":"42"},"webhook_url":"http://webhook:8080/callback"}'
```

Ответ `202 Accepted` содержит `payment_id`, `status`, `created_at`.
Денежные значения передаются строками, чтобы не терять точность decimal.

```sh
curl http://localhost:18880/api/v1/payments/PAYMENT_UUID \
  -H 'X-API-Key: local-development-key'
curl http://localhost:18881
```

Последний запрос показывает уведомления локального приёмника. Callback содержит
`event_id`, `payment_id`, `amount`, `currency`, `status`, `processed_at`, `metadata`.
Заголовки `X-Webhook-Event-ID` и `Idempotency-Key` содержат один стабильный ID события.

Повтор POST с тем же ключом и тем же содержимым возвращает тот же платёж.
Другой body с уже использованным ключом даёт `409`. Отсутствие ключа идемпотентности,
некорректная сумма/валюта/URL дают `422`, неверный API-ключ — `401`, неизвестный UUID — `404`.
Сумма положительная, максимум 16 целых и 2 дробных разряда; валюты RUB, USD, EUR.

## Доставка и повторы

1. В одной транзакции создаются `payments` и `outbox`. Уникальный индекс
   по `idempotency_key` защищает и от конкурентных запросов. SHA-256 от
   нормализованного body обнаруживает конфликт содержимого.
2. Dispatcher берёт готовую запись outbox через `FOR UPDATE SKIP LOCKED`,
   публикует persistent-сообщение с `mandatory=True` и publisher confirms,
   затем отмечает запись опубликованной. Блокировка держится до подтверждения;
   ожидание RabbitMQ ограничено 5 секундами. После сбоя публикация повторяется
   с задержкой до 30 секунд; число попыток публикации не ограничено.
3. Очередь `payments.new` durable, `prefetch_count=1`. Один обработчик выполняет
   шлюз (2–5 секунд, 90% succeeded / 10% failed), сохраняет результат, затем отправляет webhook.
   Отказ шлюза — бизнес-результат: он не запускает повторную оплату.
4. При ошибке обработки/уведомления тот же обработчик транзакционно сохраняет
   следующую попытку в outbox с `available_at`. Затем подтверждает исходное
   сообщение. Три попытки **всего**: первая сразу, ещё две через 1 и 2 секунды.
   Задержка хранится в БД, поэтому переживает перезапуск consumer.
5. После третьей ошибки в outbox записывается событие `payments.dlq`, а у платежа —
   `delivery_failed_at`. Dispatcher доставляет событие в durable DLQ через
   direct exchange `payments.dlx`, routing key `payments.failed`.
   Невалидные входящие сообщения отвергаются и попадают туда же через RabbitMQ DLX.

Если БД недоступна и результат/повтор сохранить нельзя, сообщение возвращается в очередь.
Такие сбои инфраструктуры не расходуют три сохранённые попытки обработки.
Статус платежа и результат доставки webhook независимы: уже успешный платёж
остаётся `succeeded`, даже если его уведомление попало в DLQ.

Гарантия — **at least once**, не exactly once. Сбой между публикацией и commit
может дать дубликат сообщения; row lock, сохранённый номер попытки и финальные
отметки защищают обработку от дублей. Сбой после принятого webhook, но до commit
может повторить HTTP-запрос. Получатель должен дедуплицировать `event_id`.
Для настоящего платёжного шлюза нужен его собственный idempotency key (`payment_id`)
и сверка результата; локальный эмулятор не выполняет реальных списаний.

## Настройки и границы примера

Переменные описаны в `.env.example`; значения для контейнеров заданы в `compose.yaml`.
`API_KEY` необходимо заменить перед использованием за пределами локальной проверки.
`WEBHOOK_ALLOWED_HOSTS` — JSON-массив разрешённых имён узлов. По умолчанию разрешён
только контейнер `webhook`. Для своего получателя задайте его точное имя, например:

```sh
WEBHOOK_ALLOWED_HOSTS='["hooks.example.org"]' docker compose up -d --force-recreate api consumer
```

Это явный allowlist назначения, а не открытый HTTP-прокси. URL с credentials,
fragment и схемой вне HTTP(S) не принимаются. Redirect не выполняется, proxy-переменные
окружения для callback игнорируются. Allowlist должен содержать только доверенные
узлы; для публичной многопользовательской системы дополнительно нужны сетевые
egress-ограничения, TLS, подпись webhook и отдельные ключи клиентов.

Для демонстрации вероятности и времени шлюза доступны `GATEWAY_MIN_DELAY` (2),
`GATEWAY_MAX_DELAY` (5), `GATEWAY_SUCCESS_RATE` (0.9). `RETRY_BASE_DELAY` по умолчанию 1.
Это параметры эмулятора, а не данные из запроса клиента.
Старые опубликованные записи outbox в этом примере не удаляются; для постоянной
эксплуатации нужна политика очистки. Автоматического replay DLQ нет:
после устранения причины оператор должен осознанно подготовить повтор, сохраняя ID события.

## Проверки

Python 3.12; PostgreSQL нужен для тестов, SQLite не используется:

```sh
python3.12 -m venv .venv
.venv/bin/pip install -r requirements-dev.txt
docker compose up -d postgres rabbitmq
.venv/bin/alembic upgrade head
.venv/bin/alembic check
.venv/bin/python -m pytest -q
.venv/bin/ruff check app tests migrations scripts
.venv/bin/ruff format --check app tests migrations scripts
```

Каждый тест использует отдельную временную PostgreSQL-схему. Проверяются конкурентная
идемпотентность, rollback payment/outbox при ошибке, повторы webhook без повторного
шлюза, исчерпание попыток, DLQ, отложенная публикация и параллельные dispatcher.

Полный smoke-тест с реальными RabbitMQ, API, consumer и HTTP callback:

```sh
docker compose --profile demo up --build -d
.venv/bin/python scripts/smoke.py
```

Он также проверяет повторную доставку broker-события и маршрут невалидного сообщения в DLQ.
Приёмник `scripts/webhook.py` — только тестовый инструмент, не часть публичного API.

Проверка восстановления после сбоя брокера:

```sh
.venv/bin/python scripts/recovery.py
```

Скрипт останавливает **только RabbitMQ этого Compose-проекта**, создаёт платёж
через работающий API, проверяет статус `pending`, возвращает брокер и дожидается
обработки с webhook. Брокер запускается обратно и при ошибке проверки.
Результаты выполненных проверок перечислены в [TESTING.md](TESTING.md).

Остановить локальное окружение с сохранением данных:

```sh
docker compose --profile demo down
```

