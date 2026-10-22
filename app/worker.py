import asyncio
import logging
from contextlib import suppress
from typing import Any

import httpx
from faststream import AckPolicy, FastStream
from faststream.rabbit import Channel, ExchangeType, RabbitBroker, RabbitExchange, RabbitQueue
from faststream.rabbit.annotations import RabbitMessage
from pydantic import ValidationError

from app.config import get_settings
from app.db import engine, sessions
from app.outbox import dispatch_forever
from app.processing import Processor
from app.schemas import PaymentJob

settings = get_settings()
logger = logging.getLogger(__name__)
broker = RabbitBroker(
    settings.rabbitmq_url,
    default_channel=Channel(prefetch_count=1, publisher_confirms=True),
)
dlx = RabbitExchange("payments.dlx", type=ExchangeType.DIRECT, durable=True)
dlq = RabbitQueue("payments.dlq", durable=True, routing_key="payments.failed")
queue = RabbitQueue(
    "payments.new",
    durable=True,
    arguments={
        "x-dead-letter-exchange": "payments.dlx",
        "x-dead-letter-routing-key": "payments.failed",
    },
)
http = httpx.AsyncClient(timeout=settings.webhook_timeout, follow_redirects=False, trust_env=False)
processor = Processor(sessions, http, settings)
app = FastStream(broker)
dispatcher: asyncio.Task | None = None


@broker.subscriber(queue, ack_policy=AckPolicy.MANUAL)
async def consume(body: Any, message: RabbitMessage):
    try:
        job = PaymentJob.model_validate(body)
    except ValidationError:
        await message.reject(requeue=False)
        return
    try:
        await processor.handle(job)
    except Exception:
        # Failure to durably save the outcome/retry: leave the original job on RabbitMQ.
        logger.exception("Cannot persist payment job outcome payment_id=%s", job.payment_id)
        await asyncio.sleep(1)
        await message.nack(requeue=True)
    else:
        await message.ack()


async def publish(event):
    # Bound reconnection/channel acquisition too, not only the AMQP confirmation.
    async with asyncio.timeout(5):
        await broker.publish(
            event.payload,
            queue=dlq if event.topic == "payments.dlq" else queue,
            exchange=dlx if event.topic == "payments.dlq" else None,
            persist=True,
            mandatory=True,
            timeout=5,
            message_id=str(event.id),
        )


@app.after_startup
async def start_dispatcher():
    global dispatcher
    await broker.declare_exchange(dlx)
    declared = await broker.declare_queue(dlq)
    await declared.bind(await broker.declare_exchange(dlx), routing_key="payments.failed")
    dispatcher = asyncio.create_task(
        dispatch_forever(sessions, publish, settings.outbox_poll_interval)
    )


@app.on_shutdown
async def close_resources():
    if dispatcher:
        dispatcher.cancel()
        with suppress(asyncio.CancelledError):
            await dispatcher
    await http.aclose()
    await engine.dispose()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    asyncio.run(app.run())
