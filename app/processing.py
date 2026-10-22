import asyncio
import logging
import random
from datetime import UTC, datetime, timedelta
from urllib.parse import urlsplit
from uuid import UUID

import httpx
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert

from app.config import Settings
from app.models import Outbox, Payment
from app.schemas import PaymentJob

logger = logging.getLogger(__name__)
MAX_ATTEMPTS = 3


async def emulate_gateway(settings: Settings) -> str:
    # A declined payment is a business result, not a retryable transport error.
    low, high = sorted((settings.gateway_min_delay, settings.gateway_max_delay))
    await asyncio.sleep(random.uniform(low, high))
    return "succeeded" if random.random() < settings.gateway_success_rate else "failed"


class Processor:
    def __init__(self, sessions, http: httpx.AsyncClient, settings: Settings, gateway=None):
        self.sessions = sessions
        self.http = http
        self.settings = settings
        self.gateway = gateway or (lambda: emulate_gateway(settings))

    async def handle(self, job: PaymentJob):
        try:
            await self.process(job)
        except Exception as error:
            # No URL, credentials, response body or user metadata in error messages.
            logger.warning(
                "Payment job failed payment_id=%s attempt=%s error=%s",
                job.payment_id,
                job.attempt,
                type(error).__name__,
            )
            await self.record_failure(job)

    async def process(self, job: PaymentJob):
        # Row locks make gateway execution and webhook delivery safe against duplicate jobs.
        # For a real gateway, payment.id must ALSO be the provider's idempotency key.
        async with self.sessions() as session, session.begin():
            payment = await session.scalar(
                select(Payment).where(Payment.id == job.payment_id).with_for_update()
            )
            if payment is None:
                raise LookupError(f"Payment {job.payment_id} not found")
            if self.finished(payment, job):
                return
            if payment.status == "pending":
                payment.status = await self.gateway()
                payment.processed_at = datetime.now(UTC)

        # Persist the payment result BEFORE notifying the client. A webhook retry must
        # never charge the customer again or roll back an already completed payment.
        async with self.sessions() as session, session.begin():
            payment = await session.scalar(
                select(Payment).where(Payment.id == job.payment_id).with_for_update()
            )
            if self.finished(payment, job):
                return
            if urlsplit(payment.webhook_url).hostname not in self.settings.webhook_allowed_hosts:
                raise ValueError("Webhook destination no longer allowed")
            event_id = f"payment.{payment.id}.completed"
            response = await self.http.post(
                payment.webhook_url,
                json={
                    "event_id": event_id,
                    "payment_id": str(payment.id),
                    "amount": format(payment.amount, ".2f"),
                    "currency": payment.currency,
                    "status": payment.status,
                    "processed_at": payment.processed_at.isoformat(),
                    "metadata": payment.metadata_,
                },
                headers={"Idempotency-Key": event_id, "X-Webhook-Event-ID": event_id},
            )
            response.raise_for_status()
            if not 200 <= response.status_code < 300:
                raise ValueError("Webhook endpoint did not accept the event")
            payment.webhook_delivered_at = datetime.now(UTC)
            payment.last_attempt = job.attempt

    @staticmethod
    def finished(payment: Payment, job: PaymentJob) -> bool:
        return bool(
            payment.webhook_delivered_at
            or payment.delivery_failed_at
            or payment.last_attempt >= job.attempt
        )

    async def record_failure(self, job: PaymentJob):
        async with self.sessions() as session, session.begin():
            payment = await session.scalar(
                select(Payment).where(Payment.id == job.payment_id).with_for_update()
            )
            if payment is None:
                # An orphan message cannot be retried meaningfully. Deliver it to DLQ.
                await self.enqueue(session, job.payment_id, job.attempt, dead=True)
                return
            if self.finished(payment, job):
                return
            payment.last_attempt = job.attempt
            if job.attempt >= MAX_ATTEMPTS:
                payment.delivery_failed_at = datetime.now(UTC)
                if payment.status == "pending":
                    payment.status = "failed"
                    payment.processed_at = datetime.now(UTC)
                await self.enqueue(session, payment.id, job.attempt, dead=True)
            else:
                delay = self.settings.retry_base_delay * 2 ** (job.attempt - 1)
                await self.enqueue(session, payment.id, job.attempt + 1, delay=delay)

    @staticmethod
    async def enqueue(session, payment_id: UUID, attempt: int, delay=0, dead=False):
        key = f"{payment_id}:dlq" if dead else f"{payment_id}:{attempt}"
        await session.execute(
            insert(Outbox)
            .values(
                event_key=key,
                topic="payments.dlq" if dead else "payments.new",
                payload={"payment_id": str(payment_id), "attempt": attempt},
                available_at=datetime.now(UTC) + timedelta(seconds=delay),
            )
            .on_conflict_do_nothing(index_elements=[Outbox.event_key])
        )
