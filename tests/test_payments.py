import asyncio
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import httpx
import pytest
from sqlalchemy import func, select, text, update

from app.config import Settings
from app.models import Outbox, Payment
from app.outbox import publish_one
from app.processing import Processor
from app.schemas import PaymentJob

BODY = {
    "amount": "100.00",
    "currency": "RUB",
    "description": "Order #42",
    "metadata": {"order_id": "42"},
    "webhook_url": "http://webhook:8080/callback",
}


async def create(ctx, key="order-42", body=None):
    response = await ctx.client.post(
        "/api/v1/payments", headers={"Idempotency-Key": key}, json=body or BODY
    )
    assert response.status_code == 202, response.text
    return UUID(response.json()["payment_id"])


async def get(ctx, payment_id):
    async with ctx.sessions() as session:
        return await session.get(Payment, payment_id)


async def events(ctx):
    async with ctx.sessions() as session:
        return list((await session.scalars(select(Outbox).order_by(Outbox.created_at))).all())


@pytest.mark.parametrize(
    "method,path",
    [
        ("GET", "/health"),
        ("GET", "/openapi.json"),
        ("GET", f"/api/v1/payments/{uuid4()}"),
        ("POST", "/api/v1/payments"),
    ],
)
async def test_all_endpoints_require_api_key(ctx, method, path):
    response = await ctx.client.request(method, path, headers={"X-API-Key": "wrong"})
    assert response.status_code == 401


async def test_create_and_read(ctx):
    payment_id = await create(ctx)
    response = await ctx.client.get(f"/api/v1/payments/{payment_id}")
    assert response.status_code == 200
    assert response.json()["amount"] == "100.00"
    assert response.json()["metadata"] == BODY["metadata"]
    assert response.json()["status"] == "pending"
    assert len(await events(ctx)) == 1
    assert (await ctx.client.get(f"/api/v1/payments/{uuid4()}")).status_code == 404


@pytest.mark.parametrize(
    "changes",
    [
        {"amount": "0"},
        {"amount": "-1"},
        {"amount": "1.001"},
        {"amount": "NaN"},
        {"currency": "BTC"},
        {"webhook_url": "file:///etc/passwd"},
        {"webhook_url": "http://user:password@webhook/callback"},
        {"webhook_url": "http://untrusted/callback"},
    ],
)
async def test_invalid_payload(ctx, changes):
    response = await ctx.client.post(
        "/api/v1/payments", headers={"Idempotency-Key": "invalid"}, json=BODY | changes
    )
    assert response.status_code == 422
    assert not await events(ctx)


async def test_idempotency_header_required(ctx):
    assert (await ctx.client.post("/api/v1/payments", json=BODY)).status_code == 422
    assert (
        await ctx.client.post("/api/v1/payments", json=BODY, headers={"Idempotency-Key": " "})
    ).status_code == 422


async def test_idempotency_normalizes_decimal_and_rejects_changed_body(ctx):
    first = await create(ctx, body=BODY | {"amount": "100.0"})
    assert await create(ctx) == first
    conflict = await ctx.client.post(
        "/api/v1/payments",
        headers={"Idempotency-Key": "order-42"},
        json=BODY | {"metadata": {"order_id": "different"}},
    )
    assert conflict.status_code == 409
    assert len(await events(ctx)) == 1


async def test_concurrent_requests_create_one_payment_and_one_event(ctx):
    ids = await asyncio.gather(*(create(ctx) for _ in range(12)))
    assert len(set(ids)) == 1
    async with ctx.sessions() as session:
        assert await session.scalar(select(func.count()).select_from(Payment)) == 1
    assert len(await events(ctx)) == 1


async def test_outbox_insert_failure_rolls_back_payment(ctx):
    async with ctx.engine.begin() as connection:
        await connection.execute(
            text("""
            CREATE FUNCTION reject_event() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN RAISE EXCEPTION 'injected outbox failure'; END $$
        """)
        )
        await connection.execute(
            text("""
            CREATE TRIGGER reject_event BEFORE INSERT ON outbox
            FOR EACH ROW EXECUTE FUNCTION reject_event()
        """)
        )
    with pytest.raises(Exception, match="injected outbox failure"):
        await create(ctx)
    async with ctx.sessions() as session:
        assert await session.scalar(select(func.count()).select_from(Payment)) == 0
    assert not await events(ctx)


async def test_webhook_retry_does_not_run_gateway_again(ctx):
    payment_id = await create(ctx)
    gateway = AsyncMock(return_value="succeeded")
    calls = []

    def endpoint(request):
        calls.append(request)
        return httpx.Response(503 if len(calls) < 3 else 204)

    async with httpx.AsyncClient(transport=httpx.MockTransport(endpoint)) as http:
        processor = Processor(ctx.sessions, http, Settings(), gateway)
        await processor.handle(PaymentJob(payment_id=payment_id, attempt=1))
        processed_at = (await get(ctx, payment_id)).processed_at
        await processor.handle(PaymentJob(payment_id=payment_id, attempt=1))  # redelivery
        assert len(calls) == 1
        await processor.handle(PaymentJob(payment_id=payment_id, attempt=2))
        await processor.handle(PaymentJob(payment_id=payment_id, attempt=3))
    payment = await get(ctx, payment_id)
    assert payment.webhook_delivered_at is not None
    assert payment.delivery_failed_at is None
    assert payment.processed_at == processed_at
    assert payment.status == "succeeded"
    gateway.assert_awaited_once()
    assert len(calls) == 3
    assert len({r.headers["Idempotency-Key"] for r in calls}) == 1
    retries = [e for e in await events(ctx) if e.payload["attempt"] > 1]
    for event in retries:
        expected = 2 ** (event.payload["attempt"] - 2)
        assert (
            expected - 0.1
            <= (event.available_at - event.created_at).total_seconds()
            <= expected + 1
        )


async def test_failed_webhooks_end_in_dlq_after_three_attempts(ctx):
    payment_id = await create(ctx)
    gateway = AsyncMock(return_value="succeeded")
    calls = []

    def endpoint(request):
        calls.append(request)
        return httpx.Response(503)

    async with httpx.AsyncClient(transport=httpx.MockTransport(endpoint)) as http:
        processor = Processor(ctx.sessions, http, Settings(), gateway)
        for attempt in (1, 2, 3, 3, 1):
            await processor.handle(PaymentJob(payment_id=payment_id, attempt=attempt))
    payment = await get(ctx, payment_id)
    assert payment.status == "succeeded"  # failed notification is not a failed payment
    assert payment.delivery_failed_at is not None
    assert payment.webhook_delivered_at is None
    assert len(calls) == 3
    assert len([e for e in await events(ctx) if e.topic == "payments.dlq"]) == 1
    gateway.assert_awaited_once()


async def test_duplicate_jobs_are_serialized(ctx):
    payment_id = await create(ctx)
    gateway = AsyncMock(return_value="succeeded")
    send = AsyncMock(
        return_value=httpx.Response(204, request=httpx.Request("POST", BODY["webhook_url"]))
    )
    processor = Processor(ctx.sessions, SimpleHttp(send), Settings(), gateway)
    job = PaymentJob(payment_id=payment_id)
    await asyncio.gather(processor.handle(job), processor.handle(job))
    gateway.assert_awaited_once()
    send.assert_awaited_once()


class SimpleHttp:
    def __init__(self, post):
        self.post = post


async def test_business_decline_is_not_retried(ctx):
    payment_id = await create(ctx)
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda r: httpx.Response(204))
    ) as http:
        await Processor(ctx.sessions, http, Settings(), AsyncMock(return_value="failed")).handle(
            PaymentJob(payment_id=payment_id)
        )
    payment = await get(ctx, payment_id)
    assert payment.status == "failed"
    assert payment.webhook_delivered_at is not None
    assert len(await events(ctx)) == 1  # just the original event


async def test_publisher_failure_keeps_outbox_event_for_retry(ctx):
    payment_id = await create(ctx)
    publisher = AsyncMock(side_effect=ConnectionError("broker offline"))
    assert await publish_one(ctx.sessions, publisher)
    event = (await events(ctx))[0]
    assert event.published_at is None
    assert event.publish_attempts == 1
    assert (await get(ctx, payment_id)).status == "pending"
    async with ctx.sessions() as session, session.begin():
        await session.execute(
            update(Outbox).values(available_at=datetime.now(UTC) - timedelta(seconds=1))
        )
    publisher.side_effect = None
    await publish_one(ctx.sessions, publisher)
    assert (await events(ctx))[0].published_at is not None


async def test_parallel_publishers_skip_locked_events(ctx):
    for i in range(5):
        await create(ctx, key=f"order-{i}")
    seen = []

    async def publish(event):
        seen.append(event.id)
        await asyncio.sleep(0.03)

    await asyncio.gather(*(publish_one(ctx.sessions, publish) for _ in range(8)))
    assert len(seen) == len(set(seen)) == 5
    assert all(e.published_at for e in await events(ctx))


async def test_future_outbox_event_is_not_published_early(ctx):
    await create(ctx)
    async with ctx.sessions() as session, session.begin():
        await session.execute(
            update(Outbox).values(available_at=datetime.now(UTC) + timedelta(hours=1))
        )
    publisher = AsyncMock()
    assert not await publish_one(ctx.sessions, publisher)
    publisher.assert_not_awaited()


async def test_gateway_errors_retry_and_finally_fail(ctx):
    payment_id = await create(ctx)
    gateway = AsyncMock(side_effect=RuntimeError("gateway unavailable"))
    async with httpx.AsyncClient() as http:
        processor = Processor(ctx.sessions, http, Settings(), gateway)
        for attempt in (1, 2, 3):
            await processor.handle(PaymentJob(payment_id=payment_id, attempt=attempt))
    payment = await get(ctx, payment_id)
    assert payment.status == "failed"
    assert payment.processed_at is not None
    assert payment.delivery_failed_at is not None
    assert len([e for e in await events(ctx) if e.topic == "payments.dlq"]) == 1
