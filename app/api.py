import hmac
from contextlib import asynccontextmanager
from typing import Annotated
from uuid import UUID, uuid4

from fastapi import Depends, FastAPI, Header, HTTPException
from sqlalchemy import select, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import get_settings
from app.db import engine, get_session
from app.models import Outbox, Payment
from app.schemas import PaymentAccepted, PaymentCreate, PaymentDetails


async def authenticate(x_api_key: Annotated[str | None, Header()] = None):
    expected = get_settings().api_key.get_secret_value()
    if x_api_key is None or not hmac.compare_digest(x_api_key.encode(), expected.encode()):
        raise HTTPException(401, "Invalid API key")


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield
    await engine.dispose()


app = FastAPI(
    title="Payments",
    version="0.1.0",
    dependencies=[Depends(authenticate)],
    lifespan=lifespan,
    docs_url=None,
    redoc_url=None,
    openapi_url=None,
)
Session = Annotated[AsyncSession, Depends(get_session)]


@app.get("/health")
async def health(session: Session):
    await session.execute(text("SELECT 1"))
    return {"status": "ok"}


@app.get("/openapi.json", include_in_schema=False)
async def openapi():
    return app.openapi()


@app.post("/api/v1/payments", status_code=202, response_model=PaymentAccepted)
async def create_payment(
    body: PaymentCreate,
    session: Session,
    idempotency_key: Annotated[str, Header(min_length=1, max_length=128, pattern=r"^[!-~]+$")],
):
    if body.webhook_url.host not in get_settings().webhook_allowed_hosts:
        raise HTTPException(422, "Webhook host is not in WEBHOOK_ALLOWED_HOSTS")
    fingerprint = body.fingerprint()
    payment_id = uuid4()
    async with session.begin():
        created = await session.scalar(
            insert(Payment)
            .values(
                id=payment_id,
                amount=body.amount,
                currency=body.currency.value,
                description=body.description,
                metadata_=body.metadata,
                webhook_url=str(body.webhook_url),
                idempotency_key=idempotency_key,
                request_hash=fingerprint,
            )
            .on_conflict_do_nothing(index_elements=[Payment.idempotency_key])
            .returning(Payment.id)
        )
        if created:
            session.add(
                Outbox(
                    event_key=f"{payment_id}:1",
                    topic="payments.new",
                    payload={"payment_id": str(payment_id), "attempt": 1},
                )
            )
        payment = await session.scalar(
            select(Payment).where(Payment.idempotency_key == idempotency_key)
        )
        if payment.request_hash != fingerprint:
            raise HTTPException(409, "Idempotency-Key was already used with a different request")
        result = PaymentAccepted(
            payment_id=payment.id, status=payment.status, created_at=payment.created_at
        )
    return result


@app.get("/api/v1/payments/{payment_id}", response_model=PaymentDetails)
async def get_payment(payment_id: UUID, session: Session):
    payment = await session.get(Payment, payment_id)
    if payment is None:
        raise HTTPException(404, "Payment not found")
    return PaymentDetails(
        payment_id=payment.id,
        amount=payment.amount,
        currency=payment.currency,
        description=payment.description,
        metadata=payment.metadata_,
        status=payment.status,
        created_at=payment.created_at,
        processed_at=payment.processed_at,
        webhook_delivered_at=payment.webhook_delivered_at,
        delivery_failed_at=payment.delivery_failed_at,
    )
