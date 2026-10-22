import asyncio
import logging
from datetime import UTC, datetime, timedelta

from sqlalchemy import func, select

from app.models import Outbox

logger = logging.getLogger(__name__)


async def publish_one(sessions, publish) -> bool:
    async with sessions() as session, session.begin():
        event = await session.scalar(
            select(Outbox)
            .where(Outbox.published_at.is_(None), Outbox.available_at <= func.now())
            .order_by(Outbox.available_at, Outbox.created_at)
            .with_for_update(skip_locked=True)
            .limit(1)
        )
        if event is None:
            return False
        try:
            await publish(event)
        except Exception:
            logger.exception("Outbox publication failed event_id=%s", event.id)
            event.publish_attempts += 1
            # Publication retries are unlimited: a broker outage must not lose a payment.
            delay = min(30, 2 ** min(event.publish_attempts - 1, 5))
            event.available_at = datetime.now(UTC) + timedelta(seconds=delay)
        else:
            event.publish_attempts += 1
            event.published_at = datetime.now(UTC)
    return True


async def dispatch_forever(sessions, publish, poll_interval: float):
    while True:
        try:
            found = await publish_one(sessions, publish)
        except Exception:
            logger.exception("Outbox database unavailable")
            found = False
        if not found:
            await asyncio.sleep(poll_interval)
