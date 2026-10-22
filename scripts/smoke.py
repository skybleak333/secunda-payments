"""Exercise the real API, PostgreSQL, RabbitMQ consumer and local callback receiver."""

import asyncio
import json
from uuid import uuid4

import aio_pika
import httpx

API = "http://localhost:18880"
RECEIVER = "http://localhost:18881"
HEADERS = {"X-API-Key": "local-development-key"}


async def wait_for_result(http, payment_id, delivered):
    for _ in range(100):
        response = await http.get(f"{API}/api/v1/payments/{payment_id}", headers=HEADERS)
        response.raise_for_status()
        payment = response.json()
        field = "webhook_delivered_at" if delivered else "delivery_failed_at"
        if payment[field]:
            return payment
        await asyncio.sleep(0.25)
    raise AssertionError(f"Payment {payment_id} did not reach expected terminal state")


async def main():
    async with httpx.AsyncClient() as http:
        for _ in range(100):
            try:
                response = await http.get(f"{API}/health", headers=HEADERS)
                response.raise_for_status()
                break
            except httpx.HTTPError:
                await asyncio.sleep(0.25)
        else:
            raise AssertionError("API did not become ready")
        await http.post(f"{RECEIVER}/mode/success")
        key = f"smoke-{uuid4()}"
        body = {
            "amount": "42.50",
            "currency": "EUR",
            "description": "Smoke test",
            "metadata": {"run": key},
            "webhook_url": "http://webhook:8080/callback",
        }
        response = await http.post(
            f"{API}/api/v1/payments", json=body, headers=HEADERS | {"Idempotency-Key": key}
        )
        assert response.status_code == 202, response.text
        payment_id = response.json()["payment_id"]
        repeated = await http.post(
            f"{API}/api/v1/payments", json=body, headers=HEADERS | {"Idempotency-Key": key}
        )
        assert repeated.json()["payment_id"] == payment_id
        payment = await wait_for_result(http, payment_id, delivered=True)
        assert payment["status"] in {"succeeded", "failed"}

        connection = await aio_pika.connect_robust("amqp://payments:payments@localhost:5678/")
        async with connection:
            channel = await connection.channel(publisher_confirms=True)
            # A duplicate broker event must not invoke the webhook again.
            await channel.default_exchange.publish(
                aio_pika.Message(
                    json.dumps({"payment_id": payment_id, "attempt": 1}).encode(),
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                ),
                routing_key="payments.new",
                mandatory=True,
            )
            await asyncio.sleep(1)
            callbacks = (await http.get(RECEIVER)).json()
            assert len([e for e in callbacks if e["body"]["payment_id"] == payment_id]) == 1

            # Real HTTP 503 -> three attempts -> durable DLQ publication.
            await http.post(f"{RECEIVER}/mode/fail")
            response = await http.post(
                f"{API}/api/v1/payments",
                json=body,
                headers=HEADERS | {"Idempotency-Key": f"failed-{uuid4()}"},
            )
            assert response.status_code == 202, response.text
            failed_id = response.json()["payment_id"]
            failed_payment = await wait_for_result(http, failed_id, delivered=False)
            assert failed_payment["status"] in {"succeeded", "failed"}
            callbacks = (await http.get(RECEIVER)).json()
            assert len([e for e in callbacks if e["body"]["payment_id"] == failed_id]) == 3
            dlq = await channel.declare_queue("payments.dlq", passive=True)
            found = False
            for _ in range(30):
                message = await dlq.get(fail=False)
                if message:
                    found = json.loads(message.body).get("payment_id") == failed_id
                    await message.ack()
                    if found:
                        break
                await asyncio.sleep(0.2)
            assert found, "Exhausted payment did not reach RabbitMQ DLQ"

            # Invalid broker input is rejected through RabbitMQ's DLX, not left unacked.
            await channel.default_exchange.publish(
                aio_pika.Message(b'{"payment_id":"invalid"}'), routing_key="payments.new"
            )
            malformed = None
            for _ in range(30):
                malformed = await dlq.get(fail=False)
                if malformed:
                    break
                await asyncio.sleep(0.2)
            assert malformed is not None, "Malformed message did not reach DLQ"
            assert json.loads(malformed.body)["payment_id"] == "invalid"
            await malformed.ack()
        await http.post(f"{RECEIVER}/mode/success")
    print(
        json.dumps(
            {
                "result": "passed",
                "payment_id": payment_id,
                "failed_webhook_id": failed_id,
                "checks": [
                    "202/create/read",
                    "idempotency",
                    "real consumer",
                    "duplicate job",
                    "three webhook attempts",
                    "DLQ",
                    "malformed message DLX",
                ],
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    asyncio.run(main())
