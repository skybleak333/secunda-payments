"""Stop ONLY this Compose project's broker and verify durable outbox recovery."""

import asyncio
import json
from pathlib import Path
from uuid import uuid4

import httpx
from smoke import API, HEADERS, RECEIVER, wait_for_result

PROJECT = Path(__file__).resolve().parents[1]


async def compose(*args):
    process = await asyncio.create_subprocess_exec(
        "docker",
        "compose",
        "--project-directory",
        str(PROJECT),
        *args,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL,
    )
    assert await process.wait() == 0, f"docker compose {args} failed"


async def main():
    async with httpx.AsyncClient() as http:
        await http.post(f"{RECEIVER}/mode/success")
        await compose("stop", "rabbitmq")
        try:
            response = await http.post(
                f"{API}/api/v1/payments",
                headers=HEADERS | {"Idempotency-Key": f"outage-{uuid4()}"},
                json={
                    "amount": "12.00",
                    "currency": "USD",
                    "webhook_url": "http://webhook:8080/callback",
                },
            )
            assert response.status_code == 202, response.text
            payment_id = response.json()["payment_id"]
            pending = await http.get(f"{API}/api/v1/payments/{payment_id}", headers=HEADERS)
            assert pending.json()["status"] == "pending"
            await asyncio.sleep(6)  # exercise a failed publish and its durable backoff
            pending = await http.get(f"{API}/api/v1/payments/{payment_id}", headers=HEADERS)
            assert pending.json()["status"] == "pending"
        finally:
            await compose("start", "rabbitmq")
        result = await wait_for_result(http, payment_id, delivered=True)
        print(
            json.dumps(
                {
                    "result": "passed",
                    "payment_id": payment_id,
                    "check": "API accepted payment while broker was down; delivered after recovery",
                    "status": result["status"],
                },
                indent=2,
            )
        )


if __name__ == "__main__":
    asyncio.run(main())
