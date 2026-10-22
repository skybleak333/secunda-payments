import hashlib
import json
from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import Annotated
from urllib.parse import urlsplit
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, HttpUrl, JsonValue, field_validator


class Currency(StrEnum):
    RUB = "RUB"
    USD = "USD"
    EUR = "EUR"


class PaymentCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    amount: Annotated[Decimal, Field(gt=0, max_digits=18, decimal_places=2)]
    currency: Currency
    description: str = Field(default="", max_length=1000)
    metadata: dict[str, JsonValue] = Field(default_factory=dict)
    webhook_url: HttpUrl = Field(max_length=2048)

    @field_validator("webhook_url")
    @classmethod
    def no_credentials_or_fragment(cls, value: HttpUrl) -> HttpUrl:
        parts = urlsplit(str(value))
        if parts.username or parts.password or parts.fragment:
            raise ValueError("webhook_url must not contain credentials or a fragment")
        return value

    @field_validator("metadata")
    @classmethod
    def bounded_metadata(cls, value: dict) -> dict:
        if len(json.dumps(value, ensure_ascii=False, allow_nan=False).encode()) > 16_384:
            raise ValueError("metadata must fit in 16 KiB")
        return value

    def fingerprint(self) -> str:
        value = self.model_dump(mode="json")
        value["amount"] = format(self.amount.quantize(Decimal("0.01")), ".2f")
        encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)
        return hashlib.sha256(encoded.encode()).hexdigest()


class PaymentAccepted(BaseModel):
    payment_id: UUID
    status: str
    created_at: datetime


class PaymentDetails(BaseModel):
    payment_id: UUID
    amount: Decimal
    currency: Currency
    description: str
    metadata: dict[str, JsonValue]
    status: str
    created_at: datetime
    processed_at: datetime | None
    webhook_delivered_at: datetime | None
    delivery_failed_at: datetime | None


class PaymentJob(BaseModel):
    model_config = ConfigDict(extra="forbid")
    payment_id: UUID
    attempt: int = Field(default=1, ge=1, le=3)
