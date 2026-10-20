from datetime import datetime, timezone
from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, Field, field_serializer


def to_iso_z(dt: datetime | None) -> str | None:
    """Format as ISO-8601 UTC with a Z suffix, e.g. 2024-01-15T10:30:00Z."""
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class TransactionWebhook(BaseModel):
    transaction_id: str = Field(min_length=1, max_length=128)
    source_account: str = Field(min_length=1, max_length=128)
    destination_account: str = Field(min_length=1, max_length=128)
    amount: Decimal = Field(gt=0, max_digits=18, decimal_places=2)
    currency: str = Field(pattern=r"^[A-Z]{3}$")


class WebhookAck(BaseModel):
    message: str
    transaction_id: str
    duplicate: bool


class TransactionOut(BaseModel):
    transaction_id: str
    source_account: str
    destination_account: str
    amount: float
    currency: str
    status: Literal["PROCESSING", "PROCESSED"]
    created_at: datetime
    processed_at: datetime | None

    @field_serializer("created_at", "processed_at")
    def _ser_dt(self, dt: datetime | None) -> str | None:
        return to_iso_z(dt)


class HealthOut(BaseModel):
    status: str
    current_time: str
