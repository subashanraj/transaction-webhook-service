import hashlib
import json
import math
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_serializer, field_validator, model_validator

MAX_AMOUNT = Decimal("1000000000000")  # 10^12; keeps 2-dp values exact as JSON numbers

Account = Annotated[str, Field(min_length=1, max_length=255)]


def iso_utc(dt: datetime | None) -> str | None:
    if dt is None:
        return None
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class TransactionWebhook(BaseModel):
    """Incoming webhook body. Unknown fields are ignored for forward compatibility."""

    model_config = ConfigDict(extra="ignore", str_strip_whitespace=True)

    transaction_id: Annotated[str, Field(min_length=1, max_length=255, pattern=r"^[A-Za-z0-9_\-:.]+$")]
    source_account: Account
    destination_account: Account
    amount: Decimal
    currency: Annotated[str, Field(pattern=r"^[A-Z]{3}$")]

    @field_validator("transaction_id", "source_account", "destination_account", mode="before")
    @classmethod
    def _must_be_string(cls, v: Any) -> Any:
        if not isinstance(v, str):
            raise ValueError("must be a string")
        return v

    @field_validator("amount", mode="before")
    @classmethod
    def _parse_amount(cls, v: Any) -> Decimal:
        # Only JSON numbers are accepted ("1500" as a string is rejected).
        if isinstance(v, bool) or not isinstance(v, (int, float)):
            raise ValueError("must be a number")
        if isinstance(v, float) and not math.isfinite(v):
            raise ValueError("must be a finite number")
        try:
            d = Decimal(str(v))  # str() of a float is its shortest repr: 150.5 -> "150.5"
        except InvalidOperation as e:
            raise ValueError("invalid number") from e
        if d <= 0:
            raise ValueError("must be greater than 0")
        if d > MAX_AMOUNT:
            raise ValueError(f"must not exceed {MAX_AMOUNT}")
        if -d.as_tuple().exponent > 2:
            raise ValueError("must have at most 2 decimal places")
        return d

    @field_validator("currency", mode="before")
    @classmethod
    def _upper_currency(cls, v: Any) -> Any:
        if not isinstance(v, str):
            raise ValueError("must be a string")
        return v.strip().upper()

    @model_validator(mode="after")
    def _distinct_accounts(self) -> "TransactionWebhook":
        if self.source_account == self.destination_account:
            raise ValueError("source_account and destination_account must differ")
        return self

    def payload_hash(self) -> str:
        canonical = json.dumps(
            {
                "transaction_id": self.transaction_id,
                "source_account": self.source_account,
                "destination_account": self.destination_account,
                "amount": str(self.amount.quantize(Decimal("0.01"))),
                "currency": self.currency,
            },
            sort_keys=True,
        )
        return hashlib.sha256(canonical.encode()).hexdigest()


class WebhookAck(BaseModel):
    message: Literal["accepted"] = "accepted"
    transaction_id: str
    duplicate: bool


class Health(BaseModel):
    status: Literal["HEALTHY", "UNHEALTHY"]
    current_time: str


class TransactionOut(BaseModel):
    transaction_id: str
    source_account: str
    destination_account: str
    amount: Decimal
    currency: str
    status: Literal["PROCESSING", "PROCESSED", "FAILED"]
    created_at: datetime
    processed_at: datetime | None

    @field_serializer("amount")
    def _amount(self, v: Decimal) -> int | float:
        # Emit a JSON number, not a string: 1500.00 -> 1500, 150.50 -> 150.5
        return int(v) if v == v.to_integral_value() else float(v)

    @field_serializer("created_at", "processed_at")
    def _ts(self, v: datetime | None) -> str | None:
        return iso_utc(v)


class TransactionSummary(TransactionOut):
    """Dashboard view: the public fields plus processing internals."""

    attempts: int
    webhook_count: int
    payload_mismatch: bool
    last_error: str | None
    started_at: datetime | None  # when the current processing attempt began

    @field_serializer("started_at")
    def _started(self, v: datetime | None) -> str | None:
        return v.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z") if v else None


class Stats(BaseModel):
    server_time: str
    counts: dict[str, int]
    oldest_pending_at: str | None
    processing_delay_seconds: float
    worker_concurrency: int
    in_flight: int
