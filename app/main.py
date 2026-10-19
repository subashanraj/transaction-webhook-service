import asyncio
import json
import logging
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from functools import partial

from fastapi import FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import ValidationError

from app import repository as repo
from app.config import Settings, get_settings
from app.db import create_pool, run_migrations
from app.processor import simulate_external_call
from app.schemas import Health, TransactionOut, TransactionWebhook, WebhookAck, iso_utc
from app.security import verify_signature
from app.worker import Worker

log = logging.getLogger("app")

SIGNATURE_HEADER = "X-Razorpay-Signature"


def _reject_constant(name: str):
    raise ValueError(f"{name} is not valid JSON")


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or get_settings()
    logging.basicConfig(
        level=settings.log_level.upper(),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        # Separate pools so a busy worker can never starve the webhook endpoint.
        app.state.api_pool = await create_pool(settings, settings.db_api_pool_size)
        app.state.worker_pool = await create_pool(settings, settings.db_worker_pool_size)
        if settings.auto_migrate:
            try:
                await run_migrations(app.state.api_pool)
            except Exception:
                log.exception("migration failed at startup (is the database reachable?)")
        processor = partial(
            simulate_external_call,
            delay_seconds=settings.processing_delay_seconds,
            failure_rate=settings.simulated_failure_rate,
        )
        app.state.worker = Worker(app.state.worker_pool, settings, processor)
        await app.state.worker.start()
        try:
            yield
        finally:
            await app.state.worker.stop()
            await app.state.api_pool.close()
            await app.state.worker_pool.close()

    app = FastAPI(
        title="Transaction Webhook Service",
        version="1.0.0",
        description="Acknowledges payment webhooks in < 500 ms and processes them reliably in the background.",
        lifespan=lifespan,
    )
    app.state.settings = settings

    @app.middleware("http")
    async def process_time_header(request: Request, call_next):
        # Server-side latency, so load tests can tell server time from client/network time.
        t0 = time.perf_counter()
        response = await call_next(request)
        response.headers["X-Process-Time-Ms"] = f"{(time.perf_counter() - t0) * 1000:.1f}"
        return response

    @app.get("/", response_model=Health, tags=["health"])
    async def health() -> Health:
        # Liveness only: no DB call, so it stays fast and doesn't flap on DB blips.
        return Health(status="HEALTHY", current_time=iso_utc(datetime.now(timezone.utc)))

    @app.get("/health/ready", response_model=Health, tags=["health"], responses={503: {"model": Health}})
    async def ready(request: Request):
        now = iso_utc(datetime.now(timezone.utc))
        try:
            async with asyncio.timeout(2):
                async with request.app.state.api_pool.acquire() as conn:
                    await conn.fetchval("SELECT 1")
        except Exception:
            return JSONResponse(status_code=503, content={"status": "UNHEALTHY", "current_time": now})
        return Health(status="HEALTHY", current_time=now)

    @app.post(
        "/v1/webhooks/transactions",
        status_code=202,
        response_model=WebhookAck,
        tags=["webhooks"],
        openapi_extra={
            "requestBody": {
                "required": True,
                "content": {"application/json": {"schema": TransactionWebhook.model_json_schema()}},
            }
        },
        responses={
            401: {"description": "Invalid signature (only when WEBHOOK_SECRET is set)"},
            413: {"description": "Payload too large"},
            422: {"description": "Invalid payload"},
            503: {"description": "Could not persist; the sender should retry"},
        },
    )
    async def receive_webhook(request: Request) -> WebhookAck:
        declared = request.headers.get("content-length")
        if declared and declared.isdigit() and int(declared) > settings.max_body_bytes:
            raise HTTPException(413, "payload too large")
        raw = await request.body()
        if len(raw) > settings.max_body_bytes:
            raise HTTPException(413, "payload too large")

        if settings.webhook_secret and not verify_signature(
            settings.webhook_secret, raw, request.headers.get(SIGNATURE_HEADER)
        ):
            raise HTTPException(401, "invalid signature")

        content_type = request.headers.get("content-type", "")
        if not content_type.lower().startswith("application/json"):
            raise HTTPException(422, "Content-Type must be application/json")
        try:
            data = json.loads(raw, parse_constant=_reject_constant)
        except (ValueError, UnicodeDecodeError) as e:
            raise HTTPException(422, f"malformed JSON: {e}") from None
        if not isinstance(data, dict):
            raise HTTPException(422, "body must be a JSON object")
        try:
            txn = TransactionWebhook.model_validate(data)
        except ValidationError as e:
            raise RequestValidationError(e.errors(include_url=False, include_context=False, include_input=False))

        # The one thing that must happen before we say "accepted": the row is committed.
        # If that can't be done quickly, answer 503 so the processor retries; never 202.
        try:
            result = await asyncio.wait_for(
                repo.insert_or_touch(request.app.state.api_pool, txn),
                timeout=settings.db_write_timeout_ms / 1000,
            )
        except Exception as e:
            log.error("could not persist txn=%s: %s: %s", txn.transaction_id, type(e).__name__, e)
            raise HTTPException(503, "temporarily unavailable, retry") from None

        if result.inserted:
            request.app.state.worker.notify()
            log.info("accepted txn=%s", txn.transaction_id)
        else:
            if result.mismatch:
                log.warning("duplicate txn=%s with DIFFERENT payload; original kept", txn.transaction_id)
            else:
                log.info("duplicate txn=%s status=%s", txn.transaction_id, result.status)
        return WebhookAck(transaction_id=txn.transaction_id, duplicate=not result.inserted)

    @app.get(
        "/v1/transactions/{transaction_id}",
        response_model=list[TransactionOut],
        tags=["transactions"],
        responses={404: {"description": "Transaction not found"}},
    )
    async def get_transaction(transaction_id: str, request: Request) -> list[TransactionOut]:
        try:
            row = await repo.get_transaction(request.app.state.api_pool, transaction_id)
        except Exception as e:
            log.error("lookup failed for txn=%s: %s", transaction_id, e)
            raise HTTPException(503, "temporarily unavailable, retry") from None
        if row is None:
            raise HTTPException(404, "transaction not found")
        return [TransactionOut(**dict(row))]

    return app


app = create_app()
