import asyncio
import json
import logging
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from functools import partial

from pathlib import Path
from typing import Annotated, Literal

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import ValidationError

from app import repository as repo
from app.config import Settings, get_settings
from app.db import create_pool, keep_warm, run_migrations, warm_pool
from app.intake import IntakeBatcher
from app.processor import simulate_external_call
from app.schemas import Health, Stats, TransactionOut, TransactionSummary, TransactionWebhook, WebhookAck, iso_utc
from app.security import verify_signature
from app.worker import Worker

log = logging.getLogger("app")

SIGNATURE_HEADER = "X-Razorpay-Signature"
STATIC_DIR = Path(__file__).resolve().parent / "static"
DASHBOARD_HTML = STATIC_DIR / "dashboard.html"
HOW_IT_WORKS_HTML = STATIC_DIR / "how-it-works.html"


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
        # Open (and prepare) the webhook path's connections before taking traffic.
        warm = settings.db_api_pool_size
        try:
            await asyncio.wait_for(warm_pool(app.state.api_pool, warm, repo.prime_connection), timeout=15)
            log.info("DB pool warmed with %d connections", warm)
        except Exception as e:
            log.warning("could not warm DB pool at startup: %s: %s", type(e).__name__, e)
        keepalive = asyncio.create_task(
            keep_warm(app.state.api_pool, warm, settings.db_keepalive_seconds, repo.prime_connection)
        )
        app.state.intake = IntakeBatcher(app.state.api_pool, settings.intake_flushers, settings.intake_max_batch)
        await app.state.intake.start()
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
            keepalive.cancel()
            await app.state.intake.stop()
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
                "content": {
                    "application/json": {
                        "schema": TransactionWebhook.model_json_schema(),
                        "example": {
                            "transaction_id": "txn_abc123def456",
                            "source_account": "acc_user_789",
                            "destination_account": "acc_merchant_456",
                            "amount": 1500,
                            "currency": "INR",
                        },
                    }
                },
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
                request.app.state.intake.submit(txn),
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

    @app.get("/v1/transactions", response_model=list[TransactionSummary], tags=["dashboard"])
    async def list_transactions(
        request: Request,
        limit: Annotated[int, Query(ge=1, le=200)] = 50,
        status: Literal["PROCESSING", "PROCESSED", "FAILED"] | None = None,
    ) -> list[TransactionSummary]:
        """Most recent transactions, newest first (used by the dashboard)."""
        try:
            rows = await repo.list_recent(request.app.state.api_pool, limit, status, settings.lease_seconds)
        except Exception as e:
            log.error("listing failed: %s", e)
            raise HTTPException(503, "temporarily unavailable, retry") from None
        return [TransactionSummary(**dict(r)) for r in rows]

    @app.get("/v1/stats", response_model=Stats, tags=["dashboard"])
    async def stats(request: Request) -> Stats:
        """Counts per status and worker state (used by the dashboard)."""
        try:
            rows = await repo.stats(request.app.state.api_pool)
        except Exception as e:
            log.error("stats failed: %s", e)
            raise HTTPException(503, "temporarily unavailable, retry") from None
        counts = {s: 0 for s in ("PROCESSING", "PROCESSED", "FAILED")}
        oldest = None
        for r in rows:
            counts[r["status"]] = r["n"]
            if r["status"] == "PROCESSING":
                oldest = r["oldest"]
        server_now = rows[0]["server_time"] if rows else datetime.now(timezone.utc)
        return Stats(
            server_time=server_now.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
            counts=counts,
            oldest_pending_at=iso_utc(oldest),
            processing_delay_seconds=settings.processing_delay_seconds,
            worker_concurrency=settings.worker_concurrency,
            in_flight=request.app.state.worker.in_flight,
        )

    @app.get("/dashboard", response_class=HTMLResponse, include_in_schema=False)
    async def dashboard() -> HTMLResponse:
        return HTMLResponse(DASHBOARD_HTML.read_text(encoding="utf-8"))

    @app.get("/how-it-works", response_class=HTMLResponse, include_in_schema=False)
    async def how_it_works() -> HTMLResponse:
        return HTMLResponse(HOW_IT_WORKS_HTML.read_text(encoding="utf-8"))

    return app


app = create_app()
