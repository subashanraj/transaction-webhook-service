# Transaction Webhook Service

Receives transaction webhooks from payment processors (Razorpay style). It acknowledges them with **202 in well under 500 ms** and processes each one **exactly once** in the background, with a simulated 30 s external call. It survives crashes, restarts and DB outages without losing transactions.

**Live API:** `https://<your-service>.onrender.com`. Interactive docs are at `/docs`.

| Endpoint | Description |
|---|---|
| `GET /` | Health check → `{"status":"HEALTHY","current_time":"…Z"}` |
| `POST /v1/webhooks/transactions` | Webhook intake → `202 {"message":"accepted","transaction_id":"…","duplicate":false}` |
| `GET /v1/transactions/{transaction_id}` | Status → `[{…, "status":"PROCESSING" \| "PROCESSED", "created_at", "processed_at"}]` |
| `GET /health/ready` | Extra: readiness check including the database |

More detail:
- [docs/01-TECHNICAL-DESIGN.md](docs/01-TECHNICAL-DESIGN.md): architecture, data model, flows, trade-offs
- [docs/02-USER-GUIDE.md](docs/02-USER-GUIDE.md): 36 positive, negative and failure scenarios with curl commands

---

## Quick test against the deployed service

```bash
BASE=https://<your-service>.onrender.com

curl -s $BASE/

curl -s -i -X POST $BASE/v1/webhooks/transactions -H "Content-Type: application/json" \
  -d '{"transaction_id":"txn_abc123def456","source_account":"acc_user_789","destination_account":"acc_merchant_456","amount":1500,"currency":"INR"}'

curl -s $BASE/v1/transactions/txn_abc123def456    # PROCESSING, processed_at: null
sleep 31
curl -s $BASE/v1/transactions/txn_abc123def456    # PROCESSED, processed_at ≈ created_at + 30s
```

Run all the success criteria in one go (single, duplicates, concurrent duplicates, burst, negatives, 30 s check):

```bash
./scripts/smoke_test.sh $BASE
python scripts/load_test.py --base $BASE --count 2000 --concurrency 50
```

Measured locally: **2000 webhooks at 779 req/s, all 202, p99 171 ms**, with ~4000 transactions already queued. A hard kill (`taskkill /F`) with 100 jobs in flight lost nothing: all 100 were re-claimed after lease expiry and finished `PROCESSED`.

---

## Run locally

**With Docker** (app + Postgres):

```bash
docker compose up --build        # http://localhost:8000
```

**Without Docker** (needs a Postgres, local or Supabase):

```bash
python -m venv .venv && source .venv/bin/activate      # Windows: .venv\Scripts\activate
pip install -r requirements-dev.txt
cp .env.example .env                                   # set DATABASE_URL
uvicorn app.main:app --port 8000
```

The table is created on startup (`AUTO_MIGRATE=true`). You can also run `migrations/001_init.sql` yourself.

## Run the tests

The tests are integration tests against a real Postgres, and processing is shortened to 0.5 s.

```bash
createdb webhooks_test                                  # or any empty database
export TEST_DATABASE_URL=postgresql://postgres@localhost:5432/webhooks_test
pytest -v
```

**51 tests** cover validation, idempotency (sequential, concurrent, after processed, after failed, conflicting payload), end-to-end processing, crash recovery via lease expiry, retries → `FAILED`, DB down → 503, graceful shutdown, two instances sharing one DB, HMAC signatures, and latency under load (p99 ≈ 85 ms locally with 300 jobs in flight).

## Deploy (Render + Supabase)

1. Create a Supabase project. Copy the **Session pooler** connection string (Dashboard → Connect). The direct `db.<ref>.supabase.co` host is IPv6-only, which Render can't reach.
2. Push this repo to GitHub. In Render: **New → Blueprint** → select the repo (uses `render.yaml`), or New Web Service → Docker.
3. Set `DATABASE_URL`. Use an **always-on** instance: free instances sleep, and the first request then takes 30–60 s, which breaks the 500 ms requirement.
4. Run `./scripts/smoke_test.sh https://<service>.onrender.com`.

---

## Technical choices

**FastAPI + asyncpg.** Async end-to-end, so the 30 s simulated call (`asyncio.sleep`) costs nothing while it waits, and thousands can be in flight without slowing the webhook endpoint. asyncpg is the fastest Postgres driver for Python.

**Persist first, acknowledge second, process later.** The webhook handler does one thing: a single idempotent `INSERT … ON CONFLICT` in one round-trip, then returns 202. It never awaits processing. If the row can't be committed within 400 ms, it returns **503**, never 202, so the processor retries rather than us losing a transaction we claimed to have.

**Postgres (Supabase) is both the store and the queue.** Inserting the row *is* enqueuing it, so there is no dual write (DB + Redis/SQS) that can half-fail. A background worker in the same process claims rows with `FOR UPDATE SKIP LOCKED` and a **time-limited lease**:
- Many workers or instances can run safely, because each row is claimed by exactly one.
- If a worker dies mid-processing, its lease expires and another worker picks the job up. On graceful shutdown (SIGTERM) leases are released immediately.
- Failures retry with exponential backoff, and after `MAX_ATTEMPTS` the status becomes `FAILED` so a poison message can't loop forever.

I chose this over FastAPI `BackgroundTasks`, which keeps work in memory and loses it on restart, and over Celery/Redis, which adds two more services and the dual-write problem for no benefit at this scale.

**Idempotency.** `transaction_id` is the primary key. Duplicates, even ones arriving at the same moment, become a counter bump (`webhook_count`) and get `202 {"duplicate": true}`. They never reset status, data or `processed_at`. A duplicate id with a *different* body keeps the original and sets `payload_mismatch = true` with a warning log.

**Correct money handling.** `NUMERIC(18,2)` in the DB and `Decimal` in Python. Amounts must be JSON numbers, > 0, with at most 2 decimals.

**Delivery semantics.** Storage and final state are exactly-once. The simulated external call is at-least-once: a crash after the call but before the final `UPDATE` re-runs it. A real downstream call would therefore carry `transaction_id` as its idempotency key.

### Decisions beyond the spec
| Topic | Choice |
|---|---|
| Unknown id on GET | `404` |
| Duplicate id, different payload | `202`, original kept, flagged |
| Invalid payload | `422` (retrying won't help, so 4xx) |
| `source_account == destination_account` | `422` |
| Lowercase currency | normalised to uppercase |
| Extra unknown fields | ignored (forward compatible) |
| Status values | `PROCESSING`, `PROCESSED`, plus `FAILED` after max retries |
| Signature check | HMAC-SHA256 `X-Razorpay-Signature`, enabled only when `WEBHOOK_SECRET` is set |

## Configuration

See [.env.example](.env.example). Key settings: `DATABASE_URL`, `PROCESSING_DELAY_SECONDS` (30), `WORKER_CONCURRENCY` (100), `LEASE_SECONDS` (90, must be ≥ delay + 5), `MAX_ATTEMPTS` (5), `DB_WRITE_TIMEOUT_MS` (400), `WEBHOOK_SECRET`, and `SIMULATED_FAILURE_RATE` (0–1, for demoing retries).

## Project layout

```
app/main.py         routes + lifespan (pools, migrations, worker)
app/worker.py       lease-based background worker
app/repository.py   all SQL
app/schemas.py      request validation and response shapes
app/processor.py    simulated 30 s external call
app/security.py     HMAC signature, log masking
migrations/         schema
tests/              integration tests (real Postgres)
scripts/            smoke_test.sh, load_test.py
docs/               design doc + user guide
```
