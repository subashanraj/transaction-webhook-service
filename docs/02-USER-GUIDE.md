# Transaction Webhook Service — User Guide

This guide covers running and deploying the service, and testing **every scenario**: the PDF's success criteria, positive cases, and edge and failure cases. Each scenario lists the exact request, the expected response, and what to check.

Technical background is in [01-TECHNICAL-DESIGN.md](01-TECHNICAL-DESIGN.md).

---

## 1. Quick reference

| Endpoint | Purpose | Success code |
|---|---|---|
| `GET /` | Health check | 200 |
| `POST /v1/webhooks/transactions` | Receive a transaction webhook | **202** |
| `GET /v1/transactions/{transaction_id}` | Check a transaction's status (testing) | 200 / 404 |
| `GET /dashboard` | Live dashboard in the browser (extra) | 200 |
| `GET /how-it-works` | Animated walkthrough of every scenario (extra) | 200 |
| `GET /v1/transactions?limit=50&status=PROCESSING` | Recent transactions, newest first (extra) | 200 / 422 |
| `GET /v1/stats` | Counts per status, worker load (extra) | 200 |

Transaction statuses:

| Status | Meaning |
|---|---|
| `PROCESSING` | Received and stored; waiting for or inside the 30 s processing. `processed_at` is `null`. |
| `PROCESSED` | Done. `processed_at` is set (≈ `created_at` + 30 s). |
| `FAILED` | Processing failed `MAX_ATTEMPTS` times (only seen with fault injection). |

---

## 2. Running the service

### 2.1 Prerequisites

* Python 3.11+
* Docker Desktop (for local Postgres), **or** a Supabase project
* `curl` (Git Bash on Windows includes it)

### 2.2 Configure

```bash
cp .env.example .env
```

Edit `.env`:

```ini
DATABASE_URL=postgresql://postgres:<password>@<host>:5432/postgres
PROCESSING_DELAY_SECONDS=30
WORKER_CONCURRENCY=100
LEASE_SECONDS=90
MAX_ATTEMPTS=5
WEBHOOK_SECRET=
SIMULATED_FAILURE_RATE=0
```

### 2.3 Create the table

Run `migrations/001_init.sql` once, in the Supabase SQL editor or with:

```bash
psql "$DATABASE_URL" -f migrations/001_init.sql
```

### 2.4 Start locally

Option A, everything in Docker (app + local Postgres):

```bash
docker compose up --build
```

Option B, Python venv against Supabase:

```bash
python -m venv .venv
source .venv/Scripts/activate
pip install -r requirements.txt
uvicorn app.main:app --reload --port 8000
```

The service is now at `http://localhost:8000`. Interactive API docs (Swagger) are at `http://localhost:8000/docs`.

### 2.5 Deploy to the cloud (Render example)

1. Push the repo to GitHub (public).
2. Render → **New → Web Service** → select the repo → Runtime **Docker**.
3. Instance type: **always-on** (Starter or above). Free instances sleep, and the first request after sleep takes 30–60 s, which breaks the 500 ms rule.
4. Add the env vars from §2.2. Use a Supabase region close to the Render region.
5. Deploy, then run the smoke test in §2.6 against the public URL.

### 2.6 One-command smoke test

```bash
./scripts/smoke_test.sh https://your-service.onrender.com
```

It runs S0, S1, S2, S3 and S6 and prints PASS/FAIL per check.

### 2.7 Live dashboard

Open **`/dashboard`** (e.g. `http://127.0.0.1:8001/dashboard`). It's a single page with no build step, refreshed every second.

* **Tiles:** Queued (waiting for a worker slot), Processing now, Processed, Failed, and the age of the oldest pending transaction.
* **Send a webhook:** the form is pre-filled with a random `transaction_id`, and **New** generates another. Every response is logged with its status code and latency.
* **One-click scenarios:**

  | Button | Shows | Expected |
  |---|---|---|
  | Send again | Idempotency | 202, `duplicate (already stored)` |
  | 10 duplicates | Concurrent duplicates | `10× 202 · 0 new, 10 duplicate`, and **Deliveries** goes up by 10 |
  | Burst of 20 | Performance | `20× 202`, slowest well under 500 ms |
  | Same id, new amount | Conflicting payload | 202, the row keeps the original amount and gets a **payload mismatch** tag |
  | Invalid payload | Validation | 422 `amount: … must be greater than 0` |
  | Query unknown id | Not found | 404 |

* **Table:** newest first, with filter tabs. Status is shown as **Queued** (not claimed yet), **Processing** (with a progress bar against the delay), **Processed** or **Failed** (hover for the error). **Deliveries** is how many webhooks arrived for that id; **Took** is `processed_at − created_at`. Click an id to load it into the form.

`GET /` is untouched: it still returns exactly the health JSON the assessment specifies.

### 2.7.1 Animated walkthrough (`/how-it-works`): for presenting

An animated diagram of **Payment processor → Webhook API → Group commit → Postgres → Worker**, where packets move through the system while a caption explains each step and *why*.

| # | Scenario | What it explains |
|---|---|---|
| 1 | One webhook | Persist first, 202 second, process later; lease; guarded final update |
| 2 | Many different webhooks | Group commit (8 webhooks → 1 INSERT), bounded worker slots, queueing |
| 3 | Same webhook, sent again | `ON CONFLICT` → `webhook_count + 1`, `duplicate:true`, no reprocessing even after PROCESSED |
| 4 | Duplicates at the same instant | Why there's no race: in-batch folding plus the primary-key index |
| 5 | Same id, different body | First write wins, `payload_mismatch` flag |
| 6 | Invalid payload | 422, nothing stored, why 4xx vs 5xx matters |
| 7 | Database is down | 503 rather than a false 202; the retry saves it |
| 8 | Server crashes mid-processing | Lease expiry, re-claim by another worker, `attempts = 2` |

**Presenting tips:**
* Tick **step by step**, then press **Next step** (or the **→** / **Space** key) to advance at your own pace. A click during an animation is queued, not lost.
* Each scenario shows **"What this shows"**, a one-sentence **"Say it in the interview"** line, and the **test that proves it**.
* **Run it for real** sends the same requests to the server and lists the real responses, e.g. 10 parallel duplicates → exactly one `duplicate:false` and `webhook_count = 10`. Scenarios 7 and 8 are simulation-only (they'd need the DB or a worker killed) and point to their automated tests instead.
* Deep-link a scenario: `/how-it-works#race`. Rehearse fast with `?speed=4`.

### 2.8 Debugging locally in VS Code

The repo ships `.vscode/launch.json`, `settings.json` and recommended extensions.

1. Open the folder in VS Code and install the recommended extensions when prompted (Python, Python Debugger, REST Client).
2. Make sure the interpreter is `.venv` (bottom-right status bar, or *Python: Select Interpreter*), then run `pip install -r requirements-dev.txt`.
3. Open **Run and Debug** (`Ctrl+Shift+D`), pick a configuration, and press **F5**.

| Configuration | What it runs | Use it for |
|---|---|---|
| **API: Supabase (.env)** | The real service on `http://127.0.0.1:8001`, DB from `.env`, 30 s processing | Normal debugging against the real DB |
| **API: Supabase, fast processing (5s)** | Same, with a 5 s delay | Stepping through the worker without waiting 30 s |
| **API: local Postgres (port 5433)** | Local DB `webhooks`, 5 s delay | Debugging without touching Supabase |
| **API: retries demo (always fails, local Postgres)** | `SIMULATED_FAILURE_RATE=1`, 3 attempts | Watching retry → backoff → `FAILED` |
| **Tests: current file** | `pytest` on the open test file, stops at first failure | Debugging one test (breakpoints in tests and app code) |
| **Tests: all** | The whole suite | |
| **Script: load test against :8001** | 200 webhooks against the debug server | Generating traffic while you debug |

Then send requests from **`requests.http`** (click *Send Request* above each block), or with curl using `BASE=http://127.0.0.1:8001`. The **Testing** panel (flask icon) also lists every test with run/debug buttons.

**Good breakpoints to start with:**

| File / function | What you'll see |
|---|---|
| `app/main.py` → `receive_webhook` | Raw body, validation, the 202 / 503 decision |
| `app/intake.py` → `_flush_loop` | How concurrent webhooks are grouped into one INSERT |
| `app/repository.py` → `insert_batch` | Deduplication within a batch, and `inserted` vs duplicate |
| `app/worker.py` → `_run` / `_process` | Claiming jobs, the delay, finalize / retry |
| `app/processor.py` → `simulate_external_call` | The simulated 30 s external call |

**Things to know while debugging:**

* **A breakpoint pauses the whole process,** including the event loop. Other requests and the worker freeze too, and a webhook you're paused on will answer **503** when you continue, because it overran the 400 ms budget. That's expected: the "processor" would retry, and your next send is a duplicate.
* The debug configs set `LEASE_SECONDS=600` so a long pause doesn't let a job's lease expire mid-step.
* Jobs run in arrival order. **If the database holds a backlog** (e.g. after a load test), a new webhook waits until earlier ones finish. Check with `SELECT count(*) FROM transactions WHERE status='PROCESSING';`.
* Port **8001** is used because 8000 is often taken by another local app. Use `127.0.0.1`, not `localhost`, on Windows (saves ~200 ms per request).
* Only one process should listen on 8001. Stop the debugger (`Shift+F5`) before starting another configuration.

Set the base URL once for all scenarios below:

```bash
export BASE=http://localhost:8000
```

---

## 3. Scenario checklist

| ID | Scenario | Type | Expected |
|---|---|---|---|
| S0 | Health check | ✅ Positive | 200 `HEALTHY` |
| S1 | Single transaction end-to-end | ✅ Positive (PDF #1) | 202 → `PROCESSING` → `PROCESSED` after ~30 s |
| S2 | Same webhook sent several times in sequence | ✅ Idempotency (PDF #2) | All 202, one record, processed once |
| S3 | Same webhook sent concurrently | ✅ Idempotency (PDF #2) | All 202, one record |
| S4 | Duplicate after already `PROCESSED` | ✅ Idempotency | 202, `processed_at` unchanged |
| S5 | Duplicate after `FAILED` | ✅ Idempotency | 202, stays `FAILED` |
| S6 | Burst of many different transactions | ✅ Performance (PDF #3) | Every response < 500 ms, all processed |
| S7 | Decimal amount / other currency | ✅ Positive | Stored exactly (`150.50 USD`) |
| S8 | Missing required field | ❌ Validation | 422 |
| S9 | Wrong field type | ❌ Validation | 422 |
| S10 | Zero or negative amount | ❌ Validation | 422 |
| S11 | More than 2 decimal places | ❌ Validation | 422 |
| S12 | Invalid currency / lowercase currency | ❌/✅ Validation | 422 / normalised to upper |
| S13 | Bad `transaction_id` (empty, spaces, too long) | ❌ Validation | 422 |
| S14 | Source equals destination | ❌ Business rule | 422 |
| S15 | Malformed JSON | ❌ Validation | 422 |
| S16 | Empty body | ❌ Validation | 422 |
| S17 | Extra unknown fields | ✅ Forward compatibility | 202, extras ignored |
| S18 | Oversized payload | ❌ Protection | 413 |
| S19 | Same id, different payload | ⚠️ Conflict | 202, original kept, flagged |
| S20 | Query an unknown transaction | ❌ Not found | 404 |
| S21 | Query right after posting | ✅ Positive | `PROCESSING`, `processed_at: null` |
| S22 | Wrong HTTP method | ❌ Routing | 405 |
| S23 | Unknown URL | ❌ Routing | 404 |
| S24 | Webhook signature valid / invalid | 🔐 Security (optional) | 202 / 401 |
| S25 | Database unreachable | 💥 Reliability (PDF #4) | 503, nothing falsely acknowledged |
| S26 | Server restart during processing | 💥 Reliability (PDF #4) | Recovered and `PROCESSED`, once |
| S27 | Processing keeps failing | 💥 Reliability | Retries, then `FAILED` |
| S28 | Webhook speed while 1000 are processing | ⚡ Performance (PDF #3) | p99 < 500 ms |
| S29 | Graceful shutdown | 💥 Reliability | In-flight leases released, picked up again |
| S30 | Extremely large amount | ❌ Validation | 422 |
| S31 | Wrong Content-Type | ❌ Validation | 422 |
| S32 | Non-ASCII characters in id | ❌ Validation | 422 |
| S33 | First request after idle (cold start) | ⚡ Deployment | Fast on an always-on instance |
| S34 | Multiple instances running | ✅ Scale | No double processing |
| S35 | Time format | ✅ Contract | UTC ISO-8601 with `Z` |

---

## 4. Positive scenarios

### S0 — Health check

```bash
curl -i $BASE/
```

Expected:

```http
HTTP/1.1 200 OK
{"status":"HEALTHY","current_time":"2026-10-19T10:30:00Z"}
```

Check: `current_time` is the current UTC time and ends in `Z`.

---

### S1 — Single transaction end-to-end (PDF success criterion 1)

**Step 1, send the webhook and time it:**

```bash
curl -i -w "\ntime_total: %{time_total}s\n" -X POST $BASE/v1/webhooks/transactions \
  -H "Content-Type: application/json" \
  -d '{"transaction_id":"txn_s1_001","source_account":"acc_user_789","destination_account":"acc_merchant_456","amount":1500,"currency":"INR"}'
```

Expected:

```http
HTTP/1.1 202 Accepted
{"message":"accepted","transaction_id":"txn_s1_001","duplicate":false}
time_total: 0.05s      ← must be < 0.5s
```

**Step 2, query right away (also covers S21):**

```bash
curl -s $BASE/v1/transactions/txn_s1_001
```

```json
[{"transaction_id":"txn_s1_001","source_account":"acc_user_789","destination_account":"acc_merchant_456",
  "amount":1500,"currency":"INR","status":"PROCESSING","created_at":"2026-10-19T10:30:00Z","processed_at":null}]
```

**Step 3, wait about 31 seconds and query again:**

```bash
sleep 31
curl -s $BASE/v1/transactions/txn_s1_001
```

```json
[{ ..., "status":"PROCESSED","created_at":"2026-10-19T10:30:00Z","processed_at":"2026-10-19T10:30:30Z"}]
```

Pass if `processed_at − created_at` is about 30 s (30–32 s allowed for the polling interval).

---

### S2 — Same webhook sent several times (PDF success criterion 2)

```bash
for i in 1 2 3 4 5; do
  curl -s -o /dev/null -w "%{http_code} " -X POST $BASE/v1/webhooks/transactions \
    -H "Content-Type: application/json" \
    -d '{"transaction_id":"txn_s2_dup","source_account":"acc_a","destination_account":"acc_b","amount":100,"currency":"INR"}'
done; echo
```

Expected: `202 202 202 202 202`. The first body has `"duplicate":false` and the rest `"duplicate":true`.

Verify:

```bash
curl -s $BASE/v1/transactions/txn_s2_dup          # exactly ONE element in the array
```

In the DB:

```sql
SELECT count(*), max(webhook_count), max(attempts) FROM transactions WHERE transaction_id='txn_s2_dup';
-- 1 | 5 | 1      → one row, 5 deliveries seen, processed in one attempt
```

---

### S3 — Concurrent duplicates (race condition)

Send 20 identical webhooks at the same moment:

```bash
for i in $(seq 1 20); do
  curl -s -o /dev/null -w "%{http_code}\n" -X POST $BASE/v1/webhooks/transactions \
    -H "Content-Type: application/json" \
    -d '{"transaction_id":"txn_s3_race","source_account":"acc_a","destination_account":"acc_b","amount":42,"currency":"INR"}' &
done; wait
```

Expected: twenty `202` responses, no 500s. The DB has one row with `webhook_count = 20`. After ~30 s it is `PROCESSED` with `attempts = 1`.

Why it works: `transaction_id` is the primary key, so Postgres lets exactly one insert win and the rest become counter updates.

---

### S4 — Duplicate after the transaction is already PROCESSED

1. Complete S1 so `txn_s1_001` is `PROCESSED`, and note its `processed_at`.
2. Send the same webhook again:

```bash
curl -s -X POST $BASE/v1/webhooks/transactions -H "Content-Type: application/json" \
  -d '{"transaction_id":"txn_s1_001","source_account":"acc_user_789","destination_account":"acc_merchant_456","amount":1500,"currency":"INR"}'
```

Expected: `202 {"duplicate":true}`. Wait 35 s and query: still `PROCESSED`, and **`processed_at` is unchanged**, so no reprocessing happened.

---

### S5 — Duplicate after FAILED

Prerequisite: S27 produced a `FAILED` transaction. Re-send it: you get `202 duplicate:true`, and the status stays `FAILED` because it is not automatically re-queued. A real system would offer a manual replay.

---

### S6 — Burst of different transactions (PDF success criterion 3)

```bash
for i in $(seq 1 100); do
  curl -s -o /dev/null -w "%{http_code} %{time_total}\n" -X POST $BASE/v1/webhooks/transactions \
    -H "Content-Type: application/json" \
    -d "{\"transaction_id\":\"txn_s6_$i\",\"source_account\":\"acc_a\",\"destination_account\":\"acc_b\",\"amount\":$i,\"currency\":\"INR\"}" &
done | sort -k2 -n | tail -5; wait
```

Expected: every line is `202` and even the slowest `time_total` is < 0.5 s. After ~30–35 s, all 100 are `PROCESSED`:

```sql
SELECT status, count(*) FROM transactions WHERE transaction_id LIKE 'txn_s6_%' GROUP BY status;
-- PROCESSED | 100
```

For proper numbers use the load script (S28).

---

### S7 — Decimal amount and a different currency

```bash
curl -s -X POST $BASE/v1/webhooks/transactions -H "Content-Type: application/json" \
  -d '{"transaction_id":"txn_s7_usd","source_account":"acc_user_789","destination_account":"acc_merchant_456","amount":150.50,"currency":"USD"}'
curl -s $BASE/v1/transactions/txn_s7_usd
```

Expected: 202. The query shows `"amount":150.5,"currency":"USD"`. The DB stores `150.50` exactly (`NUMERIC`, no float rounding).

---

### S17 — Extra unknown fields are ignored

```bash
curl -s -X POST $BASE/v1/webhooks/transactions -H "Content-Type: application/json" \
  -d '{"transaction_id":"txn_s17","source_account":"acc_a","destination_account":"acc_b","amount":10,"currency":"INR","event":"payment.captured","notes":{"k":"v"}}'
```

Expected: `202`. Payment processors add new fields over time, and we must not break when they do.

---

### S21 — Query immediately after posting

Covered in S1 step 2: `status:"PROCESSING"` and `processed_at:null`.

### S34 — Multiple instances (scale-out)

Run two copies against the same DB:

```bash
uvicorn app.main:app --port 8000 &
uvicorn app.main:app --port 8001 &
```

Send 50 webhooks to port 8000 and 50 to 8001, then:

```sql
SELECT max(attempts), count(DISTINCT locked_by) FROM transactions WHERE ...;
```

Expected: every row ends with `attempts = 1`, and work is spread across both workers. `FOR UPDATE SKIP LOCKED` stops two workers from claiming the same row.

### S35 — Time format

All timestamps are UTC, ISO-8601, second precision, with a `Z` suffix (`2026-10-19T10:30:00Z`), matching the PDF.

---

## 5. Validation and negative scenarios

All of these return an error and **store nothing**. To confirm, query the id and expect 404.

### S8 — Missing required field

```bash
curl -s -i -X POST $BASE/v1/webhooks/transactions -H "Content-Type: application/json" \
  -d '{"transaction_id":"txn_s8","source_account":"acc_a","amount":10,"currency":"INR"}'
```

`422`, with detail pointing at `destination_account` / "Field required".

### S9 — Wrong field type

```bash
-d '{"transaction_id":"txn_s9","source_account":"acc_a","destination_account":"acc_b","amount":"abc","currency":"INR"}'
```

`422`, `amount` must be a number. Also try `"transaction_id": 12345`, which is also 422 because the id must be a string.

### S10 — Zero or negative amount

```bash
-d '{"transaction_id":"txn_s10","source_account":"acc_a","destination_account":"acc_b","amount":0,"currency":"INR"}'
-d '{"transaction_id":"txn_s10b","source_account":"acc_a","destination_account":"acc_b","amount":-50,"currency":"INR"}'
```

`422`, amount must be greater than 0.

### S11 — Too many decimal places

```bash
-d '{"transaction_id":"txn_s11","source_account":"acc_a","destination_account":"acc_b","amount":10.999,"currency":"INR"}'
```

`422`, at most 2 decimal places.

### S12 — Currency

| Input | Result |
|---|---|
| `"INR"`, `"USD"` | 202 |
| `"inr"` | 202, stored as `INR` (normalised) |
| `"RUPEE"`, `"IN"`, `"12A"`, `""` | 422 |

### S13 — Bad transaction_id

| Input | Result |
|---|---|
| `""` or `"   "` | 422 (empty after trimming) |
| 256+ characters | 422 |
| `"txn abc"` (space), `"txn/../x"` | 422 (allowed characters: letters, digits, `_ - : .`) |
| `" txn_ok "` | 202, stored trimmed as `txn_ok` |

### S14 — Source equals destination

```bash
-d '{"transaction_id":"txn_s14","source_account":"acc_a","destination_account":"acc_a","amount":10,"currency":"INR"}'
```

`422`, "source and destination must differ".

### S15 — Malformed JSON

```bash
curl -s -i -X POST $BASE/v1/webhooks/transactions -H "Content-Type: application/json" -d '{"transaction_id": "txn_s15",'
```

`422`, JSON decode error.

### S16 — Empty body

```bash
curl -s -i -X POST $BASE/v1/webhooks/transactions -H "Content-Type: application/json"
```

`422`.

### S18 — Oversized payload

```bash
python -c "print('{\"transaction_id\":\"txn_s18\",\"pad\":\"' + 'x'*100000 + '\"}')" > big.json
curl -s -i -X POST $BASE/v1/webhooks/transactions -H "Content-Type: application/json" --data-binary @big.json
```

`413 payload too large` (limit 64 KB).

### S19 — Same transaction_id, different payload

```bash
curl -s -X POST $BASE/v1/webhooks/transactions -H "Content-Type: application/json" \
  -d '{"transaction_id":"txn_s19","source_account":"acc_a","destination_account":"acc_b","amount":100,"currency":"INR"}'
curl -s -X POST $BASE/v1/webhooks/transactions -H "Content-Type: application/json" \
  -d '{"transaction_id":"txn_s19","source_account":"acc_a","destination_account":"acc_b","amount":9999,"currency":"INR"}'
curl -s $BASE/v1/transactions/txn_s19
```

Expected: both `202` (the second has `duplicate:true`). The query still shows **`amount: 100`**, because the first write wins. The DB row has `payload_mismatch = true` and the logs carry a WARNING. The transaction is processed once.

### S20 — Unknown transaction

```bash
curl -s -i $BASE/v1/transactions/txn_does_not_exist
```

`404 {"detail":"transaction not found"}`

### S22 — Wrong HTTP method

```bash
curl -s -i $BASE/v1/webhooks/transactions                  # GET on a POST route
curl -s -i -X DELETE $BASE/v1/transactions/txn_s1_001
```

`405 Method Not Allowed`

### S23 — Unknown URL

```bash
curl -s -i $BASE/v2/whatever
```

`404`

### S30 — Extremely large amount

```bash
-d '{..."amount":1e20,...}'
```

`422`, the amount is above the 10^12 limit.

### S31 — Wrong Content-Type

```bash
curl -s -i -X POST $BASE/v1/webhooks/transactions -H "Content-Type: text/plain" -d 'hello'
```

`422`

### S32 — Non-ASCII id

```bash
-d '{"transaction_id":"txn_₹_01", ...}'
```

`422` (outside the allowed character set).

---

## 6. Security scenario (optional feature)

### S24 — Webhook signature

Enable it with `WEBHOOK_SECRET=mysecret` and restart.

Valid signature:

```bash
BODY='{"transaction_id":"txn_s24","source_account":"acc_a","destination_account":"acc_b","amount":10,"currency":"INR"}'
SIG=$(printf '%s' "$BODY" | openssl dgst -sha256 -hmac "mysecret" | awk '{print $2}')
curl -s -i -X POST $BASE/v1/webhooks/transactions -H "Content-Type: application/json" \
  -H "X-Razorpay-Signature: $SIG" -d "$BODY"
```

`202`

Missing or wrong signature (`-H "X-Razorpay-Signature: deadbeef"`): `401 invalid signature`, and nothing is stored.

When `WEBHOOK_SECRET` is empty (the default), no signature is required.

---

## 7. Reliability scenarios (PDF success criterion 4)

### S25 — Database unreachable

1. Start the app, then stop the DB (`docker compose stop db`), or point `DATABASE_URL` at a wrong host.
2. `GET /` → still `200 HEALTHY` (liveness doesn't depend on the DB). `GET /health/ready` → `503`.
3. POST a webhook → **`503 temporarily unavailable, retry`**, returned within ~0.4 s.
4. Start the DB again and re-send the same webhook → `202 duplicate:false`, then processed normally.

Why: the service never says "accepted" for something it hasn't saved. The 503 makes the payment processor retry, so nothing is lost.

### S26 — Server killed during the 30 s processing

1. Send `txn_s26` and confirm it is `PROCESSING`.
2. Within 30 s, kill the server hard (`Ctrl+C` twice, `docker kill`, or a redeploy).
3. Start it again.
4. After lease expiry (≤ 90 s) plus 30 s of processing, `GET` shows `PROCESSED`, with `attempts = 2` in the DB.

With a **graceful** stop (single `Ctrl+C` / SIGTERM) the lease is released immediately, so processing restarts as soon as the app is back (see S29).

### S27 — Processing keeps failing → retries → FAILED

1. Set `SIMULATED_FAILURE_RATE=1.0`, `PROCESSING_DELAY_SECONDS=1`, `MAX_ATTEMPTS=3` and restart.
2. Send `txn_s27`.
3. Watch the logs: attempt 1 fails, then a retry after ~2 s, then ~4 s.
4. After the 3rd attempt: `GET` → `"status":"FAILED"`. The DB `last_error` holds the reason.

With `SIMULATED_FAILURE_RATE=0.5` most transactions still finish `PROCESSED` after one or two retries. This shows transient errors are absorbed.

### S29 — Graceful shutdown

Send 10 webhooks, then within 30 s stop the app with a single `Ctrl+C` (SIGTERM). The logs show `released 10 leases`. Restart: all 10 are re-claimed straight away and end `PROCESSED`.

---

## 8. Performance scenarios

### S28 — Webhook latency under processing load

```bash
python scripts/load_test.py --base $BASE --count 2000 --concurrency 50 --processes 5
```

This sends 2000 unique webhooks from 5 client processes. One Python client process saturates long before the server does, and its own queueing would show up as false "latency". The script reports both the latency the client sees and the server's own time from the `X-Process-Time-Ms` header. Measured locally, with ~4000 transactions already queued and 100 in flight:

```
run=544f8f92 sent=2000 202=2000 other={}
throughput=779 req/s with 5 client processes x 10
client latency: p50=39ms p95=114ms p99=171ms max=240ms
server time   : p50=32ms p95=105ms p99=164ms max=233ms
SLA (<500ms client p99, all 202): PASS
```

> **Windows tip:** use `http://127.0.0.1:8000`, not `localhost`. Windows tries IPv6 `::1`
> first and only then falls back to IPv4, which adds ~200 ms to every request.

Then run it again **while** the first 1000 are still in their 30 s window. The latency stays the same because the request path never waits for processing.

All 1000 finish within about `ceil(1000 / WORKER_CONCURRENCY) × 30 s` (≈ 5 min at concurrency 100). Raise `WORKER_CONCURRENCY` or add instances to go faster.

### S33 — Cold start

On a free, sleeping instance, the first request after idle can take 30–60 s. That isn't a code problem, but it **fails the 500 ms rule**. Deploy on an always-on instance, then verify:

```bash
curl -s -o /dev/null -w "%{time_total}\n" $BASE/        # after 30+ min idle → still < 0.5s
```

---

## 9. Running the automated tests

```bash
docker compose up -d db                       # test Postgres
pytest -v                                     # TEST_DATABASE_URL; delay shortened to 0.5s in conftest
pytest tests/test_idempotency.py -v           # only the duplicate tests
pytest tests/test_reliability.py -v           # crash recovery, retries, 503
```

| Test file | Scenarios covered |
|---|---|
| `test_health.py` | S0, S35 |
| `test_webhook_validation.py` | S8–S18, S22, S23, S30–S32 |
| `test_idempotency.py` | S2, S3, S4, S5, S19 |
| `test_processing.py` | S1, S7, S21 |
| `test_reliability.py` | S24–S27, S29, S34 |
| `test_performance.py` | S6, S28 |

---

## 10. Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| Webhook takes > 500 ms | DB in a different region, or a cold start | Co-locate DB and app; use an always-on instance |
| `prepared statement "__asyncpg_stmt_1__" already exists` | Supabase transaction pooler (port 6543) | Use port 5432, or `statement_cache_size=0` |
| Stuck in `PROCESSING` for more than 2 minutes | Worker not running, or `LEASE_SECONDS` too high | Check logs for `worker started`; check config |
| Always 503 on POST | DB unreachable or wrong `DATABASE_URL` | `GET /health/ready`, check credentials and SSL |
| 401 on every webhook | `WEBHOOK_SECRET` set but no signature header sent | Unset the secret or sign the request (S24) |
| Startup error `LEASE_SECONDS must exceed delay` | Misconfiguration | Set `LEASE_SECONDS` ≥ delay + 5 |

Useful SQL:

```sql
-- queue depth and oldest pending
SELECT count(*), min(created_at) FROM transactions WHERE status = 'PROCESSING';
-- duplicates seen
SELECT transaction_id, webhook_count FROM transactions WHERE webhook_count > 1;
-- conflicting payloads
SELECT * FROM transactions WHERE payload_mismatch;
-- failures
SELECT transaction_id, attempts, last_error FROM transactions WHERE status = 'FAILED';
-- processing duration
SELECT transaction_id, processed_at - created_at AS took FROM transactions WHERE status='PROCESSED' ORDER BY created_at DESC LIMIT 10;
```
