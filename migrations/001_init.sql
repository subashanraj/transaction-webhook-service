-- Transactions table: source of truth AND durable work queue.
-- Safe to run repeatedly.
CREATE TABLE IF NOT EXISTS transactions (
    transaction_id       TEXT PRIMARY KEY,                    -- idempotency key
    source_account       TEXT          NOT NULL,
    destination_account  TEXT          NOT NULL,
    amount               NUMERIC(18,2) NOT NULL CHECK (amount > 0),
    currency             CHAR(3)       NOT NULL,
    status               TEXT          NOT NULL DEFAULT 'PROCESSING'
                         CHECK (status IN ('PROCESSING', 'PROCESSED', 'FAILED')),

    -- timing information
    created_at           TIMESTAMPTZ   NOT NULL DEFAULT now(),  -- first webhook received
    updated_at           TIMESTAMPTZ   NOT NULL DEFAULT now(),
    processed_at         TIMESTAMPTZ,                           -- NULL until PROCESSED

    -- reliability / observability
    attempts             INT           NOT NULL DEFAULT 0,
    locked_by            TEXT,                                  -- worker holding the lease
    locked_until         TIMESTAMPTZ,                           -- lease expiry
    next_attempt_at      TIMESTAMPTZ   NOT NULL DEFAULT now(),  -- retry backoff
    last_error           TEXT,
    webhook_count        INT           NOT NULL DEFAULT 1,      -- deliveries received
    payload_hash         TEXT          NOT NULL,                -- sha256 of normalised payload
    payload_mismatch     BOOLEAN       NOT NULL DEFAULT FALSE   -- same id, different body seen
);

-- Supabase exposes public tables over its REST API; RLS with no policies blocks
-- that. The service connects as the table owner, which RLS does not restrict.
ALTER TABLE transactions ENABLE ROW LEVEL SECURITY;

-- Worker scan touches only pending rows.
CREATE INDEX IF NOT EXISTS ix_transactions_pending
    ON transactions (next_attempt_at)
    WHERE status = 'PROCESSING';
