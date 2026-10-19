-- Dashboard lists the most recent transactions.
CREATE INDEX IF NOT EXISTS ix_transactions_created_at ON transactions (created_at DESC);
