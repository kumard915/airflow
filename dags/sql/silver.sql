

CREATE TABLE IF NOT EXISTS fact_transactions (
    id TEXT,
    merchant_id TEXT,
    txn_amount DOUBLE PRECISION,
    status TEXT,
    txn_type TEXT,
    ts TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_fact_txn_type_ts
ON fact_transactions (txn_type, ts);

-- PAYIN
INSERT INTO fact_transactions (
    id,
    merchant_id,
    txn_amount,
    status,
    txn_type,
    ts
)
SELECT
    id,
    "merchantId",
    "transaction.amount"::double precision,
    status,
    'PAYIN',
    "createdOn"::timestamp
FROM bronze_payins
WHERE "createdOn"::timestamp >
(
    SELECT COALESCE(MAX(ts), '1970-01-01')
    FROM fact_transactions
    WHERE txn_type = 'PAYIN'
);

-- PAYOUT
INSERT INTO fact_transactions (
    id,
    merchant_id,
    txn_amount,
    status,
    txn_type,
    ts
)
SELECT
    id,
    "merchantId",
    "transaction.amount"::double precision,
    status,
    'PAYOUT',
    "createdOn"::timestamp
FROM bronze_payouts
WHERE "createdOn"::timestamp >
(
    SELECT COALESCE(MAX(ts), '1970-01-01')
    FROM fact_transactions
    WHERE txn_type = 'PAYOUT'
);
