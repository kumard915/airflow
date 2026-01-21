
CREATE TABLE IF NOT EXISTS gold_merchant_pnl (
    merchant_name TEXT,
    category TEXT,
    kyc_status TEXT,
    total_payin DOUBLE PRECISION,
    total_payout DOUBLE PRECISION,
    txn_count BIGINT
);


-- TRUNCATE gold_merchant_pnl;
TRUNCATE TABLE gold_merchant_pnl;

INSERT INTO gold_merchant_pnl (
    merchant_name,
    category,
    kyc_status,
    total_payin,
    total_payout,
    txn_count
)
SELECT
    m.name,
    m.category,
    m."kycStatus",
    COALESCE(SUM(CASE WHEN f.txn_type = 'PAYIN' THEN f.txn_amount END), 0),
    COALESCE(SUM(CASE WHEN f.txn_type = 'PAYOUT' THEN f.txn_amount END), 0),
    COUNT(*)
FROM fact_transactions f
JOIN bronze_merchants m
  ON f.merchant_id = m.id
GROUP BY
    m.name,
    m.category,
    m."kycStatus";

