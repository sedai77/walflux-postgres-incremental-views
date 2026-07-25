-- Demo source table for WalFlux: a tiny orders ledger.
-- Loaded exactly once by the postgres container via /docker-entrypoint-initdb.d.

CREATE TABLE orders (
    id         bigserial PRIMARY KEY,
    status     text        NOT NULL,
    total      numeric(10, 2),
    coupon     text,
    created_at timestamptz DEFAULT now()
);

-- 50 deterministic seed rows spread across the four statuses, with some NULL
-- totals and coupons so the aggregates exercise NULL handling from row one.
INSERT INTO orders (status, total, coupon)
SELECT
    (ARRAY['pending', 'paid', 'shipped', 'cancelled'])[1 + (i % 4)],
    CASE WHEN i % 10 = 0
         THEN NULL
         ELSE ((1500 + (i * 137) % 48000)::numeric / 100)
    END,
    CASE WHEN i % 4 = 0
         THEN (ARRAY['SAVE10', 'VIP', 'BOGO'])[1 + (i % 3)]
         ELSE NULL
    END
FROM generate_series(1, 50) AS i;
