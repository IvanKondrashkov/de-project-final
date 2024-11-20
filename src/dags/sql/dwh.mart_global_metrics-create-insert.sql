CREATE TABLE IF NOT EXISTS STV202408069__DWH.global_metrics
(
    date_update                     TIMESTAMP NOT NULL,
    currency_from                   INT NOT NULL,
    amount_total                    NUMERIC(15,2) NOT NULL,
    cnt_transactions                INT NOT NULL,
    avg_transactions_per_account    NUMERIC(15,2) NOT NULL,
    cnt_accounts_make_transactions  INT NOT NULL
)
ORDER BY date_update, currency_from
SEGMENTED BY HASH(date_update, currency_from) ALL NODES
PARTITION BY date_update::DATE
GROUP BY CALENDAR_HIERARCHY_DAY(date_update::DATE, 3, 2);


INSERT INTO STV202408069__DWH.global_metrics(date_update, currency_from, amount_total, cnt_transactions, avg_transactions_per_account, cnt_accounts_make_transactions)
SELECT
    c.date_update                                                   AS date_update,
    t.currency_code                                                 AS currency_from,
    SUM(t.amount * c.currency_with_div / 100)                       AS amount_total,
    COUNT(t.operation_id)                                           AS cnt_transactions,
    COUNT(t.operation_id) / COUNT(DISTINCT t.account_number_from)   AS avg_transactions_per_account,
    COUNT(DISTINCT t.account_number_from)                           AS cnt_accounts_make_transactions
FROM STV202408069__STAGING.transactions AS t
LEFT JOIN STV202408069__STAGING.currencies AS c ON t.transaction_dt::DATE = c.date_update::DATE AND t.currency_code = c.currency_code
WHERE t.transaction_dt::DATE = TO_DATE('{{ds_nodash}}', 'YYYYMMDD') AND c.currency_code_with = 420 AND t.account_number_from > 0 AND t.account_number_to > 0
GROUP BY 1, 2;