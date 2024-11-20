CREATE TABLE IF NOT EXISTS STV202408069__STAGING.transactions
(
    operation_id         UUID NOT NULL,
    account_number_from  INT NOT NULL,
    account_number_to    INT NOT NULL,
    currency_code        INT NOT NULL,
    country              VARCHAR(30) NOT NULL,
    "status"             VARCHAR(30) NOT NULL,
    transaction_type     VARCHAR(30) NOT NULL,
    amount               INT NOT NULL,
    transaction_dt       TIMESTAMP NOT NULL
)
ORDER BY transaction_dt, operation_id
SEGMENTED BY HASH(transaction_dt, operation_id) ALL NODES
PARTITION BY transaction_dt::DATE
GROUP BY CALENDAR_HIERARCHY_DAY(transaction_dt::DATE, 3, 2);

COPY STV202408069__STAGING.transactions(operation_id, account_number_from, account_number_to, currency_code, country, "status", transaction_type, amount, transaction_dt)
FROM LOCAL '{{params.batch}}'
DELIMITER ','
REJECTED DATA AS TABLE STV202408069__STAGING.transactions_rej;