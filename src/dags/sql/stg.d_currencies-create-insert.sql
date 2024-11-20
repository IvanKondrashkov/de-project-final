CREATE TABLE IF NOT EXISTS STV202408069__STAGING.currencies
(
    currency_code       INT NOT NULL,
    currency_code_with  INT NOT NULL,
    date_update         TIMESTAMP NOT NULL,
    currency_with_div   NUMERIC(5,3) NOT NULL
)
ORDER BY date_update
SEGMENTED BY HASH(date_update, currency_code) ALL NODES
PARTITION BY date_update::DATE
GROUP BY CALENDAR_HIERARCHY_DAY(date_update::DATE, 3, 2);

COPY STV202408069__STAGING.currencies(currency_code, currency_code_with, date_update, currency_with_div)
FROM LOCAL '/data/currencies_history.csv'
DELIMITER ','
REJECTED DATA AS TABLE STV202408069__STAGING.currencies_rej;