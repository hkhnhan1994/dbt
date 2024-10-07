SELECT
  'CORP' as customerCategory,
  IF(substr(bank.BANK_ACCOUNT_NUMBER,5,3) = substr(counter.BANK_ACCOUNT_NUMBER,5,3) and counter.FINANCIAL_INSTITUTION_COUNTRY_CODE = 'LU', 'ONUS', 'PSPN') AS settlementChannel,
  counter.FINANCIAL_INSTITUTION_COUNTRY_CODE AS debtorPspCountry,
  ftr.transaction_currency as currency,
  'VOLU' as metric,
  count(*) as reportedamount,
  CURRENT_TIMESTAMP AS Load_timestamp,
  ""  AS Period,
FROM `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`F_ACCOUNT_TRANSACTIONS_DECRYPTED` as ftr
LEFT JOIN `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_ACCOUNT_TRANSACTION_CURRENT` as dtr  ON ftr.T_D_ACCOUNT_TRANSACTION_DIM_KEY = dtr.T_DIM_KEY
LEFT JOIN `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_IBIS_ACCOUNT_CURRENT` AS ibis ON ftr.T_D_IBIS_ACCOUNT_DIM_KEY = ibis.T_DIM_KEY
LEFT JOIN `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_BANK_ACCOUNTS_DECRYPTED` as bank ON ibis.T_D_BANK_ACCOUNT_DIM_KEY = bank.T_DIM_KEY
LEFT JOIN `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_BANK_ACCOUNTS_DECRYPTED` as counter ON counter.T_DIM_KEY = ftr.T_COUNTERPARTY_BANK_ACCOUNT_DIM_KEY
WHERE
  ftr.TRANSACTION_DIRECTION = "INBOUND"
  AND ftr.TRANSACTION_TYPE = 'REGULAR'
  AND dtr.TRANSACTION_STATUS IN ('RETURNED', 'SETTLED')
  AND ibis.ACCOUNT_TYPE = 'PAYMENT'
  AND bank.FINANCIAL_INSTITUTION_COUNTRY_CODE = 'LU'
  AND ftr.TRANSACTION_BANK_FAMILY = 'RCDT'
  AND dtr.TRANSACTION_BOOKING_DATE_AT >= TIMESTAMP(DATETIME( '2024-10-01 10:34:12.666103', 'Etc/UTC'))   -- +01 for winter time, +02 for summer time
  AND dtr.TRANSACTION_BOOKING_DATE_AT <= TIMESTAMP(DATETIME( '2024-10-31 10:34:12.666103', 'Etc/UTC'))  -- like timezone('UTC', to_timestamp('2023-12-31 UTC+01', 'YYYY-MM-DD ""UTC""TZH') + interval '1 day') -- +01 for winter time, +02 for summer time
group by 1,2,3,4

UNION ALL

SELECT
    'CORP' as customerCategory,
    IF(substr(bank.BANK_ACCOUNT_NUMBER,5,3) = substr(counter.BANK_ACCOUNT_NUMBER,5,3) and counter.FINANCIAL_INSTITUTION_COUNTRY_CODE = 'LU', 'ONUS', 'PSPN') AS settlementChannel,
    counter.FINANCIAL_INSTITUTION_COUNTRY_CODE AS debtorPspCountry,
    ftr.transaction_currency as currency,
    'VALE' as metric,
    sum (ftr.TRANSACTION_AMOUNT) as reportedAmount,
    CURRENT_TIMESTAMP AS Load_timestamp,
    ""  AS Period,
FROM `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`F_ACCOUNT_TRANSACTIONS_DECRYPTED` as ftr
LEFT JOIN `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_ACCOUNT_TRANSACTION_CURRENT` as dtr  ON ftr.T_D_ACCOUNT_TRANSACTION_DIM_KEY = dtr.T_DIM_KEY
LEFT JOIN `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_IBIS_ACCOUNT_CURRENT` AS ibis ON ftr.T_D_IBIS_ACCOUNT_DIM_KEY = ibis.T_DIM_KEY
LEFT JOIN `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_BANK_ACCOUNTS_DECRYPTED` as bank ON ibis.T_D_BANK_ACCOUNT_DIM_KEY = bank.T_DIM_KEY
LEFT JOIN `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_BANK_ACCOUNTS_DECRYPTED` as counter ON counter.T_DIM_KEY = ftr.T_COUNTERPARTY_BANK_ACCOUNT_DIM_KEY
WHERE
    ftr.TRANSACTION_DIRECTION = "INBOUND"
  AND ftr.TRANSACTION_TYPE = 'REGULAR'
  AND dtr.TRANSACTION_STATUS IN ('RETURNED', 'SETTLED')
  AND ibis.ACCOUNT_TYPE = 'PAYMENT'
  AND bank.FINANCIAL_INSTITUTION_COUNTRY_CODE = 'LU'
  AND  ftr.TRANSACTION_BANK_FAMILY = 'RCDT'
  AND dtr.TRANSACTION_BOOKING_DATE_AT >= TIMESTAMP(DATETIME( '2024-10-01 10:34:12.666103', 'Etc/UTC'))   -- +01 for winter time, +02 for summer time
  AND dtr.TRANSACTION_BOOKING_DATE_AT <= TIMESTAMP(DATETIME( '2024-10-31 10:34:12.666103', 'Etc/UTC'))  -- like timezone('UTC', to_timestamp('2023-12-31 UTC+01', 'YYYY-MM-DD ""UTC""TZH') + interval '1 day') -- +01 for winter time, +02 for summer time
group by 1,2,3,4