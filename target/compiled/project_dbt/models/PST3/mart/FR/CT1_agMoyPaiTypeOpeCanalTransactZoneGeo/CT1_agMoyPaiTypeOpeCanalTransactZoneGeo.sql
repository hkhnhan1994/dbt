SELECT
    cred.FINANCIAL_INSTITUTION_COUNTRY_CODE AS payee_psp_country,
    COUNT(*) AS outbound_ibis_payments_trx_count,
    round(SUM(ftr.TRANSACTION_AMOUNT)) AS outbound_ibis_payments_amount_sum_in_EUR,
    CURRENT_TIMESTAMP AS Load_timestamp,
    ""  AS Period,
FROM `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`F_ACCOUNT_TRANSACTIONS_DECRYPTED` ftr
LEFT JOIN `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_IBIS_ACCOUNT_CURRENT` iacc
  ON ftr.T_D_IBIS_ACCOUNT_DIM_KEY = iacc.T_DIM_KEY
  AND iacc.ACCOUNT_TYPE = "PAYMENT"
LEFT JOIN `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_BANK_ACCOUNTS_DECRYPTED` deb
  ON iacc.T_D_BANK_ACCOUNT_DIM_KEY = deb.T_DIM_KEY
LEFT JOIN `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_BANK_ACCOUNTS_DECRYPTED` cred
  ON cred.T_DIM_KEY = ftr.T_COUNTERPARTY_BANK_ACCOUNT_DIM_KEY
LEFT JOIN `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_ACCOUNT_TRANSACTION_CURRENT` dtr
  ON ftr.T_D_ACCOUNT_TRANSACTION_DIM_KEY = dtr.T_DIM_KEY
WHERE ftr.TRANSACTION_DIRECTION = 'OUTBOUND'
  AND deb.FINANCIAL_INSTITUTION_COUNTRY_CODE = 'FR'
  AND iacc.ACCOUNT_TYPE = "PAYMENT"
  AND ftr.transaction_type  = 'REGULAR'
  AND ftr.TRANSACTION_CHANNEL not IN ('DASHBOARD', 'OTHER', 'CARDS')
  AND dtr.TRANSACTION_STATUS  IN ('SETTLED', 'RETURNED')
  AND ftr.TRANSACTION_BANK_FAMILY = 'ICDT'
  AND dtr.TRANSACTION_BOOKING_DATE_AT >= TIMESTAMP(DATETIME( '2024-07-01', 'Etc/UTC'))
  AND dtr.TRANSACTION_BOOKING_DATE_AT <= TIMESTAMP(DATETIME( '2024-09-30', 'Etc/UTC'))
group by cred.FINANCIAL_INSTITUTION_COUNTRY_CODE
order by payee_psp_country ASC