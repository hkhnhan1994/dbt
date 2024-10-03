SELECT
    cred.FINANCIAL_INSTITUTION_COUNTRY_CODE AS payee_psp_country,
    COUNT(*) AS outbound_ibis_payments_trx_count,
    round(SUM(ftr.TRANSACTION_AMOUNT)) AS outbound_ibis_payments_amount_sum_in_EUR,
    CURRENT_TIMESTAMP AS Load_timestamp,
    "{{period}}"  AS Period,
FROM {{ source('source_dwh_strp,F_ACCOUNT_TRANSACTIONS_DECRYPTED') }} ftr
LEFT JOIN {{ source('source_dwh_strp,D_IBIS_ACCOUNT_CURRENT') }} iacc
  ON ftr.T_D_IBIS_ACCOUNT_DIM_KEY = iacc.T_DIM_KEY
  AND iacc.ACCOUNT_TYPE = "PAYMENT"
LEFT JOIN {{ source('source_dwh_strp,D_BANK_ACCOUNTS_DECRYPTED') }} deb
  ON iacc.T_D_BANK_ACCOUNT_DIM_KEY = deb.T_DIM_KEY
LEFT JOIN {{ source('source_dwh_strp,D_BANK_ACCOUNTS_DECRYPTED') }} cred
  ON cred.T_DIM_KEY = ftr.T_COUNTERPARTY_BANK_ACCOUNT_DIM_KEY
LEFT JOIN {{ source('source_dwh_strp,D_ACCOUNT_TRANSACTION_CURRENT') }} dtr
  ON ftr.T_D_ACCOUNT_TRANSACTION_DIM_KEY = dtr.T_DIM_KEY
WHERE ftr.TRANSACTION_DIRECTION = 'OUTBOUND'
  AND deb.FINANCIAL_INSTITUTION_COUNTRY_CODE = '{{country_code}}'
  AND iacc.ACCOUNT_TYPE = "PAYMENT"
  AND ftr.transaction_type  = 'REGULAR'
  AND ftr.TRANSACTION_CHANNEL not IN ('DASHBOARD', 'OTHER', 'CARDS')
  AND dtr.TRANSACTION_STATUS  IN ('SETTLED', 'RETURNED')
  AND ftr.TRANSACTION_BANK_FAMILY = 'ICDT'
  AND dtr.TRANSACTION_BOOKING_DATE_AT >= TIMESTAMP(DATETIME( '{{period_time['begin_date']}}', '{{time_zone}}'))
  AND dtr.TRANSACTION_BOOKING_DATE_AT <= TIMESTAMP(DATETIME( '{{period_time['end_date']}}', '{{time_zone}}'))
group by cred.FINANCIAL_INSTITUTION_COUNTRY_CODE
order by payee_psp_country ASC
