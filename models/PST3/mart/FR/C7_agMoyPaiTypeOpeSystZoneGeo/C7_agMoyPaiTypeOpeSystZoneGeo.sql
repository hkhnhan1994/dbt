
{% set period_time = period_calculate(time = 'semesterly', selection_date="today", prefix='', suffix='S' ) -%}
{% set time_zone = "Etc/UTC" -%}
{% set country_code = 'FR' -%}


        SELECT
  case WHEN  SUBSTR(deb.BANK_ACCOUNT_NUMBER,5,5) = SUBSTR(cred.BANK_ACCOUNT_NUMBER,5,5) THEN 'ON-US'
    WHEN SUBSTR(deb.BANK_ACCOUNT_NUMBER,5,5) <> SUBSTR(cred.BANK_ACCOUNT_NUMBER,5,5) THEN'OFF-US'
    ELSE 'not_mapped'
    END AS typeSysteme,
  cred.FINANCIAL_INSTITUTION_COUNTRY_CODE AS payee_psp_country,
  COUNT(*) AS outbound_ibis_payments_trx_count,
  ROUND(COALESCE(SUM(ftr.TRANSACTION_AMOUNT), 0.00), 2) AS outbound_ibis_payments_amount_sum_in_EUR,
  CURRENT_TIMESTAMP AS Load_timestamp,
  "{{period}}"  AS Period,
FROM {{ source('source_dwh_STRP','F_ACCOUNT_TRANSACTIONS_DECRYPTED') }} ftr
LEFT JOIN {{ source('source_dwh_STRP','D_IBIS_ACCOUNT_CURRENT') }} iacc
  ON ftr.T_D_IBIS_ACCOUNT_DIM_KEY = iacc.T_DIM_KEY
LEFT JOIN {{ source('source_dwh_STRP','D_BANK_ACCOUNTS_DECRYPTED') }} deb
  ON iacc.T_D_BANK_ACCOUNT_DIM_KEY = deb.T_DIM_KEY
LEFT JOIN {{ source('source_dwh_STRP','D_BANK_ACCOUNTS_DECRYPTED') }} cred
  ON cred.T_DIM_KEY = ftr.T_COUNTERPARTY_BANK_ACCOUNT_DIM_KEY
LEFT JOIN {{ source('source_dwh_STRP','D_ACCOUNT_TRANSACTION_CURRENT') }} dtr
  ON ftr.T_D_ACCOUNT_TRANSACTION_DIM_KEY = dtr.T_DIM_KEY
WHERE ftr.TRANSACTION_DIRECTION = 'OUTBOUND'
  AND deb.FINANCIAL_INSTITUTION_COUNTRY_CODE =  '{{country_code}}'
  AND iacc.ACCOUNT_TYPE = 'PAYMENT'
  AND ftr.transaction_type  = 'REGULAR'
  AND dtr.TRANSACTION_STATUS  IN ('SETTLED', 'RETURNED')
  AND ftr.TRANSACTION_BANK_FAMILY = 'ICDT'
  AND ftr.TRANSACTION_CHANNEL <> 'CARDS'
  AND dtr.TRANSACTION_BOOKING_DATE_AT >= TIMESTAMP(DATETIME( '{{begin_date}}', '{{time_zone}}'))
  AND dtr.TRANSACTION_BOOKING_DATE_AT <= TIMESTAMP(DATETIME( '{{end_date}}', '{{time_zone}}'))
GROUP BY 1,2
ORDER BY 1,2
