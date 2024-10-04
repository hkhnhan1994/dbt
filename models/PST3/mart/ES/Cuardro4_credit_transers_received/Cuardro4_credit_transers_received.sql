
{% set period_time = period_calculate(time = 'semesterly', selection_date="today", prefix='', suffix='S' ) -%}
{% set time_zone = "Etc/UTC" -%}
{% set country_code = 'ES' -%}


        SELECT
  DBA.FINANCIAL_INSTITUTION_COUNTRY_CODE AS Payer_PSP_country,
  count(*)  as inbound_ibis_payments_trx_count,
  sum(FAT.TRANSACTION_AMOUNT) AS inbound_ibis_payments_amount_sum_in_EUR,
  CURRENT_TIMESTAMP AS LOAD_TIMESTAMP,
  '{{period}}' AS PERIOD,

  FROM {{ source('source_dwh_STRP','F_ACCOUNT_TRANSACTIONS_DECRYPTED') }} AS FAT
  LEFT JOIN {{ source('source_dwh_STRP','D_ACCOUNT_TRANSACTION_CURRENT') }} AS DAT
    ON FAT.T_D_ACCOUNT_TRANSACTION_DIM_KEY = DAT.T_DIM_KEY
  LEFT JOIN {{ source('source_dwh_STRP','D_IBIS_ACCOUNT_CURRENT') }} AS IA
    ON FAT.T_D_IBIS_ACCOUNT_DIM_KEY = IA.T_DIM_KEY
  INNER JOIN {{ source('source_dwh_STRP','D_BANK_ACCOUNTS_DECRYPTED') }} AS CBA
    ON IA.T_D_BANK_ACCOUNT_DIM_KEY = CBA.T_DIM_KEY
  LEFT JOIN {{ source('source_dwh_STRP','D_BANK_ACCOUNTS_DECRYPTED') }} AS DBA
    ON DBA.T_DIM_KEY = FAT.T_COUNTERPARTY_BANK_ACCOUNT_DIM_KEY
  WHERE (FAT.TRANSACTION_DIRECTION = "INBOUND")
    AND(FAT.TRANSACTION_TYPE) = 'REGULAR'
    AND (DAT.TRANSACTION_STATUS) IN ('RETURNED', 'SETTLED')
    AND (IA.ACCOUNT_TYPE) = 'PAYMENT'
    AND  FAT.TRANSACTION_BANK_FAMILY = 'RCDT'
    AND CBA.FINANCIAL_INSTITUTION_COUNTRY_CODE = '{{country_code}}'
    AND DAT.TRANSACTION_BOOKING_DATE_AT >= TIMESTAMP(DATETIME( '{{begin_date}}', '{{time_zone}}'))
    AND DAT.TRANSACTION_BOOKING_DATE_AT <= TIMESTAMP(DATETIME( '{{end_date}}', '{{time_zone}}'))
  GROUP BY 1
  ORDER BY 1
