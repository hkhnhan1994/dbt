
{% set period_time = period_calculate(time = 'semesterly', selection_date="today", prefix='', suffix='S' ) -%}
{% set time_zone = "Etc/UTC" -%}
{% set country_code = 'LV' -%}


        SELECT
  CBA.FINANCIAL_INSTITUTION_COUNTRY_CODE AS Payee_PSP_country,
  count(*) as outbound_ibis_paymennts_trx_count,
  sum(FAT.TRANSACTION_AMOUNT) AS outbound_ibis_payments_amount_sum_in_EUR,
  CURRENT_TIMESTAMP AS LOAD_TIMESTAMP,
  '{{period_time['period']}}' AS PERIOD,
FROM {{ source('source_dwh_STRP','F_ACCOUNT_TRANSACTIONS_DECRYPTED') }} AS FAT
LEFT JOIN {{ source('source_dwh_STRP','D_ACCOUNT_TRANSACTION_CURRENT') }} AS DAT
  ON FAT.T_D_ACCOUNT_TRANSACTION_DIM_KEY = DAT.T_DIM_KEY
LEFT JOIN {{ source('source_dwh_STRP','D_IBIS_ACCOUNT_CURRENT') }} AS IA
  ON FAT.T_D_IBIS_ACCOUNT_DIM_KEY = IA.T_DIM_KEY
JOIN {{ source('source_dwh_STRP','D_BANK_ACCOUNTS_DECRYPTED') }} AS BA
  ON IA.T_D_BANK_ACCOUNT_DIM_KEY = BA.T_DIM_KEY
LEFT JOIN {{ source('source_dwh_STRP','D_BANK_ACCOUNTS_DECRYPTED') }} AS CBA
  ON CBA.T_DIM_KEY = FAT.T_COUNTERPARTY_BANK_ACCOUNT_DIM_KEY

WHERE FAT.TRANSACTION_DIRECTION = "OUTBOUND"
  AND FAT.TRANSACTION_TYPE = 'REGULAR'
  AND DAT.TRANSACTION_BOOKING_DATE_AT >= TIMESTAMP(DATETIME( '{{period_time['begin_date']}}', '{{time_zone}}'))
  AND DAT.TRANSACTION_BOOKING_DATE_AT <= TIMESTAMP(DATETIME( '{{period_time['end_date']}}', '{{time_zone}}'))
  AND DAT.TRANSACTION_STATUS IN ('RETURNED', 'SETTLED')
  AND IA.ACCOUNT_TYPE = 'PAYMENT'
  AND FAT.TRANSACTION_BANK_FAMILY = 'ICDT'
  AND FAT.TRANSACTION_CHANNEL <> 'CARDS'
  AND BA.FINANCIAL_INSTITUTION_COUNTRY_CODE = '{{country_code}}'
  group by 1
