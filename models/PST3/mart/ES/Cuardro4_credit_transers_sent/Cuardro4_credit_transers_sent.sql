
{% set period_time = period_calculate(time = 'semesterly', selection_date="today", prefix='', suffix='S' ) -%}
{% set time_zone = "Etc/UTC" -%}
{% set country_code = 'ES' -%}


        SELECT
  CBA.FINANCIAL_INSTITUTION_COUNTRY_CODE  as payee_psp_country,
  CASE
      WHEN FAT.TRANSACTION_CHANNEL in ('DASHBOARD', 'ADMIN', 'OTHER') THEN '211 Manual entry'
      WHEN FAT.TRANSACTION_CHANNEL in ('TPP','OCS','H2H' ) THEN '212 Electronic entry'
  END EntryMode,
  CASE
      WHEN FAT.TRANSACTION_CHANNEL in ('DASHBOARD', 'ADMIN', 'OTHER') THEN 'NA'
      WHEN FAT.TRANSACTION_CHANNEL in ('TPP', 'OCS') THEN '2122 single '
      WHEN FAT.TRANSACTION_CHANNEL = 'H2H' then '2121 Batch entry'
  END BatchMode,
  CASE
      WHEN FAT.TRANSACTION_CHANNEL in ('TPP') THEN 'SCA'
      WHEN FAT.TRANSACTION_CHANNEL in ('OCS') and FAT.TRANSACTION_CREDITOR_REFERENCE_VALUE like 'REF.%/%/%' then 'NoSCA - 212333 Trusted Beneficiaries'
      WHEN FAT.TRANSACTION_CHANNEL in ('OCS') and FAT.TRANSACTION_END_TO_END_ID like 'CAF%' THEN 'SCA'
      WHEN FAT.TRANSACTION_CHANNEL = 'H2H' then 'NoSCA - 212335 secure processes'
      WHEN FAT.TRANSACTION_CHANNEL is null then 'not applicable - return'
      else 'check the query'
  END SCAIndicator,
  count (*) as outbound_ibis_payments_trx_count,
  sum (round(FAT.TRANSACTION_AMOUNT)) as outbound_ibis_payments_amount_sum_in_EUR,
  CURRENT_TIMESTAMP AS LOAD_TIMESTAMP,
  '{{period}}' AS PERIOD,
FROM {{ source('source_dwh_STRP','F_ACCOUNT_TRANSACTIONS_DECRYPTED') }} AS FAT
LEFT JOIN {{ source('source_dwh_STRP','D_ACCOUNT_TRANSACTION_CURRENT') }} AS DAT
  ON FAT.T_D_ACCOUNT_TRANSACTION_DIM_KEY = DAT.T_DIM_KEY
LEFT JOIN {{ source('source_dwh_STRP','D_IBIS_ACCOUNT_CURRENT') }} AS IA
  ON FAT.T_D_IBIS_ACCOUNT_DIM_KEY = IA.T_DIM_KEY
INNER JOIN {{ source('source_dwh_STRP','D_BANK_ACCOUNTS_DECRYPTED') }} AS BA
  ON IA.T_D_BANK_ACCOUNT_DIM_KEY = BA.T_DIM_KEY
LEFT JOIN {{ source('source_dwh_STRP','D_BANK_ACCOUNTS_DECRYPTED') }} AS CBA
  ON CBA.T_DIM_KEY = FAT.T_COUNTERPARTY_BANK_ACCOUNT_DIM_KEY
WHERE(FAT.TRANSACTION_DIRECTION = "OUTBOUND")
    AND FAT.TRANSACTION_TYPE = 'REGULAR'
    AND DAT.TRANSACTION_STATUS IN ('RETURNED', 'SETTLED')
    AND IA.ACCOUNT_TYPE = 'PAYMENT'
    AND  FAT.TRANSACTION_BANK_FAMILY = 'ICDT'
    AND FAT.TRANSACTION_CHANNEL <> 'CARDS'
    AND BA.FINANCIAL_INSTITUTION_COUNTRY_CODE = '{{country_code}}'
    AND DAT.TRANSACTION_BOOKING_DATE_AT >= TIMESTAMP(DATETIME( '{{begin_date}}', '{{time_zone}}'))
    AND DAT.TRANSACTION_BOOKING_DATE_AT <= TIMESTAMP(DATETIME( '{{end_date}}', '{{time_zone}}'))
GROUP BY 1,2,3,4
ORDER BY 1,2,3,4
