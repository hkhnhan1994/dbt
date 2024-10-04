
{% set period_time = period_calculate(time = 'quarterly', selection_date="today", prefix='', suffix='Q' ) -%}
{% set time_zone = "Etc/UTC" -%}
{% set country_code = 'IT' -%}


        SELECT
  CASE
      WHEN FAT.TRANSACTION_CHANNEL in ('DASHBOARD', 'ADMIN', 'OTHER') THEN 'report in 58726 02&04'
      WHEN FAT.TRANSACTION_CHANNEL in ('TPP') THEN 'report in 58726 22&24'
      WHEN FAT.TRANSACTION_CHANNEL in ('OCS') and FAT.TRANSACTION_CREDITOR_REFERENCE_VALUE like 'REF.%/%/%' then 'report in 58726 26&28'
      WHEN FAT.TRANSACTION_CHANNEL in ('OCS') and FAT.TRANSACTION_END_TO_END_ID like 'CAF%' THEN 'report in 58726 22&24'
      WHEN FAT.TRANSACTION_CHANNEL = 'H2H' then 'report in 58726 26&28'
      WHEN FAT.TRANSACTION_CHANNEL is null then 'not applicable - return'
      ELSE 'check the query'
  END metricToBeReported,
  CASE
      WHEN FAT.TRANSACTION_CHANNEL in ('DASHBOARD', 'ADMIN', 'OTHER') THEN 'N/A'
      WHEN FAT.TRANSACTION_CHANNEL in ('TPP') THEN 'SCA'
      WHEN FAT.TRANSACTION_CHANNEL in ('OCS') and FAT.TRANSACTION_CREDITOR_REFERENCE_VALUE like 'REF.%/%/%' then '413'
      WHEN FAT.TRANSACTION_CHANNEL in ('OCS') and FAT.TRANSACTION_END_TO_END_ID like 'CAF%' THEN 'SCA'
      WHEN FAT.TRANSACTION_CHANNEL = 'H2H' then '415'
      WHEN FAT.TRANSACTION_CHANNEL is null then 'not applicable - return'
  END SCAIndicator,
  CBA.FINANCIAL_INSTITUTION_COUNTRY_CODE  as payee_psp_country,
  '396' as pisp,
  IF(SUBSTR(CBA.BANK_ACCOUNT_NUMBER,5,5) = '36330' and CBA.FINANCIAL_INSTITUTION_COUNTRY_CODE = '{{country_code}}', '398','396') AS scheme,
  count(*) AS count,
  sum (round(FAT.TRANSACTION_AMOUNT)) as Amount,
  CURRENT_TIMESTAMP AS LOAD_TIMESTAMP,
  "{{period}}"  AS Period,
  FROM {{ source('source_dwh_STRP','F_ACCOUNT_TRANSACTIONS_DECRYPTED') }} AS FAT
  LEFT JOIN {{ source('source_dwh_STRP','D_ACCOUNT_TRANSACTION_CURRENT') }} AS DAT ON FAT.T_D_ACCOUNT_TRANSACTION_DIM_KEY = DAT.T_DIM_KEY
  LEFT JOIN {{ source('source_dwh_STRP','D_IBIS_ACCOUNT_CURRENT') }} AS IA ON FAT.T_D_IBIS_ACCOUNT_DIM_KEY = IA.T_DIM_KEY
  INNER JOIN {{ source('source_dwh_STRP','D_BANK_ACCOUNTS_DECRYPTED') }} AS BA ON IA.T_D_BANK_ACCOUNT_DIM_KEY = BA.T_DIM_KEY
  LEFT JOIN {{ source('source_dwh_STRP','D_BANK_ACCOUNTS_DECRYPTED') }} AS CBA ON CBA.T_DIM_KEY = FAT.T_COUNTERPARTY_BANK_ACCOUNT_DIM_KEY
  INNER JOIN {{ source('source_dwh_STRP','F_ACCOUNT_BALANCE') }} FAB on FAB.T_D_IBIS_ACCOUNT_DIM_KEY = IA.T_DIM_KEY
  WHERE FAT.TRANSACTION_DIRECTION = "OUTBOUND"
    AND FAT.TRANSACTION_TYPE = 'REGULAR'
    AND DAT.TRANSACTION_STATUS IN ('RETURNED', 'SETTLED')
    AND IA.ACCOUNT_TYPE = 'PAYMENT'
    AND FAT.TRANSACTION_BANK_FAMILY = 'ICDT'
    AND FAT.TRANSACTION_CHANNEL <> 'CARDS'
    AND BA.FINANCIAL_INSTITUTION_COUNTRY_CODE IN ('{{country_code}}')
    AND DAT.TRANSACTION_BOOKING_DATE_AT >= TIMESTAMP(DATETIME( '{{begin_date}}', '{{time_zone}}'))
    AND DAT.TRANSACTION_BOOKING_DATE_AT <= TIMESTAMP(DATETIME( '{{end_date}}', '{{time_zone}}'))
  group by 1,2,3,4,5
