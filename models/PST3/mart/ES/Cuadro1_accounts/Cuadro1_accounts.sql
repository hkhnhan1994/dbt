
{% set period_time = period_calculate(time = 'semesterly', selection_date="today", prefix='', suffix='S' ) -%}
{% set time_zone = "Etc/UTC" -%}
{% set country_code = 'ES' -%}


        SELECT
count(*) AS number_of_accounts,
CURRENT_TIMESTAMP AS LOAD_TIMESTAMP,
'{{period}}' AS PERIOD,
FROM {{ source('source_dwh_STRP','D_IBIS_ACCOUNT_CURRENT') }} IACC
LEFT JOIN {{ source('source_dwh_STRP','D_BANK_ACCOUNTS_DECRYPTED') }} BANK
 ON IACC.T_D_BANK_ACCOUNT_DIM_KEY = BANK.T_DIM_KEY
WHERE ACCOUNT_TYPE = 'PAYMENT'
  AND BANK.FINANCIAL_INSTITUTION_COUNTRY_CODE = '{{country_code}}'
  AND (
    IACC.ACCOUNT_STATUS = 'OPEN'
    OR (
      IACC.ACCOUNT_STATUS = 'CLOSED'
      AND IACC.ACCOUNT_UPDATED_AT >= TIMESTAMP(DATETIME( '{{begin_date}}', '{{time_zone}}'))
    )
    AND IACC.ACCOUNT_CREATED_AT <= TIMESTAMP(DATETIME( '{{end_date}}', '{{time_zone}}'))
  )
