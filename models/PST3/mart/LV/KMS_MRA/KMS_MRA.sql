SELECT
  BA.BANK_ACCOUNT_NUMBER as large_biller_account,
  CURRENT_TIMESTAMP AS LOAD_TIMESTAMP,
  '{{period}}' AS PERIOD,
FROM {{ source('source_dwh_strp,D_MERCHANTS_CURRENT') }} AS M
left JOIN {{ source('source_dwh_strp,D_BANK_ACCOUNTS_DECRYPTED') }} AS BA
  ON M.T_D_MRA_BANK_ACCOUNT_DIM_KEY = BA.T_DIM_KEY
WHERE M.T_D_MRA_BANK_ACCOUNT_DIM_KEY is not null
  AND BA.FINANCIAL_INSTITUTION_COUNTRY_CODE = '{{country_code}}'
  AND MERCHANT_CREATED_AT >= TIMESTAMP(DATETIME( '{{period_time['begin_date']}}', '{{time_zone}}'))
  AND MERCHANT_CREATED_AT <= TIMESTAMP(DATETIME( '{{period_time['end_date']}}', '{{time_zone}}'))
