SELECT
  COUNT( distinct ibis.ACCOUNT_MASTER_DATA_ID) AS count,
  CURRENT_TIMESTAMP AS Load_timestamp,
  "{{period}}"  AS Period,
FROM {{ source('source_dwh_strp,D_IBIS_ACCOUNT_CURRENT') }} AS ibis
LEFT JOIN {{ source('source_dwh_strp,D_BANK_ACCOUNTS_DECRYPTED') }} AS bank
  ON bank.T_DIM_KEY = ibis.T_D_BANK_ACCOUNT_DIM_KEY
WHERE ibis.ACCOUNT_TYPE = "PAYMENT"
AND (ibis.ACCOUNT_STATUS = 'OPEN'
  OR
    (
      ibis.ACCOUNT_STATUS = 'CLOSED'
      AND ibis.ACCOUNT_UPDATED_AT >= TIMESTAMP(DATETIME( '{{period_time['end_date']}}', '{{time_zone}}'))
    )
)
-- as timezone('UTC', to_timestamp('2023-06-30 UTC+02', 'YYYY-MM-DD "UTC"TZH') + interval '1 day')))
AND bank.FINANCIAL_INSTITUTION_COUNTRY_CODE = '{{country_code}}'
AND ibis.ACCOUNT_CREATED_AT >= TIMESTAMP(DATETIME( '{{period_time['end_date']}}', '{{time_zone}}'))
-- as timezone('UTC', to_timestamp('2023-06-30 UTC+02', 'YYYY-MM-DD "UTC"TZH') + interval '1 day')
