SELECT
count(*) nr_accounts,
CURRENT_TIMESTAMP AS LOAD_TIMESTAMP,
'{{period}}' AS PERIOD,
FROM {{ source('source_dwh_strp,D_IBIS_ACCOUNT_CURRENT') }} IACC
left join {{ source('source_dwh_strp,D_BANK_ACCOUNTS_DECRYPTED') }} BANK
  on IACC.T_D_BANK_ACCOUNT_DIM_KEY = BANK.T_DIM_KEY
WHERE ACCOUNT_TYPE = 'PAYMENT'
  AND BANK.FINANCIAL_INSTITUTION_COUNTRY_CODE = 'NL'
  AND (
    IACC.ACCOUNT_STATUS = 'OPEN'
    OR (
      IACC.ACCOUNT_STATUS = 'CLOSED'
      and IACC.ACCOUNT_UPDATED_AT > TIMESTAMP(DATETIME( '{{period_time['end_date']}}', '{{time_zone}}'))
      )
  )
  and IACC.ACCOUNT_CREATED_AT < TIMESTAMP(DATETIME( '{{period_time['end_date']}}', '{{time_zone}}'))
