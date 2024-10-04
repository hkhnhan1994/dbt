
{% set period_time = period_calculate(time = 'quarterly', selection_date="today", prefix='', suffix='Q' ) -%}
{% set time_zone = "Etc/UTC" -%}
{% set country_code = 'IT' -%}


        SELECT
    IF(FAB.BALANCE_AMOUNT <= 100,'small balance', 'big balance') AS balance_size,
    iacc.account_currency,
    count(*) AS amount,
    ceil(sum (FAB.BALANCE_AMOUNT)) as booked_balance,
    CURRENT_TIMESTAMP AS Load_timestamp,
    "{{period}}"  AS Period,
FROM {{ source('source_dwh_STRP','F_ACCOUNT_BALANCE') }} FAB
JOIN  {{ source('source_dwh_STRP','D_IBIS_ACCOUNT_CURRENT') }} iacc
  on FAB.T_D_IBIS_ACCOUNT_DIM_KEY = iacc.T_DIM_KEY
WHERE
      iacc.ACCOUNT_TYPE = "PAYMENT"
  AND  iacc.ACCOUNT_COUNTRY_CODE = '{{country_code}}'
  AND  (
    iacc.ACCOUNT_STATUS = 'OPEN'
    OR (
      iacc.ACCOUNT_STATUS = 'CLOSED'
      AND iacc.ACCOUNT_UPDATED_AT > TIMESTAMP(DATETIME( '{{period_time['end_date']}}', '{{time_zone}}'))
      )
  )
  AND iacc.ACCOUNT_UPDATED_AT <= TIMESTAMP(DATETIME( '{{period_time['end_date']}}', '{{time_zone}}'))
  AND FAB.BALANCE_DATE = (
      SELECT max(BALANCE_DATE)
      FROM {{ source('source_dwh_STRP','F_ACCOUNT_BALANCE') }} max
      JOIN {{ source('source_dwh_STRP','D_IBIS_ACCOUNT_CURRENT') }}  a
        ON max.T_D_IBIS_ACCOUNT_DIM_KEY = a.T_DIM_KEY
        WHERE TIMESTAMP(max.BALANCE_DATE) <= TIMESTAMP(DATETIME( '{{period_time['end_date']}}', '{{time_zone}}'))
  )
group by 1,2
