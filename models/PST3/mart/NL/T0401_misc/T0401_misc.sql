
{% set period_time = period_calculate(time = 'semesterly', selection_date="today", prefix='', suffix='S' ) -%}
{% set time_zone = "Etc/UTC" -%}
{% set country_code = 'NL' -%}


        SELECT
  FAM.MOVEMENT_FAMILY as bank_family,
  FAM.MOVEMENT_SUBFAMILY as bank_subfamily,
  count(*) as count,
  COALESCE(SUM(FAM.MOVEMENT_AMOUNT), 0) as amount,
  CURRENT_TIMESTAMP AS LOAD_TIMESTAMP,
  '{{period_time['period']}}' AS PERIOD
FROM {{ source('source_dwh_STRP','F_ACCOUNT_MOVEMENTS_DECRYPTED') }} AS FAM
LEFT JOIN {{ source('source_dwh_STRP','D_IBIS_ACCOUNT_CURRENT') }} AS DIAT
  ON FAM.T_D_IBIS_ACCOUNT_DIM_KEY = DIAT.T_DIM_KEY
INNER JOIN {{ source('source_dwh_STRP','D_BANK_ACCOUNTS_DECRYPTED') }} AS DBA
  ON DIAT.T_D_BANK_ACCOUNT_DIM_KEY = DBA.T_DIM_KEY
LEFT JOIN {{ source('source_dwh_STRP','D_BANK_ACCOUNTS_DECRYPTED') }} AS DCA
  ON DCA.T_DIM_KEY = FAM.T_COUNTERPARTY_BANK_ACCOUNT_DIM_KEY
WHERE DIAT.ACCOUNT_TYPE = 'PAYMENT'
  AND DBA.FINANCIAL_INSTITUTION_COUNTRY_CODE = '{{country_code}}'
  AND FAM.MOVEMENT_BOOKING_DATE_AT >= TIMESTAMP(DATETIME('{{period_time['begin_date']}}', '{{time_zone}}'))
  AND FAM.MOVEMENT_BOOKING_DATE_AT <= TIMESTAMP(DATETIME( '{{period_time['end_date']}}', '{{time_zone}}'))
GROUP BY 1,2
ORDER BY 1,2
