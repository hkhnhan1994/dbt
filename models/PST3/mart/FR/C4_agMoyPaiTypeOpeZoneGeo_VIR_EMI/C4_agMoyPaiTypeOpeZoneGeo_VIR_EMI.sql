
{% set period_time = period_calculate(time = 'semesterly', selection_date="today", prefix='', suffix='S' ) -%}
{% set time_zone = "Etc/UTC" -%}
{% set country_code = 'FR' -%}


        WITH country_codes AS (
SELECT *
FROM UNNEST (['FR','DE','AT','BE','BG','CY','HR','DK','ES','EE','FI','GR','HU','IE','IS','IT','LV','LI','LT','LU','MT','NO','NL','PL','PT','CZ','RO','SK','SI','SE']) as code
),
DM as (
  SELECT
    cred.FINANCIAL_INSTITUTION_COUNTRY_CODE AS payee_psp_country,
    COUNT(*) AS outbound_ibis_payments_trx_count,
    ROUND(COALESCE(SUM(ftr.TRANSACTION_AMOUNT), 0.00), 2) AS outbound_ibis_payments_amount_sum_in_EUR,
  FROM {{ source('source_dwh_STRP','F_ACCOUNT_TRANSACTIONS_DECRYPTED') }} ftr
  LEFT JOIN {{ source('source_dwh_STRP','D_IBIS_ACCOUNT_CURRENT') }} iacc
    ON ftr.T_D_IBIS_ACCOUNT_DIM_KEY = iacc.T_DIM_KEY
  LEFT JOIN {{ source('source_dwh_STRP','D_BANK_ACCOUNTS_DECRYPTED') }} deb
    ON iacc.T_D_BANK_ACCOUNT_DIM_KEY = deb.T_DIM_KEY
  LEFT JOIN {{ source('source_dwh_STRP','D_BANK_ACCOUNTS_DECRYPTED') }} cred
    ON cred.T_DIM_KEY = ftr.T_COUNTERPARTY_BANK_ACCOUNT_DIM_KEY
  LEFT JOIN {{ source('source_dwh_STRP','D_ACCOUNT_TRANSACTION_CURRENT') }} dtr
    ON ftr.T_D_ACCOUNT_TRANSACTION_DIM_KEY = dtr.T_DIM_KEY
  WHERE ftr.TRANSACTION_DIRECTION = 'OUTBOUND'
    AND deb.FINANCIAL_INSTITUTION_COUNTRY_CODE =  '{{country_code}}'
    AND iacc.ACCOUNT_TYPE = 'PAYMENT'
    AND ftr.transaction_type  = 'REGULAR'
    AND dtr.TRANSACTION_STATUS  IN ('SETTLED', 'RETURNED')
    AND ftr.TRANSACTION_BANK_FAMILY = 'ICDT'
    AND ftr.TRANSACTION_CHANNEL <> 'CARDS'
    AND dtr.TRANSACTION_BOOKING_DATE_AT >= TIMESTAMP(DATETIME( '{{period_time['begin_date']}}', '{{time_zone}}'))
    AND dtr.TRANSACTION_BOOKING_DATE_AT <= TIMESTAMP(DATETIME( '{{period_time['end_date']}}', '{{time_zone}}'))
  GROUP BY cred.FINANCIAL_INSTITUTION_COUNTRY_CODE
  ORDER BY payee_psp_country ASC
)
SELECT
   c.code,
  DM.*,
  (SELECT SUM(outbound_ibis_payments_trx_count) FROM DM WHERE payee_psp_country != 'FR') as total_trx_count_without_FR,
  (SELECT SUM(outbound_ibis_payments_amount_sum_in_EUR) FROM DM WHERE payee_psp_country != 'FR') as total_amount_sum_without_FR,
  CURRENT_TIMESTAMP AS Load_timestamp,
  "{{period}}"  AS Period,
FROM DM
FULL OUTER JOIN country_codes as c on c.code = DM.payee_psp_country
