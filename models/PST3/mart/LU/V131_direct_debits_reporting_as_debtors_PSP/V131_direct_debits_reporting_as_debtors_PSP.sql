
{% set period_time = period_calculate(time = 'monthly', selection_date="today", prefix='', suffix='M' ) -%}
{% set time_zone = "Etc/UTC" -%}
{% set country_code = 'LU' -%}


        SELECT
  'CORP' as customerCategory,
  IF(substr(bank.BANK_ACCOUNT_NUMBER,5,3) = substr(counter.BANK_ACCOUNT_NUMBER,5,3) and counter.FINANCIAL_INSTITUTION_COUNTRY_CODE = '{{country_code}}', 'ONUS', 'PSPN') AS settlementChannel,
  'SEPA' as paymentScheme,
  "Electronic file/batch" AS initiationChannel ,
  '' as consent,
  '' as fraudType,
  counter.FINANCIAL_INSTITUTION_COUNTRY_CODE AS debtorPspCountry,
  ftr.transaction_currency as currency,
  'VOLU' as metric,
  count(*) as reportedAmount,
  CURRENT_TIMESTAMP AS Load_timestamp,
  "{{period}}"  AS Period,
FROM {{ source('source_dwh_STRP','F_ACCOUNT_TRANSACTIONS_DECRYPTED') }} as ftr
LEFT JOIN {{ source('source_dwh_STRP','D_ACCOUNT_TRANSACTION_CURRENT') }} as dtr  ON ftr.T_D_ACCOUNT_TRANSACTION_DIM_KEY = dtr.T_DIM_KEY
LEFT JOIN {{ source('source_dwh_STRP','D_IBIS_ACCOUNT_CURRENT') }} AS ibis ON ftr.T_D_IBIS_ACCOUNT_DIM_KEY = ibis.T_DIM_KEY
LEFT JOIN {{ source('source_dwh_STRP','D_BANK_ACCOUNTS_DECRYPTED') }} as bank ON ibis.T_D_BANK_ACCOUNT_DIM_KEY = bank.T_DIM_KEY
LEFT JOIN {{ source('source_dwh_STRP','D_BANK_ACCOUNTS_DECRYPTED') }} as counter ON counter.T_DIM_KEY = ftr.T_COUNTERPARTY_BANK_ACCOUNT_DIM_KEY
WHERE
    ftr.TRANSACTION_DIRECTION = "OUTBOUND"
  AND ftr.TRANSACTION_TYPE = 'REGULAR'
  AND dtr.TRANSACTION_STATUS IN ('RETURNED', 'SETTLED')
  AND ibis.ACCOUNT_TYPE = 'PAYMENT'
  AND bank.FINANCIAL_INSTITUTION_COUNTRY_CODE = '{{country_code}}'
  AND ftr.TRANSACTION_BANK_FAMILY = 'RDDT'
  AND ftr.TRANSACTION_BANK_SUBFAMILY in ('ESDD', 'OODD', 'BBDD')
  AND dtr.TRANSACTION_BOOKING_DATE_AT >= TIMESTAMP(DATETIME( '{{begin_date}}', '{{time_zone}}'))   -- +01 for winter time, +02 for summer time
  AND dtr.TRANSACTION_BOOKING_DATE_AT <= TIMESTAMP(DATETIME( '{{end_date}}', '{{time_zone}}'))  -- like timezone('UTC', to_timestamp('2023-12-31 UTC+01', 'YYYY-MM-DD ""UTC""TZH') + interval '1 day') -- +01 for winter time, +02 for summer time
group by 1,2,3,4,5,6,7,8
union all

SELECT
    'CORP' as customerCategory,
    IF(substr(bank.BANK_ACCOUNT_NUMBER,5,3) = substr(counter.BANK_ACCOUNT_NUMBER,5,3) and counter.FINANCIAL_INSTITUTION_COUNTRY_CODE = '{{country_code}}', 'ONUS', 'PSPN') AS settlementChannel,
    'SEPA' as paymentScheme,
    "Electronic file/batch" AS initiationChannel ,
    '' as consent,
    '' as fraudType,
    counter.FINANCIAL_INSTITUTION_COUNTRY_CODE AS debtorPspCountry,
    ftr.transaction_currency as currency,
    'VALE' as metric,
    SUM(ftr.TRANSACTION_AMOUNT)  as reportedAmount,
    CURRENT_TIMESTAMP AS Load_timestamp,
    "{{period}}"  AS Period,
FROM {{ source('source_dwh_STRP','F_ACCOUNT_TRANSACTIONS_DECRYPTED') }} as ftr
LEFT JOIN {{ source('source_dwh_STRP','D_ACCOUNT_TRANSACTION_CURRENT') }} as dtr  ON ftr.T_D_ACCOUNT_TRANSACTION_DIM_KEY = dtr.T_DIM_KEY
LEFT JOIN {{ source('source_dwh_STRP','D_IBIS_ACCOUNT_CURRENT') }} AS ibis ON ftr.T_D_IBIS_ACCOUNT_DIM_KEY = ibis.T_DIM_KEY
LEFT JOIN {{ source('source_dwh_STRP','D_BANK_ACCOUNTS_DECRYPTED') }} as bank ON ibis.T_D_BANK_ACCOUNT_DIM_KEY = bank.T_DIM_KEY
LEFT JOIN {{ source('source_dwh_STRP','D_BANK_ACCOUNTS_DECRYPTED') }} as counter ON counter.T_DIM_KEY = ftr.T_COUNTERPARTY_BANK_ACCOUNT_DIM_KEY
WHERE
    ftr.TRANSACTION_DIRECTION = "OUTBOUND"
  AND ftr.TRANSACTION_TYPE = 'REGULAR'
  AND dtr.TRANSACTION_STATUS IN ('RETURNED', 'SETTLED')
  AND ibis.ACCOUNT_TYPE = 'PAYMENT'
  AND bank.FINANCIAL_INSTITUTION_COUNTRY_CODE = '{{country_code}}'
  AND ftr.TRANSACTION_BANK_FAMILY = 'RDDT'
  AND ftr.TRANSACTION_BANK_SUBFAMILY in ('ESDD', 'OODD', 'BBDD')
  AND dtr.TRANSACTION_BOOKING_DATE_AT >= TIMESTAMP(DATETIME( '{{begin_date}}', '{{time_zone}}'))   -- +01 for winter time, +02 for summer time
  AND dtr.TRANSACTION_BOOKING_DATE_AT <= TIMESTAMP(DATETIME( '{{end_date}}', '{{time_zone}}'))  -- like timezone('UTC', to_timestamp('2023-12-31 UTC+01', 'YYYY-MM-DD ""UTC""TZH') + interval '1 day') -- +01 for winter time, +02 for summer time
group by 1,2,3,4,5,6,7,8
order by 1,2,3,4,5,6,7,8
