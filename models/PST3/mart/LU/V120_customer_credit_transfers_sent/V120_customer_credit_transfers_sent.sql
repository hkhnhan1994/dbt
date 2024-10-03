SELECT
  'CORP' AS customerCategory,
  IF(substr(bank.BANK_ACCOUNT_NUMBER,5,3) = substr(counter.BANK_ACCOUNT_NUMBER,5,3) and counter.FINANCIAL_INSTITUTION_COUNTRY_CODE = '{{country_code}}', 'ONUS', 'PSPN') AS settlementChannel,
  'SEPA' as paymentScheme,
  case
    when ftr.transaction_channel in ('DASHBOARD','ADMIN') then 'PAPR'
    when ftr.transaction_channel in ('OCS','TPP') then 'WEBB'
    when ftr.transaction_channel = 'H2H' then 'ELFB'
  end initiationChannel ,
  'REM1' as initiationSubchannel,
  'CUST' as initiatorType,
  case
    when ftr.transaction_channel in ('DASHBOARD','ADMIN') then 'NOAP'
    when ftr.transaction_channel in ('TPP') then 'SCA1'
    when ftr.transaction_channel in ('OCS') and ftr.transaction_creditor_reference_value like 'REF.%/%/%' then 'TRBN'
    when ftr.transaction_channel in ('OCS') and ftr.TRANSACTION_END_TO_END_ID like 'CAF%' then 'SCA1'
    when ftr.transaction_channel = 'H2H' then 'SECO'
  else 'check the query'
  end SCA,
  'NOAP' as fraudType,
  counter.FINANCIAL_INSTITUTION_COUNTRY_CODE as creditorPspCountry,
  ftr.transaction_currency as currency,
  'VOLU' as metric,
  COUNT(*) as reportedAmount,
  CURRENT_TIMESTAMP AS Load_timestamp,
  "{{period}}"  AS Period,
FROM {{ source('source_dwh_strp,F_ACCOUNT_TRANSACTIONS_DECRYPTED') }} AS ftr
LEFT JOIN {{ source('source_dwh_strp,D_ACCOUNT_TRANSACTION_CURRENT') }} AS dtr
  ON ftr.T_D_ACCOUNT_TRANSACTION_DIM_KEY = dtr.T_DIM_KEY
LEFT JOIN {{ source('source_dwh_strp,D_IBIS_ACCOUNT_CURRENT') }} AS ibis
  ON ftr.T_D_IBIS_ACCOUNT_DIM_KEY = ibis.T_DIM_KEY
LEFT JOIN {{ source('source_dwh_strp,D_BANK_ACCOUNTS_DECRYPTED') }} AS bank
  ON ibis.T_D_BANK_ACCOUNT_DIM_KEY = bank.T_DIM_KEY
LEFT JOIN {{ source('source_dwh_strp,D_BANK_ACCOUNTS_DECRYPTED') }} AS counter
  ON counter.T_DIM_KEY = ftr.T_COUNTERPARTY_BANK_ACCOUNT_DIM_KEY
WHERE
    ftr.TRANSACTION_DIRECTION = "OUTBOUND"
  AND ftr.TRANSACTION_TYPE = 'REGULAR'
  AND dtr.TRANSACTION_STATUS IN ('RETURNED', 'SETTLED')
  AND ibis.ACCOUNT_TYPE = 'PAYMENT'
  AND bank.FINANCIAL_INSTITUTION_COUNTRY_CODE = '{{country_code}}'
  AND ftr.TRANSACTION_BANK_FAMILY = 'ICDT'
  AND ftr.transaction_channel <> 'CARDS'
  AND dtr.TRANSACTION_BOOKING_DATE_AT >= TIMESTAMP(DATETIME( '{{period_time['begin_date']}}', '{{time_zone}}'))  -- +01 for winter time, +02 for summer time
  -- AND dtr.TRANSACTION_BOOKING_DATE_AT >= TIMESTAMP(DATETIME( '{{period_time['begin_date']}}', '{{time_zone}}')) -- +01 for winter time, +02 for summer time
  AND dtr.TRANSACTION_BOOKING_DATE_AT <= TIMESTAMP(DATETIME( '{{period_time['end_date']}}', '{{time_zone}}'))  -- like timezone('UTC', to_timestamp('2023-12-31 UTC+01', 'YYYY-MM-DD ""UTC""TZH') + interval '1 day') -- +01 for winter time, +02 for summer time

GROUP BY 1,2,3,4,5,6,7,8,9,10

UNION ALL

SELECT
  'CORP' AS customerCategory,
  IF(substr(bank.BANK_ACCOUNT_NUMBER,5,3) = substr(counter.BANK_ACCOUNT_NUMBER,5,3) and counter.FINANCIAL_INSTITUTION_COUNTRY_CODE = '{{country_code}}', 'ONUS', 'PSPN') AS settlementChannel,
  'SEPA' as paymentScheme,
  case
    when ftr.transaction_channel in ('DASHBOARD','ADMIN') then 'PAPR'
    when ftr.transaction_channel in ('OCS','TPP') then 'WEBB'
    when ftr.transaction_channel = 'H2H' then 'ELFB'
  end initiationChannel ,
  'REM1' as initiationSubchannel,
  'CUST' as initiatorType,
  case
    when ftr.transaction_channel in ('DASHBOARD','ADMIN') then 'NOAP'
    when ftr.transaction_channel in ('TPP') then 'SCA1'
    when ftr.transaction_channel in ('OCS') and ftr.transaction_creditor_reference_value like 'REF.%/%/%' then 'TRBN'
    when ftr.transaction_channel in ('OCS') and ftr.TRANSACTION_END_TO_END_ID like 'CAF%' then 'SCA1'
    when ftr.transaction_channel = 'H2H' then 'SECO'
    else 'check the query'
  end SCA,
  'NOAP' as fraudType,
  counter.FINANCIAL_INSTITUTION_COUNTRY_CODE as creditorPspCountry,
  ftr.transaction_currency as currency,
  'VALE' as metric,
  sum (ftr.TRANSACTION_AMOUNT) as reportedAmount,
  CURRENT_TIMESTAMP AS Load_timestamp,
  "{{period}}"  AS Period,
FROM {{ source('source_dwh_strp,F_ACCOUNT_TRANSACTIONS_DECRYPTED') }} AS ftr
LEFT JOIN {{ source('source_dwh_strp,D_ACCOUNT_TRANSACTION_CURRENT') }} AS dtr
  ON ftr.T_D_ACCOUNT_TRANSACTION_DIM_KEY = dtr.T_DIM_KEY
LEFT JOIN {{ source('source_dwh_strp,D_IBIS_ACCOUNT_CURRENT') }} AS ibis
  ON ftr.T_D_IBIS_ACCOUNT_DIM_KEY = ibis.T_DIM_KEY
LEFT JOIN {{ source('source_dwh_strp,D_BANK_ACCOUNTS_DECRYPTED') }} AS bank
  ON ibis.T_D_BANK_ACCOUNT_DIM_KEY = bank.T_DIM_KEY
LEFT JOIN {{ source('source_dwh_strp,D_BANK_ACCOUNTS_DECRYPTED') }} AS counter
  ON counter.T_DIM_KEY = ftr.T_COUNTERPARTY_BANK_ACCOUNT_DIM_KEY
WHERE
  ftr.TRANSACTION_DIRECTION = "OUTBOUND"
  AND ftr.TRANSACTION_TYPE = 'REGULAR'
  AND dtr.TRANSACTION_STATUS IN ('RETURNED', 'SETTLED')
  AND ibis.ACCOUNT_TYPE = 'PAYMENT'
  AND bank.FINANCIAL_INSTITUTION_COUNTRY_CODE = '{{country_code}}'
  AND  ftr.TRANSACTION_BANK_FAMILY = 'ICDT'
  AND ftr.transaction_channel <> 'CARDS'
  AND dtr.TRANSACTION_BOOKING_DATE_AT >= TIMESTAMP(DATETIME( '{{period_time['begin_date']}}', '{{time_zone}}'))   -- +01 for winter time, +02 for summer time
  AND dtr.TRANSACTION_BOOKING_DATE_AT <= TIMESTAMP(DATETIME( '{{period_time['end_date']}}', '{{time_zone}}'))   -- +01 for winter time, +02 for summer time
GROUP BY 1,2,3,4,5,6,7,8,9,10
ORDER BY 1,2,3,4,5,6,7,8,9,10
