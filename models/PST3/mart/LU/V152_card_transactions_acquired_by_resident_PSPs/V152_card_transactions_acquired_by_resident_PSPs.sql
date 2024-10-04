
{% set period_time = period_calculate(time = 'monthly', selection_date="today", prefix='', suffix='M' ) -%}
{% set time_zone = "Etc/UTC" -%}
{% set country_code = 'LU' -%}


        SELECT
  'CRCA' as PaymentCardType,
  case when f_payment_transactions.card_network = 'MASTERCARD' then 'MSTR'
    when f_payment_transactions.card_network = 'VISA' then 'VISA'
    else 'OTHER' END PaymentCardScheme,
  'ECOM' as TerminalType,
  IF(f_payment_transactions.PAYMENT_TRANSACTION_TYPE = 'REFUND', 'DPSP', 'CPSP') as OperationType,
  'RMTR' as InitiationChannel,
  'REM1' as InitiationsubChannel,
  'MITR' as SCA,
  'NOAP' as FraudType,
  d_merchants.MERCHANT_COUNTRY AS CountryofIssuer,
  d_merchants.MERCHANT_COUNTRY AS  CountryofTerminal,
  d_payment_item_info.PAYMENT_ITEM_CURRENCY_CODE as currency,
  'Volu' as Metric,
  count(*) as ReportedAmount,
  CURRENT_TIMESTAMP AS LOAD_TIMESTAMP,
  "{{period}}"  AS Period,
FROM {{ source('source_dwh_STRP','D_PAYMENT_ITEM_INFO_DECRYPTED') }} AS d_payment_item_info
INNER JOIN {{ source('source_dwh_STRP','F_PAYMENT_ITEMS') }} AS f_payment_items
  ON d_payment_item_info.T_DIM_KEY = f_payment_items.T_D_PAYMENT_ITEM_INFO_DIM_KEY
LEFT JOIN {{ source('source_dwh_STRP','D_MERCHANTS_DECRYPTED') }} AS d_merchants
  ON f_payment_items.T_D_MERCHANT_DIM_KEY = d_merchants.T_DIM_KEY AND d_merchants.T_SOURCE = "P1_PBEE"
LEFT JOIN {{ source('source_dwh_STRP','F_PAYMENT_TRANSACTIONS_DECRYPTED') }}  AS f_payment_transactions
  ON d_payment_item_info.T_DIM_KEY = f_payment_transactions.T_D_PAYMENT_ITEM_INFO_DIM_KEY
LEFT JOIN {{ source('source_dwh_STRP','D_PAYMENT_TRANSACTION_INFO_DECRYPTED') }} AS d_payment_transaction_info
  ON d_payment_transaction_info.T_DIM_KEY =f_payment_transactions.T_D_PAYMENT_TRANSACTION_INFO_DIM_KEY
LEFT JOIN {{ source('source_dwh_STRP','D_PAYMENT_PRODUCTS') }} AS d_payment_products
  ON f_payment_transactions.T_D_PRODUCT_DIM_KEY = d_payment_products.T_DIM_KEY
WHERE (d_merchants.MERCHANT_COUNTRY )= '{{country_code}}'
  AND d_payment_products.PRODUCT_CODE IN ('SILVERFLOW_CARDS')
  AND d_payment_transaction_info.PAYMENT_TRANSACTION_CUTOFF_AT >= TIMESTAMP(DATETIME( '{{period_time['begin_date']}}', '{{time_zone}}'))
  AND d_payment_transaction_info.PAYMENT_TRANSACTION_CUTOFF_AT <= TIMESTAMP(DATETIME( '{{period_time['end_date']}}', '{{time_zone}}'))
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
