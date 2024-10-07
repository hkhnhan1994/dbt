
  
    

    create or replace table `pj-bu-dw-data-sbx`.`dev_dm_pst3_LU`.`V142_intermediated_payment_transactions`
      
    
    

    OPTIONS()
    as (
      SELECT
  'CUCT' as PaymentInstructionType,
  'PSPN' as CustomerType,
  'CPSP' as RoleofReporting,
  'PSPN' as Settlementchannel,
  d_payment_transaction_info.payment_transaction_payer_country   as debtorPSPs,
  'LU' as CreditorPSPs,
  d_payment_transaction_info.payment_transaction_currency_code as currency,
  'Volu' as Metric,
  count(*) as ReportedAmount,
  CURRENT_TIMESTAMP AS LOAD_TIMESTAMP,
  ""  AS Period,
FROM `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_PAYMENT_ITEM_INFO_DECRYPTED`
    AS d_payment_item_info
INNER JOIN `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`F_PAYMENT_ITEMS` AS f_payment_items ON d_payment_item_info.T_DIM_KEY = f_payment_items.T_D_PAYMENT_ITEM_INFO_DIM_KEY
LEFT JOIN `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_MERCHANTS_DECRYPTED` AS d_merchants ON f_payment_items.T_D_MERCHANT_DIM_KEY = d_merchants.T_DIM_KEY AND d_merchants.T_SOURCE = "P1_PBEE"
LEFT JOIN `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`F_PAYMENT_TRANSACTIONS_DECRYPTED`  AS f_payment_transactions ON d_payment_item_info.T_DIM_KEY = f_payment_transactions.T_D_PAYMENT_ITEM_INFO_DIM_KEY
LEFT JOIN `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_PAYMENT_TRANSACTION_INFO_DECRYPTED` AS d_payment_transaction_info ON d_payment_transaction_info.T_DIM_KEY =f_payment_transactions.
T_D_PAYMENT_TRANSACTION_INFO_DIM_KEY
LEFT JOIN `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_PAYMENT_PRODUCTS` AS d_payment_products ON f_payment_transactions.T_D_PRODUCT_DIM_KEY = d_payment_products.T_DIM_KEY
WHERE
    f_payment_transactions.PAYMENT_TRANSACTION_TYPE != 'REFUND'
    AND (d_merchants.MERCHANT_COUNTRY )= 'LU'
    AND d_payment_products.PRODUCT_CODE IN ('SILVERFLOW_CARDS')
    AND d_payment_transaction_info.PAYMENT_TRANSACTION_CUTOFF_AT >= TIMESTAMP(DATETIME( '2024-10-01 10:34:15.201787', 'Etc/UTC'))   -- +01 for winter time, +02 for summer time
    AND d_payment_transaction_info.PAYMENT_TRANSACTION_CUTOFF_AT <= TIMESTAMP(DATETIME( '2024-10-31 10:34:15.201787', 'Etc/UTC')) -- like timezone('UTC', to_timestamp('2023-12-31 UTC+01', 'YYYY-MM-DD ""UTC""TZH') + interval '1 day') -- +01 for winter time, +02 for summer time
  group by 1,2,3,4,5,6,7,8
union all

SELECT
  'CUCT' as PaymentInstructionType,
  'PSPN' as CustomerType,
  IF(f_payment_transactions.PAYMENT_TRANSACTION_TYPE = 'REFUND', 'DPSP', 'CPSP') as RoleofReporting,
  'PSPN' as Settlementchannel,
  IF(f_payment_transactions.PAYMENT_TRANSACTION_TYPE = 'REFUND', 'LU', d_payment_transaction_info.payment_transaction_payer_country)   as debtorPSPs,
  IF(f_payment_transactions.PAYMENT_TRANSACTION_TYPE = 'REFUND', REFUND.payment_transaction_payer_country, 'LU') as CreditorPSPs,
  IF(f_payment_transactions.PAYMENT_TRANSACTION_TYPE = 'REFUND', REFUND.payment_transaction_currency_code,  d_payment_transaction_info.payment_transaction_currency_code) as currency,
  'Vale' as Metric,
  sum ( f_payment_items.PAYMENT_ITEM_AMOUNT) as ReportedAmount,
  CURRENT_TIMESTAMP AS LOAD_TIMESTAMP,
  ""  AS Period,
FROM `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_PAYMENT_ITEM_INFO_DECRYPTED`
    AS d_payment_item_info
INNER JOIN `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`F_PAYMENT_ITEMS` AS f_payment_items ON d_payment_item_info.T_DIM_KEY = f_payment_items.T_D_PAYMENT_ITEM_INFO_DIM_KEY
LEFT JOIN `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_MERCHANTS_DECRYPTED` AS d_merchants ON f_payment_items.T_D_MERCHANT_DIM_KEY = d_merchants.T_DIM_KEY AND d_merchants.T_SOURCE = "P1_PBEE"
LEFT JOIN `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`F_PAYMENT_TRANSACTIONS_DECRYPTED`  AS f_payment_transactions ON d_payment_item_info.T_DIM_KEY = f_payment_transactions.T_D_PAYMENT_ITEM_INFO_DIM_KEY
LEFT JOIN `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_PAYMENT_TRANSACTION_INFO_DECRYPTED` AS d_payment_transaction_info ON d_payment_transaction_info.T_DIM_KEY =f_payment_transactions.T_D_PAYMENT_TRANSACTION_INFO_DIM_KEY
LEFT JOIN `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_PAYMENT_PRODUCTS` AS d_payment_products ON f_payment_transactions.T_D_PRODUCT_DIM_KEY = d_payment_products.T_DIM_KEY
LEFT JOIN `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_PAYMENT_TRANSACTION_INFO_DECRYPTED` AS REFUND ON REFUND.T_DIM_KEY = f_payment_transactions.REFUND_ORIGINAL_PAYMENT_TRANSACTION_DIM_KEY
WHERE (d_merchants.MERCHANT_COUNTRY )= 'LU'
    AND d_payment_products.PRODUCT_CODE IN ('SILVERFLOW_CARDS')
    AND d_payment_transaction_info.PAYMENT_TRANSACTION_CUTOFF_AT >= TIMESTAMP(DATETIME( '2024-10-01 10:34:15.201787', 'Etc/UTC'))   -- +01 for winter time, +02 for summer time
    AND d_payment_transaction_info.PAYMENT_TRANSACTION_CUTOFF_AT <= TIMESTAMP(DATETIME( '2024-10-31 10:34:15.201787', 'Etc/UTC')) -- like timezone('UTC', to_timestamp('2023-12-31 UTC+01', 'YYYY-MM-DD ""UTC""TZH') + interval '1 day') -- +01 for winter time, +02 for summer time
group by 1,2,3,4,5,6,7,8
union all

SELECT

  'CUCT' as PaymentInstructionType,
  'PSPN' as CustomerType,
    'DPSP' as RoleofReporting,
  'PSPN' as Settlementchannel,
  'LU'  as debtorPSPs,
    REFUND.payment_transaction_payer_country as CreditorPSPs,
    REFUND.payment_transaction_currency_code as currency,
    'Volu' as Metric,
    count(*) as ReportedAmount,
    CURRENT_TIMESTAMP AS LOAD_TIMESTAMP,
    ""  AS Period,

  FROM `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_PAYMENT_ITEM_INFO_DECRYPTED`
      AS d_payment_item_info
  INNER JOIN `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`F_PAYMENT_ITEMS` AS f_payment_items ON d_payment_item_info.T_DIM_KEY = f_payment_items.T_D_PAYMENT_ITEM_INFO_DIM_KEY
  LEFT JOIN `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_MERCHANTS_DECRYPTED` AS d_merchants ON f_payment_items.T_D_MERCHANT_DIM_KEY = d_merchants.T_DIM_KEY AND d_merchants.T_SOURCE = "P1_PBEE"
  LEFT JOIN `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`F_PAYMENT_TRANSACTIONS_DECRYPTED`  AS f_payment_transactions ON d_payment_item_info.T_DIM_KEY = f_payment_transactions.T_D_PAYMENT_ITEM_INFO_DIM_KEY
  LEFT JOIN `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_PAYMENT_TRANSACTION_INFO_DECRYPTED` AS d_payment_transaction_info ON d_payment_transaction_info.T_DIM_KEY =f_payment_transactions.
  T_D_PAYMENT_TRANSACTION_INFO_DIM_KEY
  LEFT JOIN `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_PAYMENT_PRODUCTS` AS d_payment_products ON f_payment_transactions.T_D_PRODUCT_DIM_KEY = d_payment_products.T_DIM_KEY
  LEFT JOIN `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_PAYMENT_TRANSACTION_INFO_DECRYPTED` AS REFUND ON REFUND.T_DIM_KEY = f_payment_transactions.REFUND_ORIGINAL_PAYMENT_TRANSACTION_DIM_KEY
  WHERE
      f_payment_transactions.PAYMENT_TRANSACTION_TYPE = 'REFUND'
      AND (d_merchants.MERCHANT_COUNTRY )= 'LU'
      AND d_payment_products.PRODUCT_CODE IN ('SILVERFLOW_CARDS')
      AND REFUND.PAYMENT_TRANSACTION_CUTOFF_AT >= TIMESTAMP(DATETIME( '2024-10-01 10:34:15.201787', 'Etc/UTC'))   -- +01 for winter time, +02 for summer time
      AND REFUND.PAYMENT_TRANSACTION_CUTOFF_AT <= TIMESTAMP(DATETIME( '2024-10-31 10:34:15.201787', 'Etc/UTC')) -- like timez
    group by 1,2,3,4,5,6,7,8
    union all

SELECT
  'CUCT' as PaymentInstructionType,
  'PSPN' as CustomerType,
    'DPSP' as RoleofReporting,
  'PSPN' as Settlementchannel,
  'LU'  as debtorPSPs,
  REFUND.payment_transaction_payer_country as CreditorPSPs,
  REFUND.payment_transaction_currency_code as currency,
  'Vale' as Metric,
  sum ( f_payment_items.PAYMENT_ITEM_AMOUNT) as ReportedAmount,
  CURRENT_TIMESTAMP AS LOAD_TIMESTAMP,
  ""  AS Period,

FROM `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_PAYMENT_ITEM_INFO_DECRYPTED`
    AS d_payment_item_info
INNER JOIN `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`F_PAYMENT_ITEMS` AS f_payment_items ON d_payment_item_info.T_DIM_KEY = f_payment_items.T_D_PAYMENT_ITEM_INFO_DIM_KEY
LEFT JOIN `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_MERCHANTS_DECRYPTED` AS d_merchants ON f_payment_items.T_D_MERCHANT_DIM_KEY = d_merchants.T_DIM_KEY AND d_merchants.T_SOURCE = "P1_PBEE"
LEFT JOIN `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`F_PAYMENT_TRANSACTIONS_DECRYPTED`  AS f_payment_transactions ON d_payment_item_info.T_DIM_KEY = f_payment_transactions.T_D_PAYMENT_ITEM_INFO_DIM_KEY
LEFT JOIN `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_PAYMENT_TRANSACTION_INFO_DECRYPTED` AS d_payment_transaction_info ON d_payment_transaction_info.T_DIM_KEY =f_payment_transactions.
T_D_PAYMENT_TRANSACTION_INFO_DIM_KEY
LEFT JOIN `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_PAYMENT_PRODUCTS` AS d_payment_products ON f_payment_transactions.T_D_PRODUCT_DIM_KEY = d_payment_products.T_DIM_KEY
LEFT JOIN `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_PAYMENT_TRANSACTION_INFO_DECRYPTED` AS REFUND ON REFUND.T_DIM_KEY = f_payment_transactions.REFUND_ORIGINAL_PAYMENT_TRANSACTION_DIM_KEY
WHERE
    f_payment_transactions.PAYMENT_TRANSACTION_TYPE = 'REFUND'
    AND (d_merchants.MERCHANT_COUNTRY )= 'LU'
    AND d_payment_products.PRODUCT_CODE IN ('SILVERFLOW_CARDS')
    AND REFUND.PAYMENT_TRANSACTION_CUTOFF_AT >= TIMESTAMP(DATETIME( '2024-10-01 10:34:15.201787', 'Etc/UTC'))   -- +01 for winter time, +02 for summer time
    AND REFUND.PAYMENT_TRANSACTION_CUTOFF_AT <= TIMESTAMP(DATETIME( '2024-10-31 10:34:15.201787', 'Etc/UTC')) -- like timez
  group by 1,2,3,4,5,6,7,8
    );
  