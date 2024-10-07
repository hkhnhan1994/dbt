SELECT
  COALESCE( nullif( d_payment_transaction_info.PAYMENT_TRANSACTION_PAYER_COUNTRY, 'NA'), left(dl_trans_pro.value,2))  AS PAYER_COUNTRY,
  d_payment_item_info.PAYMENT_ITEM_CURRENCY_CODE AS CURRENCY,
  COUNT(DISTINCT f_payment_items.T_FACT_KEY) AS VOLUME,
  ROUND(SUM(f_payment_items.PAYMENT_ITEM_AMOUNT)) AS AMOUNT,
  CURRENT_TIMESTAMP AS LOAD_TIMESTAMP,
  ""  AS Period,
  TIMESTAMP(DATETIME( '2024-01-01', 'Etc/UTC'))  AS Period_begin_date,
  TIMESTAMP(DATETIME( '2024-06-30', 'Etc/UTC'))  AS Period_end_date,
FROM `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_PAYMENT_ITEM_INFO_DECRYPTED`
    AS d_payment_item_info
LEFT JOIN `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`F_PAYMENT_TRANSACTIONS`
    AS f_payment_transactions ON d_payment_item_info.T_DIM_KEY = f_payment_transactions.T_D_PAYMENT_ITEM_INFO_DIM_KEY
LEFT JOIN `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_PAYMENT_PRODUCTS`
    AS d_payment_products ON f_payment_transactions.T_D_PRODUCT_DIM_KEY = d_payment_products.T_DIM_KEY
INNER JOIN `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`F_PAYMENT_ITEMS`   AS f_payment_items ON d_payment_item_info.T_DIM_KEY = f_payment_items.T_D_PAYMENT_ITEM_INFO_DIM_KEY
LEFT JOIN `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_MERCHANTS_DECRYPTED`
    AS d_merchants ON f_payment_items.T_D_MERCHANT_DIM_KEY = d_merchants.T_DIM_KEY
LEFT JOIN `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_PAYMENT_TRANSACTION_INFO_DECRYPTED`
    AS d_payment_transaction_info ON d_payment_transaction_info.T_DIM_KEY =f_payment_transactions.T_D_PAYMENT_TRANSACTION_INFO_DIM_KEY
LEFT JOIN (
SELECT
  transaction_id,
  key,
  AEAD.DECRYPT_STRING(k0.KEYSET, value, TO_HEX(_flycs_metadata_keyset_0)) value,
  from `pj-bu-dw-dl-prod`.`prd_datalake_P1_PSTA`.`dwh_transaction_properties_current`
  left join `pj-bu-dw-ks-prod`.`prd_keysets`.`merged` k0
  on lower(k0.SOURCE) = "p1_psta_dwh_transaction_properties"
  and _flycs_metadata_keyset_0 = k0.HASH_ID
  WHERE key = "PAYER_ACCOUNT_ID"
) as dl_trans_pro
 on cast (dl_trans_pro.transaction_id as string) = d_payment_transaction_info.T_SOURCE_PK_ID
WHERE d_merchants.MERCHANT_COUNTRY  IS NOT NULL
    and d_payment_products.PRODUCT_CODE = "IDEAL_COLLECT"
    and d_payment_transaction_info.payment_transaction_cutoff_at >= TIMESTAMP(DATETIME( '2024-01-01', 'Etc/UTC'))   -- start of reporting period
    and d_payment_transaction_info.payment_transaction_cutoff_at <= TIMESTAMP(DATETIME( '2024-06-30', 'Etc/UTC'))   -- end of reporting period
GROUP BY
    1,
    2