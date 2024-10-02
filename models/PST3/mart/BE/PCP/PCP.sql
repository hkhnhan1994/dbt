{% set period_time = period_calculate(time = "semesterly", selection_date="today", prefix="", suffix="S" ) -%}
{% set time_zone = "Etc/UTC" -%}
{% set country_code = "BE" -%}

  SELECT
    d_payment_products.PRODUCT_CODE AS PRODUCT,
    d_payment_transaction_info.PAYMENT_TRANSACTION_PAYER_COUNTRY  AS PAYERCOUNTRY,
    d_merchants.MERCHANT_COUNTRY  AS MERCHANTCOUNTRY,
      CASE
          WHEN d_payment_products.PRODUCT_CODE = 'BANCONTACT_COLLECT' THEN 'Bancontact'
          WHEN d_payment_products.PRODUCT_CODE = 'SILVERFLOW_CARDS' AND f_payment_transactions.CARD_NETWORK = 'MASTERCARD' THEN 'Mastercard'
          WHEN d_payment_products.PRODUCT_CODE = 'SILVERFLOW_CARDS' AND f_payment_transactions.CARD_NETWORK = 'VISA' THEN 'Visa'
      END AS PCP_TYPE,
      CASE
          WHEN d_payment_products.PRODUCT_CODE = 'BANCONTACT_COLLECT' AND f_payment_transactions.PAYMENT_TRANSACTION_WIP_TYPE = 'BANCONTACT' THEN 'WIP transaction'
          WHEN d_payment_products.PRODUCT_CODE = 'BANCONTACT_COLLECT' AND f_payment_transactions.PAYMENT_TRANSACTION_WIP_TYPE = 'NONE' THEN 'single transaction'
          WHEN d_payment_products.PRODUCT_CODE = 'SILVERFLOW_CARDS' AND f_payment_transactions.PAYMENT_TRANSACTION_WIP_TYPE = 'CREDITCARD' THEN 'WIP transaction'
          WHEN d_payment_products.PRODUCT_CODE = 'SILVERFLOW_CARDS' AND f_payment_transactions.PAYMENT_TRANSACTION_WIP_TYPE = 'NONE' THEN 'single transaction'
      END AS SINGLEWIP,
      d_payment_transaction_info.T_SOURCE_PK_UUID  AS  TRANSACTION_PUBLIC_IDENTIFIER,
      f_payment_transactions.CARD_COUNTRY  AS CARD_COUNTRY,
      f_payment_transactions.CARD_NETWORK_AUTHORIZATION_CODE  AS NETWORK_CODE,
      f_payment_transactions.SCA_RESULT  AS SCA_RESULT,
    CASE
      WHEN d_payment_products.PRODUCT_CODE = 'BANCONTACT_COLLECT' THEN
        IF(f_payment_transactions.PAYMENT_TRANSACTION_WIP_TYPE = 'BANCONTACT','non-SCA used: reason is merchant initiated transaction (MIT)','SCA used')
      WHEN d_payment_products.PRODUCT_CODE = 'SILVERFLOW_CARDS' THEN
        IF (f_payment_transactions.PAYMENT_TRANSACTION_WIP_TYPE = 'CREDITCARD', 'non-SCA used: reason is merchant initiated transaction (MIT)',map.SCA_REASON )
      END AS SCA_REASON,
    f_payment_transactions.MASTERCARD_PRODUCT_ID  AS LICENSEDPRODUCTID,
    f_payment_transactions.MASTERCARD_PRODUCT_CATEGORY_CODE AS PRODUCTCATEGORYCODE,
    f_payment_transactions.MASTERCARD_PRODUCT_CATEGORY_DESCRIPTION  AS  PRODUCTCATEGORYDESCRIPTION,
    CASE
      WHEN d_payment_products.PRODUCT_CODE = 'BANCONTACT_COLLECT' THEN 'Debit'
      WHEN d_payment_products.PRODUCT_CODE = 'SILVERFLOW_CARDS' THEN
        IF(
          f_payment_transactions.CARD_NETWORK = 'MASTERCARD', --condition
          --true ('MASTERCARD')
          IF(f_payment_transactions.MASTERCARD_PRODUCT_CATEGORY_DESCRIPTION = 'Prepaid',
            'Debit',
            f_payment_transactions.MASTERCARD_PRODUCT_CATEGORY_DESCRIPTION
            ),
          --false (!='MASTERCARD')
          IF(
              f_payment_transactions.CARD_NETWORK = 'VISA',
              IF( f_payment_transactions.CARD_FUNDING_SOURCE = 'Prepaid',
                'Debit',
                F_Payment_transactions.CARD_FUNDING_SOURCE
              ),
              'NA'
            )
            )
      ELSE 'NA'
      END AS CARDFUNCTION,
    f_payment_transactions.CARD_FUNDING_SOURCE  AS ACCOUNTFUNDINGSOURCE,
    d_merchants.T_SOURCE_PK_UUID AS MERCHANT_PUBLIC_IDENTIFIER,
    f_payment_items.PAYMENT_ITEM_AMOUNT  AS AMOUNT,
    d_payment_item_info.PAYMENT_ITEM_CURRENCY_CODE AS CURRENCY,
    f_payment_transactions.CARD_NETWORK,
    CURRENT_TIMESTAMP AS LOAD_TIMESTAMP,
    "{{period}}"  AS Period,
    TIMESTAMP(DATETIME( '{{begin_date}}', '{{time_zone}}'))  AS Period_begin_date,
    TIMESTAMP(DATETIME( '{{end_date}}', '{{time_zone}}'))  AS Period_end_date,
  FROM {{ source('source_pst3_strp', 'D_PAYMENT_ITEM_INFO_DECRYPTED') }}
      AS d_payment_item_info
  INNER JOIN {{ source('source_pst3_strp', 'F_PAYMENT_ITEMS') }} AS f_payment_items ON d_payment_item_info.T_DIM_KEY = f_payment_items.T_D_PAYMENT_ITEM_INFO_DIM_KEY
  LEFT JOIN {{ source('source_pst3_strp', 'D_MERCHANTS_DECRYPTED') }} AS d_merchants ON f_payment_items.T_D_MERCHANT_DIM_KEY = d_merchants.T_DIM_KEY AND d_merchants.T_SOURCE = "P1_PBEE"
  LEFT JOIN {{ source('source_pst3_strp', 'F_PAYMENT_TRANSACTIONS_DECRYPTED') }}  AS f_payment_transactions ON d_payment_item_info.T_DIM_KEY = f_payment_transactions.T_D_PAYMENT_ITEM_INFO_DIM_KEY
  LEFT JOIN {{ source('source_pst3_strp', 'D_PAYMENT_TRANSACTION_INFO_DECRYPTED') }} AS d_payment_transaction_info ON d_payment_transaction_info.T_DIM_KEY =f_payment_transactions.T_D_PAYMENT_TRANSACTION_INFO_DIM_KEY
  LEFT JOIN {{ source('source_pst3_strp', 'D_PAYMENT_PRODUCTS') }} AS d_payment_products ON f_payment_transactions.T_D_PRODUCT_DIM_KEY = d_payment_products.T_DIM_KEY
  LEFT JOIN {{ref('SCA_MAPPING_TABLE') }} AS map  ON f_payment_transactions.SCA_RESULT = map.SCA_RESULT
  WHERE (d_merchants.MERCHANT_COUNTRY ) IS NOT NULL
      AND d_payment_products.PRODUCT_CODE IN ('BANCONTACT_COLLECT', 'SILVERFLOW_CARDS')
      AND d_payment_transaction_info.PAYMENT_TRANSACTION_CUTOFF_AT >= TIMESTAMP(DATETIME( '{{begin_date}}', '{{time_zone}}'))
      AND d_payment_transaction_info.PAYMENT_TRANSACTION_CUTOFF_AT <= TIMESTAMP(DATETIME( '{{end_date}}', '{{time_zone}}'))
