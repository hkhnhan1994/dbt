{% set period_time = period_calculate(time = "semesterly", selection_date="today", prefix="", suffix="S" ) -%}
{% set time_zone = "Etc/UTC" -%}
{% set country_code = "BE" -%}

 with pre as (
  SELECT
    FCT.CARD_TRANSACTION_MERCHANT_COUNTRY_CODE as Counterpart_area,
    FCT.CARD_TRANSACTION_MERCHANT_COUNTRY_CODE as POS_location,
    CASE
      WHEN  FCT.CARD_TRANSACTION_PAYMENT_CHANNEL = 'OTHER' then 'non-remote'
      WHEN  FCT.CARD_TRANSACTION_PAYMENT_CHANNEL = 'ECOMMERCE' then 'remote'
      WHEN  FCT.CARD_TRANSACTION_PAYMENT_CHANNEL IS NULL then 'non-remote'
      ELSE 'check the query' end as remote_non_remote_initiation,
    CASE
      WHEN  FCT.CARD_TRANSACTION_PAYMENT_CHANNEL = 'ECOMMERCE'
            AND FCT.CARD_TRANSACTION_AUTHENTICATION_METHOD = 'CHALLENGE'
            AND FCT.CARD_TRANSACTION_AUTHENTICATION_STATUS =  'SUCCESSFUL'
            then 'SCA is used'
      WHEN  FCT.CARD_TRANSACTION_PAYMENT_CHANNEL = 'ECOMMERCE'
            AND FCT.CARD_TRANSACTION_AUTHENTICATION_METHOD = 'FRICTIONLESS'
            AND FCT.CARD_TRANSACTION_AUTHENTICATION_STATUS =  'EXEMPTED'
            then 'non-SCA is used'
      WHEN  FCT.CARD_TRANSACTION_PAYMENT_CHANNEL = 'ECOMMERCE'
            AND FCT.CARD_TRANSACTION_AUTHENTICATION_STATUS  in('UNAVAILABLE', 'THREEDS_REQUESTER_SCA_EXEMPTION')
            or FCT.CARD_TRANSACTION_AUTHENTICATION_STATUS is null
            then 'non-SCA is used'
      WHEN  FCT.CARD_TRANSACTION_PAYMENT_CHANNEL = 'OTHER'
            AND FCT.CARD_TRANSACTION_PIN_PRESENT is false
            then 'non-SCA is used'
      WHEN  FCT.CARD_TRANSACTION_PAYMENT_CHANNEL = 'OTHER'
                AND FCT.CARD_TRANSACTION_PIN_PRESENT is true
                then 'SCA is used'
    END AS SCA_used,
    CASE
      WHEN  FCT.CARD_TRANSACTION_PAYMENT_CHANNEL = 'ECOMMERCE'
            AND FCT.CARD_TRANSACTION_AUTHENTICATION_METHOD = 'CHALLENGE'
            AND FCT.CARD_TRANSACTION_AUTHENTICATION_STATUS =  'SUCCESS'
            then 'N/A'
      WHEN  FCT.CARD_TRANSACTION_PAYMENT_CHANNEL = 'ECOMMERCE'
            AND FCT.CARD_TRANSACTION_AUTHENTICATION_METHOD = 'FRICTIONLESS'
            AND FCT.CARD_TRANSACTION_AUTHENTICATION_STATUS =  'EXEMPTED'
            then 'low value'
      WHEN  FCT.CARD_TRANSACTION_PAYMENT_CHANNEL = 'ECOMMERCE'
            AND FCT.CARD_TRANSACTION_AUTHENTICATION_STATUS IN('UNAVAILABLE' ,'THREEDS_REQUESTER_SCA_EXEMPTION')
            AND FCT.CARD_TRANSACTION_ACQUIRER_EXEMPTION = 'RECURRING_PAYMENT'
            then  'Recurring payment'
      WHEN  FCT.CARD_TRANSACTION_PAYMENT_CHANNEL = 'ECOMMERCE'
            AND FCT.CARD_TRANSACTION_AUTHENTICATION_STATUS IN('UNAVAILABLE' ,'THREEDS_REQUESTER_SCA_EXEMPTION')
            AND FCT.CARD_TRANSACTION_ACQUIRER_EXEMPTION = 'MERCHANT_INITIATED_TRANSACTION'
            then  'Merchant initiated transaction (MIT)'
      WHEN  FCT.CARD_TRANSACTION_PAYMENT_CHANNEL = 'ECOMMERCE'
            AND FCT.CARD_TRANSACTION_AUTHENTICATION_STATUS IN('UNAVAILABLE' ,'THREEDS_REQUESTER_SCA_EXEMPTION')
            AND FCT.CARD_TRANSACTION_ACQUIRER_EXEMPTION = 'TRANSACTION_RISK_ANALYSIS'
            then  'Transaction risk analysis'
      WHEN  FCT.CARD_TRANSACTION_PAYMENT_CHANNEL = 'ECOMMERCE'
            AND FCT.CARD_TRANSACTION_AUTHENTICATION_STATUS IN('UNAVAILABLE' ,'THREEDS_REQUESTER_SCA_EXEMPTION')
            AND FCT.CARD_TRANSACTION_ACQUIRER_EXEMPTION = 'SECURE_CORPORATE_PAYMENT'
            then  'Secure corporate payment processes AND protocols'
      WHEN  FCT.CARD_TRANSACTION_PAYMENT_CHANNEL = 'ECOMMERCE'
            AND FCT.CARD_TRANSACTION_AUTHENTICATION_STATUS IN('UNAVAILABLE' ,'THREEDS_REQUESTER_SCA_EXEMPTION')
            then  'Others'
      WHEN  FCT.CARD_TRANSACTION_PAYMENT_CHANNEL = 'OTHER'
            AND FCT.CARD_TRANSACTION_PAN_ENTRY_MODE =  'CHIP_CONTACTLESS'
            AND FCT.CARD_TRANSACTION_PIN_PRESENT is false
            then 'Contactless low value'
      WHEN  FCT.CARD_TRANSACTION_PAYMENT_CHANNEL = 'OTHER'
            AND FCT.CARD_TRANSACTION_PIN_PRESENT  is true then 'N/A'
      END AS SCA_Exemption_reason,
      count(*) as number_of_transactions	,
      sum (FCT.card_transaction_cleared_amount) + sum(FCT.card_transaction_refunded_amount) as total_sum,
  FROM {{ source('source_pst3_strp', 'F_CI_CARD_TRANSACTION_DECRYPTED') }}  FCT
  LEFT JOIN {{ source('source_pst3_strp', 'D_CI_CARD') }} C on FCT.T_D_CI_CARD_DIM_KEY = C.T_DIM_KEY
  LEFT JOIN {{ source('source_pst3_strp', 'D_CI_CARD_TRANSACTION') }} DCT on DCT.T_DIM_KEY = FCT.T_D_CI_CARD_TRANSACTION_DIM_KEY
  LEFT JOIN {{ source('source_pst3_strp', 'F_CI_CARD_TRANSACTION_EVENT') }} FTE on FTE.T_D_CI_CARD_TRANSACTION_DIM_KEY = DCT.T_DIM_KEY
  LEFT JOIN {{ source('source_pst3_strp', 'D_CI_CARD_TRANSACTION_EVENT') }} DTE on FTE.T_D_CI_CARD_TRANSACTION_EVENT_DIM_KEY = DTE.T_DIM_KEY
  LEFT JOIN {{ source('source_pst3_strp', 'D_CI_CARD_PRODUCT_DECRYPTED') }} CP on C.T_D_CI_CARD_PRODUCT_DIM_KEY = CP.T_DIM_KEY
  WHERE FCT.CARD_TRANSACTION_USER_TIME >= TIMESTAMP(DATETIME( '{{begin_date}}', '{{time_zone}}'))
      AND FCT.CARD_TRANSACTION_USER_TIME <= TIMESTAMP(DATETIME( '{{end_date}}', '{{time_zone}}'))
      AND FTE.TRANSACTION_EVENT_TYPE in ('authorization','refund.authorization')
      AND FCT.CARD_TRANSACTION_CLEARED_AMOUNT > 0
      AND DTE.TRANSACTION_EVENT_STATUS = 'COMPLETED'
      AND FCT.CARD_TRANSACTION_PAYMENT_CHANNEL <> 'ATM'
      AND CP.CARD_PRODUCT_CARD_COUNTRY = '{{country_code}}'
      group by 1,2,3,4,5
  ),
  NBB_onegate as (
        SELECT 'low value' AS sca_reason, 201 AS sca_reason_code
        UNION ALL
        SELECT 'Contactless low value', 202
        UNION ALL
        SELECT 'Trusted beneficiaries', 204
        UNION ALL
        SELECT 'Recurring Payment', 205
        UNION ALL
        SELECT 'Unattended terminal for transport fares or parking fees', 206
        UNION ALL
        SELECT 'Secure corporate payment processes and protocols', 207
        UNION ALL
        SELECT 'Transaction risk analysis', 208
        UNION ALL
        SELECT 'Merchant initiated transaction (MIT)', 209
        UNION ALL
        SELECT 'Others', 210
        UNION ALL
        SELECT 'N/A', 100
  )
  SELECT
    IF(pre.Counterpart_area = 'BE','W2',
          IF(map_area.code is null,'G1', pre.Counterpart_area)) AS Counterpart_area_GEO3,
    IF(pre.POS_location = 'BE','W2',
          IF(map_location.code is null,'G1', pre.POS_location)) AS POS_location_GEO3,
    'CPO' as Transaction_type,
    '2000' as Initiation_channel,
    if (pre.remote_non_remote_initiation = 'non-remote', 'NR', 'R') AS remoteness,
    'PCS_ALL' as Scheme,
    '11' as Card_function,
    '_Z' as Card_function_irrelevant,
    NBB_onegate.sca_reason_code as SCA,
    '_X' as SCA_irrelevant,
    '_Z'as Fraud_type_irrelevant,
    pre.number_of_transactions,
    pre.total_sum,
    CURRENT_TIMESTAMP AS LOAD_TIMESTAMP,
    "{{period}}"  AS Period,
  FROM pre
  LEFT JOIN PPST_BE.data_mart.PST3_MAPPING map_area
    ON map_area.code = pre.Counterpart_area
    AND map_area.type = 'Geographical area'
  LEFT JOIN PPST_BE.data_mart.PST3_MAPPING map_location
    ON map_location.code = pre.POS_location
    AND map_location.type = 'Geographical area'
  LEFT JOIN NBB_onegate on  NBB_onegate.sca_reason = pre.SCA_Exemption_reason

