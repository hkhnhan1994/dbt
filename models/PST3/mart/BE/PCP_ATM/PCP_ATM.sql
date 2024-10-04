
{% set period_time = period_calculate(time = 'semesterly', selection_date="today", prefix='', suffix='S' ) -%}
{% set time_zone = "Etc/UTC" -%}
{% set country_code = 'BE' -%}


        SELECT
  FCT.CARD_TRANSACTION_MERCHANT_COUNTRY_CODE AS Counterpart_area,
  FCT.CARD_TRANSACTION_MERCHANT_COUNTRY_CODE AS POS_location,
  FCT.CARD_TRANSACTION_PAYMENT_CHANNEL AS remote_non_remote_initiation,
  COUNT(*) AS number_of_transactions,
  SUM(FCT.card_transaction_cleared_amount) + SUM(FCT.card_transaction_refunded_amount) AS total_sum,
  "{{period}}"  AS Period,
  TIMESTAMP(DATETIME( '{{period_time['begin_date']}}', '{{time_zone}}'))  AS Period_begin_date,
  TIMESTAMP(DATETIME( '{{period_time['end_date']}}', '{{time_zone}}'))  AS Period_end_date,
  CURRENT_TIMESTAMP AS LOAD_TIMESTAMP,
FROM {{ source('source_dwh_STRP','F_CI_CARD_TRANSACTION_DECRYPTED') }}  FCT
INNER JOIN {{ source('source_dwh_STRP','D_CI_CARD') }} C on FCT.T_D_CI_CARD_DIM_KEY = C.T_DIM_KEY
INNER JOIN {{ source('source_dwh_STRP','D_CI_CARD_TRANSACTION') }} DCT on DCT.T_DIM_KEY = FCT.T_D_CI_CARD_TRANSACTION_DIM_KEY
INNER JOIN {{ source('source_dwh_STRP','F_CI_CARD_TRANSACTION_EVENT') }} FTE on FTE.T_D_CI_CARD_TRANSACTION_DIM_KEY = DCT.T_DIM_KEY
INNER JOIN {{ source('source_dwh_STRP','D_CI_CARD_PRODUCT_DECRYPTED') }} CP on C.T_D_CI_CARD_PRODUCT_DIM_KEY = CP.T_DIM_KEY
WHERE
FCT.CARD_TRANSACTION_PAYMENT_CHANNEL = 'ATM'
AND CP.CARD_PRODUCT_CARD_COUNTRY = 'BE'
-- AND FCT.CARD_TRANSACTION_USER_TIME >= TIMESTAMP(DATETIME( '{{period_time['begin_date']}}', '{{time_zone}}'))
-- AND FCT.CARD_TRANSACTION_USER_TIME <= TIMESTAMP(DATETIME( '{{period_time['end_date']}}', '{{time_zone}}'))
GROUP BY 1,2,3
