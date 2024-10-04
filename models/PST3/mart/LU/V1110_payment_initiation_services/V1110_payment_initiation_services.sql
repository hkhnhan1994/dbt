
{% set period_time = period_calculate(time = 'monthly', selection_date="today", prefix='', suffix='M' ) -%}
{% set time_zone = "Etc/UTC" -%}
{% set country_code = 'LU' -%}


        with pre_source AS(
  SELECT
  'Banqup applications' as TYPEOFAPPLICATION,
  IT.INBOUND_TRANSACTION_CURRENCY_CODE  AS CURRENCY,
  credacc.FINANCIAL_INSTITUTION_COUNTRY_CODE AS COUNTERPARTY_COUNTRY,
  COUNT(*)  AS SUCCESS_TRX_COUNT,
  SUM(IT.INBOUND_TRANSACTION_AMOUNT) AS SUCCESS_TRX_SUMMED_VALUE,
FROM {{ source('source_dwh_STRP','D_INBOUND_PAYMENT_INFO_CURRENT') }} AS IP
LEFT JOIN {{ source('source_dwh_STRP','D_FINANCIAL_PLATFORMS_DECRYPTED') }} AS FP
    ON IP.T_D_FINANCIAL_PLATFORM_DIM_KEY=FP.T_DIM_KEY
LEFT JOIN {{ source('source_dwh_STRP','D_FINANCIAL_INSTITUTIONS') }} AS FI
    ON FI.T_DIM_KEY = FP.T_D_FINANCIAL_INSTITUTION_DIM_KEY
LEFT JOIN {{ source('source_dwh_STRP','F_INBOUND_TRANSACTIONS_DECRYPTED') }} AS IT
    ON IP.T_DIM_KEY=IT.T_D_INBOUND_PAYMENT_INFO_DIM_KEY
INNER JOIN {{ source('source_dwh_STRP','D_INBOUND_TRANSACTION_INFO') }} AS DIT
    ON DIT.T_DIM_KEY = IT.T_D_INBOUND_TRANSACTION_INFO_DIM_KEY
LEFT JOIN {{ source('source_dwh_STRP','D_PAYMENT_INITIATIONS') }} AS PI
    ON IP.T_DIM_KEY=PI.T_D_INBOUND_PAYMENT_DIM_KEY
LEFT JOIN {{ source('source_dwh_STRP','D_APPLICATIONS_DECRYPTED') }} AS APP
    ON IP.T_D_APPLICATION_DIM_KEY = APP.T_DIM_KEY
LEFT JOIN {{ source('source_dwh_STRP','D_BANK_ACCOUNTS_DECRYPTED') }} AS debacc
    ON IP.T_D_DEBTOR_BANK_ACCOUNTS_DIM_KEY = debacc.T_DIM_KEY
LEFT JOIN {{ source('source_dwh_STRP','D_BANK_ACCOUNTS_DECRYPTED') }} credacc
    ON IT.T_D_CREDITOR_BANK_ACCOUNTS_DIM_KEY = credacc.T_DIM_KEY
WHERE FI.FINANCIAL_INSTITUTION_CODE  <> 'IBIS'
    AND PI.PAYMENT_INITIATION_STATUS = 'SUCCESSFUL'
    AND (
        APP.APPLICATION_NAME  = '	PAY-PXG-BANQUPLU')
    AND TIMESTAMP(DIT.INBOUND_TRANSACTION_CREATED_AT) >= TIMESTAMP(DATETIME( '{{begin_date}}', '{{time_zone}}'))  --pt winter time
    AND TIMESTAMP(DIT.INBOUND_TRANSACTION_CREATED_AT) <= TIMESTAMP(DATETIME( '{{end_date}}', '{{time_zone}}'))   --pt winter time


GROUP BY 1, 2, 3

UNION ALL

SELECT
      'OCS application' AS TYPEOFAPPLICATION,
      IT.INBOUND_TRANSACTION_CURRENCY_CODE AS CURRENCY,
      credacc.FINANCIAL_INSTITUTION_COUNTRY_CODE AS COUNTERPARTY_COUNTRY,
      COUNT(*)  AS SUCCESS_TRX_COUNT,
      SUM(IT.INBOUND_TRANSACTION_AMOUNT) AS SUCCESS_TRX_SUMMED_VALUE,
FROM {{ source('source_dwh_STRP','D_INBOUND_PAYMENT_INFO_CURRENT') }} AS IP
LEFT JOIN {{ source('source_dwh_STRP','D_FINANCIAL_PLATFORMS_DECRYPTED') }} AS FP
    ON IP.T_D_FINANCIAL_PLATFORM_DIM_KEY=FP.T_DIM_KEY
LEFT JOIN {{ source('source_dwh_STRP','D_FINANCIAL_INSTITUTIONS') }} AS FI
    ON FI.T_DIM_KEY = FP.T_D_FINANCIAL_INSTITUTION_DIM_KEY
LEFT JOIN {{ source('source_dwh_STRP','F_INBOUND_TRANSACTIONS_DECRYPTED') }} AS IT
    ON IP.T_DIM_KEY = IT.T_D_INBOUND_PAYMENT_INFO_DIM_KEY
INNER JOIN {{ source('source_dwh_STRP','D_INBOUND_TRANSACTION_INFO') }} AS DIT
    ON DIT.T_DIM_KEY = IT.T_D_INBOUND_TRANSACTION_INFO_DIM_KEY
LEFT JOIN {{ source('source_dwh_STRP','D_PAYMENT_INITIATIONS') }} AS PI
    ON IP.T_DIM_KEY = PI.T_D_INBOUND_PAYMENT_DIM_KEY
LEFT JOIN {{ source('source_dwh_STRP','D_APPLICATIONS_DECRYPTED') }} AS APP
    ON IP.T_D_APPLICATION_DIM_KEY = APP.T_DIM_KEY
LEFT JOIN {{ source('source_dwh_STRP','D_BANK_ACCOUNTS_DECRYPTED') }} debacc
    ON IP.T_D_DEBTOR_BANK_ACCOUNTS_DIM_KEY = debacc.T_DIM_KEY
LEFT JOIN {{ source('source_dwh_STRP','D_BANK_ACCOUNTS_DECRYPTED') }} credacc
    ON IT.T_D_CREDITOR_BANK_ACCOUNTS_DIM_KEY = credacc.T_DIM_KEY

WHERE
  PI.PAYMENT_INITIATION_STATUS = 'SUCCESSFUL'
  AND APP.APPLICATION_NAME  = 'PAY-PXG-OCS'
  AND FI.FINANCIAL_INSTITUTION_CODE <> 'IBIS'
  AND NOT (credacc.FINANCIAL_INSTITUTION_COUNTRY_CODE = 'LU' AND  SUBSTR( credacc.BANK_ACCOUNT_NUMBER,5,3) = '625')
  AND TIMESTAMP(DIT.INBOUND_TRANSACTION_CREATED_AT) >= TIMESTAMP(DATETIME( '{{begin_date}}', '{{time_zone}}'))  --pt winter time
  AND TIMESTAMP(DIT.INBOUND_TRANSACTION_CREATED_AT) <= TIMESTAMP(DATETIME( '{{end_date}}', '{{time_zone}}'))   --pt winter time

GROUP BY 1, 2, 3
ORDER BY 4 DESC
),
  country_mapping AS (
      SELECT 'Bulgaria' AS country, 'BG' AS code UNION ALL
      SELECT 'Croatia', 'HR' UNION ALL
      SELECT 'Cyprus', 'CY' UNION ALL
      SELECT 'Czechia', 'CZ' UNION ALL
      SELECT 'Denmark', 'DK' UNION ALL
      SELECT 'Estonia', 'EE' UNION ALL
      SELECT 'Finland', 'FI' UNION ALL
      SELECT 'France', 'FR' UNION ALL
      SELECT 'Germany', 'DE' UNION ALL
      SELECT 'Greece', 'GR' UNION ALL
      SELECT 'Hungary', 'HU' UNION ALL
      SELECT 'Ireland', 'IE' UNION ALL
      SELECT 'Italy', 'IT' UNION ALL
      SELECT 'Latvia', 'LV' UNION ALL
      SELECT 'Lithuania', 'LT' UNION ALL
      SELECT 'Luxembourg', 'LU' UNION ALL
      SELECT 'Malta', 'MT' UNION ALL
      SELECT 'Netherlands', 'NL' UNION ALL
      SELECT 'Poland', 'PL' UNION ALL
      SELECT 'Portugal', 'PT' UNION ALL
      SELECT 'Romania', 'RO' UNION ALL
      SELECT 'Slovakia', 'SK' UNION ALL
      SELECT 'Slovenia', 'SI' UNION ALL
      SELECT 'Spain', 'ES' UNION ALL
      SELECT 'Sweden', 'SE' UNION ALL
      SELECT 'Iceland', 'IS' UNION ALL
      SELECT 'Liechtenstein', 'LI' UNION ALL
      SELECT 'Norway', 'NO' UNION ALL
      SELECT 'Belgium', 'BE' UNION ALL
      SELECT 'Extra EEA', ''

  )
  SELECT
      pre_source.*,
      country,
      CURRENT_TIMESTAMP AS LOAD_TIMESTAMP,
      CAST ("{{period}}" AS STRING) AS Period,
      TIMESTAMP(DATETIME( '{{begin_date}}', '{{time_zone}}'))  AS Period_begin_date,
      TIMESTAMP(DATETIME( '{{end_date}}', '{{time_zone}}'))  AS Period_end_date,
  FROM pre_source
  LEFT JOIN country_mapping on country_mapping.code =pre_source.COUNTERPARTY_COUNTRY
