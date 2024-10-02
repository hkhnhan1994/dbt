{% set period_time = period_calculate(time = "semesterly", selection_date="today", prefix="", suffix="S" ) -%}
{% set time_zone = "Etc/UTC" -%}
{% set country_code = "BE" -%}

  with pre_source AS(
    SELECT
    'Banqup applications' as TYPEOFAPPLICATION,
    IT.INBOUND_TRANSACTION_CURRENCY_CODE  AS CURRENCY,
    credacc.FINANCIAL_INSTITUTION_COUNTRY_CODE AS COUNTERPARTY_COUNTRY,
    COUNT(*)  AS SUCCESS_TRX_COUNT,
    SUM(IT.INBOUND_TRANSACTION_AMOUNT) AS SUCCESS_TRX_SUMMED_VALUE,
    CURRENT_TIMESTAMP AS LOAD_TIMESTAMP,
    "{{period}}"  AS Period,
    TIMESTAMP(DATETIME( '{{begin_date}}', '{{time_zone}}'))  AS Period_begin_date,
    TIMESTAMP(DATETIME( '{{end_date}}', '{{time_zone}}'))  AS Period_end_date,
  FROM {{ source('source_pst3_strp', 'D_INBOUND_PAYMENT_INFO_CURRENT') }} AS IP
  LEFT JOIN {{ source('source_pst3_strp', 'D_FINANCIAL_PLATFORMS_DECRYPTED') }} AS FP
      ON IP.T_D_FINANCIAL_PLATFORM_DIM_KEY=FP.T_DIM_KEY
  LEFT JOIN {{ source('source_pst3_strp', 'D_FINANCIAL_INSTITUTIONS') }} AS FI
      ON FI.T_DIM_KEY = FP.T_D_FINANCIAL_INSTITUTION_DIM_KEY
  LEFT JOIN {{ source('source_pst3_strp', 'F_INBOUND_TRANSACTIONS_DECRYPTED') }} AS IT
      ON IP.T_DIM_KEY=IT.T_D_INBOUND_PAYMENT_INFO_DIM_KEY
  INNER JOIN {{ source('source_pst3_strp', 'D_INBOUND_TRANSACTION_INFO') }} AS DIT
      ON DIT.T_DIM_KEY = IT.T_D_INBOUND_TRANSACTION_INFO_DIM_KEY
  LEFT JOIN {{ source('source_pst3_strp', 'D_PAYMENT_INITIATIONS') }} AS PI
      ON IP.T_DIM_KEY=PI.T_D_INBOUND_PAYMENT_DIM_KEY
  LEFT JOIN {{ source('source_pst3_strp', 'D_APPLICATIONS_DECRYPTED') }} AS APP
      ON IP.T_D_APPLICATION_DIM_KEY = APP.T_DIM_KEY
  LEFT JOIN {{ source('source_pst3_strp', 'D_BANK_ACCOUNTS_DECRYPTED') }} AS debacc
      ON IP.T_D_DEBTOR_BANK_ACCOUNTS_DIM_KEY = debacc.T_DIM_KEY
  LEFT JOIN {{ source('source_pst3_strp', 'D_BANK_ACCOUNTS_DECRYPTED') }} credacc
      ON IT.T_D_CREDITOR_BANK_ACCOUNTS_DIM_KEY = credacc.T_DIM_KEY
  WHERE FI.FINANCIAL_INSTITUTION_CODE  <> 'IBIS'
      AND PI.PAYMENT_INITIATION_STATUS = 'SUCCESSFUL'
      AND (
          APP.APPLICATION_NAME  = 'PAY-PXG-COMMUNITY'
          OR APP.APPLICATION_NAME = 'PAY-PXG-GOCOMPTA'
          OR (
              APP.APPLICATION_NAME = 'PAY-PXG-JEFACTURE'
              AND (credacc.FINANCIAL_INSTITUTION_COUNTRY_CODE = "FR"
                  AND  SUBSTR( credacc.BANK_ACCOUNT_NUMBER ,5,3) = '504'))
          OR APP.APPLICATION_NAME = 'PAY-PXG-BANQUPLT'
          OR APP.APPLICATION_NAME = 'PAY-PXG-BANQUPBG'
          OR APP.APPLICATION_NAME = 'PAY-PXG-BANQUPHR'
          OR APP.APPLICATION_NAME = 'PAY-PXG-BANQUPRO')
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
        CURRENT_TIMESTAMP AS LOAD_TIMESTAMP,
        "{{period}}"  AS Period,
        TIMESTAMP(DATETIME( '{{begin_date}}', '{{time_zone}}'))  AS Period_begin_date,
        TIMESTAMP(DATETIME( '{{end_date}}', '{{time_zone}}'))  AS Period_end_date,
  FROM {{ source('source_pst3_strp', 'D_INBOUND_PAYMENT_INFO_CURRENT') }} AS IP
  LEFT JOIN {{ source('source_pst3_strp', 'D_FINANCIAL_PLATFORMS_DECRYPTED') }} AS FP
      ON IP.T_D_FINANCIAL_PLATFORM_DIM_KEY=FP.T_DIM_KEY
  LEFT JOIN {{ source('source_pst3_strp', 'D_FINANCIAL_INSTITUTIONS') }} AS FI
      ON FI.T_DIM_KEY = FP.T_D_FINANCIAL_INSTITUTION_DIM_KEY
  LEFT JOIN {{ source('source_pst3_strp', 'F_INBOUND_TRANSACTIONS_DECRYPTED') }} AS IT
      ON IP.T_DIM_KEY = IT.T_D_INBOUND_PAYMENT_INFO_DIM_KEY
  INNER JOIN {{ source('source_pst3_strp', 'D_INBOUND_TRANSACTION_INFO') }} AS DIT
      ON DIT.T_DIM_KEY = IT.T_D_INBOUND_TRANSACTION_INFO_DIM_KEY
  LEFT JOIN {{ source('source_pst3_strp', 'D_PAYMENT_INITIATIONS') }} AS PI
      ON IP.T_DIM_KEY = PI.T_D_INBOUND_PAYMENT_DIM_KEY
  LEFT JOIN {{ source('source_pst3_strp', 'D_APPLICATIONS_DECRYPTED') }} AS APP
      ON IP.T_D_APPLICATION_DIM_KEY = APP.T_DIM_KEY
  LEFT JOIN {{ source('source_pst3_strp', 'D_BANK_ACCOUNTS_DECRYPTED') }} debacc
      ON IP.T_D_DEBTOR_BANK_ACCOUNTS_DIM_KEY = debacc.T_DIM_KEY
  LEFT JOIN {{ source('source_pst3_strp', 'D_BANK_ACCOUNTS_DECRYPTED') }} credacc
      ON IT.T_D_CREDITOR_BANK_ACCOUNTS_DIM_KEY = credacc.T_DIM_KEY

  WHERE
    PI.PAYMENT_INITIATION_STATUS = 'SUCCESSFUL'
    AND APP.APPLICATION_NAME  = 'PAY-PXG-OCS'
    AND FI.FINANCIAL_INSTITUTION_CODE <> 'IBIS'
    AND NOT (credacc.FINANCIAL_INSTITUTION_COUNTRY_CODE = 'FR' AND  SUBSTR( credacc.BANK_ACCOUNT_NUMBER,5,5) = '27933')
    AND NOT (credacc.FINANCIAL_INSTITUTION_COUNTRY_CODE = 'DE' AND  SUBSTR( credacc.BANK_ACCOUNT_NUMBER,5,8) = '50339900')
    AND NOT (credacc.FINANCIAL_INSTITUTION_COUNTRY_CODE = 'EE' AND  SUBSTR( credacc.BANK_ACCOUNT_NUMBER,5,2) = '72')
    AND NOT (credacc.FINANCIAL_INSTITUTION_COUNTRY_CODE = 'ES' AND  SUBSTR( credacc.BANK_ACCOUNT_NUMBER,5,4) = '6918')
    AND NOT (credacc.FINANCIAL_INSTITUTION_COUNTRY_CODE = 'IT' AND  SUBSTR( credacc.BANK_ACCOUNT_NUMBER,5,5) = '36330')
    -- AND NOT (credacc.FINANCIAL_INSTITUTION_COUNTRY_CODE = 'LT' AND  SUBSTR( credacc.BANK_ACCOUNT_NUMBER,5,5) = '39816')
    AND NOT (credacc.FINANCIAL_INSTITUTION_COUNTRY_CODE = 'LU' AND  SUBSTR( credacc.BANK_ACCOUNT_NUMBER,5,3) = '625')
    AND NOT (credacc.FINANCIAL_INSTITUTION_COUNTRY_CODE = 'LV' AND  SUBSTR( credacc.BANK_ACCOUNT_NUMBER,5,4) = 'PANX')
    AND NOT (credacc.FINANCIAL_INSTITUTION_COUNTRY_CODE = 'NL' AND  SUBSTR( credacc.BANK_ACCOUNT_NUMBER,5,4) = 'PANX')
    AND NOT (credacc.FINANCIAL_INSTITUTION_COUNTRY_CODE = 'PT' AND  SUBSTR( credacc.BANK_ACCOUNT_NUMBER,5,4) = '5845')
    AND NOT (credacc.FINANCIAL_INSTITUTION_COUNTRY_CODE = 'SK' AND  SUBSTR( credacc.BANK_ACCOUNT_NUMBER,5,4) = '9955')
    -- AND NOT (credacc.FINANCIAL_INSTITUTION_COUNTRY_CODE = 'RO' AND  SUBSTR( credacc.BANK_ACCOUNT_NUMBER,5,4) = 'PANX')
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
        pre_source.* EXCEPT(PERIOD),
        country,
        PERIOD,
    FROM pre_source
    LEFT JOIN country_mapping on country_mapping.code =pre_source.COUNTERPARTY_COUNTRY

SHOW_QUERY: false

SELECTION_DATE:
  - "2022-01-12"
  - "2022-07-12"
  - "2023-01-12"
  - "2023-07-12"
  - "2024-01-12"
  - "2024-07-12"
  - "2025-01-12"
  # - TODAY

PERIOD:
  TIME: "SEMESTER"
  PREFIX: ""
  SUBFIX: "S"

WRITE_DISPOSITION: WRITE_TRUNCATE # WRITE_TRUNCATE WRITE_APPEND
