SELECT
  'Banqup applications' as TypeOfApplication,
  credacc.FINANCIAL_INSTITUTION_COUNTRY_CODE AS counterparty_country,
  IT.INBOUND_TRANSACTION_CURRENCY_CODE  AS currency,
  COUNT(*)  AS success_trx_count,
  SUM(IT.INBOUND_TRANSACTION_AMOUNT) AS success_trx_summed_value,
  CURRENT_TIMESTAMP AS LOAD_TIMESTAMP,
  '{{period}}' AS PERIOD,
FROM {{ source('source_dwh_strp,D_INBOUND_PAYMENT_INFO_CURRENT') }} AS IP
LEFT JOIN {{ source('source_dwh_strp,D_FINANCIAL_PLATFORMS_DECRYPTED') }} AS FP
    ON IP.T_D_FINANCIAL_PLATFORM_DIM_KEY=FP.T_DIM_KEY
LEFT JOIN {{ source('source_dwh_strp,D_FINANCIAL_INSTITUTIONS') }} AS FI
    ON FI.T_DIM_KEY = FP.T_D_FINANCIAL_INSTITUTION_DIM_KEY
LEFT JOIN {{ source('source_dwh_strp,F_INBOUND_TRANSACTIONS_DECRYPTED') }} AS IT
    ON IP.T_DIM_KEY=IT.T_D_INBOUND_PAYMENT_INFO_DIM_KEY
INNER JOIN {{ source('source_dwh_strp,D_INBOUND_TRANSACTION_INFO') }} AS DIT
    ON DIT.T_DIM_KEY = IT.T_D_INBOUND_TRANSACTION_INFO_DIM_KEY
LEFT JOIN {{ source('source_dwh_strp,D_PAYMENT_INITIATIONS') }} AS PI
    ON IP.T_DIM_KEY=PI.T_D_INBOUND_PAYMENT_DIM_KEY
LEFT JOIN {{ source('source_dwh_strp,D_APPLICATIONS_DECRYPTED') }} AS APP
    ON IP.T_D_APPLICATION_DIM_KEY = APP.T_DIM_KEY
LEFT JOIN {{ source('source_dwh_strp,D_BANK_ACCOUNTS_DECRYPTED') }} AS debacc
    ON IP.T_D_DEBTOR_BANK_ACCOUNTS_DIM_KEY = debacc.T_DIM_KEY
LEFT JOIN {{ source('source_dwh_strp,D_BANK_ACCOUNTS_DECRYPTED') }} credacc
    ON IT.T_D_CREDITOR_BANK_ACCOUNTS_DIM_KEY = credacc.T_DIM_KEY
WHERE FI.FINANCIAL_INSTITUTION_CODE  <> 'IBIS'
    AND PI.PAYMENT_INITIATION_STATUS = 'SUCCESSFUL'
    AND APP.APPLICATION_NAME = 'PAY-PXG-BANQUPLV'
    AND TIMESTAMP(DIT.INBOUND_TRANSACTION_CREATED_AT) >= TIMESTAMP(DATETIME( '{{period_time['begin_date']}}', '{{time_zone}}'))
    AND TIMESTAMP(DIT.INBOUND_TRANSACTION_CREATED_AT) <= TIMESTAMP(DATETIME( '{{period_time['end_date']}}', '{{time_zone}}'))
 GROUP BY 1, 2, 3

UNION ALL

SELECT
  'Banqup applications' as TypeOfApplication,
  credacc.FINANCIAL_INSTITUTION_COUNTRY_CODE AS counterparty_country,
  IT.INBOUND_TRANSACTION_CURRENCY_CODE  AS currency,
  COUNT(*)  AS success_trx_count,
  SUM(IT.INBOUND_TRANSACTION_AMOUNT) AS success_trx_summed_value,
  CURRENT_TIMESTAMP AS LOAD_TIMESTAMP,
  '{{period}}' AS PERIOD,
FROM {{ source('source_dwh_strp,D_INBOUND_PAYMENT_INFO_CURRENT') }} AS IP
LEFT JOIN {{ source('source_dwh_strp,D_FINANCIAL_PLATFORMS_DECRYPTED') }} AS FP
    ON IP.T_D_FINANCIAL_PLATFORM_DIM_KEY=FP.T_DIM_KEY
LEFT JOIN {{ source('source_dwh_strp,D_FINANCIAL_INSTITUTIONS') }} AS FI
    ON FI.T_DIM_KEY = FP.T_D_FINANCIAL_INSTITUTION_DIM_KEY
LEFT JOIN {{ source('source_dwh_strp,F_INBOUND_TRANSACTIONS_DECRYPTED') }} AS IT
    ON IP.T_DIM_KEY = IT.T_D_INBOUND_PAYMENT_INFO_DIM_KEY
INNER JOIN {{ source('source_dwh_strp,D_INBOUND_TRANSACTION_INFO') }} AS DIT
    ON DIT.T_DIM_KEY = IT.T_D_INBOUND_TRANSACTION_INFO_DIM_KEY
LEFT JOIN {{ source('source_dwh_strp,D_PAYMENT_INITIATIONS') }} AS PI
    ON IP.T_DIM_KEY = PI.T_D_INBOUND_PAYMENT_DIM_KEY
LEFT JOIN {{ source('source_dwh_strp,D_APPLICATIONS_DECRYPTED') }} AS APP
    ON IP.T_D_APPLICATION_DIM_KEY = APP.T_DIM_KEY
LEFT JOIN {{ source('source_dwh_strp,D_BANK_ACCOUNTS_DECRYPTED') }} debacc
    ON IP.T_D_DEBTOR_BANK_ACCOUNTS_DIM_KEY = debacc.T_DIM_KEY
LEFT JOIN {{ source('source_dwh_strp,D_BANK_ACCOUNTS_DECRYPTED') }} credacc
    ON IT.T_D_CREDITOR_BANK_ACCOUNTS_DIM_KEY = credacc.T_DIM_KEY

WHERE
  PI.PAYMENT_INITIATION_STATUS = 'SUCCESSFUL'
  AND APP.APPLICATION_NAME  = 'PAY-PXG-OCS'
  AND FI.FINANCIAL_INSTITUTION_CODE <> 'IBIS'
  AND credacc.FINANCIAL_INSTITUTION_COUNTRY_CODE = '{{country_code}}'
  AND  SUBSTR( credacc.BANK_ACCOUNT_NUMBER,5,4) = 'PANX72'
  AND TIMESTAMP(DIT.INBOUND_TRANSACTION_CREATED_AT) >= TIMESTAMP(DATETIME( '{{period_time['begin_date']}}', '{{time_zone}}'))
  AND TIMESTAMP(DIT.INBOUND_TRANSACTION_CREATED_AT) <= TIMESTAMP(DATETIME( '{{period_time['end_date']}}', '{{time_zone}}'))

GROUP BY 1, 2, 3
ORDER BY 1,2 DESC
