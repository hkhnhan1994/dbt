
{% set period_time = period_calculate(time = 'semesterly', selection_date="today", prefix='', suffix='S' ) -%}
{% set time_zone = "Etc/UTC" -%}
{% set country_code = 'ES' -%}


        SELECT
  IT.INBOUND_TRANSACTION_CURRENCY_CODE as trx_currency,
  credacc.FINANCIAL_INSTITUTION_COUNTRY_CODE  as counterparty_country,
  count(*)  AS success_trx_count,
  sum(IT.INBOUND_TRANSACTION_AMOUNT) AS success_trx_summed_value,
  CURRENT_TIMESTAMP AS LOAD_TIMESTAMP,
  '{{period}}' AS PERIOD,
  FROM {{ source('source_dwh_STRP','D_INBOUND_PAYMENT_INFO_DECRYPTED') }} as IP
  LEFT JOIN {{ source('source_dwh_STRP','D_FINANCIAL_PLATFORMS_DECRYPTED') }} FP
      ON IP.T_D_FINANCIAL_PLATFORM_DIM_KEY=FP.T_DIM_KEY
  LEFT JOIN {{ source('source_dwh_STRP','D_FINANCIAL_INSTITUTIONS') }} FI
      ON FI.T_DIM_KEY = FP.T_D_FINANCIAL_INSTITUTION_DIM_KEY
  LEFT JOIN {{ source('source_dwh_STRP','F_INBOUND_TRANSACTIONS_DECRYPTED') }} as IT
      ON IP.T_DIM_KEY=IT.T_D_INBOUND_PAYMENT_INFO_DIM_KEY
  INNER JOIN {{ source('source_dwh_STRP','D_INBOUND_TRANSACTION_INFO') }} as DIT
      ON DIT.T_DIM_KEY = IT.T_D_INBOUND_TRANSACTION_INFO_DIM_KEY
  LEFT JOIN {{ source('source_dwh_STRP','D_PAYMENT_INITIATIONS_CURRENT') }} as PI
      ON IP.T_DIM_KEY=PI.T_D_INBOUND_PAYMENT_DIM_KEY
  LEFT JOIN {{ source('source_dwh_STRP','D_APPLICATIONS_DECRYPTED') }} as APP
      ON IP.T_D_APPLICATION_DIM_KEY = APP.T_DIM_KEY
  LEFT JOIN {{ source('source_dwh_STRP','D_BANK_ACCOUNTS_DECRYPTED') }} debacc
      ON IP.T_D_DEBTOR_BANK_ACCOUNTS_DIM_KEY = debacc.T_DIM_KEY
  LEFT JOIN {{ source('source_dwh_STRP','D_BANK_ACCOUNTS_DECRYPTED') }} credacc
      ON IT.T_D_CREDITOR_BANK_ACCOUNTS_DIM_KEY = credacc.T_DIM_KEY
  WHERE  FI.FINANCIAL_INSTITUTION_CODE  <> 'IBIS'
    AND PI.PAYMENT_INITIATION_STATUS = 'SUCCESSFUL'
    AND ( APP.APPLICATION_NAME = 'PAY-PXG-BANQUPES'
          OR (
            APP.APPLICATION_NAME = 'PAY-PXG-OCS'
            AND credacc.FINANCIAL_INSTITUTION_COUNTRY_CODE  = '{{country_code}}'
            AND substr( credacc.BANK_ACCOUNT_NUMBER,5,4)= '6918'
            )
        )
    AND DIT.INBOUND_TRANSACTION_CREATED_AT >= TIMESTAMP(DATETIME( '{{begin_date}}', '{{time_zone}}'))
    AND DIT.INBOUND_TRANSACTION_CREATED_AT <= TIMESTAMP(DATETIME( '{{end_date}}', '{{time_zone}}'))
  GROUP BY 1,2
