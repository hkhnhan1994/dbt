
  
    

    create or replace table `pj-bu-dw-data-sbx`.`dev_dm_pst3_ES`.`Cuardro4_PIS`
      
    
    

    OPTIONS()
    as (
      SELECT
  IT.INBOUND_TRANSACTION_CURRENCY_CODE as trx_currency,
  credacc.FINANCIAL_INSTITUTION_COUNTRY_CODE  as counterparty_country,
  count(*)  AS success_trx_count,
  sum(IT.INBOUND_TRANSACTION_AMOUNT) AS success_trx_summed_value,
  CURRENT_TIMESTAMP AS LOAD_TIMESTAMP,
  '2024S1' AS PERIOD,
  FROM `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_INBOUND_PAYMENT_INFO_DECRYPTED` as IP
  LEFT JOIN `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_FINANCIAL_PLATFORMS_DECRYPTED` FP
      ON IP.T_D_FINANCIAL_PLATFORM_DIM_KEY=FP.T_DIM_KEY
  LEFT JOIN `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_FINANCIAL_INSTITUTIONS` FI
      ON FI.T_DIM_KEY = FP.T_D_FINANCIAL_INSTITUTION_DIM_KEY
  LEFT JOIN `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`F_INBOUND_TRANSACTIONS_DECRYPTED` as IT
      ON IP.T_DIM_KEY=IT.T_D_INBOUND_PAYMENT_INFO_DIM_KEY
  INNER JOIN `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_INBOUND_TRANSACTION_INFO` as DIT
      ON DIT.T_DIM_KEY = IT.T_D_INBOUND_TRANSACTION_INFO_DIM_KEY
  LEFT JOIN `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_PAYMENT_INITIATIONS_CURRENT` as PI
      ON IP.T_DIM_KEY=PI.T_D_INBOUND_PAYMENT_DIM_KEY
  LEFT JOIN `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_APPLICATIONS_DECRYPTED` as APP
      ON IP.T_D_APPLICATION_DIM_KEY = APP.T_DIM_KEY
  LEFT JOIN `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_BANK_ACCOUNTS_DECRYPTED` debacc
      ON IP.T_D_DEBTOR_BANK_ACCOUNTS_DIM_KEY = debacc.T_DIM_KEY
  LEFT JOIN `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_BANK_ACCOUNTS_DECRYPTED` credacc
      ON IT.T_D_CREDITOR_BANK_ACCOUNTS_DIM_KEY = credacc.T_DIM_KEY
  WHERE  FI.FINANCIAL_INSTITUTION_CODE  <> 'IBIS'
    AND PI.PAYMENT_INITIATION_STATUS = 'SUCCESSFUL'
    AND ( APP.APPLICATION_NAME = 'PAY-PXG-BANQUPES'
          OR (
            APP.APPLICATION_NAME = 'PAY-PXG-OCS'
            AND credacc.FINANCIAL_INSTITUTION_COUNTRY_CODE  = 'ES'
            AND substr( credacc.BANK_ACCOUNT_NUMBER,5,4)= '6918'
            )
        )
    AND DIT.INBOUND_TRANSACTION_CREATED_AT >= TIMESTAMP(DATETIME( '2024-01-01', 'Etc/UTC'))
    AND DIT.INBOUND_TRANSACTION_CREATED_AT <= TIMESTAMP(DATETIME( '2024-06-30', 'Etc/UTC'))
  GROUP BY 1,2
    );
  