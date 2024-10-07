
  
    

    create or replace table `pj-bu-dw-data-sbx`.`dev_dm_pst3_DE`.`A1_Number_of_payment_accounts`
      
    
    

    OPTIONS()
    as (
      SELECT
  COUNT(*) as Count,
  CURRENT_TIMESTAMP AS LOAD_TIMESTAMP,
  '2024S1' AS PERIOD,
FROM `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_IBIS_ACCOUNT_CURRENT` IACC
LEFT JOIN `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_BANK_ACCOUNTS_DECRYPTED` BANK
  ON IACC.T_D_BANK_ACCOUNT_DIM_KEY = BANK.T_DIM_KEY
WHERE ACCOUNT_TYPE = 'PAYMENT'
  AND BANK.FINANCIAL_INSTITUTION_COUNTRY_CODE = 'DE'
  AND (
    IACC.ACCOUNT_STATUS = 'OPEN'
    OR (IACC.ACCOUNT_STATUS = 'CLOSED'
        AND IACC.ACCOUNT_UPDATED_AT >= TIMESTAMP(DATETIME( '2024-01-01', 'Etc/UTC'))
      )
    )
  and IACC.ACCOUNT_CREATED_AT <= TIMESTAMP(DATETIME( '2024-06-30', 'Etc/UTC'))
    );
  