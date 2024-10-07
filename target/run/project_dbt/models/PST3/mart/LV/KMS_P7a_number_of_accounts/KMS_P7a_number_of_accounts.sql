
  
    

    create or replace table `pj-bu-dw-data-sbx`.`dev_dm_pst3_LV`.`KMS_P7a_number_of_accounts`
      
    
    

    OPTIONS()
    as (
      SELECT
  count(*) as number_of_accounts,
  CURRENT_TIMESTAMP AS LOAD_TIMESTAMP,
  '2024S1' AS PERIOD,
FROM `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_IBIS_ACCOUNT_CURRENT` IACC
left join `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_BANK_ACCOUNTS_DECRYPTED` BANK on IACC.T_D_BANK_ACCOUNT_DIM_KEY = BANK.T_DIM_KEY
where ACCOUNT_TYPE = 'PAYMENT'
  AND BANK.FINANCIAL_INSTITUTION_COUNTRY_CODE = 'LV'
  AND (
      IACC.ACCOUNT_STATUS = 'OPEN'
      OR (
        IACC.ACCOUNT_STATUS = 'CLOSED'
        AND IACC.ACCOUNT_UPDATED_AT >= TIMESTAMP(DATETIME('2024-01-01', 'Etc/UTC'))
      )
    )
  AND IACC.ACCOUNT_CREATED_AT <= TIMESTAMP(DATETIME( '2024-06-30', 'Etc/UTC'))
    );
  