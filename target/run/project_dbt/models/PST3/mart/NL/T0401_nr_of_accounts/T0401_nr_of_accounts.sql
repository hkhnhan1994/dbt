
  
    

    create or replace table `pj-bu-dw-data-sbx`.`dev_dm_pst3_NL`.`T0401_nr_of_accounts`
      
    
    

    OPTIONS()
    as (
      SELECT
count(*) nr_accounts,
CURRENT_TIMESTAMP AS LOAD_TIMESTAMP,
'2023Y' AS PERIOD,
FROM `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_IBIS_ACCOUNT_CURRENT` IACC
left join `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_BANK_ACCOUNTS_DECRYPTED` BANK
  on IACC.T_D_BANK_ACCOUNT_DIM_KEY = BANK.T_DIM_KEY
WHERE ACCOUNT_TYPE = 'PAYMENT'
  AND BANK.FINANCIAL_INSTITUTION_COUNTRY_CODE = 'NL'
  AND (
    IACC.ACCOUNT_STATUS = 'OPEN'
    OR (
      IACC.ACCOUNT_STATUS = 'CLOSED'
      and IACC.ACCOUNT_UPDATED_AT > TIMESTAMP(DATETIME( '2023-12-31', 'Etc/UTC'))
      )
  )
  and IACC.ACCOUNT_CREATED_AT < TIMESTAMP(DATETIME( '2023-12-31', 'Etc/UTC'))
    );
  