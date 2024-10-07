
  
    

    create or replace table `pj-bu-dw-data-sbx`.`dev_dm_pst3_LV`.`KMS_MRA`
      
    
    

    OPTIONS()
    as (
      SELECT
  BA.BANK_ACCOUNT_NUMBER as large_biller_account,
  CURRENT_TIMESTAMP AS LOAD_TIMESTAMP,
  '2024S1' AS PERIOD,
FROM `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_MERCHANTS_CURRENT` AS M
left JOIN `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_BANK_ACCOUNTS_DECRYPTED` AS BA
  ON M.T_D_MRA_BANK_ACCOUNT_DIM_KEY = BA.T_DIM_KEY
WHERE M.T_D_MRA_BANK_ACCOUNT_DIM_KEY is not null
  AND BA.FINANCIAL_INSTITUTION_COUNTRY_CODE = 'LV'
  AND MERCHANT_CREATED_AT >= TIMESTAMP(DATETIME( '2024-01-01', 'Etc/UTC'))
  AND MERCHANT_CREATED_AT <= TIMESTAMP(DATETIME( '2024-06-30', 'Etc/UTC'))
    );
  