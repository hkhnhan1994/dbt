
  
    

    create or replace table `pj-bu-dw-data-sbx`.`dev_dm_pst3_FR`.`Banque_en_ligne`
      
    
    

    OPTIONS()
    as (
      SELECT
  COUNT( distinct ibis.ACCOUNT_MASTER_DATA_ID) AS count,
  CURRENT_TIMESTAMP AS Load_timestamp,
  ""  AS Period,
FROM `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_IBIS_ACCOUNT_CURRENT` AS ibis
LEFT JOIN `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_BANK_ACCOUNTS_DECRYPTED` AS bank
  ON bank.T_DIM_KEY = ibis.T_D_BANK_ACCOUNT_DIM_KEY
WHERE ibis.ACCOUNT_TYPE = "PAYMENT"
AND (ibis.ACCOUNT_STATUS = 'OPEN'
  OR
    (
      ibis.ACCOUNT_STATUS = 'CLOSED'
      AND ibis.ACCOUNT_UPDATED_AT >= TIMESTAMP(DATETIME( '2024-06-30', 'Etc/UTC'))
    )
)
-- as timezone('UTC', to_timestamp('2023-06-30 UTC+02', 'YYYY-MM-DD "UTC"TZH') + interval '1 day')))
AND bank.FINANCIAL_INSTITUTION_COUNTRY_CODE = 'FR'
AND ibis.ACCOUNT_CREATED_AT >= TIMESTAMP(DATETIME( '2024-06-30', 'Etc/UTC'))
-- as timezone('UTC', to_timestamp('2023-06-30 UTC+02', 'YYYY-MM-DD "UTC"TZH') + interval '1 day')
    );
  