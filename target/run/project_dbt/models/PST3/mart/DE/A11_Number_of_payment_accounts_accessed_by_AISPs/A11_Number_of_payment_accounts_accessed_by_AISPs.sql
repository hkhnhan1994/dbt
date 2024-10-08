
  
    

    create or replace table `pj-bu-dw-data-sbx`.`dev_dm_pst3_DE`.`A11_Number_of_payment_accounts_accessed_by_AISPs`
      
    
    

    OPTIONS()
    as (
      SELECT *,
CURRENT_TIMESTAMP AS LOAD_TIMESTAMP,
'2024S1' AS PERIOD,
FROM `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_ASPSP_TPP_DECRYPTED` t
WHERE t.T_SOURCE_PK_UUID <> '93773d5d-00b9-422d-af5c-b90259cf50ee'
AND SERVICE_PROVIDER_CREATED_AT >= TIMESTAMP(DATETIME( '2024-01-01', 'Etc/UTC'))
AND SERVICE_PROVIDER_CREATED_AT <= TIMESTAMP(DATETIME( '2024-06-30', 'Etc/UTC'))
AND T_LOAD_TIMESTAMP is not null
    );
  