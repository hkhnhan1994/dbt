
  
    

    create or replace table `pj-bu-dw-data-sbx`.`dev_dm_pst3_BE`.`PI_PXG_APPLICATION_MAPPING`
      
    
    

    OPTIONS()
    as (
      SELECT
    distinct ap.APPLICATION_NAME AS APPLICATION_NAME,
    -- ap.APPLICATION_CREATED_AT AS CREATION_DATETIME,
    -- ao.APPLICATION_OWNER_NAME AS APPLICATION_OWNER_NAME,
    CURRENT_TIMESTAMP AS LOAD_TIMESTAMP,
    ""  AS Period,
    CAST(NULL AS TIMESTAMP) AS Period_begin_date,
    CAST(NULL AS TIMESTAMP) AS Period_end_date,
FROM (
    WITH current_table AS (
    SELECT *, ROW_NUMBER() OVER(PARTITION BY T_BUS_KEY ORDER BY T_INGESTION_TIMESTAMP desc, T_LOAD_TIMESTAMP desc) AS rn
    FROM  `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_APPLICATIONS_DECRYPTED`
)
SELECT * EXCEPT(rn)
FROM current_table
WHERE rn = 1
) AS ap
LEFT  JOIN `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_APPLICATION_OWNERS_DECRYPTED` AS ao
    ON ap.T_D_OWNER_DIM_KEY =  ao.T_DIM_KEY
LEFT JOIN `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_APPLICATION_ACCOUNT_INFO_DECRYPTED` AS aa
    ON ap.t_dim_key = aa.T_D_APPLICATION_DIM_KEY
LEFT JOIN `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_CONTRACT_INFO_CURRENT` AS ci
    ON aa.T_DIM_KEY= ci.T_D_APPLICATION_ACCOUNT_DIM_KEY
LEFT JOIN `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_PXG_PAYMENT_ACCOUNT_DECRYPTED`
    AS pa on pa.T_DIM_KEY= ci.T_D_PXG_PAYMENT_ACCOUNT_DIM_KEY
LEFT JOIN `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_FINANCIAL_PLATFORMS_DECRYPTED`
    AS fp ON pa.T_D_FINANCIAL_PLATFORM_DIM_KEY=fp.T_DIM_KEY
LEFT JOIN `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_FINANCIAL_INSTITUTIONS`
    AS fi ON fi.T_DIM_KEY = fp.T_D_FINANCIAL_INSTITUTION_DIM_KEY
-- WHERE ap.APPLICATION_NAME <>"NA"
    -- AND TIMESTAMP(ap.APPLICATION_UPDATED_AT) >= TIMESTAMP(DATETIME( '2024-01-01', 'Etc/UTC')) --pt winter time
    -- AND TIMESTAMP(ap.APPLICATION_UPDATED_AT) <= TIMESTAMP(DATETIME( '2024-06-30', 'Etc/UTC'))  --pt winter time
    );
  