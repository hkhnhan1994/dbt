
  
    

    create or replace table `pj-bu-dw-data-sbx`.`dev_dm_pst3_BE`.`PI_FINAL_QUERY`
      
    
    

    OPTIONS()
    as (
      SELECT
  a.APPLICATION_NAME AS Application_name,
  fi.FINANCIAL_INSTITUTION_COUNTRY as CountryOfAISP,
  count(*) AS number_of_consents,
  ""  AS Period,
  CURRENT_TIMESTAMP AS load_timestamp,
  TIMESTAMP(DATETIME( '2024-01-01', 'Etc/UTC'))  AS Period_begin_date,
  TIMESTAMP(DATETIME( '2024-06-30', 'Etc/UTC'))  AS Period_end_date,
FROM `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_ACCESS_CONSENT_INFO_CURRENT` AS ac
JOIN `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_ACCESS_CONSENT_INFO_DECRYPTED` acd on acd.T_DIM_KEY = ac.T_DIM_KEY
JOIN `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_PXG_PAYMENT_ACCOUNT_DECRYPTED`
  AS pa ON pa.T_DIM_KEY=ac.T_D_PXG_PAYMENT_ACCOUNT_DIM_KEY
JOIN `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_FINANCIAL_PLATFORMS_DECRYPTED`
  AS fp ON pa.T_D_FINANCIAL_PLATFORM_DIM_KEY=fp.T_DIM_KEY
JOIN `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_FINANCIAL_INSTITUTIONS`
  AS fi ON fi.T_DIM_KEY = fp.T_D_FINANCIAL_INSTITUTION_DIM_KEY
JOIN `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_CONTRACT_INFO_CURRENT`
  AS c ON pa.T_DIM_KEY=c.T_D_PXG_PAYMENT_ACCOUNT_DIM_KEY
JOIN `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_APPLICATION_ACCOUNT_INFO_DECRYPTED`
  AS aa ON c.T_D_APPLICATION_ACCOUNT_DIM_KEY=aa.T_DIM_KEY
JOIN `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_APPLICATIONS_DECRYPTED`
  AS a ON aa.T_D_APPLICATION_DIM_KEY = a.T_DIM_KEY
JOIN `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_APPLICATION_OWNERS_DECRYPTED`
  AS ao ON a.T_D_OWNER_DIM_KEY =  ao.T_DIM_KEY
WHERE
  fi.FINANCIAL_INSTITUTION_CODE <> 'IBIS'
  AND (
    (
    TIMESTAMP(ac.ACCESS_CONSENT_STATUS_AT) >= TIMESTAMP(DATETIME( '2024-01-01', 'Etc/UTC'))  --pt winter time
    AND TIMESTAMP(ac.ACCESS_CONSENT_STATUS_AT) <= TIMESTAMP(DATETIME( '2024-06-30', 'Etc/UTC'))   --pt winter time
    )
    OR
    (
    TIMESTAMP(ac.ACCESS_CONSENT_CREATED_AT) >= TIMESTAMP(DATETIME( '2024-01-01', 'Etc/UTC'))  --pt winter time
    AND TIMESTAMP(ac.ACCESS_CONSENT_CREATED_AT) <= TIMESTAMP(DATETIME( '2024-06-30', 'Etc/UTC'))   --pt winter time
    )
  )
GROUP BY 1,2
    );
  