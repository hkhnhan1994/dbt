
  
    

    create or replace table `pj-bu-dw-data-sbx`.`dev_dm_pst3_DE`.`NC1_Number_of_payment_accounts_accessed_by_UPP`
      
    
    

    OPTIONS()
    as (
      WITH pre AS (
  SELECT
    'DE' AS country,
    ac.USER_TOKEN,
    count(*),
  FROM `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_PXG_PAYMENT_ACCOUNT_DECRYPTED` AS pa
  LEFT JOIN `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_FINANCIAL_PLATFORMS_DECRYPTED` AS fp
    ON pa.T_D_FINANCIAL_PLATFORM_DIM_KEY=fp.T_DIM_KEY
  LEFT JOIN `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_FINANCIAL_INSTITUTIONS` AS fi
    ON fi.T_DIM_KEY = fp.T_D_FINANCIAL_INSTITUTION_DIM_KEY
  LEFT JOIN `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_ACCESS_CONSENT_INFO` AS ac
    ON pa.T_DIM_KEY=ac.T_D_PXG_PAYMENT_ACCOUNT_DIM_KEY
  LEFT JOIN `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_CONTRACT_INFO` AS c
    ON pa.T_DIM_KEY=c.T_D_PXG_PAYMENT_ACCOUNT_DIM_KEY
  LEFT JOIN `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_APPLICATION_ACCOUNT_INFO_DECRYPTED` AS aa
    ON c.T_D_APPLICATION_ACCOUNT_DIM_KEY=aa.T_DIM_KEY
  LEFT JOIN `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_APPLICATIONS_DECRYPTED` AS a
    ON aa.T_D_APPLICATION_DIM_KEY = a.T_DIM_KEY
  WHERE a.APPLICATION_NAME = 'PAY-PXG-BANQUPDE'
    AND fi.FINANCIAL_INSTITUTION_CODE <> 'IBIS'
    AND (
      TIMESTAMP(ac.ACCESS_CONSENT_STATUS_AT) >= TIMESTAMP(DATETIME( '2024-01-01', 'Etc/UTC'))
      AND TIMESTAMP(ac.ACCESS_CONSENT_STATUS_AT) <= TIMESTAMP(DATETIME( '2024-06-30', 'Etc/UTC'))
      OR
      (
      TIMESTAMP(ac.ACCESS_CONSENT_CREATED_AT) >= TIMESTAMP(DATETIME( '2024-01-01', 'Etc/UTC'))
      AND TIMESTAMP(ac.ACCESS_CONSENT_CREATED_AT) <= TIMESTAMP(DATETIME( '2024-06-30', 'Etc/UTC'))
      )
    )
  GROUP BY 1,2
)
SELECT
  country ,
  count(distinct USER_TOKEN) as unique_users_count_in_period,
  CURRENT_TIMESTAMP AS LOAD_TIMESTAMP,
  '2024S1' AS PERIOD,
FROM PRE
GROUP BY 1
    );
  