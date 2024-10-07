
  
    

    create or replace table `pj-bu-dw-data-sbx`.`dev_dm_pst3_ES`.`Cuadro1_number_of_AIS_PSUs`
      
    
    

    OPTIONS()
    as (
      WITH obsolete_and_active_consents_count_in_reporting_period AS (
  SELECT
    ac.USER_TOKEN,
    COUNT(*) AS number_of_consents,
  FROM  `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_ACCESS_CONSENT_INFO` AS ac
  inner JOIN `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_PXG_PAYMENT_ACCOUNT_CURRENT` AS pa
    ON pa.T_DIM_KEY=ac.T_D_PXG_PAYMENT_ACCOUNT_DIM_KEY
  inner JOIN `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_FINANCIAL_PLATFORMS_DECRYPTED` AS fp
    ON pa.T_D_FINANCIAL_PLATFORM_DIM_KEY=fp.T_DIM_KEY
  inner JOIN `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_FINANCIAL_INSTITUTIONS` AS fi
    ON fi.T_DIM_KEY = fp.T_D_FINANCIAL_INSTITUTION_DIM_KEY
  inner JOIN `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_CONTRACT_INFO` AS c
    ON pa.T_DIM_KEY=c.T_D_PXG_PAYMENT_ACCOUNT_DIM_KEY
  inner JOIN `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_APPLICATION_ACCOUNT_INFO_DECRYPTED` AS aa
    ON c.T_D_APPLICATION_ACCOUNT_DIM_KEY=aa.T_DIM_KEY
  inner JOIN `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_APPLICATIONS_DECRYPTED` AS a
    ON aa.T_D_APPLICATION_DIM_KEY = a.T_DIM_KEY
  WHERE
    a.APPLICATION_NAME in ('PAY-PXG-BANQUPES')
    AND fi.FINANCIAL_INSTITUTION_CODE <> 'IBIS' -- consents on UPP's own accounts are not counted
    AND (
        ac.ACCESS_CONSENT_CREATED_AT >= TIMESTAMP(DATETIME( '2024-01-01', 'Etc/UTC'))
        AND ac.ACCESS_CONSENT_CREATED_AT <= TIMESTAMP(DATETIME( '2024-06-30', 'Etc/UTC'))
        OR (
          ac.ACCESS_CONSENT_STATUS_AT > TIMESTAMP(DATETIME( '2024-01-01', 'Etc/UTC'))
          AND ac.ACCESS_CONSENT_STATUS_AT <= TIMESTAMP(DATETIME( '2024-06-30', 'Etc/UTC'))
          )
    )
  GROUP BY ac.USER_TOKEN
  )
  SELECT
  COUNT(DISTINCT USER_TOKEN) AS unique_users_count_in_period,
  CURRENT_TIMESTAMP AS LOAD_TIMESTAMP,
  ""  AS Period,
  FROM obsolete_and_active_consents_count_in_reporting_period
  WHERE USER_TOKEN <>'NA'
    );
  