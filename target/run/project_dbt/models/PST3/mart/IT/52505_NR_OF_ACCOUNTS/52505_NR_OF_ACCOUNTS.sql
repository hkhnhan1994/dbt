
  
    

    create or replace table `pj-bu-dw-data-sbx`.`dev_dm_pst3_IT`.`52505_NR_OF_ACCOUNTS`
      
    
    

    OPTIONS()
    as (
      SELECT
    IF(FAB.BALANCE_AMOUNT <= 100,'small balance', 'big balance') AS balance_size,
    iacc.account_currency,
    count(*) AS amount,
    ceil(sum (FAB.BALANCE_AMOUNT)) as booked_balance,
    CURRENT_TIMESTAMP AS Load_timestamp,
    ""  AS Period,
FROM `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`F_ACCOUNT_BALANCE` FAB
JOIN  `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_IBIS_ACCOUNT_CURRENT` iacc
  on FAB.T_D_IBIS_ACCOUNT_DIM_KEY = iacc.T_DIM_KEY
WHERE
      iacc.ACCOUNT_TYPE = "PAYMENT"
  AND  iacc.ACCOUNT_COUNTRY_CODE = 'IT'
  AND  (
    iacc.ACCOUNT_STATUS = 'OPEN'
    OR (
      iacc.ACCOUNT_STATUS = 'CLOSED'
      AND iacc.ACCOUNT_UPDATED_AT > TIMESTAMP(DATETIME( '2024-09-30', 'Etc/UTC'))
      )
  )
  AND iacc.ACCOUNT_UPDATED_AT <= TIMESTAMP(DATETIME( '2024-09-30', 'Etc/UTC'))
  AND FAB.BALANCE_DATE = (
      SELECT max(BALANCE_DATE)
      FROM `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`F_ACCOUNT_BALANCE` max
      JOIN `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_IBIS_ACCOUNT_CURRENT`  a
        ON max.T_D_IBIS_ACCOUNT_DIM_KEY = a.T_DIM_KEY
        WHERE TIMESTAMP(max.BALANCE_DATE) <= TIMESTAMP(DATETIME( '2024-09-30', 'Etc/UTC'))
  )
group by 1,2
    );
  