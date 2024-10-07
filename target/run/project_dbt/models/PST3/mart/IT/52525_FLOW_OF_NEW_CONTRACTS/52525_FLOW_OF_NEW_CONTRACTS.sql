
  
    

    create or replace table `pj-bu-dw-data-sbx`.`dev_dm_pst3_IT`.`52525_FLOW_OF_NEW_CONTRACTS`
      
    
    

    OPTIONS()
    as (
      select
  count(*) AS number_of_new_contracts,
  CURRENT_TIMESTAMP AS Load_timestamp,
  ""  AS Period,
from `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_IBIS_ACCOUNT_CURRENT` iacc
WHERE
  iacc.ACCOUNT_TYPE = "PAYMENT"
  AND  iacc.ACCOUNT_COUNTRY_CODE = 'IT'
  AND iacc.ACCOUNT_CREATED_AT >= TIMESTAMP(DATETIME( '2024-07-01', 'Etc/UTC'))
  AND iacc.ACCOUNT_CREATED_AT <= TIMESTAMP(DATETIME( '2024-09-30', 'Etc/UTC'))
  AND iacc.ACCOUNT_CREATED_AT >= TIMESTAMP(DATETIME( '2024-07-01', 'Etc/UTC'))
  AND iacc.ACCOUNT_CREATED_AT <= TIMESTAMP(DATETIME( '2024-09-30', 'Etc/UTC'))
    );
  