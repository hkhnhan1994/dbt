
  
    

    create or replace table `pj-bu-dw-data-sbx`.`dev_dl_h3_hehe`.`public_ServiceAuthorization_1_13_current`
      
    
    

    OPTIONS()
    as (
      WITH current_table AS (
    SELECT *,
    ROW_NUMBER() OVER(PARTITION BY _pk_id ORDER BY INSERT_HIST_TIMESTAMP DESC) AS rn
    FROM (
        SELECT
        CONCAT( `ServiceAuthorizationId`  ,"") AS _pk_id,
        *
        FROM  `pj-bu-dw-data-sbx`.`dev_dl_h3_hehe`.`public_ServiceAuthorization_1_13` 
    ) 
)
SELECT * EXCEPT(rn)
FROM current_table
WHERE rn = 1
    );
  