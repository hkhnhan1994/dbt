
  
    

    create or replace table `pj-bu-dw-data-sbx`.`dev_dl_h3_hehe`.`public_ChainedOrganisation_current`
      
    
    

    OPTIONS()
    as (
      WITH current_table AS (
    SELECT *,
    ROW_NUMBER() OVER(PARTITION BY _pk_id ORDER BY INSERT_HIST_TIMESTAMP DESC) AS rn
    FROM (
        SELECT
        CONCAT( `ChainedOrganisationId`  ,"") AS _pk_id,
        *
        FROM  `pj-bu-dw-data-sbx`.`dev_dl_h3_hehe`.`public_ChainedOrganisation` 
    ) 
)
SELECT * EXCEPT(rn)
FROM current_table
WHERE rn = 1
    );
  