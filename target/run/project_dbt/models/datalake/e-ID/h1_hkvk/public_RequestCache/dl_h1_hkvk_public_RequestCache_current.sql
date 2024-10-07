
  
    

    create or replace table `pj-bu-dw-data-sbx`.`dev_dl_h1_hkvk`.`public_RequestCache_current`
      
    
    

    OPTIONS()
    as (
      WITH current_table AS (
    SELECT *,
    ROW_NUMBER() OVER(PARTITION BY _pk_id ORDER BY INSERT_HIST_TIMESTAMP DESC) AS rn
    FROM (
        SELECT
        CONCAT( `RequestCacheID`  ,  `CacheKey`  ,"") AS _pk_id,
        *
        FROM  `pj-bu-dw-data-sbx`.`dev_dl_h1_hkvk`.`public_RequestCache` 
    ) 
)
SELECT * EXCEPT(rn)
FROM current_table
WHERE rn = 1
    );
  