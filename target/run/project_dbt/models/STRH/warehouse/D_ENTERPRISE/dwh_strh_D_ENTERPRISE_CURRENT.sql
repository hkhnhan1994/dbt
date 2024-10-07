
  
    

    create or replace table `pj-bu-dw-data-sbx`.`dev_dwh_view_cmd`.`D_ENTERPRISE_CURRENT`
      
    
    

    OPTIONS()
    as (
      WITH current_table AS (
    SELECT *,
    ROW_NUMBER() OVER(PARTITION BY T_ROW_HASH ORDER BY T_LOAD_TIMESTAMP DESC) AS rn
    FROM  `pj-bu-dw-data-sbx`.`dev_dwh_view_cmd`.`D_ENTERPRISE`
    )
SELECT * EXCEPT(rn)
FROM current_table
WHERE rn = 1
    );
  