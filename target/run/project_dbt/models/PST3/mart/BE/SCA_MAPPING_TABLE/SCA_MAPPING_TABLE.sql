
  
    

    create or replace table `pj-bu-dw-data-sbx`.`dev_dm_pst3_BE`.`SCA_MAPPING_TABLE`
      
    
    

    OPTIONS()
    as (
      SELECT
  "full-auth" AS SCA_RESULT,
  "SCA used" AS SCA_REASON,
UNION ALL
SELECT
  "attempt" AS SCA_RESULT,
  "non-SCA used: reason is others" AS SCA_REASON,
UNION ALL
SELECT
  "non-authenticated" AS SCA_RESULT,
  "non-SCA used: reason is others" AS SCA_REASON,
UNION ALL
SELECT
  NULL AS SCA_RESULT,
  "non-SCA used: reason is others" AS SCA_REASON,
    );
  