

  create or replace view `pj-bu-dw-data-sbx`.`dev_dwh_view_cmd`.`D_AUTHORIZED_PERSON_CURRENT`
  OPTIONS()
  as WITH current_table AS (
    SELECT *,
    ROW_NUMBER() OVER(PARTITION BY row_hash) AS rn
    FROM  `pj-bu-dw-data-sbx`.`dev_dwh_view_cmd`.`D_AUTHORIZED_PERSON`
    )
SELECT * EXCEPT(rn)
FROM current_table
WHERE rn = 1;

