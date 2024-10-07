WITH current_table AS (
    SELECT *,
    ROW_NUMBER() OVER(PARTITION BY _pk_id ORDER BY INSERT_HIST_TIMESTAMP DESC) AS rn
    FROM (
        SELECT
        CONCAT( `relation_audit_id`  ,"") AS _pk_id,
        *
        FROM  `pj-bu-dw-data-sbx`.`dev_dl_h3_hklc`.`public_DA_relation_audit` 
    ) 
)
SELECT * EXCEPT(rn)
FROM current_table
WHERE rn = 1