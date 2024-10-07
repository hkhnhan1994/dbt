WITH current_table AS (
    SELECT *,
    ROW_NUMBER() OVER(PARTITION BY _pk_id ORDER BY INSERT_HIST_TIMESTAMP DESC) AS rn
    FROM (
        SELECT
        CONCAT( `KlantID`  ,  `SysteemID`  ,  `AttribuutID`  ,  `rowguid`  ,"") AS _pk_id,
        *
        FROM  `pj-bu-dw-data-sbx`.`dev_dl_h3_hklc`.`dl_h3_hklc_hist_public_tblKlantSysAttr` 
    ) 
)
SELECT * EXCEPT(rn)
FROM current_table
WHERE rn = 1