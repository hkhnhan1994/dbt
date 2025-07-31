
WITH current_table AS (
    SELECT *,
    ROW_NUMBER() OVER(PARTITION BY _pk_id ORDER BY INSERT_HIST_TIMESTAMP DESC) AS rn
    FROM (
        SELECT
        CONCAT( `BTWID`  ,"") AS _pk_id,
        *
        FROM  {{  ref('dl_h3_hklc_public_tblBTW')  }} 
    ) 
)
SELECT * EXCEPT(rn)
FROM current_table
WHERE rn = 1
        