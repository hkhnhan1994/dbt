WITH current_table AS (
    SELECT *,
    ROW_NUMBER() OVER(PARTITION BY _pk_id ORDER BY INSERT_HIST_TIMESTAMP DESC) AS rn
    FROM (
        SELECT
        CONCAT( `PersonOrganisationLinkId`  ,"") AS _pk_id,
        *
        FROM  `pj-bu-dw-data-sbx`.`dev_dl_h3_hehe`.`public_PersonOrganisationLink` 
    ) 
)
SELECT * EXCEPT(rn)
FROM current_table
WHERE rn = 1