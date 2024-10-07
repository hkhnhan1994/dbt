WITH current_table AS (
    SELECT *,
      ROW_NUMBER() OVER(PARTITION BY row_hash ORDER BY INSERT_HIST_TIMESTAMP DESC) AS rn
    FROM  `pj-bu-dw-data-sbx`.`dev_dl_h3_hehe`.`hist_public_Person`
  )
SELECT * EXCEPT(rn)
FROM current_table
WHERE rn = 1