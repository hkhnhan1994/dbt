WITH current_table AS (
    SELECT *,
    ROW_NUMBER() OVER(PARTITION BY T_ROW_HASH ORDER BY T_LOAD_TIMESTAMP DESC) AS rn
    FROM  {{ref('dwh_strh_D_ENTERPRISE')}}
    )
SELECT * EXCEPT(rn)
FROM current_table
WHERE rn = 1