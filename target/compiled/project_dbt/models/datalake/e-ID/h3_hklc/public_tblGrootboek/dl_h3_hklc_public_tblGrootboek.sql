WITH dedup_table AS (

SELECT * EXCEPT(rn)
FROM (
    SELECT *,
    ROW_NUMBER() OVER(PARTITION BY ingestion_meta_data_uuid ORDER BY ingestion_meta_data_processing_timestamp) AS rn
    FROM   `pj-bu-dw-data-sbx`.`dev_lake_view_cmd`.`public_tblGrootboek` 
)
WHERE rn = 1
),
surrogate_key as(
    select
         
    
to_hex(md5(cast(coalesce(cast(ingestion_meta_data_uuid as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(row_hash as string), '_dbt_utils_surrogate_key_null_') as string)))  as T_UNIQUE_KEY, 
    CURRENT_TIMESTAMP AS INSERT_HIST_TIMESTAMP,
        *
    FROM dedup_table
)
SELECT 
    *
FROM surrogate_key