
WITH dedup_table AS (

SELECT * EXCEPT(rn)
FROM (
    SELECT *,
    ROW_NUMBER() OVER(PARTITION BY ingestion_meta_data_uuid ORDER BY ingestion_meta_data_processing_timestamp) AS rn
    FROM   {{  source('h3_hklc', 'public_tblKlantBankMachtiging')  }} 
)
WHERE rn = 1
),
surrogate_key as(
    select
         {{  dbt_utils.generate_surrogate_key([
                'ingestion_meta_data_uuid', 
                'row_hash'
            ])
         }}  as T_UNIQUE_KEY, 
    CURRENT_TIMESTAMP AS INSERT_HIST_TIMESTAMP,
        *
    FROM dedup_table
)
SELECT 
    *
FROM surrogate_key
        