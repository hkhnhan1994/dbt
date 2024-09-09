with 
-- staging as (
--     {{ref('stg_D_AUTHORIZED_PERSON')}}
-- ),
-- dwh_table_exists as (
--   {{ ref('dwh_D_AUTHORIZED_PERSON') }}
-- ),
dummy_data as (
    select
        CAST(0 AS INT64) AS T_DIM_KEY,
        CAST("NA" AS STRING) AS T_BATCH_ID,
        CAST("NA" AS STRING) AS T_SOURCE_PK_ID,
        CAST(NULL AS TIMESTAMP) AS T_LOAD_TIMESTAMP,
        CAST(NULL AS TIMESTAMP) AS T_INGESTION_TIMESTAMP,
        CAST("NA" AS STRING) AS T_DML_TYPE,
        CAST(NULL AS TIMESTAMP) AS AUTHORIZED_PERSON_CREATED_AT,
        CAST(NULL AS TIMESTAMP) AS AUTHORIZED_PERSON_UPDATED_AT,
        CAST("NA" AS STRING) AS BRAND,
        CAST("NA" AS STRING) AS AUTHORIZED_PERSON_NAME,
        CAST("NA" AS STRING) AS AUTHORIZED_PERSON_FIRST_NAME,
        CAST("NA" AS STRING) AS AUTHORIZED_PERSON_LAST_NAME,
        CAST("NA" AS STRING) AS AUTHORIZED_PERSON_LAST_NAME_PREFIX,
        CAST("NA" AS STRING) AS AUTHORIZED_PERSON_EMAIL,
        CAST("NA" AS STRING) AS AUTHORIZED_PERSON_PLACE_OF_BIRTH,
        CAST("NA" AS STRING) AS AUTHORIZED_PERSON_GENDER,
        CAST("NA" AS STRING) AS AUTHORIZED_PERSON_MOBILE_PHONE,
        CAST("NA" AS STRING) AS AUTHORIZED_PERSON_CSN,
        CAST(NULL AS BYTES) AS T_ROW_HASH
)
-- ,
-- dwh as (
--     select * from dummy_data
--     -- {% if is_incremental() %}
--     -- union all
--     -- select * from dwh_table_exists
--     -- {% endif %}
-- )
select * from dummy_data
-- SELECT 
-- (SELECT COALESCE(MAX(T_DIM_KEY), 1) FROM dwh ) + ROW_NUMBER() over (Order by staging.T_INGESTION_TIMESTAMP ASC) as T_DIM_KEY,
-- *
-- FROM staging