
{% set period_time = period_calculate(time = 'semesterly', selection_date="today", prefix='', suffix='S' ) -%}
{% set time_zone = "Etc/UTC" -%}
{% set country_code = 'BE' -%}


        SELECT
    distinct ap.APPLICATION_NAME AS APPLICATION_NAME,
    -- ap.APPLICATION_CREATED_AT AS CREATION_DATETIME,
    -- ao.APPLICATION_OWNER_NAME AS APPLICATION_OWNER_NAME,
    CURRENT_TIMESTAMP AS LOAD_TIMESTAMP,
    "{{period}}"  AS Period,
    CAST(NULL AS TIMESTAMP) AS Period_begin_date,
    CAST(NULL AS TIMESTAMP) AS Period_end_date,
FROM (
    WITH current_table AS (
    SELECT *, ROW_NUMBER() OVER(PARTITION BY T_BUS_KEY ORDER BY T_INGESTION_TIMESTAMP desc, T_LOAD_TIMESTAMP desc) AS rn
    FROM  {{ source('source_dwh_STRP','D_APPLICATIONS_DECRYPTED') }}
)
SELECT * EXCEPT(rn)
FROM current_table
WHERE rn = 1
) AS ap
LEFT  JOIN {{ source('source_dwh_STRP','D_APPLICATION_OWNERS_DECRYPTED') }} AS ao
    ON ap.T_D_OWNER_DIM_KEY =  ao.T_DIM_KEY
LEFT JOIN {{ source('source_dwh_STRP','D_APPLICATION_ACCOUNT_INFO_DECRYPTED') }} AS aa
    ON ap.t_dim_key = aa.T_D_APPLICATION_DIM_KEY
LEFT JOIN {{ source('source_dwh_STRP','D_CONTRACT_INFO_CURRENT') }} AS ci
    ON aa.T_DIM_KEY= ci.T_D_APPLICATION_ACCOUNT_DIM_KEY
LEFT JOIN {{ source('source_dwh_STRP','D_PXG_PAYMENT_ACCOUNT_DECRYPTED') }}
    AS pa on pa.T_DIM_KEY= ci.T_D_PXG_PAYMENT_ACCOUNT_DIM_KEY
LEFT JOIN {{ source('source_dwh_STRP','D_FINANCIAL_PLATFORMS_DECRYPTED') }}
    AS fp ON pa.T_D_FINANCIAL_PLATFORM_DIM_KEY=fp.T_DIM_KEY
LEFT JOIN {{ source('source_dwh_STRP','D_FINANCIAL_INSTITUTIONS') }}
    AS fi ON fi.T_DIM_KEY = fp.T_D_FINANCIAL_INSTITUTION_DIM_KEY
-- WHERE ap.APPLICATION_NAME <>"NA"
    -- AND TIMESTAMP(ap.APPLICATION_UPDATED_AT) >= TIMESTAMP(DATETIME( '{{begin_date}}', '{{time_zone}}')) --pt winter time
    -- AND TIMESTAMP(ap.APPLICATION_UPDATED_AT) <= TIMESTAMP(DATETIME( '{{end_date}}', '{{time_zone}}'))  --pt winter time
