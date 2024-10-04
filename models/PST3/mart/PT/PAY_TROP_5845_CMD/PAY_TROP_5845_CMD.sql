
{% set period_time = period_calculate(time = 'daily', selection_date="today", prefix='', suffix='D' ) -%}
{% set time_zone = "Etc/UTC" -%}
{% set country_code = 'BE' -%}


        SELECT
    pa.PAYMENT_ACCOUNT_NUMBER AS identifier,
    le.ENTERPRISE_ADDRESS_NUMBER AS enterprise_number,
    le.ENTERPRISE_LEGAL_FORM AS legal_form,
    le.ENTERPRISE_ADDRESS_COUNTRY AS country,
    le.ENTERPRISE_ADDRESS_POSTAL_CODE AS postal_code,
FROM(
  WITH current_table AS (
  SELECT *, ROW_NUMBER() OVER(PARTITION BY T_BUS_KEY ORDER BY T_INGESTION_TIMESTAMP desc, T_LOAD_TIMESTAMP desc) AS rn
  FROM  {{ source('source_dwh_STRP','D_PAYMENT_ACCOUNT_DECRYPTED') }}
)
SELECT * EXCEPT(rn)
FROM current_table
WHERE rn = 1
)  AS pa
LEFT JOIN(
select *,
row_number() over(partition by T_D_PAYMENT_ACCOUNT_DIM_KEY order by T_INGESTION_TIMESTAMP desc) as rn
from {{ source('source_dwh_STRP','F_LEGAL_ENTITY_PAYMENT_ACCOUNT_ROLES_DECRYPTED') }}
)  lepar
    ON pa.T_DIM_KEY = lepar.T_D_PAYMENT_ACCOUNT_DIM_KEY AND lepar.T_FACT_KEY != 0
    and rn = 1
LEFT JOIN(
  WITH current_table AS (
    SELECT *, ROW_NUMBER() OVER(PARTITION BY T_BUS_KEY ORDER BY T_INGESTION_TIMESTAMP desc, T_LOAD_TIMESTAMP desc) AS rn
    FROM  {{ source('source_dwh_STRP','D_LEGAL_ENTITY_DECRYPTED') }}
  )
  SELECT * EXCEPT(rn)
  FROM current_table
  WHERE rn = 1
)  AS le
    ON lepar.T_D_LEGAL_ENTITY_DIM_KEY = le.T_DIM_KEY AND le.T_DIM_KEY <> 0
WHERE pa.T_DIM_KEY <> 0 AND pa.PAYMENT_ACCOUNT_COUNTRY = '{{country_code}}'
    AND pa.T_SOURCE = 'P1_PCMD'
