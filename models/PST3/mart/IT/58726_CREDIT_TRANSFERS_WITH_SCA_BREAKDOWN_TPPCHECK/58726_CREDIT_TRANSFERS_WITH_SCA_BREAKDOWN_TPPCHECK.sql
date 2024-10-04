
{% set period_time = period_calculate(time = 'quarterly', selection_date="today", prefix='', suffix='Q' ) -%}
{% set time_zone = "Etc/UTC" -%}
{% set country_code = 'IT' -%}


        SELECT
  D.T_SOURCE_PK_ID,
  D.T_SOURCE_PK_UUID,
  D.SERVICE_PROVIDER_VERSION ,
  D.SERVICE_PROVIDER_ACTIVE ,
  D.SERVICE_PROVIDER_PSP_AUTHORITY_ID ,
  D.SERVICE_PROVIDER_DISPLAY_NAME,
  D.SERVICE_PROVIDER_CREATED_AT,
  D.SERVICE_PROVIDER_UPDATED_AT,
  CURRENT_TIMESTAMP AS LOAD_TIMESTAMP,
  "{{period}}"  AS Period,
FROM {{ source('source_dwh_STRP','D_ASPSP_TPP_CURRENT') }} C
JOIN  {{ source('source_dwh_STRP','D_ASPSP_TPP_DECRYPTED') }} D
  ON C.T_SOURCE_PK_UUID = D.T_SOURCE_PK_UUID
WHERE D.T_SOURCE_PK_UUID <> '93773d5d-00b9-422d-af5c-b90259cf50ee'
  AND D.SERVICE_PROVIDER_CREATED_AT >= TIMESTAMP(DATETIME( '{{begin_date}}', '{{time_zone}}'))
  AND D.SERVICE_PROVIDER_CREATED_AT <= TIMESTAMP(DATETIME( '{{end_date}}', '{{time_zone}}'))
