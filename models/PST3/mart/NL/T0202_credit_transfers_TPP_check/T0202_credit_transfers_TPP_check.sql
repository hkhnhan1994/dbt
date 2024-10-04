
{% set period_time = period_calculate(time = 'semesterly', selection_date="today", prefix='', suffix='S' ) -%}
{% set time_zone = "Etc/UTC" -%}
{% set country_code = 'NL' -%}


        select
  D.T_SOURCE_PK_ID,
  D.T_SOURCE_PK_UUID,
  D.SERVICE_PROVIDER_VERSION ,
  D.SERVICE_PROVIDER_ACTIVE ,
  D.SERVICE_PROVIDER_PSP_AUTHORITY_ID ,
  D.SERVICE_PROVIDER_DISPLAY_NAME,
  D.SERVICE_PROVIDER_CREATED_AT,
  D.SERVICE_PROVIDER_UPDATED_AT,
  CURRENT_TIMESTAMP AS LOAD_TIMESTAMP,
  '{{period}}' AS PERIOD,
from {{ source('source_dwh_STRP','D_ASPSP_TPP_CURRENT') }} C
inner join  {{ source('source_dwh_STRP','D_ASPSP_TPP_DECRYPTED') }} D
  on C.T_SOURCE_PK_UUID = D.T_SOURCE_PK_UUID
where D.T_SOURCE_PK_UUID <> '93773d5d-00b9-422d-af5c-b90259cf50ee'
  AND C.SERVICE_PROVIDER_CREATED_AT >= TIMESTAMP(DATETIME('{{begin_date}}', '{{time_zone}}'))
  AND C.SERVICE_PROVIDER_CREATED_AT <= TIMESTAMP(DATETIME( '{{end_date}}', '{{time_zone}}'))
