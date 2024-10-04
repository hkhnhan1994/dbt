
{% set period_time = period_calculate(time = 'yearly', selection_date="today", prefix='', suffix='Y' ) -%}
{% set time_zone = "Etc/UTC" -%}
{% set country_code = 'NL' -%}


        select
  left(t.SERVICE_PROVIDER_PSP_AUTHORITY_ID,2) countryOfAISP,
  c.consent_iban,
  CURRENT_TIMESTAMP AS LOAD_TIMESTAMP,
  '{{period_time['period']}}' AS PERIOD,
from {{ source('source_dwh_STRP','D_ASPSP_CONSENT_DECRYPTED') }} c
    inner join {{ source('source_dwh_STRP','D_ASPSP_CONSENT_DECRYPTED') }} cc on c.T_DIM_KEY = cc.T_DIM_KEY
    inner join {{ source('source_dwh_STRP','D_ASPSP_TPP') }} t on c.T_D_ASPSP_TPP_DIM_KEY = t.T_DIM_KEY
where
  c.CONSENT_STATUS = 'VALID'
  and left(c.consent_iban,2) = '{{country_code}}'
  and c.CONSENT_CREATED_AT >= TIMESTAMP(DATETIME('{{period_time['begin_date']}}', '{{time_zone}}'))
  and c.CONSENT_EXPIRED_AT <= TIMESTAMP(DATETIME( '{{period_time['end_date']}}', '{{time_zone}}'))
  and  t.T_SOURCE_PK_UUID <> '93773d5d-00b9-422d-af5c-b90259cf50ee'
