
{% set period_time = period_calculate(time = 'quarterly', selection_date="today", prefix='', suffix='Q' ) -%}
{% set time_zone = "Etc/UTC" -%}
{% set country_code = 'IT' -%}


        select
  count(*) AS number_of_new_contracts,
  CURRENT_TIMESTAMP AS Load_timestamp,
  "{{period}}"  AS Period,
from {{ source('source_dwh_STRP','D_IBIS_ACCOUNT_CURRENT') }} iacc
WHERE
  iacc.ACCOUNT_TYPE = "PAYMENT"
  AND  iacc.ACCOUNT_COUNTRY_CODE = '{{country_code}}'
  AND iacc.ACCOUNT_CREATED_AT >= TIMESTAMP(DATETIME( '{{begin_date}}', '{{time_zone}}'))
  AND iacc.ACCOUNT_CREATED_AT <= TIMESTAMP(DATETIME( '{{end_date}}', '{{time_zone}}'))
  AND iacc.ACCOUNT_CREATED_AT >= TIMESTAMP(DATETIME( '{{begin_date}}', '{{time_zone}}'))
  AND iacc.ACCOUNT_CREATED_AT <= TIMESTAMP(DATETIME( '{{end_date}}', '{{time_zone}}'))
