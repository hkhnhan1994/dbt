
{% set period_time = period_calculate(time = 'semesterly', selection_date="today", prefix='', suffix='S' ) -%}
{% set time_zone = "Etc/UTC" -%}
{% set country_code = 'BE' -%}


        SELECT
  a.APPLICATION_NAME AS Application_name,
  aa.APPLICATION_ACCOUNT_NAME Application_account_name,
  aa.T_SOURCE_PK_ID as Application_account_id ,
  "{{period}}"  AS Period,
  CURRENT_TIMESTAMP AS load_timestamp,
  TIMESTAMP(DATETIME( '{{period_time['begin_date']}}', '{{time_zone}}')) AS Period_begin_date,
  CAST(NULL AS TIMESTAMP) AS Period_end_date,
FROM {{ source('source_dwh_STRP','D_ACCESS_CONSENT_INFO_CURRENT') }} AS ac
JOIN {{ source('source_dwh_STRP','D_ACCESS_CONSENT_INFO_DECRYPTED') }} acd
  ON acd.T_DIM_KEY = ac.T_DIM_KEY
JOIN {{ source('source_dwh_STRP','D_PXG_PAYMENT_ACCOUNT_DECRYPTED') }}
  AS pa ON pa.T_DIM_KEY=ac.T_D_PXG_PAYMENT_ACCOUNT_DIM_KEY
JOIN {{ source('source_dwh_STRP','D_FINANCIAL_PLATFORMS_DECRYPTED') }}
  AS fp ON pa.T_D_FINANCIAL_PLATFORM_DIM_KEY=fp.T_DIM_KEY
JOIN {{ source('source_dwh_STRP','D_FINANCIAL_INSTITUTIONS') }}
  AS fi ON fi.T_DIM_KEY = fp.T_D_FINANCIAL_INSTITUTION_DIM_KEY
JOIN {{ source('source_dwh_STRP','D_CONTRACT_INFO_CURRENT') }}
  AS c ON pa.T_DIM_KEY=c.T_D_PXG_PAYMENT_ACCOUNT_DIM_KEY
JOIN {{ source('source_dwh_STRP','D_APPLICATION_ACCOUNT_INFO_DECRYPTED') }}
  AS aa ON c.T_D_APPLICATION_ACCOUNT_DIM_KEY=aa.T_DIM_KEY
JOIN {{ source('source_dwh_STRP','D_APPLICATIONS_DECRYPTED') }}
  AS a ON aa.T_D_APPLICATION_DIM_KEY = a.T_DIM_KEY
JOIN {{ source('source_dwh_STRP','D_APPLICATION_OWNERS_DECRYPTED') }}
  AS ao ON a.T_D_OWNER_DIM_KEY =  ao.T_DIM_KEY
WHERE
    a.APPLICATION_NAME = 'PAY-PXG-JEFACTURE'
AND fi.FINANCIAL_INSTITUTION_COUNTRY = 'BE'
AND fi.FINANCIAL_INSTITUTION_CODE = 'IBIS'
AND (
  ac.ACCESS_CONSENT_STATUS = 'ACTIVE'
  OR (
    ac.ACCESS_CONSENT_STATUS = 'OBSOLETE'
    AND TIMESTAMP(ac.ACCESS_CONSENT_STATUS_AT) >= TIMESTAMP(DATETIME( '{{period_time['begin_date']}}', '{{time_zone}}'))
  ) --==> always start of the period
)
