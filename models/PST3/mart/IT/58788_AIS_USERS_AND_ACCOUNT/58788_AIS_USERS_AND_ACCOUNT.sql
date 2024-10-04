
{% set period_time = period_calculate(time = 'semesterly', selection_date="today", prefix='', suffix='S' ) -%}
{% set time_zone = "Etc/UTC" -%}
{% set country_code = 'IT' -%}


        WITH obsolete_and_active_consents_count_in_reporting_period AS (
SELECT
  '86' as CountryOfAISP,
  ac.USER_TOKEN,
  COUNT(*) AS number_of_consents,
FROM  {{ source('source_dwh_STRP','D_ACCESS_CONSENT_INFO_CURRENT') }} AS ac
inner JOIN {{ source('source_dwh_STRP','D_PXG_PAYMENT_ACCOUNT_CURRENT') }} AS pa
  ON pa.T_DIM_KEY=ac.T_D_PXG_PAYMENT_ACCOUNT_DIM_KEY
 inner JOIN {{ source('source_dwh_STRP','D_FINANCIAL_PLATFORMS_DECRYPTED') }} AS fp
  ON pa.T_D_FINANCIAL_PLATFORM_DIM_KEY=fp.T_DIM_KEY
inner JOIN {{ source('source_dwh_STRP','D_FINANCIAL_INSTITUTIONS') }} AS fi
  ON fi.T_DIM_KEY = fp.T_D_FINANCIAL_INSTITUTION_DIM_KEY
inner JOIN {{ source('source_dwh_STRP','D_CONTRACT_INFO') }} AS c
  ON pa.T_DIM_KEY=c.T_D_PXG_PAYMENT_ACCOUNT_DIM_KEY
inner JOIN {{ source('source_dwh_STRP','D_APPLICATION_ACCOUNT_INFO_DECRYPTED') }} AS aa
  ON c.T_D_APPLICATION_ACCOUNT_DIM_KEY=aa.T_DIM_KEY
inner JOIN {{ source('source_dwh_STRP','D_APPLICATIONS_DECRYPTED') }} AS a
  ON aa.T_D_APPLICATION_DIM_KEY = a.T_DIM_KEY
WHERE a.APPLICATION_NAME in ('PAY-PXG-BANQUPIT')
    AND fi.FINANCIAL_INSTITUTION_CODE <> 'IBIS' -- consents on UPP's own accounts are not counted
    AND (
        (ac.ACCESS_CONSENT_CREATED_AT >= TIMESTAMP(DATETIME( '{{begin_date}}', '{{time_zone}}'))
            AND ac.ACCESS_CONSENT_CREATED_AT <= TIMESTAMP(DATETIME( '{{end_date}}', '{{time_zone}}'))
        )
        OR (ac.ACCESS_CONSENT_STATUS_AT > TIMESTAMP(DATETIME( '{{begin_date}}', '{{time_zone}}'))
            AND ac.ACCESS_CONSENT_STATUS_AT <= TIMESTAMP(DATETIME( '{{end_date}}', '{{time_zone}}'))
            )
    )
GROUP BY
            CountryOfAISP,
            ac.USER_TOKEN
)
SELECT
CountryOfAISP,
COUNT(DISTINCT USER_TOKEN) AS unique_users_count_in_period,
CURRENT_TIMESTAMP AS LOAD_TIMESTAMP,
"{{period}}"  AS Period,
FROM obsolete_and_active_consents_count_in_reporting_period
GROUP BY CountryOfAISP
order by CountryOfAISP asc
