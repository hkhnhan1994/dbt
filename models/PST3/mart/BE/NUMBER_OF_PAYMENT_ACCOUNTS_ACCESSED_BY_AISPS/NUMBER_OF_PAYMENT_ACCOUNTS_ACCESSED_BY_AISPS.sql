{% set period_time = period_calculate(time = "semesterly", selection_date="today", prefix="", suffix="S" ) -%}
{% set time_zone = "Etc/UTC" -%}
{% set country_code = "BE" -%}

SELECT
        LEFT(t.SERVICE_PROVIDER_PSP_AUTHORITY_ID,2) countryOfAISP,
        c.consent_iban,
        '{{period_time['period']}}'  AS Period,
        TIMESTAMP(DATETIME( '{{period_time['begin_date']}}', "{{time_zone}}"))  AS Period_begin_date,
        TIMESTAMP(DATETIME( '{{period_time['end_date']}}', "{{time_zone}}"))  AS Period_end_date,
        CURRENT_TIMESTAMP AS LOAD_TIMESTAMP,
        
FROM {{ source('source_pst3_strp', 'D_ASPSP_CONSENT_DECRYPTED') }} c
JOIN {{ source('source_pst3_strp', 'D_ASPSP_CONSENT_DECRYPTED') }} cc
        ON c.T_DIM_KEY = cc.T_DIM_KEY
JOIN {{ source('source_pst3_strp', 'D_ASPSP_TPP') }} t 
        ON c.T_D_ASPSP_TPP_DIM_KEY = t.T_DIM_KEY
WHERE c.CONSENT_STATUS = 'VALID'
        AND left(c.consent_iban,2) = '{{country_code}}'
        AND c.CONSENT_CREATED_AT >= TIMESTAMP(DATETIME( '{{period_time['begin_date']}}', "{{time_zone}}"))
        AND c.CONSENT_EXPIRED_AT <= TIMESTAMP(DATETIME( '{{period_time['end_date']}}', "{{time_zone}}"))
        AND  t.T_SOURCE_PK_UUID <> '93773d5d-00b9-422d-af5c-b90259cf50ee'

    