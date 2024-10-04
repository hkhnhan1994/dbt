
{% set period_time = period_calculate(time = 'monthly', selection_date="today", prefix='', suffix='M' ) -%}
{% set time_zone = "Etc/UTC" -%}
{% set country_code = 'BE' -%}


        SELECT
    "Y" AS Ot,
    row_number() over () as IDReg,
    LAST_DAY(date_add(DATE(TIMESTAMP(DATETIME( '{{period_time['begin_date']}}', '{{time_zone}}'))), INTERVAL -1 MONTH)) as DtRef,
    'PSDBE-NBB-0649860804' as AISP,
    '{{country_code}}' as PasCl,
    COUNT(DISTINCT acic.USER_TOKEN) as NCl,
    "{{period}}"  AS Period,
    CURRENT_TIMESTAMP AS LOAD_TIMESTAMP,
FROM {{ source('source_dwh_STRP','D_ACCESS_CONSENT_INFO_CURRENT') }} as acic
JOIN {{ source('source_dwh_STRP','D_PXG_PAYMENT_ACCOUNT') }} AS pac
    ON pac.T_DIM_KEY = acic.T_D_PXG_PAYMENT_ACCOUNT_DIM_KEY
JOIN {{ source('source_dwh_STRP','D_FINANCIAL_PLATFORMS') }} AS fp
        ON pac.T_D_FINANCIAL_PLATFORM_DIM_KEY = fp.T_DIM_KEY
JOIN {{ source('source_dwh_STRP','D_FINANCIAL_INSTITUTIONS') }} AS fi
    ON fp.T_D_FINANCIAL_INSTITUTION_DIM_KEY = fi.T_DIM_KEY
JOIN {{ source('source_dwh_STRP','D_CONTRACT_INFO') }} AS ci
    ON ci.T_D_PXG_PAYMENT_ACCOUNT_DIM_KEY = pac.T_DIM_KEY
JOIN {{ source('source_dwh_STRP','D_APPLICATION_ACCOUNT_INFO') }} AS aai
    ON aai.T_DIM_KEY = ci.T_D_APPLICATION_ACCOUNT_DIM_KEY
JOIN {{ source('source_dwh_STRP','D_APPLICATIONS_DECRYPTED') }} AS ad
    ON ad.T_DIM_KEY = aai.T_D_APPLICATION_DIM_KEY
WHERE
    -- ad.APPLICATION_NAME = 'PAY-PXG-BANQUPPT'
    ad.APPLICATION_NAME in ('PAY-PXG-COMMUNITY', 'PAY-PXG-GOCOMPTA', 'PAY-PXG-MAGIC4BUSINESS', 'PAY-PXG-YOURSMINC', 'PAY-PXG-MIJNBOEKHOUDER')  -- Replace previous premise with thise for test with BE data
    and fi.FINANCIAL_INSTITUTION_CODE != 'IBIS'
    AND acic.ACCESS_CONSENT_CREATED_AT >= TIMESTAMP(DATETIME( '{{period_time['begin_date']}}', '{{time_zone}}'))
    AND acic.ACCESS_CONSENT_CREATED_AT <= TIMESTAMP(DATETIME( '{{period_time['end_date']}}', '{{time_zone}}'))
GROUP BY 1,3,4,5,7,8
