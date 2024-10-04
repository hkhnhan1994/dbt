
{% set period_time = period_calculate(time = 'semesterly', selection_date="today", prefix='', suffix='S' ) -%}
{% set time_zone = "Etc/UTC" -%}
{% set country_code = 'BE' -%}


        SELECT
    ma.COUNT_AREA,
    ma.TRMNL_LCTN,
    ma.INTTN_CHNNL,
    ma.RMT_INTTN,
    ma.PYMNT_SCHM,
    ma.CRD_FNCTN AS CRD_FCTN,
    ma.FRD_TYP,
    COUNT(ma.TRANSACTION_PUBLIC_IDENTIFIER) AS COUNT_OF_TRANSACTION_PUBLIC_IDENTIFIER,
    SUM(ma.AMOUNT) AS SUM_OF_AMOUNT,
    ma.Period,
    ma.Period_begin_date,
    ma.Period_end_date,
    ma.LOAD_TIMESTAMP,
FROM {{ ref('PCP_MERCHANT_ACCT_TRX') }} as ma
GROUP BY 1,2,3,4,5,6,7,10,11,12,13
