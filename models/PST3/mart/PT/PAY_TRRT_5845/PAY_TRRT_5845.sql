
{% set period_time = period_calculate(time = 'daily', selection_date="today", prefix='', suffix='D' ) -%}
{% set time_zone = "Etc/UTC" -%}
{% set country_code = 'BE' -%}


        SELECT
    'O' AS Ot,
    ftr.T_SOURCE_PK_ID AS Ref,
    '' AS ORef,
    IF(deb.FINANCIAL_INSTITUTION_COUNTRY_CODE = '{{country_code}}',SUBSTR(deb.BANK_ACCOUNT_NUMBER, 5, 4), '9999') AS Ord,
    IF(cred.FINANCIAL_INSTITUTION_COUNTRY_CODE = '{{country_code}}', SUBSTR(cred.BANK_ACCOUNT_NUMBER, 5, 4), '9999') AS Ben,
    '' AS PayID,
    deb.BANK_ACCOUNT_BIC AS BICOrd,
    cred.BANK_ACCOUNT_BIC AS BICBen,
    '' AS MotrTransSCT,
    '' AS MotrTransInst,
    '' AS MotrTransTns,
    '' AS MotrTransTgtSwift,
    DATE(dtr.TRANSACTION_BOOKING_DATE_AT) AS DtLiqRtrans,
    '4' AS TiprTransTRF,
    "{{period}}"  AS Period,
    CURRENT_TIMESTAMP AS load_timestamp,
FROM {{ source('source_dwh_STRP','F_ACCOUNT_TRANSACTIONS_DECRYPTED') }} ftr
LEFT JOIN {{ source('source_dwh_STRP','D_IBIS_ACCOUNT_CURRENT') }} iacc
    ON ftr.T_D_IBIS_ACCOUNT_DIM_KEY = iacc.T_DIM_KEY
INNER JOIN {{ source('source_dwh_STRP','D_BANK_ACCOUNTS_DECRYPTED') }} deb
    ON iacc.T_D_BANK_ACCOUNT_DIM_KEY = deb.T_DIM_KEY
LEFT JOIN {{ source('source_dwh_STRP','D_BANK_ACCOUNTS_DECRYPTED') }} cred
    ON cred.T_DIM_KEY = ftr.T_COUNTERPARTY_BANK_ACCOUNT_DIM_KEY
LEFT JOIN {{ source('source_dwh_STRP','D_ACCOUNT_TRANSACTION_CURRENT') }} dtr
    ON ftr.T_D_ACCOUNT_TRANSACTION_DIM_KEY = dtr.T_DIM_KEY
WHERE ftr.TRANSACTION_DIRECTION = 'OUTBOUND'
    AND deb.FINANCIAL_INSTITUTION_COUNTRY_CODE = '{{country_code}}'
    AND ftr.transaction_type = 'RETURN'
    AND dtr.TRANSACTION_STATUS IN ('SETTLED', 'RETURNED')
    AND ftr.TRANSACTION_BANK_FAMILY = 'ICDT'
    AND ftr.TRANSACTION_CHANNEL <> 'CARDS'
    AND TIMESTAMP(dtr.TRANSACTION_BOOKING_DATE_AT) >= TIMESTAMP(DATETIME( '{{begin_date}}', '{{time_zone}}')) --pt winter time
    AND TIMESTAMP(dtr.TRANSACTION_BOOKING_DATE_AT) <= TIMESTAMP(DATETIME( '{{end_date}}', '{{time_zone}}'))   --pt winter time
UNION ALL
SELECT
    'B' AS Ot,
    ftr.T_SOURCE_PK_ID AS Ref,
    '' AS ORef,
    IF(deb.FINANCIAL_INSTITUTION_COUNTRY_CODE = '{{country_code}}',SUBSTR(deb.BANK_ACCOUNT_NUMBER, 5, 4), '9999') AS Ord,
    IF(cred.FINANCIAL_INSTITUTION_COUNTRY_CODE = '{{country_code}}', SUBSTR(cred.BANK_ACCOUNT_NUMBER, 5, 4), '9999') AS Ben,
    '' AS PayID,
    deb.BANK_ACCOUNT_BIC AS BICOrd,
    cred.BANK_ACCOUNT_BIC AS BICBen,
    '' AS MotrTransSCT,
    '' AS MotrTransInst,
    '' AS MotrTransTns,
    '' AS MotrTransTgtSwift,
    DATE(dtr.TRANSACTION_BOOKING_DATE_AT) AS DtLiqRtrans,
    '4' AS TiprTransTRF,
    "{{period}}"  AS Period,
    CURRENT_TIMESTAMP AS load_timestamp,
FROM {{ source('source_dwh_STRP','F_ACCOUNT_TRANSACTIONS_DECRYPTED') }} ftr
LEFT JOIN {{ source('source_dwh_STRP','D_IBIS_ACCOUNT_CURRENT') }} iacc
    ON ftr.T_D_IBIS_ACCOUNT_DIM_KEY = iacc.T_DIM_KEY
INNER JOIN {{ source('source_dwh_STRP','D_BANK_ACCOUNTS_DECRYPTED') }} cred
    ON iacc.T_D_BANK_ACCOUNT_DIM_KEY = cred.T_DIM_KEY
LEFT JOIN {{ source('source_dwh_STRP','D_BANK_ACCOUNTS_DECRYPTED') }} deb
    ON deb.T_DIM_KEY = ftr.T_COUNTERPARTY_BANK_ACCOUNT_DIM_KEY
LEFT JOIN {{ source('source_dwh_STRP','D_ACCOUNT_TRANSACTION_CURRENT') }} dtr
    ON ftr.T_D_ACCOUNT_TRANSACTION_DIM_KEY = dtr.T_DIM_KEY
WHERE ftr.TRANSACTION_DIRECTION = 'INBOUND'
    AND cred.FINANCIAL_INSTITUTION_COUNTRY_CODE in ('{{country_code}}')
    AND iacc.ACCOUNT_TYPE = 'PAYMENT'
    AND ftr.transaction_type IN ('RETURN')
    AND dtr.TRANSACTION_STATUS IN ('SETTLED', 'RETURNED')
    AND ftr.TRANSACTION_BANK_FAMILY = 'RCDT'
    AND TIMESTAMP(dtr.TRANSACTION_BOOKING_DATE_AT) >= TIMESTAMP(DATETIME( '{{begin_date}}', '{{time_zone}}')) --pt winter time
    AND TIMESTAMP(dtr.TRANSACTION_BOOKING_DATE_AT) <= TIMESTAMP(DATETIME( '{{end_date}}', '{{time_zone}}'))   --pt winter time
--UNMATCHED TRANSACTIONS
UNION ALL
SELECT
    'O' AS Ot,
    utr.T_SOURCE_PK_ID AS Ref,
    '' AS ORef,
    IF(deb.FINANCIAL_INSTITUTION_COUNTRY_CODE = '{{country_code}}',SUBSTR(deb.BANK_ACCOUNT_NUMBER, 5, 4), '9999') AS Ord,
    IF(cred.FINANCIAL_INSTITUTION_COUNTRY_CODE = '{{country_code}}', SUBSTR(cred.BANK_ACCOUNT_NUMBER, 5, 4), '9999') AS Ben,
    '' AS PayID,
    deb.BANK_ACCOUNT_BIC AS BICOrd,
    cred.BANK_ACCOUNT_BIC AS BICBen,
    '' AS MotrTransSCT,
    '' AS MotrTransInst,
    '' AS MotrTransTns,
    '' AS MotrTransTgtSwift,
    DATE(utr.TRANSACTION_BOOKING_DATE_AT) AS DtLiqRtrans,
    '4' AS TiprTransTRF,
    "{{period}}"  AS Period,
    CURRENT_TIMESTAMP AS load_timestamp,
FROM {{ source('source_dwh_STRP','F_UNMATCHED_ACCOUNT_TRANSACTIONS') }} utr
INNER JOIN {{ source('source_dwh_STRP','D_BANK_ACCOUNTS_DECRYPTED') }} deb
    ON utr.T_D_MISSING_BANK_ACCOUNT_DIM_KEY = deb.T_DIM_KEY
LEFT JOIN {{ source('source_dwh_STRP','D_BANK_ACCOUNTS_DECRYPTED') }} cred
    ON cred.T_DIM_KEY = utr.T_COUNTERPARTY_BANK_ACCOUNT_DIM_KEY
-- LEFT JOIN {{ source('source_dwh_STRP','D_ACCOUNT_TRANSACTION_CURRENT') }} dtr
--     ON utr.T_D_ACCOUNT_TRANSACTION_DIM_KEY = dtr.T_DIM_KEY
WHERE utr.TRANSACTION_DIRECTION = 'OUTBOUND'
    AND DEB.FINANCIAL_INSTITUTION_COUNTRY_CODE in ('{{country_code}}')
    AND utr.transaction_type = 'RETURN'
    AND utr.TRANSACTION_STATUS IN ('SETTLED', 'RETURNED')
    AND utr.TRANSACTION_BANK_FAMILY = 'ICDT'
    AND utr.TRANSACTION_CHANNEL <> 'CARDS'
    AND TIMESTAMP(utr.TRANSACTION_BOOKING_DATE_AT) >= TIMESTAMP(DATETIME( '{{begin_date}}', '{{time_zone}}')) --pt winter time
    AND TIMESTAMP(utr.TRANSACTION_BOOKING_DATE_AT) <= TIMESTAMP(DATETIME( '{{end_date}}', '{{time_zone}}'))   --pt winter time
UNION ALL
SELECT
    'B' AS Ot,
    utr.T_SOURCE_PK_ID AS Ref,
    '' AS ORef,
    IF(deb.FINANCIAL_INSTITUTION_COUNTRY_CODE = '{{country_code}}',SUBSTR(deb.BANK_ACCOUNT_NUMBER, 5, 4), '9999') AS Ord,
    IF(cred.FINANCIAL_INSTITUTION_COUNTRY_CODE = '{{country_code}}', SUBSTR(cred.BANK_ACCOUNT_NUMBER, 5, 4), '9999') AS Ben,
    '' AS PayID,
    deb.BANK_ACCOUNT_BIC AS BICOrd,
    cred.BANK_ACCOUNT_BIC AS BICBen,
    '' AS MotrTransSCT,
    '' AS MotrTransInst,
    '' AS MotrTransTns,
    '' AS MotrTransTgtSwift,
    DATE(utr.TRANSACTION_BOOKING_DATE_AT) AS DtLiqRtrans,
    '4' AS TiprTransTRF,
    "{{period}}"  AS Period,
    CURRENT_TIMESTAMP AS load_timestamp,
FROM {{ source('source_dwh_STRP','F_UNMATCHED_ACCOUNT_TRANSACTIONS') }} utr
INNER JOIN {{ source('source_dwh_STRP','D_BANK_ACCOUNTS_DECRYPTED') }} CRED
    ON utr.T_D_MISSING_BANK_ACCOUNT_DIM_KEY = CRED.T_DIM_KEY
LEFT JOIN {{ source('source_dwh_STRP','D_BANK_ACCOUNTS_DECRYPTED') }} DEB
    ON DEB.T_DIM_KEY = utr.T_COUNTERPARTY_BANK_ACCOUNT_DIM_KEY
-- LEFT JOIN {{ source('source_dwh_STRP','D_ACCOUNT_TRANSACTION_CURRENT') }} dtr
--     ON utr.T_D_ACCOUNT_TRANSACTION_DIM_KEY = dtr.T_DIM_KEY
WHERE utr.TRANSACTION_DIRECTION = 'INBOUND'
    AND CRED.FINANCIAL_INSTITUTION_COUNTRY_CODE in ('{{country_code}}')
    AND utr.transaction_type = 'RETURN'
    AND utr.TRANSACTION_STATUS IN ('SETTLED', 'RETURNED')
    AND utr.TRANSACTION_BANK_FAMILY = 'RCDT'
    AND TIMESTAMP(utr.TRANSACTION_BOOKING_DATE_AT) >= TIMESTAMP(DATETIME( '{{begin_date}}', '{{time_zone}}')) --pt winter time
    AND TIMESTAMP(utr.TRANSACTION_BOOKING_DATE_AT) <= TIMESTAMP(DATETIME( '{{end_date}}', '{{time_zone}}'))   --pt winter time
