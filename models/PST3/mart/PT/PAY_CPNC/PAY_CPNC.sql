SELECT
    "X" AS Ot,
    row_number() over () as IDReg,
    LAST_DAY(date_add(DATE(TIMESTAMP(DATETIME( '{{period_time['begin_date']}}', '{{time_zone}}'))), INTERVAL -1 MONTH)) as DtRef,
    "5845" AS ASPSP,
    "5493000RZ2KSLKCYNN98" AS LEIASPSP,
    "N.E." AS `If`,
    "2" AS TipCont,
    "" AS DepOvTr,
    le.ENTERPRISE_ADDRESS_COUNTRY as PasTit,
    le.ENTERPRISE_ADDRESS_POSTAL_CODE AS CPos,
    pa.PAYMENT_ACCOUNT_CURRENCY AS Div,
    "Y" AS ContrOB,
    count(1) as NCont,
    count(1) as NContAOn,
    0 AS NContAISPM,
    0 AS SaldoME,
    "{{period}}"  AS Period,
    CURRENT_TIMESTAMP AS LOAD_TIMESTAMP,
FROM {{ source('source_dwh_strp,D_IBIS_ACCOUNT_CURRENT') }} as iac
JOIN {{ source('source_dwh_strp,D_PAYMENT_ACCOUNT_DECRYPTED') }} AS pa
    ON iac.T_D_PAYMENT_ACCOUNT_DIM_KEY = pa.T_DIM_KEY
    AND LEFT(pa.PAYMENT_ACCOUNT_NUMBER,2) = '{{country_code}}'
LEFT JOIN {{ source('source_dwh_strp,F_LEGAL_ENTITY_PAYMENT_ACCOUNT_ROLES') }} AS lep
        ON lep.T_D_PAYMENT_ACCOUNT_DIM_KEY = pa.T_DIM_KEY
LEFT JOIN {{ source('source_dwh_strp,D_LEGAL_ENTITY_DECRYPTED') }} AS le
    ON le.T_DIM_KEY = lep.T_D_LEGAL_ENTITY_DIM_KEY
WHERE
    (
        pa.PAYMENT_ACCOUNT_STATUS = 'ACTIVE'
        OR(
            pa.PAYMENT_ACCOUNT_STATUS != 'ACTIVE'
            AND pa.PAYMENT_ACCOUNT_UPDATED_AT >= TIMESTAMP('{{period_time['begin_date']}}')
        )
    )
    AND left(pa.PAYMENT_ACCOUNT_NUMBER,2) = '{{country_code}}'
    AND pa.PAYMENT_ACCOUNT_CREATED_AT <= TIMESTAMP('{{period_time['end_date']}}')
GROUP BY 1,3,4,5,6,7,8,9,10,11,12,15,16,17,18
