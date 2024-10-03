SELECT
     COUNT(distinct c.consent_iban) AS COUNT,
     CURRENT_TIMESTAMP AS LOAD_TIMESTAMP,
     "{{period}}"  AS Period,
FROM
     {{ source('source_dwh_strp,D_ASPSP_CONSENT_DECRYPTED') }} c
     INNER JOIN {{ source('source_dwh_strp,D_ASPSP_CONSENT_DECRYPTED') }} cc
          ON c.T_DIM_KEY = cc.T_DIM_KEY
     INNER JOIN {{ source('source_dwh_strp,D_ASPSP_TPP') }} t
          ON c.T_D_ASPSP_TPP_DIM_KEY = t.T_DIM_KEY
WHERE
     c.CONSENT_STATUS = 'VALID'
     AND LEFT(c.consent_iban, 2) = '{{country_code}}'
     AND c.CONSENT_TRANSACTION_ACCESS_ALLOWED IS TRUE
     AND c.CONSENT_CREATED_AT <= TIMESTAMP(DATETIME( '{{period_time['end_date']}}', '{{time_zone}}'))
     AND c.CONSENT_EXPIRED_AT >= TIMESTAMP(DATETIME( '{{period_time['begin_date']}}', '{{time_zone}}'))
     --AND  t.T_SOURCE_PK_ID <> '1'
     AND t.T_SOURCE_PK_UUID <> '93773d5d-00b9-422d-af5c-b90259cf50ee'
