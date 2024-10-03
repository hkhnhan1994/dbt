SELECT *,
CURRENT_TIMESTAMP AS LOAD_TIMESTAMP,
'{{period}}' AS PERIOD,
FROM {{ source('source_dwh_strp,D_ASPSP_TPP_DECRYPTED') }} t
WHERE t.T_SOURCE_PK_UUID <> '93773d5d-00b9-422d-af5c-b90259cf50ee'
AND SERVICE_PROVIDER_CREATED_AT >= TIMESTAMP(DATETIME( '{{period_time['begin_date']}}', '{{time_zone}}'))
AND SERVICE_PROVIDER_CREATED_AT <= TIMESTAMP(DATETIME( '{{period_time['end_date']}}', '{{time_zone}}'))
AND T_LOAD_TIMESTAMP is not null
