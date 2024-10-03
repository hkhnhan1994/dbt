WITH pre AS (
  SELECT
    'DE' AS country,
    ac.USER_TOKEN,
    count(*),
  FROM {{ source('source_dwh_strp,D_PXG_PAYMENT_ACCOUNT_DECRYPTED') }} AS pa
  LEFT JOIN {{ source('source_dwh_strp,D_FINANCIAL_PLATFORMS_DECRYPTED') }} AS fp
    ON pa.T_D_FINANCIAL_PLATFORM_DIM_KEY=fp.T_DIM_KEY
  LEFT JOIN {{ source('source_dwh_strp,D_FINANCIAL_INSTITUTIONS') }} AS fi
    ON fi.T_DIM_KEY = fp.T_D_FINANCIAL_INSTITUTION_DIM_KEY
  LEFT JOIN {{ source('source_dwh_strp,D_ACCESS_CONSENT_INFO') }} AS ac
    ON pa.T_DIM_KEY=ac.T_D_PXG_PAYMENT_ACCOUNT_DIM_KEY
  LEFT JOIN {{ source('source_dwh_strp,D_CONTRACT_INFO') }} AS c
    ON pa.T_DIM_KEY=c.T_D_PXG_PAYMENT_ACCOUNT_DIM_KEY
  LEFT JOIN {{ source('source_dwh_strp,D_APPLICATION_ACCOUNT_INFO_DECRYPTED') }} AS aa
    ON c.T_D_APPLICATION_ACCOUNT_DIM_KEY=aa.T_DIM_KEY
  LEFT JOIN {{ source('source_dwh_strp,D_APPLICATIONS_DECRYPTED') }} AS a
    ON aa.T_D_APPLICATION_DIM_KEY = a.T_DIM_KEY
  WHERE a.APPLICATION_NAME = 'PAY-PXG-BANQUPDE'
    AND fi.FINANCIAL_INSTITUTION_CODE <> 'IBIS'
    AND (
      TIMESTAMP(ac.ACCESS_CONSENT_STATUS_AT) >= TIMESTAMP(DATETIME( '{{period_time['begin_date']}}', '{{time_zone}}'))
      AND TIMESTAMP(ac.ACCESS_CONSENT_STATUS_AT) <= TIMESTAMP(DATETIME( '{{period_time['end_date']}}', '{{time_zone}}'))
      OR
      (
      TIMESTAMP(ac.ACCESS_CONSENT_CREATED_AT) >= TIMESTAMP(DATETIME( '{{period_time['begin_date']}}', '{{time_zone}}'))
      AND TIMESTAMP(ac.ACCESS_CONSENT_CREATED_AT) <= TIMESTAMP(DATETIME( '{{period_time['end_date']}}', '{{time_zone}}'))
      )
    )
  GROUP BY 1,2
)
SELECT
  country ,
  count(distinct USER_TOKEN) as unique_users_count_in_period,
  CURRENT_TIMESTAMP AS LOAD_TIMESTAMP,
  '{{period}}' AS PERIOD,
FROM PRE
GROUP BY 1
