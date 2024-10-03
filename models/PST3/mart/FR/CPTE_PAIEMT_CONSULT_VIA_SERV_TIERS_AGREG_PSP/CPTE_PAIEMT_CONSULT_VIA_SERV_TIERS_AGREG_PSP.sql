SELECT
     d_access_consent_info.USER_EMAIL AS user_token,
     COUNT(DISTINCT(d_access_consent_info.USER_EMAIL)) OVER(PARTITION BY d_access_consent_info.USER_EMAIL) unique_users_count_in_period,
     COUNT(DISTINCT(d_access_consent_info.T_BUS_KEY)) OVER(PARTITION BY d_access_consent_info.T_BUS_KEY) number_of_consents,
     CURRENT_TIMESTAMP AS Load_timestamp,
     "{{period}}"  AS Period,
FROM {{ source('source_dwh_strp,D_PXG_PAYMENT_ACCOUNT_DECRYPTED') }} AS d_pxg_payment_account
LEFT JOIN {{ source('source_dwh_strp,D_FINANCIAL_PLATFORMS_DECRYPTED') }} AS d_financial_platforms
     ON d_pxg_payment_account.T_D_FINANCIAL_PLATFORM_DIM_KEY=d_financial_platforms.T_DIM_KEY
LEFT JOIN {{ source('source_dwh_strp,D_FINANCIAL_INSTITUTIONS') }} AS d_financial_institutions
     ON d_financial_institutions.T_DIM_KEY = d_financial_platforms.T_D_FINANCIAL_INSTITUTION_DIM_KEY
LEFT JOIN {{ source('source_dwh_strp,D_ACCESS_CONSENT_INFO_DECRYPTED') }} AS d_access_consent_info
     ON d_pxg_payment_account.T_DIM_KEY=d_access_consent_info.T_D_PXG_PAYMENT_ACCOUNT_DIM_KEY
LEFT JOIN {{ source('source_dwh_strp,D_CONTRACT_INFO') }} AS d_contract_info
     ON d_pxg_payment_account.T_DIM_KEY=d_contract_info.T_D_PXG_PAYMENT_ACCOUNT_DIM_KEY
LEFT JOIN {{ source('source_dwh_strp,D_APPLICATION_ACCOUNT_INFO_DECRYPTED') }} AS d_application_account_info
     ON d_contract_info.T_D_APPLICATION_ACCOUNT_DIM_KEY=d_application_account_info.T_DIM_KEY
LEFT JOIN {{ source('source_dwh_strp,D_APPLICATIONS_DECRYPTED') }} AS d_applications
     ON d_application_account_info.T_D_APPLICATION_DIM_KEY = d_applications.T_DIM_KEY
WHERE  (
     d_applications.APPLICATION_NAME  = 'PAY-PXG-JEFACTURE'
     -- AND d_application_account_info.T_SOURCE_PK_ID not in ('8856', '4545', '4571', '5170', '5188', '5244', '5275', '5301',
     --                                                        '5343', '5411', '5422', '5443', '5495', '5511', '5560', '5593',
     --                                                        '5628', '5651', '5652', '5653', '5654', '5655', '5657', '5658',
     --                                                        '5659', '5660', '5661', '5662', '6315', '6354', '6745', '6922',
     --                                                        '6960', '7175', '7222', '7386', '7428', '7582', '7666', '7753', '7768'
     --                                                        )
     )
     AND d_financial_institutions.FINANCIAL_INSTITUTION_CODE  <> 'IBIS'
     AND
     (
          (
          d_access_consent_info.ACCESS_CONSENT_CREATED_AT >= TIMESTAMP(DATETIME( '{{period_time['begin_date']}}', '{{time_zone}}'))
          AND d_access_consent_info.ACCESS_CONSENT_CREATED_AT <= TIMESTAMP(DATETIME( '{{period_time['end_date']}}', '{{time_zone}}'))
          )
     OR
          (
          d_access_consent_info.ACCESS_CONSENT_STATUS_AT >= TIMESTAMP(DATETIME( '{{period_time['begin_date']}}', '{{time_zone}}'))
          AND d_access_consent_info.ACCESS_CONSENT_STATUS_AT <= TIMESTAMP(DATETIME( '{{period_time['end_date']}}', '{{time_zone}}'))
          )
     )
