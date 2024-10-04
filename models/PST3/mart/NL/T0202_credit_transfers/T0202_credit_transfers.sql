
{% set period_time = period_calculate(time = 'yearly', selection_date="today", prefix='', suffix='Y' ) -%}
{% set time_zone = "Etc/UTC" -%}
{% set country_code = 'NL' -%}


        SELECT
  CASE
      WHEN DCA.FINANCIAL_INSTITUTION_COUNTRY_CODE = 'GB' THEN 'Extra-EEA'
      WHEN DCA.FINANCIAL_INSTITUTION_COUNTRY_CODE = 'CH' THEN 'Extra-EEA'
      WHEN DCA.FINANCIAL_INSTITUTION_COUNTRY_CODE = 'NL' THEN 'Domestic'
      ELSE DCA.FINANCIAL_INSTITUTION_COUNTRY_CODE
  END AS payee_psp_country,
  CASE
      WHEN FAT.TRANSACTION_CHANNEL IN ('DASHBOARD', 'ADMIN', 'OTHER') THEN 'Paper-based form'
      WHEN FAT.TRANSACTION_CHANNEL IN ('TPP', 'OCS', 'H2H') THEN 'Electronically'
      WHEN FAT.TRANSACTION_CHANNEL IS NULL THEN 'not applicable - return'
      ELSE 'check the query'
  END AS initiationChannel,
  'remote' as remoteness,
  CASE
      WHEN FAT.TRANSACTION_CHANNEL IN ('DASHBOARD', 'ADMIN', 'OTHER') THEN 'not applicable'
      WHEN FAT.TRANSACTION_CHANNEL = 'TPP' THEN 'SCA'
      WHEN FAT.TRANSACTION_CHANNEL = 'OCS'
           AND TRANSACTION_CREDITOR_REFERENCE_VALUE LIKE 'REF.%/%/%' THEN 'Trusted Beneficiaries exemption'
      WHEN FAT.TRANSACTION_CHANNEL = 'OCS'
           AND TRANSACTION_END_TO_END_ID LIKE 'CAF%' THEN 'SCA'
      WHEN FAT.TRANSACTION_CHANNEL = 'H2H' THEN 'secure corp process exemption'
      WHEN FAT.TRANSACTION_CHANNEL IS NULL THEN 'not applicable - return'
      ELSE 'check the query'
  END AS SCAIndicator,
  COUNT(DISTINCT FAT.T_SOURCE_PK_ID) AS outbound_ibis_payments_trx_count,
  COALESCE(SUM(FAT.TRANSACTION_AMOUNT), 0) AS outbound_ibis_payments_amount_sum_in_EUR,
  CURRENT_TIMESTAMP AS LOAD_TIMESTAMP,
  '{{period_time['period']}}' AS PERIOD,

FROM
    {{ source('source_dwh_STRP','F_ACCOUNT_TRANSACTIONS_DECRYPTED') }} AS FAT
LEFT JOIN
    {{ source('source_dwh_STRP','D_ACCOUNT_TRANSACTION_CURRENT') }} AS DAT
    ON FAT.T_D_ACCOUNT_TRANSACTION_DIM_KEY = DAT.T_DIM_KEY
LEFT JOIN
    {{ source('source_dwh_STRP','D_IBIS_ACCOUNT_CURRENT') }} AS DIAT
    ON FAT.T_D_IBIS_ACCOUNT_DIM_KEY = DIAT.T_DIM_KEY
INNER JOIN
    {{ source('source_dwh_STRP','D_BANK_ACCOUNTS_DECRYPTED') }} AS DBA
    ON DIAT.T_D_BANK_ACCOUNT_DIM_KEY = DBA.T_DIM_KEY
LEFT JOIN
    {{ source('source_dwh_STRP','D_BANK_ACCOUNTS_DECRYPTED') }} AS DCA
    ON DCA.T_DIM_KEY = FAT.T_COUNTERPARTY_BANK_ACCOUNT_DIM_KEY

WHERE
    FAT.TRANSACTION_DIRECTION = 'OUTBOUND'
    AND FAT.TRANSACTION_TYPE = 'REGULAR'
    AND DAT.TRANSACTION_BOOKING_DATE_AT >= TIMESTAMP(DATETIME('{{period_time['begin_date']}}', '{{time_zone}}'))
    AND DAT.TRANSACTION_BOOKING_DATE_AT <= TIMESTAMP(DATETIME( '{{period_time['end_date']}}', '{{time_zone}}'))
    AND DAT.TRANSACTION_STATUS IN ('RETURNED', 'SETTLED')
    AND DIAT.ACCOUNT_TYPE = 'PAYMENT'
    AND FAT.TRANSACTION_BANK_FAMILY = 'ICDT'
    AND DBA.FINANCIAL_INSTITUTION_COUNTRY_CODE = '{{country_code}}'
GROUP BY 1,2,3,4
ORDER BY 1,2,3,4
