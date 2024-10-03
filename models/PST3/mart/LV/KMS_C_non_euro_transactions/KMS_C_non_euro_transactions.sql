SELECT
  count(*) as number_of_non_EUR_transactions,
  CURRENT_TIMESTAMP AS LOAD_TIMESTAMP,
  '{{period}}' AS PERIOD,
FROM {{ source('source_dwh_strp,F_ACCOUNT_TRANSACTIONS_DECRYPTED') }} AS FAT
LEFT JOIN {{ source('source_dwh_strp,D_ACCOUNT_TRANSACTION_CURRENT') }} AS DAT
  ON FAT.T_D_ACCOUNT_TRANSACTION_DIM_KEY = DAT.T_DIM_KEY
LEFT JOIN {{ source('source_dwh_strp,D_IBIS_ACCOUNT_CURRENT') }} AS IA
  ON FAT.T_D_IBIS_ACCOUNT_DIM_KEY = IA.T_DIM_KEY
JOIN {{ source('source_dwh_strp,D_BANK_ACCOUNTS_DECRYPTED') }} AS BA
  ON IA.T_D_BANK_ACCOUNT_DIM_KEY = BA.T_DIM_KEY
LEFT JOIN {{ source('source_dwh_strp,D_BANK_ACCOUNTS_DECRYPTED') }} AS CBA
  ON CBA.T_DIM_KEY = FAT.T_COUNTERPARTY_BANK_ACCOUNT_DIM_KEY
WHERE FAT.TRANSACTION_CURRENCY <> 'EUR'
AND TRANSACTION_DATE_AT >= TIMESTAMP(DATETIME( '{{period_time['begin_date']}}', '{{time_zone}}'))
AND TRANSACTION_DATE_AT <= TIMESTAMP(DATETIME( '{{period_time['end_date']}}', '{{time_zone}}'))
