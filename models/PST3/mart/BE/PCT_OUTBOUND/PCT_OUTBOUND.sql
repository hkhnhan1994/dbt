
{% set period_time = period_calculate(time = 'semesterly', selection_date="today", prefix='', suffix='S' ) -%}
{% set time_zone = "Etc/UTC" -%}
{% set country_code = 'BE' -%}


        SELECT
    CASE
        WHEN f_account_transactions.TRANSACTION_CHANNEL in ('DASHBOARD', 'ADMIN', 'OTHER') THEN 'Paper-based form'
        WHEN f_account_transactions.TRANSACTION_CHANNEL in ('TPP', 'OCS', 'H2H') THEN 'Electronic'
        WHEN f_account_transactions.TRANSACTION_CHANNEL IS NULL THEN 'not applicable - return'
        ELSE 'check the query'
    END INITIATIONCHANNEL,
    CASE
        WHEN f_account_transactions.TRANSACTION_CHANNEL in ('DASHBOARD', 'ADMIN', 'OTHER') THEN 'not applicable'
        WHEN f_account_transactions.TRANSACTION_CHANNEL in ('TPP') THEN 'SCA'
        WHEN f_account_transactions.TRANSACTION_CHANNEL in ('OCS') AND TRANSACTION_CREDITOR_REFERENCE_VALUE like 'REF.%/%/%' THEN 'Trusted Beneficiaries exemption'
        WHEN f_account_transactions.TRANSACTION_CHANNEL in ('OCS') AND TRANSACTION_END_TO_END_ID like 'CAF%' THEN 'SCA'
        WHEN f_account_transactions.TRANSACTION_CHANNEL = 'H2H' THEN 'secure corp process exemption'
        WHEN f_account_transactions.TRANSACTION_CHANNEL IS NULL THEN 'not applicable - return'
        ELSE 'check the query'
    END SCAINDICATOR,
    d_counterparty_bank_accounts.FINANCIAL_INSTITUTION_COUNTRY_CODE AS PAYEE_PSP_COUNTRY,
    COUNT(DISTINCT f_account_transactions.T_SOURCE_PK_ID) AS OUTBOUND_IBIS_PAYMENTS_TRX_COUNT,
    COALESCE(SUM(f_account_transactions.TRANSACTION_AMOUNT), 0) AS OUTBOUND_IBIS_PAYMENTS_AMOUNT_SUM_IN_EUR,
    CURRENT_TIMESTAMP AS LOAD_TIMESTAMP,
    "{{period}}"  AS Period,
    TIMESTAMP(DATETIME( '{{begin_date}}', '{{time_zone}}'))  AS Period_begin_date,
    TIMESTAMP(DATETIME( '{{end_date}}', '{{time_zone}}'))  AS Period_end_date,
FROM {{ source('source_dwh_STRP','F_ACCOUNT_TRANSACTIONS_DECRYPTED') }} AS f_account_transactions
LEFT JOIN {{ source('source_dwh_STRP','D_ACCOUNT_TRANSACTION_CURRENT') }} AS d_account_transaction ON f_account_transactions.T_D_ACCOUNT_TRANSACTION_DIM_KEY = d_account_transaction.T_DIM_KEY
LEFT JOIN {{ source('source_dwh_STRP','D_IBIS_ACCOUNT_CURRENT') }} AS d_ibis_account ON f_account_transactions.T_D_IBIS_ACCOUNT_DIM_KEY = d_ibis_account.T_DIM_KEY
INNER JOIN {{ source('source_dwh_STRP','D_BANK_ACCOUNTS_DECRYPTED') }} AS d_bank_accounts ON d_ibis_account.T_D_BANK_ACCOUNT_DIM_KEY = d_bank_accounts.T_DIM_KEY
LEFT JOIN {{ source('source_dwh_STRP','D_BANK_ACCOUNTS_DECRYPTED') }} AS d_counterparty_bank_accounts ON d_counterparty_bank_accounts.T_DIM_KEY = f_account_transactions.T_COUNTERPARTY_BANK_ACCOUNT_DIM_KEY
WHERE f_account_transactions.TRANSACTION_DIRECTION = "OUTBOUND"
    AND f_account_transactions.TRANSACTION_TYPE = 'REGULAR'
    AND d_account_transaction.TRANSACTION_BOOKING_DATE_AT >= TIMESTAMP(DATETIME( '{{begin_date}}', '{{time_zone}}'))
    AND d_account_transaction.TRANSACTION_BOOKING_DATE_AT <= TIMESTAMP(DATETIME( '{{end_date}}', '{{time_zone}}'))
    AND d_account_transaction.TRANSACTION_STATUS IN ('RETURNED', 'SETTLED')
    AND d_ibis_account.ACCOUNT_TYPE = 'PAYMENT'
    AND f_account_transactions.TRANSACTION_CHANNEL <> 'CARDS'
    AND f_account_transactions.TRANSACTION_BANK_FAMILY = 'ICDT'
    AND d_bank_accounts.FINANCIAL_INSTITUTION_COUNTRY_CODE IN ('BE', 'BG', 'LT', 'PL', 'RO', 'HR')
GROUP BY 1, 2, 3
