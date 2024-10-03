{% set period_time = period_calculate(time = "semesterly", selection_date="today", prefix="", suffix="S" ) -%}
{% set time_zone = "Etc/UTC" -%}
{% set country_code = "BE" -%}

    SELECT
      COUNT(*) AS d_ibis_account_count_ibis_accounts,
      CURRENT_TIMESTAMP AS LOAD_TIMESTAMP,
      "{{period_time['period']}}"  AS Period,
      CAST(NULL AS TIMESTAMP) AS Period_begin_date,
      TIMESTAMP(DATETIME( '{{period_time['end_date']}}', '{{time_zone}}')) AS Period_end_date,
    FROM {{ source('source_dwh_strp', 'D_IBIS_ACCOUNT_CURRENT') }} AS ibis
    LEFT JOIN {{ source('source_dwh_strp', 'D_BANK_ACCOUNTS_DECRYPTED') }} AS bank
        ON ibis.T_D_BANK_ACCOUNT_DIM_KEY = bank.T_DIM_KEY
    WHERE
        ibis.ACCOUNT_TYPE  in ('PAYMENT')
        AND ibis.ACCOUNT_IS_CUSTOMER_ACCOUNT IS TRUE
        AND bank.FINANCIAL_INSTITUTION_COUNTRY_CODE IN ('BE', 'LT', 'RO', 'HR', 'BG')
        AND (
            ibis.ACCOUNT_STATUS = 'OPEN'
            OR ( ibis.ACCOUNT_STATUS = 'CLOSED'
               AND TIMESTAMP(ibis.ACCOUNT_UPDATED_AT) > TIMESTAMP(DATETIME( '{{period_time['end_date']}}', '{{time_zone}}'))
             )
            )
        -- Anca & Kris noticed that the for 269 accounts, the account_update_timestamp does not reflect the last_update_date of IBISv2 accounts table. Mail sent to Kathleen 25/08/23
        -- the v2 query gave 67 accounts less for 2023 S1 due to this difference
        AND TIMESTAMP(ibis.ACCOUNT_CREATED_AT) <= TIMESTAMP(DATETIME( '{{period_time['end_date']}}', '{{time_zone}}'))  --pt winter time
