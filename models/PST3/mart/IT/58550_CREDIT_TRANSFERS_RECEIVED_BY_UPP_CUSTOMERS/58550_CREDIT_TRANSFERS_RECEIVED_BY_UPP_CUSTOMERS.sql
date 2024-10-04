
{% set period_time = period_calculate(time = 'quarterly', selection_date="today", prefix='', suffix='Q' ) -%}
{% set time_zone = "Etc/UTC" -%}
{% set country_code = 'IT' -%}


        WITH credit_transfers as (
  SELECT
  DBA.FINANCIAL_INSTITUTION_COUNTRY_CODE AS Payer_PSP_country,
  IA.ACCOUNT_MASTER_DATA_ID as account_master_data_id_4lookup,
  case when FAB.BALANCE_AMOUNT < 12500 then '66'
        when FAB.BALANCE_AMOUNT between 12500 and 50000 then '67'
        when FAB.BALANCE_AMOUNT > 50000 then '89'
  end balance_class,
  FAT.TRANSACTION_AMOUNT AS amount,
  CURRENT_TIMESTAMP AS LOAD_TIMESTAMP,
  "{{period}}"  AS Period,
  FROM  {{ source('source_dwh_STRP','F_ACCOUNT_TRANSACTIONS_DECRYPTED') }} AS FAT
  LEFT JOIN  {{ source('source_dwh_STRP','D_ACCOUNT_TRANSACTION_CURRENT') }} AS DAT
    ON FAT.T_D_ACCOUNT_TRANSACTION_DIM_KEY = DAT.T_DIM_KEY
  LEFT JOIN  {{ source('source_dwh_STRP','D_IBIS_ACCOUNT_CURRENT') }} AS IA
    ON FAT.T_D_IBIS_ACCOUNT_DIM_KEY = IA.T_DIM_KEY
  INNER JOIN  {{ source('source_dwh_STRP','D_BANK_ACCOUNTS_DECRYPTED') }} AS CBA
    ON IA.T_D_BANK_ACCOUNT_DIM_KEY = CBA.T_DIM_KEY
  LEFT JOIN  {{ source('source_dwh_STRP','D_BANK_ACCOUNTS_DECRYPTED') }} AS DBA
    ON DBA.T_DIM_KEY = FAT.T_COUNTERPARTY_BANK_ACCOUNT_DIM_KEY
  INNER JOIN  {{ source('source_dwh_STRP','F_ACCOUNT_BALANCE') }} FAB
    on FAB.T_D_IBIS_ACCOUNT_DIM_KEY = IA.T_DIM_KEY
  WHERE FAT.TRANSACTION_DIRECTION = "INBOUND"
      AND FAT.TRANSACTION_TYPE = 'REGULAR'
      AND DAT.TRANSACTION_BOOKING_DATE_AT >= TIMESTAMP(DATETIME( '{{period_time['begin_date']}}', '{{time_zone}}'))
      AND DAT.TRANSACTION_BOOKING_DATE_AT <= TIMESTAMP(DATETIME( '{{period_time['end_date']}}', '{{time_zone}}'))
      AND DAT.TRANSACTION_STATUS IN ('RETURNED', 'SETTLED')
      AND IA.ACCOUNT_TYPE = 'PAYMENT'
      AND FAT.TRANSACTION_BANK_FAMILY = 'RCDT'
      AND CBA.FINANCIAL_INSTITUTION_COUNTRY_CODE IN ('{{country_code}}')
      AND  FAB.BALANCE_DATE = (
        SELECT max(max.BALANCE_DATE)
        FROM  {{ source('source_dwh_STRP','F_ACCOUNT_BALANCE') }} max
        JOIN  {{ source('source_dwh_STRP','D_IBIS_ACCOUNT_CURRENT') }} a
          ON FAB.T_D_IBIS_ACCOUNT_DIM_KEY = a.T_DIM_KEY
        WHERE TIMESTAMP(max.BALANCE_DATE) <= TIMESTAMP(DATETIME( '{{period_time['end_date']}}', '{{time_zone}}'))
        )
  ),
cmd as (
  SELECT
        P.T_SOURCE_PK_ID as account_Master_Data_Id_4lookup_cmd,
        L.ENTERPRISE_COUNTRY_OF_INCORPORATION,
        L.ENTERPRISE_PROVINCE_OF_INCORPORATION,
        --ba.code as ""NACEcodeToBeMappedToSettore"" ==> to add by CMD team (dragos)
    FROM  {{ source('source_dwh_STRP','D_PAYMENT_ACCOUNT_CURRENT') }} P
    inner join {{ source('source_dwh_STRP','F_LEGAL_ENTITY_PAYMENT_ACCOUNT_ROLES') }} LPR on LPR.T_D_PAYMENT_ACCOUNT_DIM_KEY = P.T_DIM_KEY
    inner join {{ source('source_dwh_STRP','D_LEGAL_ENTITY_DECRYPTED') }} L on LPR.T_D_LEGAL_ENTITY_DIM_KEY = L.T_DIM_KEY
    WHERE
            LPR.ROLE = 'ACCOUNT_HOLDER'
      -- AND  ba.main is true ==> to add by CMD team (dragos)
      and L.ENTERPRISE_COUNTRY_OF_INCORPORATION = '{{country_code}}'
)
SELECT * FROM credit_transfers
inner join cmd
on credit_transfers.account_master_data_id_4lookup = cmd.account_Master_Data_Id_4lookup_cmd
