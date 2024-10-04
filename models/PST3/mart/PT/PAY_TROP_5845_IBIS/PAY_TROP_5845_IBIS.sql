
{% set period_time = period_calculate(time = 'daily', selection_date="today", prefix='', suffix='D' ) -%}
{% set time_zone = "Etc/UTC" -%}
{% set country_code = 'BE' -%}


        WITH outbound as (
  SELECT
  'O' AS Ot,
  fat.T_SOURCE_PK_ID AS Ref,
  '' AS ORef,
  baBA.BANK_ACCOUNT_NUMBER as debtor_account_identifier ,
  baCB.BANK_ACCOUNT_NUMBER as creditor_account_identifier,
  substr(baBA.BANK_ACCOUNT_NUMBER,5,4) AS Ord,
  IF(left(baCB.BANK_ACCOUNT_NUMBER,2)='{{country_code}}', substr(baCB.BANK_ACCOUNT_NUMBER,5,4), '9999') AS Ben,
  '' AS PayID,
  baBA.BANK_ACCOUNT_BIC AS BICOrd,
  baCB.BANK_ACCOUNT_BIC AS BICBen,
  '' AS BICSen,
  '' AS BICRec,
  baBA.FINANCIAL_INSTITUTION_COUNTRY_CODE AS PasOrd,
  baCB.FINANCIAL_INSTITUTION_COUNTRY_CODE AS PasBen,
  '5493000RZ2KSLKCYNN98' AS LEIOrd,
  '' AS LEIBen,
  '4' AS Sch,
  IF(baCB.BANK_ACCOUNT_BIC = 'PANXPTP2','3','9') AS Pro,
  date(ats.TRANSACTION_BOOKING_DATE_AT) AS DtLiq,
  cast(null as date) AS DtPISP,
  '' AS TsOrd,
  '' AS TsBen,
  '1' AS TipTR,
  fat.TRANSACTION_CURRENCY AS Div,
  fat.TRANSACTION_AMOUNT AS Mont,
  fat.TRANSACTION_AMOUNT AS MontOrg,
  CASE
    WHEN fat.TRANSACTION_CHANNEL = 'OTHER' THEN'8'
    WHEN fat.TRANSACTION_CHANNEL IN ('TPP','OCS') THEN'4'
    WHEN fat.TRANSACTION_CHANNEL = 'H2H' THEN'6'
    ELSE 'check the query'
  END AS TipCan,

  CASE
    WHEN fat.TRANSACTION_CHANNEL IN ('OTHER','TPP','OCS') THEN'2'
    WHEN fat.TRANSACTION_CHANNEL = 'H2H' THEN'1'
    ELSE 'check the query'
  END AS FormEnv,

  CASE
    WHEN fat.TRANSACTION_CHANNEL IN ('TPP','OCS','H2H') THEN'Y'
    WHEN fat.TRANSACTION_CHANNEL IN ('OTHER') THEN'N'
    ELSE 'check the query'
  END AS OperElet,

  'Y' AS OperRem,
  'Y' AS OperECom,
  'N' AS IniPISP,
  '1' AS ModAc,
  CASE
    WHEN fat.TRANSACTION_CHANNEL IN ('TPP','OCS','OTHER') THEN'Y'
    WHEN fat.TRANSACTION_CHANNEL IN ('H2H') THEN'N'
    ELSE 'check the query'
  END AS SCA,

  case
    WHEN fat.TRANSACTION_CHANNEL IN ('TPP','OCS','OTHER') then ''
    WHEN fat.TRANSACTION_CHANNEL IN ('H2H') then '5'
    ELSE 'check the query'
  END AS MnonSCA,

  'N.E.' AS SI,
  baBA.FINANCIAL_INSTITUTION_COUNTRY_CODE AS PasmC,
  baCB.FINANCIAL_INSTITUTION_COUNTRY_CODE AS PasnC,
  '' AS CPosm,
  '' AS TipDoc,
  '' AS NumDoc,
  '' AS LEIC,
  'N' AS ENI,
  '' AS InfUltBen,
  '' AS InfUltOrd,
  "{{period}}"  AS Period,
  CURRENT_TIMESTAMP AS load_timestamp,

FROM {{ source('source_dwh_STRP','F_ACCOUNT_TRANSACTIONS_DECRYPTED') }} as fat
LEFT JOIN {{ source('source_dwh_STRP','D_IBIS_ACCOUNT_CURRENT') }} ibis
  ON fat.T_D_IBIS_ACCOUNT_DIM_KEY = ibis.T_DIM_KEY
JOIN {{ source('source_dwh_STRP','D_BANK_ACCOUNTS_DECRYPTED') }} as baBA
  ON ibis.T_D_BANK_ACCOUNT_DIM_KEY = baBA.T_DIM_KEY
  -- AND baBA.T_SOURCE_TABLE = "READ_DWH_TRANSACTIONS"
LEFT JOIN {{ source('source_dwh_STRP','D_BANK_ACCOUNTS_DECRYPTED') }} as baCB
  ON baCB.T_DIM_KEY = fat.T_COUNTERPARTY_BANK_ACCOUNT_DIM_KEY
  -- AND baCB.T_SOURCE_TABLE = "READ_DWH_TRANSACTIONS"
LEFT JOIN {{ source('source_dwh_STRP','D_ACCOUNT_TRANSACTION_DECRYPTED') }} AS ats
  ON fat.T_D_ACCOUNT_TRANSACTION_DIM_KEY = ats.T_DIM_KEY

WHERE fat.TRANSACTION_DIRECTION = "OUTBOUND"
  AND baBA.Financial_institution_country_code = '{{country_code}}'
  AND (fat.TRANSACTION_TYPE ) = 'REGULAR'
  AND (ats.TRANSACTION_STATUS ) IN ('RETURNED', 'SETTLED')
  AND (ibis.ACCOUNT_TYPE ) = 'PAYMENT'
  AND fat.TRANSACTION_BANK_FAMILY = 'ICDT'
  AND fat.TRANSACTION_CHANNEL <> 'CARDS'
  AND TIMESTAMP(ats.TRANSACTION_BOOKING_DATE_AT) >= TIMESTAMP(DATETIME( '{{begin_date}}', '{{time_zone}}')) --pt winter time
  AND TIMESTAMP(ats.TRANSACTION_BOOKING_DATE_AT) <= TIMESTAMP(DATETIME( '{{end_date}}', '{{time_zone}}'))  --pt winter time
),
inbound as (
  SELECT
  'B' AS Ot,
  fat.T_SOURCE_PK_ID AS Ref,
  '' AS ORef,
  baBA.BANK_ACCOUNT_NUMBER as debtor_account_identifier ,
  baCB.BANK_ACCOUNT_NUMBER as creditor_account_identifier,
  IF(left(baCB.BANK_ACCOUNT_NUMBER,2)='{{country_code}}', substr(baCB.BANK_ACCOUNT_NUMBER,5,4), '9999') AS Ord,
  substr(baBA.BANK_ACCOUNT_NUMBER,5,4) AS Ben,
  '' AS PayID,
  baCB.BANK_ACCOUNT_BIC AS BICOrd,
  baBA.BANK_ACCOUNT_BIC AS BICBen,
  '' AS BICSen,
  '' AS BICRec,
  baCB.FINANCIAL_INSTITUTION_COUNTRY_CODE AS PasOrd,
  baBA.FINANCIAL_INSTITUTION_COUNTRY_CODE AS PasBen,
  '' AS LEIOrd,
  '5493000RZ2KSLKCYNN98' AS LEIBen,
  '4' AS Sch,
  IF(baCB.BANK_ACCOUNT_BIC = 'PANXPTP2','3','9') AS Pro,
  date(ats.TRANSACTION_BOOKING_DATE_AT) AS DtLiq,
  cast(null as date) AS DtPISP,
  '' AS TsOrd,
  '' AS TsBen,
  '1' AS TipTR,
  fat.TRANSACTION_CURRENCY AS Div,
  fat.TRANSACTION_AMOUNT AS Mont,
  fat.TRANSACTION_AMOUNT AS MontOrg,
  '' AS TipCan,
  '' AS FormEnv,
  '' AS OperElet,
  '' AS OperRem,
  'N' AS OperECom,
  '' AS IniPISP,
  '' AS ModAc,
  '' AS SCA,
  '' AS MnonSCA,
  'N.E.' AS SI,
  baBA.FINANCIAL_INSTITUTION_COUNTRY_CODE AS PasmC,
  baBA.FINANCIAL_INSTITUTION_COUNTRY_CODE AS PasnC,
  '' AS CPosm,
  '' AS TipDoc,
  '' AS NumDoc,
  '' AS LEIC,
  'N' AS ENI,
  '' AS InfUltBen,
  '' AS InfUltOrd,
  "{{period}}"  AS Period,
  CURRENT_TIMESTAMP AS load_timestamp,

FROM {{ source('source_dwh_STRP','F_ACCOUNT_TRANSACTIONS_DECRYPTED') }} as fat
LEFT JOIN {{ source('source_dwh_STRP','D_IBIS_ACCOUNT_CURRENT') }} ibis
  ON fat.T_D_IBIS_ACCOUNT_DIM_KEY = ibis.T_DIM_KEY
JOIN {{ source('source_dwh_STRP','D_BANK_ACCOUNTS_DECRYPTED') }} as baBA
  ON ibis.T_D_BANK_ACCOUNT_DIM_KEY = baBA.T_DIM_KEY
  -- AND baBA.T_SOURCE_TABLE = "READ_DWH_TRANSACTIONS"
LEFT JOIN {{ source('source_dwh_STRP','D_BANK_ACCOUNTS_DECRYPTED') }} as baCB
  ON baCB.T_DIM_KEY = fat.T_COUNTERPARTY_BANK_ACCOUNT_DIM_KEY
  -- AND baCB.T_SOURCE_TABLE = "READ_DWH_TRANSACTIONS"
LEFT JOIN {{ source('source_dwh_STRP','D_ACCOUNT_TRANSACTION_DECRYPTED') }} AS ats
  ON fat.T_D_ACCOUNT_TRANSACTION_DIM_KEY = ats.T_DIM_KEY

WHERE fat.TRANSACTION_DIRECTION = "INBOUND"
  AND baBA.Financial_institution_country_code = '{{country_code}}'
  AND (fat.TRANSACTION_TYPE ) = 'REGULAR'
  AND (ats.TRANSACTION_STATUS ) IN ('RETURNED', 'SETTLED')
  AND (ibis.ACCOUNT_TYPE ) = 'PAYMENT'
  AND fat.TRANSACTION_BANK_FAMILY = 'RCDT'
  AND TIMESTAMP(ats.TRANSACTION_BOOKING_DATE_AT) >= TIMESTAMP(DATETIME( '{{begin_date}}', '{{time_zone}}')) --pt winter time
  AND TIMESTAMP(ats.TRANSACTION_BOOKING_DATE_AT) <= TIMESTAMP(DATETIME( '{{end_date}}', '{{time_zone}}'))  --pt winter time
)
SELECT * FROM outbound
UNION ALL
SELECT * FROM inbound
