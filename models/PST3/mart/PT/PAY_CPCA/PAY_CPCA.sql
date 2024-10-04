
{% set period_time = period_calculate(time = 'monthly', selection_date="today", prefix='', suffix='M' ) -%}
{% set time_zone = "Etc/UTC" -%}
{% set country_code = 'BE' -%}


        WITH count_NContAISPS_last_six_months AS(
  SELECT
    row_number() over () as IDReg,
    'Y' as Ot,     -- PXG perspective
    LAST_DAY(date_add(DATE(TIMESTAMP(DATETIME( '{{period_time['begin_date']}}', '{{time_zone}}'))), INTERVAL -1 MONTH)) as DtRef,
    substr(bad.BANK_ACCOUNT_NUMBER,5,4) as ASPSP,
    'PSDBE-NBB-0649860804' as AISP,
    '{{country_code}}' as PasASPSP,
    '{{country_code}}' as PasAISP,
    '' AS LEIASPSP,
    '5493000RZ2KSLKCYNN98' AS LEIAISP,
    '1' AS ModAc,
    0 as NContAISPM,                                             -- Number of accounts with consent within reporting month (numbers are in next part of the union)
    count(distinct bad.BANK_ACCOUNT_NUMBER) as NContAISPS,       -- Number of accounts with consent in last 6 months
    0 as NPedInUt,                                               -- Number of access to account initiated by user within reporting month (numbers are in next part of the union)
    0 as NPedSInUt,                                               -- Number of automatic access to account within reporting month (numbers are in next part of the union)
  FROM {{ source('source_dwh_STRP','D_ACCESS_CONSENT_INFO_CURRENT') }} acic
  JOIN {{ source('source_dwh_STRP','D_PXG_PAYMENT_ACCOUNT') }} pac
    ON pac.T_DIM_KEY = acic.T_D_PXG_PAYMENT_ACCOUNT_DIM_KEY
  JOIN {{ source('source_dwh_STRP','D_BANK_ACCOUNTS_DECRYPTED') }} bad
    ON pac.T_D_BANK_ACCOUNT_DIM_KEY = bad.T_DIM_KEY
  JOIN {{ source('source_dwh_STRP','D_FINANCIAL_PLATFORMS') }} fp
    ON pac.T_D_FINANCIAL_PLATFORM_DIM_KEY = fp.T_DIM_KEY
  JOIN {{ source('source_dwh_STRP','D_FINANCIAL_INSTITUTIONS') }} fi
    ON fp.T_D_FINANCIAL_INSTITUTION_DIM_KEY = fi.T_DIM_KEY
  JOIN {{ source('source_dwh_STRP','D_CONTRACT_INFO') }} ci
    ON ci.T_D_PXG_PAYMENT_ACCOUNT_DIM_KEY = pac.T_DIM_KEY
  JOIN {{ source('source_dwh_STRP','D_APPLICATION_ACCOUNT_INFO') }} aai
    ON aai.T_DIM_KEY = ci.T_D_APPLICATION_ACCOUNT_DIM_KEY
  JOIN {{ source('source_dwh_STRP','D_APPLICATIONS_DECRYPTED') }} ad
    ON ad.T_DIM_KEY = aai.T_D_APPLICATION_DIM_KEY
  WHERE
  -- ad.APPLICATION_NAME = 'PAY-PXG-BANQUPPT' --PT
  ad.APPLICATION_NAME in ('PAY-PXG-COMMUNITY', 'PAY-PXG-GOCOMPTA', 'PAY-PXG-MAGIC4BUSINESS', 'PAY-PXG-YOURSMINC', 'PAY-PXG-MIJNBOEKHOUDER')  -- BE data
  AND fi.FINANCIAL_INSTITUTION_CODE != 'IBIS'
  AND acic.ACCESS_CONSENT_CREATED_AT <= TIMESTAMP(DATETIME( '{{period_time['begin_date']}}', '{{time_zone}}'))
  AND (
    acic.ACCESS_CONSENT_STATUS = 'ACTIVE'
    OR DATE(acic.ACCESS_CONSENT_STATUS_AT) >= DATE_ADD(DATE(TIMESTAMP(DATETIME( '{{period_time['begin_date']}}', '{{time_zone}}'))), INTERVAL -6 MONTH)
  )
  -- Created before the end of the reporting period AND still active or status last changed within 6 months before the reporting period
  GROUP BY DtRef, bad.BANK_ACCOUNT_NUMBER
),
count_NContAISPM_previous_month AS(
  SELECT
    row_number() over () as IDReg,
   'Y' as Ot,     -- PXG perspective
   LAST_DAY(date_add(DATE(TIMESTAMP(DATETIME( '{{period_time['begin_date']}}', '{{time_zone}}'))), INTERVAL -1 MONTH)) as DtRef,
   substr(bad.BANK_ACCOUNT_NUMBER,5,4) as ASPSP,
   'PSDBE-NBB-0649860804' as AISP,
   '{{country_code}}' as PasASPSP,
   '{{country_code}}' as PasAISP,
   '' AS LEIASPSP,
   '5493000RZ2KSLKCYNN98' AS LEIAISP,
   '1' AS ModAc,
    count(distinct bad.BANK_ACCOUNT_NUMBER) as NContAISPM,       -- Number of accounts with consent in month
    0 as NContAISPS,                                             -- Number of accounts with consent in last 6 months (numbers were in previous part of the union)
    count(distinct bad.BANK_ACCOUNT_NUMBER) as NPedInUt,         -- Assuming one access per day
    4 * count(distinct bad.BANK_ACCOUNT_NUMBER) * extract(DAY FROM LAST_DAY(date_add(DATE(TIMESTAMP(DATETIME( '{{period_time['begin_date']}}', '{{time_zone}}'))), INTERVAL -1 MONTH)))  as NPedSInUt,     -- 4 * Number of days in month * number of accounts with consent in month
  FROM {{ source('source_dwh_STRP','D_ACCESS_CONSENT_INFO_CURRENT') }} acic
  JOIN {{ source('source_dwh_STRP','D_PXG_PAYMENT_ACCOUNT') }} pac
    ON pac.T_DIM_KEY = acic.T_D_PXG_PAYMENT_ACCOUNT_DIM_KEY
  JOIN {{ source('source_dwh_STRP','D_BANK_ACCOUNTS_DECRYPTED') }} bad
    ON pac.T_D_BANK_ACCOUNT_DIM_KEY = bad.T_DIM_KEY
  JOIN {{ source('source_dwh_STRP','D_FINANCIAL_PLATFORMS') }} fp
    ON pac.T_D_FINANCIAL_PLATFORM_DIM_KEY = fp.T_DIM_KEY
  JOIN {{ source('source_dwh_STRP','D_FINANCIAL_INSTITUTIONS') }} fi
    ON fp.T_D_FINANCIAL_INSTITUTION_DIM_KEY = fi.T_DIM_KEY
  JOIN {{ source('source_dwh_STRP','D_CONTRACT_INFO') }} ci
    ON ci.T_D_PXG_PAYMENT_ACCOUNT_DIM_KEY = pac.T_DIM_KEY
  JOIN {{ source('source_dwh_STRP','D_APPLICATION_ACCOUNT_INFO') }} aai
    ON aai.T_DIM_KEY = ci.T_D_APPLICATION_ACCOUNT_DIM_KEY
  JOIN {{ source('source_dwh_STRP','D_APPLICATIONS_DECRYPTED') }} ad
    ON ad.T_DIM_KEY = aai.T_D_APPLICATION_DIM_KEY
  WHERE
  -- ad.APPLICATION_NAME = 'PAY-PXG-BANQUPPT' -- PT
  ad.APPLICATION_NAME in ('PAY-PXG-COMMUNITY', 'PAY-PXG-GOCOMPTA', 'PAY-PXG-MAGIC4BUSINESS', 'PAY-PXG-YOURSMINC', 'PAY-PXG-MIJNBOEKHOUDER')  -- BE data
  and fi.FINANCIAL_INSTITUTION_CODE != 'IBIS'
  AND acic.ACCESS_CONSENT_CREATED_AT <= TIMESTAMP(DATETIME( '{{period_time['begin_date']}}', '{{time_zone}}'))
  AND (
    acic.ACCESS_CONSENT_STATUS = 'ACTIVE'
    OR DATE(acic.ACCESS_CONSENT_STATUS_AT) >= DATE_ADD(DATE(TIMESTAMP(DATETIME( '{{period_time['begin_date']}}', '{{time_zone}}'))), INTERVAL -1 MONTH)
  )
  -- Created before the end of the reporting period and (is still active or status last changed before the start of the reporting period)
  GROUP BY DtRef, bad.BANK_ACCOUNT_NUMBER
),
count_NContAISPM AS(
  SELECT
    ROW_NUMBER() OVER () AS IDReg,
    'X' AS Ot,
    -- PXG perspective
    LAST_DAY(DATE_ADD(DATE(TIMESTAMP(DATETIME( '{{period_time['begin_date']}}', '{{time_zone}}'))), INTERVAL -1 MONTH)) AS DtRef,
    '5845' AS ASPSP,
    REPLACE (t.SERVICE_PROVIDER_PSP_AUTHORISATION_NUMBER, '.', '') as AISP,
    '{{country_code}}' AS PasASPSP,
    '{{country_code}}' AS PasAISP,
    '' AS LEIASPSP,
    '5493000RZ2KSLKCYNN98' AS LEIAISP,
    '1' AS ModAc,
    COUNT(*) AS NContAISPM,
    -- Number of accounts with consent within reporting month (numbers are in next part of the union)
    0 AS NContAISPS,
    -- Number of accounts with consent in last 6 months
    0 AS NPedInUt,
    -- Number of access to account initiated by user within reporting month (numbers are in next part of the union)
    0 AS NPedSInUt,                                         -- Number of automatic access to account within reporting month (numbers are in next part of the union)
  FROM {{ source('source_dwh_STRP','D_ASPSP_CONSENT_DECRYPTED') }} c
  INNER JOIN {{ source('source_dwh_STRP','D_ASPSP_CONSENT_DECRYPTED') }} cc
    ON c.T_DIM_KEY = cc.T_DIM_KEY
  INNER JOIN {{ source('source_dwh_STRP','D_ASPSP_TPP') }} t
    ON c.T_D_ASPSP_TPP_DIM_KEY = t.T_DIM_KEY
  WHERE
    -- c.CONSENT_STATUS = 'VALID'
    LEFT(c.consent_iban,2) ='{{country_code}}'
    AND c.CONSENT_CREATED_AT <= TIMESTAMP(DATETIME( '{{period_time['begin_date']}}', '{{time_zone}}'))
    -- AND (c.CONSENT_STATUS = 'VALID'
    --   OR DATE(acic.ACCESS_CONSENT_STATUS_AT) >=  DATE_ADD(DATE(TIMESTAMP(DATETIME( '{{period_time['begin_date']}}', '{{time_zone}}'))), INTERVAL -1 MONTH)
    --   ) -- ==> geen consent_status_at in DWH ook niet in DL
    AND t.T_SOURCE_PK_UUID <> '93773d5d-00b9-422d-af5c-b90259cf50ee'
  GROUP BY DtRef, t.SERVICE_PROVIDER_PSP_AUTHORISATION_NUMBER
),
count_NContAISPS AS(
  SELECT
    ROW_NUMBER() OVER () AS IDReg,
    'X' AS Ot,
    -- PXG perspective
    LAST_DAY(DATE_ADD(DATE(TIMESTAMP(DATETIME( '{{period_time['begin_date']}}', '{{time_zone}}'))), INTERVAL -1 MONTH)) AS DtRef,
    '5845' AS ASPSP,
    REPLACE (t.SERVICE_PROVIDER_PSP_AUTHORISATION_NUMBER, '.', '') as AISP,
    '{{country_code}}' AS PasASPSP,
    '{{country_code}}' AS PasAISP,
    '' AS LEIASPSP,
    '5493000RZ2KSLKCYNN98' AS LEIAISP,
    '1' AS ModAc,
    0 AS NContAISPM,
    -- Number of accounts with consent within reporting month (numbers are in next part of the union)
    COUNT(*) AS NContAISPS,
    -- Number of accounts with consent in last 6 months
    0 AS NPedInUt,
    -- Number of access to account initiated by user within reporting month (numbers are in next part of the union)
    0 AS NPedSInUt                                               -- Number of automatic access to account within reporting month (numbers are in next part of the union)
  FROM {{ source('source_dwh_STRP','D_ASPSP_CONSENT_DECRYPTED') }} c
  INNER JOIN {{ source('source_dwh_STRP','D_ASPSP_CONSENT_DECRYPTED') }} cc
  ON c.T_DIM_KEY = cc.T_DIM_KEY
  INNER JOIN {{ source('source_dwh_STRP','D_ASPSP_TPP') }} t
  ON c.T_D_ASPSP_TPP_DIM_KEY = t.T_DIM_KEY
  WHERE
    --  c.CONSENT_STATUS = 'VALID'
    LEFT(c.consent_iban,2) ='{{country_code}}'
    AND c.CONSENT_CREATED_AT <= TIMESTAMP(DATETIME( '{{period_time['begin_date']}}', '{{time_zone}}'))
    -- AND (acic.ACCESS_CONSENT_STATUS = 'ACTIVE'
    --   or DATE(acic.ACCESS_CONSENT_STATUS_AT) >= DATE_ADD(DATE(TIMESTAMP(DATETIME( '{{period_time['begin_date']}}', '{{time_zone}}'))), INTERVAL -6 MONTH)
    --   ) ==> no consent status at?
    AND t.T_SOURCE_PK_UUID <> '93773d5d-00b9-422d-af5c-b90259cf50ee'
  GROUP BY DtRef, t.SERVICE_PROVIDER_PSP_AUTHORISATION_NUMBER
)
SELECT
  Ot,IDReg,DtRef,ASPSP,AISP, PasASPSP,
  PasAISP, LEIASPSP, LEIAISP, ModAc,
  sum(NContAISPM) as NContAISPM,
  sum(NContAISPS) as NContAISPS,
  sum(NPedInUt) as NPedInUt,
  sum(NPedSInUt) as NPedSInUt,
  "{{period}}"  AS Period,
  CURRENT_TIMESTAMP AS LOAD_TIMESTAMP,
FROM (
  SELECT * FROM count_NContAISPS_last_six_months
  UNION ALL
  SELECT * FROM count_NContAISPM_previous_month
)
-- This must still be unioned with data from the ASPSP API for TPPs other than UP (second perspective)
GROUP BY 1,2,3,4,5,6,7,8,9,10
UNION ALL
SELECT
  Ot,IDReg,DtRef,ASPSP,AISP, PasASPSP,
  PasAISP, LEIASPSP, LEIAISP, ModAc,
  sum(NContAISPM) as NContAISPM,
  sum(NContAISPS) as NContAISPS,
  sum(NPedInUt) as NPedInUt,
  sum(NPedSInUt) as NPedSInUt,
  "{{period}}"  AS Period,
  CURRENT_TIMESTAMP AS LOAD_TIMESTAMP,
FROM (
  SELECT * FROM count_NContAISPM
  UNION ALL
  SELECT * FROM count_NContAISPS
)
-- This must still be unioned with data from the ASPSP API for TPPs other than UP (second perspective)
GROUP BY 1,2,3,4,5,6,7,8,9,10
