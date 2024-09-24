with RequestCache_current as (
  WITH current_table AS (
    SELECT *,
      ROW_NUMBER() OVER(PARTITION BY row_hash) AS rn
    FROM  {{ source('stg_strh_hkvk', 'public_RequestCache') }}
  )
  SELECT * EXCEPT(rn)
  FROM current_table
  WHERE rn = 1
),
cast_respond_data AS(
  SELECT
    RequestCacheID, ResponseTimeUTC,datatype,CacheKey,ingestion_meta_data_processing_timestamp,source_metadata_change_type,ingestion_meta_data_uuid,
    ingestion_meta_data_source_timestamp,
    CAST (KVKNumber AS STRING) AS LEGAL_ENTITY_IDENTIFIER,
    CAST (KVKName AS STRING) AS LEGAL_ENTITY_NAME,
    CAST (SAFE.PARSE_TIMESTAMP ('%Y%m%d', KVKenddate) AS TIMESTAMP) AS LEGAL_ENTITY_ENDED_AT,
    CAST (SAFE.PARSE_TIMESTAMP ('%Y%m%d', KVKstartdate) AS TIMESTAMP) AS LEGAL_ENTITY_INCORPORATION_DATE,
    CAST (KVKLegalForm AS STRING) AS LEGAL_ENTITY_JURIDICAL_FORM,
    CAST (KVKSBIcode AS STRING) AS LEGAL_ENTITY_ACTIVITY_CODE,
    CAST (KVKNumberOfEmployees AS INT64) AS LEGAL_ENTITY_NUMBER_OF_EMPLOYEES,
    CAST (KVKSBIdescription AS STRING) AS LEGAL_ENTITY_ACTIVITY,
  FROM(
    SELECT
      RequestCacheID, ResponseTimeUTC,datatype,CacheKey,ingestion_meta_data_processing_timestamp,source_metadata_change_type,ingestion_meta_data_uuid,
      ingestion_meta_data_source_timestamp,
      CASE
      WHEN kr.datatype in (0, 1, 3) THEN
      REGEXP_EXTRACT(kr.ResponseData, r'<kvkNummer>(\w+)</kvkNummer>')
      WHEN kr.datatype = 2 THEN
      JSON_VALUE(kr.ResponseData, '$.data.items[0].kvkNumber')
      WHEN kr.datatype = 4 THEN
      JSON_VALUE(kr.ResponseData, '$.kvkNummer')
      END as KVKNumber,
      CASE
      WHEN kr.datatype in (0, 1, 3) THEN
      REGEXP_EXTRACT(kr.ResponseData, r'<naam>(.*?)</naam>')
      WHEN kr.datatype = 2 THEN
      COALESCE(
        JSON_VALUE(kr.ResponseData, '$.data.items[0].tradeNames.businessName')
        -- ,JSON_VALUE(kr.ResponseData, '$.data.items[0].tradeNames.shortBusinessName')
        ,JSON_VALUE(kr.ResponseData, '$.data.items[0].tradeNames.currentStatutoryNames[0]')
        -- ,JSON_VALUE(kr.ResponseData, '$.data.items[0].tradeNames.currentTradeNames[0]')
      )
      WHEN kr.datatype = 4 THEN
      JSON_VALUE(kr.ResponseData, '$.naam')
      END as KVKName,
      CASE
      WHEN kr.datatype in (0, 1) THEN
      REGEXP_EXTRACT(kr.ResponseData, r'(?s)<hoofdSbiActiviteit>[\t\n\f\r ]*<sbiCode>(\w+)</sbiCode>')
      WHEN kr.datatype = 2 THEN
      JSON_VALUE(kr.ResponseData, '$.data.items[0].businessActivities[0].sbiCode')
      WHEN kr.datatype = 3 THEN
      REGEXP_EXTRACT(kr.ResponseData, r'(?s)<sbiCode>[\t\n\f\r ]*<code>(\w+)</code>.*<isHoofdactiviteit>[\t\n\f\r ]*<code>J')
      WHEN kr.datatype = 4 THEN
      JSON_VALUE(kr.ResponseData, '$.sbiActiviteiten[0].sbiCode')
      END as KVKSBIcode,

      CASE
      WHEN kr.datatype in (0, 1, 3) THEN
      REGEXP_EXTRACT(kr.ResponseData, r'<totaalWerkzamePersonen>(\w+)</totaalWerkzamePersonen>')
      WHEN kr.datatype = 2 THEN
      JSON_VALUE(kr.ResponseData, '$.data.items[0].employees')
      WHEN kr.datatype = 4 THEN
      JSON_VALUE(kr.ResponseData, '$.totaalWerkzamePersonen')
      END as KVKNumberOfEmployees,

      CASE
      WHEN kr.datatype in (0, 1, 3) THEN
      REGEXP_EXTRACT(kr.ResponseData, r'<persoonRechtsvorm>(.*?)</persoonRechtsvorm>')
      WHEN kr.datatype = 2 THEN
      JSON_VALUE(kr.ResponseData, '$.data.items[0].legalForm')
      WHEN kr.datatype = 4 THEN
      JSON_VALUE(kr.ResponseData, '$._embedded.eigenaar.rechtsvorm')
      END as KVKLegalForm,

      CASE
      WHEN kr.datatype in (0, 1, 3) THEN
      REGEXP_EXTRACT(kr.ResponseData, r'(?s)<registratie registratie.*?<datumAanvang>(\w+)</datumAanvang>')
      WHEN kr.datatype = 2 THEN
      JSON_VALUE(kr.ResponseData, '$.data.items[0].foundationDate')
      WHEN kr.datatype = 4 THEN
      JSON_VALUE(kr.ResponseData, '$.materieleRegistratie.datumAanvang')
      END as KVKstartdate,
      CASE
      WHEN kr.datatype in (0, 1, 3) THEN
      REGEXP_EXTRACT(kr.ResponseData, r'(?s)<registratie registratie.*?<datumEinde>(\w+)</datumEinde>')
      WHEN kr.datatype = 4 THEN
      JSON_VALUE(kr.ResponseData, '$.materieleRegistratie.datumEinde')
      END as KVKenddate,
      CASE
      WHEN kr.datatype in (0, 1) THEN
          REGEXP_EXTRACT(kr.ResponseData, '(?s)<hoofdSbiActiviteit>[\t\n\f\r ]*<sbiCode>.*</sbiCode>[\t\n\f\r ]*<omschrijving>(.*?)</omschrijving>')
      WHEN kr.datatype = 2 THEN
          JSON_VALUE(kr.ResponseData, '$.data.items[0].businessActivities[0].sbiCodeDescription')
      WHEN kr.datatype = 3 THEN
          REGEXP_EXTRACT(kr.ResponseData, '(?s)<sbiCode>[\t\n\f\r ]*<code>.*</code>[\t\n\f\r ]*<omschrijving>(.*?)</omschrijving>.*<isHoofdactiviteit>[\t\n\f\r ]*<code>J')
      WHEN kr.datatype = 4 THEN
          JSON_VALUE(kr.ResponseData, '$.sbiActiviteiten[0].sbiOmschrijving')
      END as KVKSBIdescription,
    FROM(
      SELECT *,
      ROW_NUMBER()OVER(PARTITION BY RequestCacheID ORDER BY responsetimeutc DESC, ingestion_meta_data_processing_timestamp DESC) AS rn,
      FROM RequestCache_current kr
    ) AS kr
    WHERE kr.rn = 1
  )
  ),
  source_table AS (
    SELECT
      CAST (re_ca.ingestion_meta_data_uuid AS STRING) AS T_BATCH_ID,
      CAST (re_ca.ingestion_meta_data_source_timestamp AS TIMESTAMP) AS T_LOAD_TIMESTAMP,
      TO_BASE64(SHA256(CONCAT("HKVK", "$", CAST(re_ca.CacheKey AS STRING),CAST(re_ca.RequestCacheID AS STRING)))) AS T_BUS_KEY,
      "HKVK" AS T_SOURCE,
      CAST(re_ca.RequestCacheID AS STRING) AS T_SOURCE_PK_ID,
      -- CAST(re_ca.RequestCacheID AS STRING) AS T_SOURCE_PK_UUID,
      CAST(re_ca.ResponseTimeUTC AS TIMESTAMP) AS T_INGESTION_TIMESTAMP,
      re_ca.source_metadata_change_type AS T_DML_TYPE,
      COALESCE (re_ca.LEGAL_ENTITY_NAME, "NA") AS LEGAL_ENTITY_NAME,
      COALESCE (re_ca.LEGAL_ENTITY_IDENTIFIER, "NA") AS LEGAL_ENTITY_IDENTIFIER,
      CAST ("KVK" AS STRING) AS LEGAL_ENTITY_IDENTIFIER_TYPE,
      CAST(COALESCE(re_ca.ResponseTimeUTC,NULL) AS TIMESTAMP) AS LEGAL_ENTITY_UPDATED_AT,
      re_ca.LEGAL_ENTITY_ENDED_AT,
      re_ca.LEGAL_ENTITY_INCORPORATION_DATE,
      COALESCE (re_ca.LEGAL_ENTITY_JURIDICAL_FORM, "NA") AS LEGAL_ENTITY_JURIDICAL_FORM,
      COALESCE (re_ca.LEGAL_ENTITY_ACTIVITY_CODE, "NA") AS LEGAL_ENTITY_ACTIVITY_CODE,
      COALESCE (re_ca.LEGAL_ENTITY_ACTIVITY, "NA") AS LEGAL_ENTITY_ACTIVITY,
      re_ca.LEGAL_ENTITY_NUMBER_OF_EMPLOYEES AS LEGAL_ENTITY_NUMBER_OF_EMPLOYEES,
    FROM cast_respond_data AS re_ca
  )
  SELECT
    src.*,
    TO_BASE64(SHA256(FORMAT("%T", (SELECT AS STRUCT src.* EXCEPT(T_INGESTION_TIMESTAMP, T_DML_TYPE))))) AS T_ROW_HASH
  FROM source_table AS src