with tblKlant_current as (
    WITH current_table AS (
    SELECT *,
    ROW_NUMBER() OVER(PARTITION BY row_hash) AS rn
    FROM  {{ source('stg_strh_hklc', 'public_tblKlant') }}
    )
    SELECT * EXCEPT(rn)
    FROM current_table
    WHERE rn = 1
),
tblSysteemAttribuut_current as (
    WITH current_table AS (
    SELECT *,
    ROW_NUMBER() OVER(PARTITION BY row_hash) AS rn
    FROM  {{ source('stg_strh_hklc', 'public_tblSysteemAttribuut') }}
    )
    SELECT * EXCEPT(rn)
    FROM current_table
    WHERE rn = 1
),
tblKlantSysAttr_current as (
    WITH current_table AS (
    SELECT *,
    ROW_NUMBER() OVER(PARTITION BY row_hash) AS rn
    FROM  {{ source('stg_strh_hklc', 'public_tblKlantSysAttr') }}
    )
    SELECT * EXCEPT(rn)
    FROM current_table
    WHERE rn = 1
),
tblLand_current as (
    WITH current_table AS (
    SELECT *,
    ROW_NUMBER() OVER(PARTITION BY row_hash) AS rn
    FROM  {{ source('stg_strh_hklc', 'public_tblLand') }}
    )
    SELECT * EXCEPT(rn)
    FROM current_table
    WHERE rn = 1
),
pre_kal_sys AS (
    SELECT
      "H1_HKLC" AS T_SOURCE,
      kal_tblant_sys.KlantID,
      IF(sys_at.code = 'EHERKENNING3KVK',kal_tblant_sys.Waarde, NULL) AS ENTERPRISE_IDENTIFIER,
      IF(sys_at.code = 'EHERKENNING3KVKVESTIGINGSNUMMER',kal_tblant_sys.Waarde, NULL) AS ENTERPRISE_LOCATION_IDENTIFIER,
      IF(sys_at.code = 'EHERKENNINGCOMPLEVEL',kal_tblant_sys.Waarde, NULL) ENTERPRISE_EH_AUTHENTICATION_LEVEL,
    FROM (
      SELECT
        ROW_NUMBER() OVER ( PARTITION BY klantid, AttribuutID, AttribuutID, waarde ORDER BY ingestion_meta_data_processing_timestamp) AS RN,
        KlantID, Waarde,AttribuutID
      FROM tblKlantSysAttr_current
      WHERE systeemid = 1
      AND Waarde IS NOT NULL
      AND Waarde != ""
    ) AS  kal_tblant_sys
    JOIN tblSysteemAttribuut_current AS sys_at
      ON sys_at.AttribuutID=kal_tblant_sys.AttribuutID
    WHERE kal_tblant_sys.RN = 1
    AND sys_at.code in('EHERKENNING3KVK','EHERKENNING3KVKVESTIGINGSNUMMER','EHERKENNINGCOMPLEVEL')
  ),
  kal_sys AS (
     SELECT
      KlantID,
      STRING_AGG(ENTERPRISE_IDENTIFIER) ENTERPRISE_IDENTIFIER,
      STRING_AGG(ENTERPRISE_LOCATION_IDENTIFIER) ENTERPRISE_LOCATION_IDENTIFIER,
      MAX(ENTERPRISE_EH_AUTHENTICATION_LEVEL) ENTERPRISE_EH_AUTHENTICATION_LEVEL
    FROM pre_kal_sys
    GROUP BY KlantID
  ),
  source_table AS (
      SELECT
        CAST (kal_klant.ingestion_meta_data_uuid AS STRING) AS T_BATCH_ID,
        CAST (kal_klant.ingestion_meta_data_source_timestamp AS STRING) AS T_LOAD_TIMESTAMP,
        TO_BASE64(SHA256(CONCAT("HKLC", "$", CAST(kal_klant.KlantID AS STRING)))) AS T_BUS_KEY,
        "HKLC" AS T_SOURCE,
        CAST(kal_klant.KlantID AS STRING) AS T_SOURCE_PK_ID,
        CAST(kal_klant.DateChange AS TIMESTAMP) AS T_INGESTION_TIMESTAMP,
        kal_klant.source_metadata_change_type AS T_DML_TYPE,
        CAST(COALESCE (kal_klant.DateCreate, NULL) AS TIMESTAMP) ENTERPRISE_CREATED_AT,
        CAST(COALESCE (kal_klant.DateChange, NULL) AS TIMESTAMP) ENTERPRISE_UPDATED_AT,
        COALESCE(leg.T_DIM_KEY,0) AS T_D_LEGAL_ENTITY_DIM_KEY,
        "Z_login" AS BRAND,
        COALESCE (kal_klant.bedrijfsnaam, "NA") AS ENTERPRISE_NAME,
        CASE CONCAT(IF(COALESCE (kal_klant.Voorletters, "")!="",CONCAT(kal_klant.Voorletters," "),""),
              IF(COALESCE (kal_klant.tussenvoegsel, "")!="",CONCAT(kal_klant.tussenvoegsel," "),""),
              COALESCE (kal_klant.Achternaam, ""))
        WHEN "" THEN "NA"
        ELSE CONCAT(IF(COALESCE (kal_klant.Voorletters, "")!="",CONCAT(kal_klant.Voorletters," "),""),
              IF(COALESCE (kal_klant.tussenvoegsel, "")!="",CONCAT(kal_klant.tussenvoegsel," "),""),
              COALESCE (kal_klant.Achternaam, ""))
        END AS ENTERPRISE_CONTACT,
        COALESCE (kal_klant.Telefoon, "NA") AS ENTERPRISE_PHONE_NUMBER,
        COALESCE (kal_klant.Mobiel, "NA") AS ENTERPRISE_MOBILE_NUMBER,
        COALESCE (kal_klant.Straat, "NA") AS ENTERPRISE_STREET,
        COALESCE (kal_klant.HuisNr, "NA") AS ENTERPRISE_HOUSE_NBR,
        COALESCE (kal_klant.Postcode, "NA") AS ENTERPRISE_ZIP_CODE,
        COALESCE (kal_klant.Plaats, "NA") AS ENTERPRISE_CITY,
        COALESCE (kal_tblant.exactcode, "NA") AS ENTERPRISE_COUNTRY,
        COALESCE (kal_klant.EmailAccount, "NA") AS ENTERPRISE_EMAIL,
        COALESCE (CAST(kal_klant.BedrijfsvormID AS STRING), "NA") AS ENTERPRISE_JURIDICAL_FORM,
        COALESCE (kal_klant.btwnr, "NA") AS ENTERPRISE_VAT_NUMBER,
        COALESCE (kal_sys.ENTERPRISE_IDENTIFIER, "NA") AS ENTERPRISE_IDENTIFIER,
        CAST("KVK" AS STRING) AS ENTERPRISE_IDENTIFIER_TYPE,
        COALESCE (kal_sys.ENTERPRISE_LOCATION_IDENTIFIER, "NA") AS ENTERPRISE_LOCATION_IDENTIFIER,
        COALESCE (CAST( kal_sys.ENTERPRISE_EH_AUTHENTICATION_LEVEL AS STRING), "NA") AS ENTERPRISE_EH_AUTHENTICATION_LEVEL,
        COALESCE (CAST(kal_klant.KlantNr AS STRING), "NA") AS ENTERPRISE_CUSTOMER_NUMBER,
        COALESCE (kal_klant.AgroSwitchID, "NA") AS ENTERPRISE_AGRO_SWITH_ID,
      FROM tblKlant_current AS kal_klant
      LEFT JOIN tblLand_current AS kal_tblant ON kal_klant.LandID = kal_tblant.LandID
      LEFT JOIN kal_sys
        ON kal_sys.KlantID=kal_klant.KlantID
      LEFT JOIN {{ref('dwh_strh_D_LEGAL_ENTITY_CURRENT')}}  AS leg
        ON leg.T_SOURCE = "H1_HKVK"
        AND kal_sys.ENTERPRISE_IDENTIFIER = leg.LEGAL_ENTITY_IDENTIFIER
  )
  SELECT
    src.*,
    TO_BASE64(SHA256(FORMAT("%T", (SELECT AS STRUCT src.* EXCEPT(T_INGESTION_TIMESTAMP, T_DML_TYPE))))) AS T_ROW_HASH
  FROM source_table AS src