
  
    

    create or replace table `pj-bu-dw-data-sbx`.`dev_staging_view_cmd`.`D_AUTHORIZED_PERSON`
      
    
    

    OPTIONS()
    as (
      with
persion_atribute_table AS(
    SELECT
        PersonId,
        STRING_AGG (DISTINCT (AUTHORIZED_PERSON_FIRST_NAME)) AS AUTHORIZED_PERSON_FIRST_NAME,
        STRING_AGG (DISTINCT (AUTHORIZED_PERSON_LAST_NAME)) AS AUTHORIZED_PERSON_LAST_NAME,
        STRING_AGG (DISTINCT (AUTHORIZED_PERSON_LAST_NAME_PREFIX)) AS AUTHORIZED_PERSON_LAST_NAME_PREFIX,
        STRING_AGG (DISTINCT (AUTHORIZED_PERSON_PLACE_OF_BIRTH)) AS AUTHORIZED_PERSON_PLACE_OF_BIRTH,
        STRING_AGG (DISTINCT (AUTHORIZED_PERSON_GENDER)) AS AUTHORIZED_PERSON_GENDER,
        STRING_AGG (DISTINCT (AUTHORIZED_PERSON_MOBILE_PHONE)) AS AUTHORIZED_PERSON_MOBILE_PHONE,
        STRING_AGG (DISTINCT (AUTHORIZED_PERSON_DATE_OF_BIRTH)) AS AUTHORIZED_PERSON_DATE_OF_BIRTH,
    FROM(
        SELECT
            he_per_atb.PersonId,
            IF (he_per_atb.AttributeUrnId=1, he_per_atb.Value, NULL) AS AUTHORIZED_PERSON_FIRST_NAME,
            IF (he_per_atb.AttributeUrnId=4, he_per_atb.Value, NULL) AS AUTHORIZED_PERSON_LAST_NAME,
            IF (he_per_atb.AttributeUrnId=3, he_per_atb.Value, NULL) AS AUTHORIZED_PERSON_LAST_NAME_PREFIX,
            IF (he_per_atb.AttributeUrnId=6, he_per_atb.Value, NULL) AS AUTHORIZED_PERSON_PLACE_OF_BIRTH,
            IF (he_per_atb.AttributeUrnId=7, he_per_atb.Value, NULL) AS AUTHORIZED_PERSON_GENDER,
            IF (he_per_atb.AttributeUrnId=12, he_per_atb.Value, NULL) AS AUTHORIZED_PERSON_MOBILE_PHONE,
            IF (he_per_atb.AttributeUrnId=5, "Y", NULL) AS AUTHORIZED_PERSON_DATE_OF_BIRTH,
        FROM `pj-bu-dw-data-sbx`.`dev_dl_h3_hehe`.`public_PersonAttribute_current` AS he_per_atb
    )
    GROUP BY PersonId
),
source_table as(
    SELECT
        CAST (he_per.ingestion_meta_data_uuid AS STRING) AS T_BATCH_ID,
        TO_BASE64((SHA256(CONCAT("HEHE", "$", CAST (he_per.PersonId AS STRING))))) AS T_BUS_KEY,
        "HEHE" AS T_SOURCE,
        CAST (he_per.PersonId AS STRING) AS T_SOURCE_PK_ID,
        CURRENT_TIMESTAMP AS T_LOAD_TIMESTAMP,
        CAST (he_per.INSERT_HIST_TIMESTAMP AS TIMESTAMP) AS T_INGESTION_TIMESTAMP,
        he_per.source_metadata_change_type AS T_DML_TYPE,
        CAST (COALESCE (he_per.InsertDate, NULL) AS TIMESTAMP) AUTHORIZED_PERSON_CREATED_AT,
        CAST (COALESCE (he_per.LastUpdatedDate, NULL) AS TIMESTAMP) AUTHORIZED_PERSON_UPDATED_AT,
        "We-ID" AS BRAND,
        COALESCE (he_per.Name, "NA") AS AUTHORIZED_PERSON_NAME,
        COALESCE (he_per_atb.AUTHORIZED_PERSON_FIRST_NAME, "NA") AS AUTHORIZED_PERSON_FIRST_NAME,
        COALESCE (he_per_atb.AUTHORIZED_PERSON_LAST_NAME, "NA") AS AUTHORIZED_PERSON_LAST_NAME,
        COALESCE (he_per_atb.AUTHORIZED_PERSON_LAST_NAME_PREFIX, "NA") AS AUTHORIZED_PERSON_LAST_NAME_PREFIX,
        COALESCE (he_per.EmailAddress, "NA") AS AUTHORIZED_PERSON_EMAIL,
        COALESCE (he_per_atb.AUTHORIZED_PERSON_PLACE_OF_BIRTH, "NA") AS AUTHORIZED_PERSON_PLACE_OF_BIRTH,
        COALESCE (he_per_atb.AUTHORIZED_PERSON_GENDER, "NA") AS AUTHORIZED_PERSON_GENDER,
        COALESCE (he_per_atb.AUTHORIZED_PERSON_MOBILE_PHONE, "NA") AS AUTHORIZED_PERSON_MOBILE_PHONE,
        COALESCE (he_per.BsnkId, "NA") AS AUTHORIZED_PERSON_CSN,

      FROM `pj-bu-dw-data-sbx`.`dev_dl_h3_hehe`.`public_Person_current` AS he_per
      LEFT JOIN persion_atribute_table AS he_per_atb
        ON he_per.PersonId = he_per_atb.PersonId
)
SELECT
    src.*,
    TO_BASE64(SHA256(FORMAT("%T", (SELECT AS STRUCT src.* EXCEPT(T_BATCH_ID,T_INGESTION_TIMESTAMP,T_LOAD_TIMESTAMP, T_DML_TYPE))))) AS T_ROW_HASH
  FROM source_table AS src
    );
  