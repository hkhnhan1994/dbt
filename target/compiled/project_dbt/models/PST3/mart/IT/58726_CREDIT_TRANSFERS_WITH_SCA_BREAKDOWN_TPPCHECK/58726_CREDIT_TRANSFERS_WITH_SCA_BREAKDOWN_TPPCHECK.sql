SELECT
  D.T_SOURCE_PK_ID,
  D.T_SOURCE_PK_UUID,
  D.SERVICE_PROVIDER_VERSION ,
  D.SERVICE_PROVIDER_ACTIVE ,
  D.SERVICE_PROVIDER_PSP_AUTHORITY_ID ,
  D.SERVICE_PROVIDER_DISPLAY_NAME,
  D.SERVICE_PROVIDER_CREATED_AT,
  D.SERVICE_PROVIDER_UPDATED_AT,
  CURRENT_TIMESTAMP AS LOAD_TIMESTAMP,
  ""  AS Period,
FROM `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_ASPSP_TPP_CURRENT` C
JOIN  `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_ASPSP_TPP_DECRYPTED` D
  ON C.T_SOURCE_PK_UUID = D.T_SOURCE_PK_UUID
WHERE D.T_SOURCE_PK_UUID <> '93773d5d-00b9-422d-af5c-b90259cf50ee'
  AND D.SERVICE_PROVIDER_CREATED_AT >= TIMESTAMP(DATETIME( '2024-07-01', 'Etc/UTC'))
  AND D.SERVICE_PROVIDER_CREATED_AT <= TIMESTAMP(DATETIME( '2024-09-30', 'Etc/UTC'))