SELECT
     COUNT(distinct c.consent_iban) AS COUNT,
     CURRENT_TIMESTAMP AS LOAD_TIMESTAMP,
     ""  AS Period,
FROM
     `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_ASPSP_CONSENT_DECRYPTED` c
     INNER JOIN `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_ASPSP_CONSENT_DECRYPTED` cc
          ON c.T_DIM_KEY = cc.T_DIM_KEY
     INNER JOIN `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_ASPSP_TPP` t
          ON c.T_D_ASPSP_TPP_DIM_KEY = t.T_DIM_KEY
WHERE
     c.CONSENT_STATUS = 'VALID'
     AND LEFT(c.consent_iban, 2) = 'FR'
     AND c.CONSENT_TRANSACTION_ACCESS_ALLOWED IS TRUE
     AND c.CONSENT_CREATED_AT <= TIMESTAMP(DATETIME( '2024-06-30', 'Etc/UTC'))
     AND c.CONSENT_EXPIRED_AT >= TIMESTAMP(DATETIME( '2024-01-01', 'Etc/UTC'))
     --AND  t.T_SOURCE_PK_ID <> '1'
     AND t.T_SOURCE_PK_UUID <> '93773d5d-00b9-422d-af5c-b90259cf50ee'