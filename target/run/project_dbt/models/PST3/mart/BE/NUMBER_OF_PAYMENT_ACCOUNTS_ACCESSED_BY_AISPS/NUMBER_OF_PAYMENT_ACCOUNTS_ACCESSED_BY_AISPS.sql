
  
    

    create or replace table `pj-bu-dw-data-sbx`.`dev_dm_pst3_BE`.`NUMBER_OF_PAYMENT_ACCOUNTS_ACCESSED_BY_AISPS`
      
    
    

    OPTIONS()
    as (
      SELECT
    LEFT(t.SERVICE_PROVIDER_PSP_AUTHORITY_ID,2) countryOfAISP,
    c.consent_iban,
    ""  AS Period,
    TIMESTAMP(DATETIME( '2024-01-01', 'Etc/UTC'))  AS Period_begin_date,
    TIMESTAMP(DATETIME( '2024-06-30', 'Etc/UTC'))  AS Period_end_date,
    CURRENT_TIMESTAMP AS LOAD_TIMESTAMP,
FROM `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_ASPSP_CONSENT_DECRYPTED` c
JOIN `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_ASPSP_CONSENT_DECRYPTED` cc
    ON c.T_DIM_KEY = cc.T_DIM_KEY
JOIN `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_ASPSP_TPP` t
    ON c.T_D_ASPSP_TPP_DIM_KEY = t.T_DIM_KEY
WHERE
    c.CONSENT_STATUS = 'VALID'
    AND left(c.consent_iban,2) = 'BE'
    AND c.CONSENT_CREATED_AT >= TIMESTAMP(DATETIME( '2024-01-01', 'Etc/UTC'))
    AND c.CONSENT_EXPIRED_AT <= TIMESTAMP(DATETIME( '2024-06-30', 'Etc/UTC'))
    AND  t.T_SOURCE_PK_UUID <> '93773d5d-00b9-422d-af5c-b90259cf50ee'
    );
  