
  
    

    create or replace table `pj-bu-dw-data-sbx`.`dev_dm_pst3_ES`.`Cuardro1_number_of_AISPs_accessing_UPP_accounts`
      
    
    

    OPTIONS()
    as (
      SELECT
LEFT(t.SERVICE_PROVIDER_PSP_AUTHORITY_ID,2) countryOfAISP,
c.consent_iban,
CURRENT_TIMESTAMP AS LOAD_TIMESTAMP,
""  AS Period,
from `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_ASPSP_CONSENT_DECRYPTED` c
INNER JOIN `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_ASPSP_CONSENT_DECRYPTED` cc
  on c.T_DIM_KEY = cc.T_DIM_KEY
INNER JOIN `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_ASPSP_TPP` t
  on c.T_D_ASPSP_TPP_DIM_KEY = t.T_DIM_KEY
WHERE
  c.CONSENT_STATUS = 'VALID'
  and left(c.consent_iban,2) ='ES'
  and c.CONSENT_EXPIRED_AT >= TIMESTAMP(DATETIME( '2024-01-01', 'Etc/UTC'))
  and c.CONSENT_CREATED_AT <= TIMESTAMP(DATETIME( '2024-06-30', 'Etc/UTC'))
  and  t.T_SOURCE_PK_UUID <> '93773d5d-00b9-422d-af5c-b90259cf50ee'
    );
  