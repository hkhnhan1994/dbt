select
  left(t.SERVICE_PROVIDER_PSP_AUTHORITY_ID,2) countryOfAISP,
  c.consent_iban,
  CURRENT_TIMESTAMP AS LOAD_TIMESTAMP,
  '2023Y' AS PERIOD,
from `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_ASPSP_CONSENT_DECRYPTED` c
    inner join `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_ASPSP_CONSENT_DECRYPTED` cc on c.T_DIM_KEY = cc.T_DIM_KEY
    inner join `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_ASPSP_TPP` t on c.T_D_ASPSP_TPP_DIM_KEY = t.T_DIM_KEY
where
  c.CONSENT_STATUS = 'VALID'
  and left(c.consent_iban,2) = 'NL'
  and c.CONSENT_CREATED_AT >= TIMESTAMP(DATETIME('2023-01-01', 'Etc/UTC'))
  and c.CONSENT_EXPIRED_AT <= TIMESTAMP(DATETIME( '2023-12-31', 'Etc/UTC'))
  and  t.T_SOURCE_PK_UUID <> '93773d5d-00b9-422d-af5c-b90259cf50ee'