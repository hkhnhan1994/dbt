
  
    

    create or replace table `pj-bu-dw-data-sbx`.`dev_dm_pst3_FR`.`C8_agMoyPaiTypeOpeCanalTypeCanalZoneGeo_NON_ELECTRONIQUE`
      
    
    

    OPTIONS()
    as (
      WITH country_codes AS (
SELECT *
FROM UNNEST (['FR','DE','AT','BE','BG','CY','HR','DK','ES','EE','FI','GR','HU','IE','IS','IT','LV','LI','LT','LU','MT','NO','NL','PL','PT','CZ','RO','SK','SI','SE']) as code
),
DM as (
  SELECT
    cred.FINANCIAL_INSTITUTION_COUNTRY_CODE AS payee_psp_country,
    COUNT(*) AS outbound_ibis_payments_trx_count,
    ROUND(COALESCE(SUM(ftr.TRANSACTION_AMOUNT), 0.00), 2) AS outbound_ibis_payments_amount_sum_in_EUR,
  FROM `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`F_ACCOUNT_TRANSACTIONS_DECRYPTED` ftr
  JOIN `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_IBIS_ACCOUNT_CURRENT` iacc
    ON ftr.T_D_IBIS_ACCOUNT_DIM_KEY = iacc.T_DIM_KEY
  JOIN `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_BANK_ACCOUNTS_DECRYPTED` deb
    ON iacc.T_D_BANK_ACCOUNT_DIM_KEY = deb.T_DIM_KEY
  LEFT JOIN `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_BANK_ACCOUNTS_DECRYPTED` cred
    ON cred.T_DIM_KEY = ftr.T_COUNTERPARTY_BANK_ACCOUNT_DIM_KEY
  JOIN `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_ACCOUNT_TRANSACTION_CURRENT` dtr
    ON ftr.T_D_ACCOUNT_TRANSACTION_DIM_KEY = dtr.T_DIM_KEY
    AND dtr.TRANSACTION_BOOKING_DATE_AT >= TIMESTAMP(DATETIME( '2024-01-01', 'Etc/UTC'))
    AND dtr.TRANSACTION_BOOKING_DATE_AT <= TIMESTAMP(DATETIME( '2024-06-30', 'Etc/UTC'))
    AND dtr.TRANSACTION_STATUS  IN ('SETTLED', 'RETURNED')
    AND deb.FINANCIAL_INSTITUTION_COUNTRY_CODE =  'FR'
    AND iacc.ACCOUNT_TYPE = 'PAYMENT'
  WHERE ftr.TRANSACTION_DIRECTION = 'OUTBOUND'
    AND ftr.transaction_type  = 'REGULAR'
    AND ftr.TRANSACTION_CHANNEL IN ('DASHBOARD','ADMIN', 'OTHER', 'CARDS')
    AND ftr.TRANSACTION_BANK_FAMILY = 'ICDT'

  GROUP BY 1
  ORDER BY 1
)
SELECT
  c.code,
  DM.*,
  (SELECT SUM(outbound_ibis_payments_trx_count) FROM DM WHERE payee_psp_country != 'FR') as total_trx_count_without_FR,
  (SELECT SUM(outbound_ibis_payments_amount_sum_in_EUR) FROM DM WHERE payee_psp_country != 'FR') as total_amount_sum_without_FR,
  CURRENT_TIMESTAMP AS Load_timestamp,
  ""  AS Period,
FROM DM
FULL OUTER JOIN country_codes as c on c.code = DM.payee_psp_country
    );
  